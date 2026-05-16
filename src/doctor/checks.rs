//! Per-flow checks. Each function appends `Finding`s to a report.
//!
//! Parsing reuses the canonical `persistence::read_*` functions so doctor
//! validates exactly what `render`/`submit` would see (including
//! `read_flow`'s partition injection).
//!
//! EXTENSIBILITY SEAM: a future "preflight everything before run" check
//! (sbatch present, partition exists via `sinfo`, project_root writable,
//! env sanity) is added by writing one `check_*` fn here and one
//! `report.extend(checks::check_*(..))` line in `run_doctor` — no
//! restructuring. None are implemented now (YAGNI per spec).

use std::collections::BTreeSet;
use std::path::Path;
use std::str::FromStr;

use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::JobFlow;

use crate::error::JobManagerError;
use crate::persistence::{read_flow, read_flow_effective, read_job_run, read_plan};
use crate::plan::ExperimentPlan;

use super::report::Finding;

fn err_msg(e: &JobManagerError) -> String {
    e.to_string()
}

/// Parse `<flow_dir>/flow.toml` via `read_flow` (with partition
/// injection). Returns the parsed flow on success so structural checks
/// can reuse it. A `PartitionMissing`/`PartitionWrongType`/`TomlParse`
/// error becomes a FAIL.
pub fn check_flow(flow_dir: &Path, common: &CommonConfig) -> (Option<JobFlow>, Vec<Finding>) {
    let path = flow_dir.join("flow.toml");
    match read_flow(&path, common) {
        Ok(flow) => (
            Some(flow),
            vec![Finding::pass(&path, "flow.toml parses as JobFlow")],
        ),
        Err(e) => (None, vec![Finding::fail(&path, err_msg(&e))]),
    }
}

/// Parse `<flow_dir>/plan.toml` as `ExperimentPlan`. Absence is a FAIL
/// (plan.toml is required user input alongside flow.toml).
pub fn check_plan(flow_dir: &Path) -> Vec<Finding> {
    let path = flow_dir.join("plan.toml");
    if !path.exists() {
        return vec![Finding::fail(&path, "plan.toml is missing")];
    }
    match read_plan(&path) {
        Ok(_) => vec![Finding::pass(&path, "plan.toml parses as ExperimentPlan")],
        Err(e) => vec![Finding::fail(&path, err_msg(&e))],
    }
}

/// Parse `<flow_dir>/.jm/flow.effective.toml` if present. Absence is OK
/// (flow not yet rendered). Malformed-when-present is a FAIL.
pub fn check_flow_effective(flow_dir: &Path) -> Vec<Finding> {
    let path = flow_dir.join(".jm").join("flow.effective.toml");
    if !path.exists() {
        return vec![Finding::pass(&path, "no snapshot yet (not rendered) — ok")];
    }
    match read_flow_effective(&path) {
        Ok(_) => vec![Finding::pass(
            &path,
            "flow.effective.toml parses as JobFlow",
        )],
        Err(e) => vec![Finding::fail(&path, err_msg(&e))],
    }
}

/// Parse every `<flow_dir>/.jm/<job_id>/status.toml` that exists.
/// `job_ids` comes from the parsed flow (so we only look where a job
/// actually exists). Absence is OK (job not yet submitted).
pub fn check_status_files<'a>(
    flow_dir: &Path,
    job_ids: impl IntoIterator<Item = &'a str>,
) -> Vec<Finding> {
    let mut out = Vec::new();
    for jid in job_ids {
        let path = flow_dir.join(".jm").join(jid).join("status.toml");
        if !path.exists() {
            continue;
        }
        match read_job_run(&path) {
            Ok(_) => out.push(Finding::pass(&path, "status.toml parses as JobRun")),
            Err(e) => out.push(Finding::fail(&path, err_msg(&e))),
        }
    }
    out
}

/// `flow.toml`'s `uuid` must equal the flow directory name. FAIL on a
/// mismatch or a non-UUID directory name.
pub fn check_uuid_matches_dir(flow_dir: &Path, flow: &JobFlow) -> Vec<Finding> {
    let path = flow_dir.join("flow.toml");
    let dir_name = flow_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("<non-utf8>");
    match uuid::Uuid::from_str(dir_name) {
        Ok(dir_uuid) if dir_uuid == flow.uuid => vec![Finding::pass(
            &path,
            format!("uuid matches directory name ({dir_uuid})"),
        )],
        Ok(dir_uuid) => vec![Finding::fail(
            &path,
            format!(
                "uuid {} does not match directory name {dir_uuid}",
                flow.uuid
            ),
        )],
        Err(_) => vec![Finding::fail(
            &path,
            format!("flow directory name {dir_name:?} is not a valid UUID"),
        )],
    }
}

/// Every `JobEdge.from` must reference a job key that exists in
/// `flow.jobs`. FAIL on a dangling parent.
pub fn check_parents_resolve(flow_dir: &Path, flow: &JobFlow) -> Vec<Finding> {
    let path = flow_dir.join("flow.toml");
    let mut out = Vec::new();
    for (jid, job) in &flow.jobs {
        for edge in &job.parents {
            if !flow.jobs.contains_key(&edge.from) {
                out.push(Finding::fail(
                    &path,
                    format!(
                        "job {:?} has parent {:?} which is not a job in this flow",
                        jid.0, edge.from.0
                    ),
                ));
            }
        }
    }
    if out.is_empty() {
        out.push(Finding::pass(&path, "all parent edges resolve"));
    }
    out
}

/// `plan.toml`'s job set should cover `flow.toml`'s job set. WARN (not
/// FAIL) on missing/extra entries — an empty plan table is allowed by
/// existing convention, so this is advisory. Returns nothing when
/// plan.toml is absent or unparsable (`check_plan` already FAILed).
pub fn check_plan_coverage(flow_dir: &Path, flow: &JobFlow) -> Vec<Finding> {
    let path = flow_dir.join("plan.toml");
    if !path.exists() {
        return Vec::new();
    }
    let plan: ExperimentPlan = match read_plan(&path) {
        Ok(p) => p,
        Err(_) => return Vec::new(),
    };
    let flow_jobs: BTreeSet<&String> = flow.jobs.keys().map(|j| &j.0).collect();
    let plan_jobs: BTreeSet<&String> = plan.jobs.keys().map(|j| &j.0).collect();
    let missing: Vec<&&String> = flow_jobs.difference(&plan_jobs).collect();
    let extra: Vec<&&String> = plan_jobs.difference(&flow_jobs).collect();
    let mut out = Vec::new();
    if !missing.is_empty() {
        out.push(Finding::warn(
            &path,
            format!("plan has no entry for flow jobs: {missing:?}"),
        ));
    }
    if !extra.is_empty() {
        out.push(Finding::warn(
            &path,
            format!("plan has entries for unknown jobs: {extra:?}"),
        ));
    }
    if out.is_empty() {
        out.push(Finding::pass(&path, "plan covers exactly the flow's jobs"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::report::Severity;
    use crate::persistence::synth_empty_common;
    use tempfile::tempdir;

    const GOOD_FLOW: &str = r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"

[jobs.opt]
program = "echo"
body = "echo hi\n"

[jobs.opt.config]
partition = "long"
"#;

    #[test]
    fn check_flow_passes_on_valid_toml() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("flow.toml"), GOOD_FLOW).unwrap();
        let (flow, fs) = check_flow(d.path(), &synth_empty_common());
        assert!(flow.is_some());
        assert_eq!(fs[0].severity, Severity::Pass);
    }

    #[test]
    fn check_flow_fails_on_unknown_field() {
        let d = tempdir().unwrap();
        let bad = format!("{GOOD_FLOW}\nbogus = 1\n");
        std::fs::write(d.path().join("flow.toml"), bad).unwrap();
        let (flow, fs) = check_flow(d.path(), &synth_empty_common());
        assert!(flow.is_none());
        assert_eq!(fs[0].severity, Severity::Fail);
    }

    #[test]
    fn check_flow_fails_when_partition_unresolvable() {
        let d = tempdir().unwrap();
        let no_part = r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "x\n"
[jobs.opt.config]
"#;
        std::fs::write(d.path().join("flow.toml"), no_part).unwrap();
        let (_f, fs) = check_flow(d.path(), &synth_empty_common());
        assert_eq!(fs[0].severity, Severity::Fail);
    }

    #[test]
    fn check_plan_fails_when_missing() {
        let d = tempdir().unwrap();
        let fs = check_plan(d.path());
        assert_eq!(fs[0].severity, Severity::Fail);
    }

    #[test]
    fn check_plan_passes_on_valid() {
        let d = tempdir().unwrap();
        std::fs::write(d.path().join("plan.toml"), "[jobs.opt]\nnproc = 1\n").unwrap();
        let fs = check_plan(d.path());
        assert_eq!(fs[0].severity, Severity::Pass);
    }

    #[test]
    fn check_flow_effective_absent_is_pass() {
        let d = tempdir().unwrap();
        let fs = check_flow_effective(d.path());
        assert_eq!(fs[0].severity, Severity::Pass);
    }

    #[test]
    fn check_status_files_fails_on_corrupt_present_file() {
        let d = tempdir().unwrap();
        let sp = d.path().join(".jm").join("opt");
        std::fs::create_dir_all(&sp).unwrap();
        std::fs::write(sp.join("status.toml"), "lifecycle = \"bogus\"\n").unwrap();
        let fs = check_status_files(d.path(), ["opt"]);
        assert_eq!(fs[0].severity, Severity::Fail);
    }

    fn write_flow_with(d: &Path, body: &str) -> JobFlow {
        std::fs::write(d.join("flow.toml"), body).unwrap();
        let (f, _) = check_flow(d, &synth_empty_common());
        f.expect("fixture flow should parse")
    }

    #[test]
    fn uuid_match_pass_and_fail() {
        let d = tempdir().unwrap();
        let good = d.path().join("01999999-0000-7000-8000-000000000000");
        std::fs::create_dir_all(&good).unwrap();
        let flow = write_flow_with(&good, GOOD_FLOW);
        assert_eq!(
            check_uuid_matches_dir(&good, &flow)[0].severity,
            Severity::Pass
        );

        let bad = d.path().join("not-a-uuid");
        std::fs::create_dir_all(&bad).unwrap();
        let flow2 = write_flow_with(&bad, GOOD_FLOW);
        assert_eq!(
            check_uuid_matches_dir(&bad, &flow2)[0].severity,
            Severity::Fail
        );
    }

    #[test]
    fn dangling_parent_is_fail() {
        let d = tempdir().unwrap();
        let f = r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.freq]
program = "echo"
body = "x\n"
[jobs.freq.config]
partition = "long"
[[jobs.freq.parents]]
from = "ghost"
kind = "afterok"
"#;
        let flow = write_flow_with(d.path(), f);
        let fs = check_parents_resolve(d.path(), &flow);
        assert_eq!(fs[0].severity, Severity::Fail);
    }

    #[test]
    fn plan_coverage_warns_on_missing_entry() {
        let d = tempdir().unwrap();
        let flow = write_flow_with(d.path(), GOOD_FLOW); // job: opt
        std::fs::write(d.path().join("plan.toml"), "[jobs.other]\nx=1\n").unwrap();
        let fs = check_plan_coverage(d.path(), &flow);
        assert!(fs.iter().any(|f| f.severity == Severity::Warn));
    }
}
