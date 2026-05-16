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

use std::path::Path;

use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::JobFlow;

use crate::error::JobManagerError;
use crate::persistence::{read_flow, read_flow_effective, read_job_run, read_plan};

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
}
