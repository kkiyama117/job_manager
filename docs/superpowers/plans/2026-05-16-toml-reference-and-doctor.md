# TOML Reference + `examples/full/` + `jm doctor` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a consolidated `docs/toml-reference.md`, an exhaustive valid `examples/full/` tree, and a `jm doctor` subcommand that validates a `<root>` tree's TOML and structural invariants.

**Architecture:** A new pure-Rust `src/doctor/` library module (report types + a `Check` extensibility seam + parse/structural checks + a `run_doctor` orchestrator) reusing the existing `persistence::read_*` readers. A thin `jm doctor` CLI wrapper. `examples/full/` mirrors the real on-disk layout and is drift-guarded by an integration test that runs `run_doctor` over it.

**Tech Stack:** Rust 2024 nightly, `serde`/`toml`, `clap` (derive), `thiserror` (`JobManagerError`), `anyhow` (bin only), `tempfile`/`assert_cmd`/`predicates` (dev).

---

## Source-of-truth facts (verified, do not re-derive)

- Readers (all `Result<T, job_manager::error::JobManagerError>`), re-exported from crate root:
  - `read_common(&Path) -> CommonConfig`
  - `read_flow(&Path, &CommonConfig) -> JobFlow` — runs `inject_partition_defaults`; returns `JobManagerError::PartitionMissing` / `PartitionWrongType` when partition is unresolvable, `TomlParse` on bad TOML.
  - `read_plan(&Path) -> ExperimentPlan`
  - `read_flow_effective(&Path) -> JobFlow` — returns `JobManagerError::SnapshotMissing` if the path does not exist (so doctor must check `path.exists()` first and treat absence as OK).
  - `read_job_run(&Path) -> JobRun`
  - `synth_empty_common() -> CommonConfig` (partition `""`).
- `PathResolver::new(root)`; `.common_toml()`, `.flow_dir(&Uuid)`, `.flow_toml(&Uuid)`, `.plan_toml(&Uuid)`, `.flow_effective_toml(&Uuid)`, `.status_file(&Uuid,&JobId)`. Doctor enumerates by directory name (which may *not* be a valid UUID — that is itself a check), so it builds per-flow paths directly from the flow directory `PathBuf`, not via `PathResolver(uuid)`.
- Types (from `gaussian_job_shared::entities::workflow`):
  - `JobFlow { uuid: Uuid, created_at, tags: BTreeMap<String,String>, jobs: BTreeMap<JobId, Job> }`
  - `Job { spec: JobSpec (#[serde(flatten)]: program: Program, config: SlurmJobConfig, body: String), parents: Vec<JobEdge> }`
  - `JobEdge { from: JobId, kind: DependencyType }` — `kind` serializes lowercase: `afterok afterany after afternotok aftercorr afterburstbuffer singleton`
  - `JobId(pub String)`, `Program(pub String)` — `#[serde(transparent)]`; `JobId::from("x")`, `jid.0`
- `SlurmJobConfig` fields (exhaustive, from `synth_empty_common`): `partition: String` (required), and `Option<…>`: `time_limit`, `log_stdout` (PathBuf), `log_stderr` (PathBuf), `comment`, `job_name`, `array_spec`, `dependency`, `mail_user`, `mail_types`, `resource_spec`.
- `JobRun { lifecycle: Lifecycle, updated_at, slurm_jobid: Option<u64>, slurm_status: Option<JobStatus>, note: Option<String> }`. `Lifecycle` serializes snake_case: `queued running success failed skipped`. `JobStatus { state, reason }` → `[slurm_status]` table.
- `jm` CLI: `src/bin/jm.rs`, `clap::Subcommand` enum `Cmd`, global `--root`, `JM_ROOT` fallback via `resolve_root`, `parse_target(root,&str)->anyhow::Result<Uuid>`, `main() -> anyhow::Result<()>`, subcommand handlers print to stdout and return `anyhow::Result<()>`.
- Test conventions: plain `#[test]` + `tempfile::tempdir()` (see `src/persistence/job_run.rs` tests); CLI tests use `assert_cmd` (dev-dep present). Co-locate unit tests in `#[cfg(test)] mod tests`.
- `examples/` currently holds `simple/` and `sweep/`. New tree is `examples/full/`.

## File structure

- Create `src/doctor/report.rs` — `Severity`, `Finding`, `DoctorReport` (+ `Display`).
- Create `src/doctor/checks.rs` — per-flow parse + structural check fns returning `Vec<Finding>`.
- Create `src/doctor/mod.rs` — `DoctorScope`, `flow_dirs`, `run_doctor`, re-exports.
- Modify `src/lib.rs` — add `pub mod doctor;` + re-exports.
- Modify `src/bin/jm.rs` — add `Doctor` subcommand variant + `cmd_doctor`.
- Create `tests/doctor_examples.rs` — drift guard over `examples/full/`.
- Create `examples/full/{README.md,common.toml,<uuid>/flow.toml,<uuid>/plan.toml,<uuid>/.jm/flow.effective.toml,<uuid>/.jm/<JobId>/status.toml}`.
- Create `docs/toml-reference.md`.
- Modify `README.md`, `CLAUDE.md`, `docs/API.md`, `docs/development.md`, `docs/architecture.md` — pointers.

The canonical example UUID is `01999999-0000-7000-8000-000000000000` (reused from `examples/simple/`). Example jobs: `opt` (root) and `freq` (afterok opt).

---

### Task 1: Doctor report types

**Files:**
- Create: `src/doctor/report.rs`
- Modify: `src/lib.rs`
- Test: co-located in `src/doctor/report.rs`

- [ ] **Step 1: Create `src/doctor/report.rs` with the failing test**

```rust
//! Doctor findings & aggregated report.
//!
//! A `Finding` is one check outcome at one path. `DoctorReport` collects
//! them and renders a stable, greppable summary. `Severity::Fail` is the
//! only level that makes `jm doctor` exit non-zero.

use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Pass,
    Warn,
    Fail,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Severity::Pass => "PASS",
            Severity::Warn => "WARN",
            Severity::Fail => "FAIL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Finding {
    pub severity: Severity,
    pub path: PathBuf,
    pub message: String,
}

impl Finding {
    pub fn pass(path: &Path, message: impl Into<String>) -> Self {
        Self { severity: Severity::Pass, path: path.to_path_buf(), message: message.into() }
    }
    pub fn warn(path: &Path, message: impl Into<String>) -> Self {
        Self { severity: Severity::Warn, path: path.to_path_buf(), message: message.into() }
    }
    pub fn fail(path: &Path, message: impl Into<String>) -> Self {
        Self { severity: Severity::Fail, path: path.to_path_buf(), message: message.into() }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DoctorReport {
    pub findings: Vec<Finding>,
}

impl DoctorReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, f: Finding) {
        self.findings.push(f);
    }

    pub fn extend(&mut self, fs: impl IntoIterator<Item = Finding>) {
        self.findings.extend(fs);
    }

    pub fn count(&self, sev: Severity) -> usize {
        self.findings.iter().filter(|f| f.severity == sev).count()
    }

    /// True iff at least one `Fail` finding exists.
    pub fn has_fail(&self) -> bool {
        self.count(Severity::Fail) > 0
    }
}

impl fmt::Display for DoctorReport {
    fn fmt(&self, fm: &mut fmt::Formatter<'_>) -> fmt::Result {
        for f in &self.findings {
            writeln!(fm, "{:<4}  {}  {}", f.severity.label(), f.path.display(), f.message)?;
        }
        writeln!(
            fm,
            "summary: {} pass, {} warn, {} fail",
            self.count(Severity::Pass),
            self.count(Severity::Warn),
            self.count(Severity::Fail),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn has_fail_only_when_a_fail_finding_present() {
        let mut r = DoctorReport::new();
        r.push(Finding::pass(Path::new("/a"), "ok"));
        r.push(Finding::warn(Path::new("/b"), "meh"));
        assert!(!r.has_fail());
        r.push(Finding::fail(Path::new("/c"), "boom"));
        assert!(r.has_fail());
        assert_eq!(r.count(Severity::Pass), 1);
        assert_eq!(r.count(Severity::Warn), 1);
        assert_eq!(r.count(Severity::Fail), 1);
    }

    #[test]
    fn display_renders_summary_line() {
        let mut r = DoctorReport::new();
        r.push(Finding::fail(Path::new("/x"), "bad"));
        let s = r.to_string();
        assert!(s.contains("FAIL  /x  bad"), "got: {s}");
        assert!(s.contains("summary: 0 pass, 0 warn, 1 fail"), "got: {s}");
    }
}
```

- [ ] **Step 2: Wire the module into the crate**

In `src/lib.rs`, add `pub mod doctor;` to the module list (alphabetically near `pub mod plan;`). Create `src/doctor/mod.rs` containing only:

```rust
//! `jm doctor` — validate a `<root>` tree's TOML files and structural
//! invariants before `render`/`submit`.

pub mod report;

pub use report::{DoctorReport, Finding, Severity};
```

Add to the `pub use` block in `src/lib.rs`:

```rust
pub use doctor::{DoctorReport, Finding, Severity};
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `cargo test --lib --no-default-features doctor::report`
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
git add src/doctor/mod.rs src/doctor/report.rs src/lib.rs
git commit -m "feat: add doctor report types (Severity/Finding/DoctorReport)"
```

---

### Task 2: Flow-directory enumeration

**Files:**
- Modify: `src/doctor/mod.rs`
- Test: co-located in `src/doctor/mod.rs`

- [ ] **Step 1: Write the failing test**

Append to `src/doctor/mod.rs`:

```rust
use std::path::{Path, PathBuf};

use crate::error::JobManagerError;

/// Return every immediate `<root>/<dir>/` that contains a `flow.toml`,
/// sorted by directory name for deterministic output. The directory name
/// is *not* validated as a UUID here — that is a structural check.
pub fn flow_dirs(root: &Path) -> Result<Vec<PathBuf>, JobManagerError> {
    let mut out = Vec::new();
    let rd = std::fs::read_dir(root).map_err(|source| JobManagerError::Io {
        path: root.to_path_buf(),
        source,
    })?;
    for entry in rd {
        let entry = entry.map_err(|source| JobManagerError::Io {
            path: root.to_path_buf(),
            source,
        })?;
        let p = entry.path();
        if p.is_dir() && p.join("flow.toml").is_file() {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn flow_dirs_lists_only_dirs_with_flow_toml_sorted() {
        let d = tempdir().unwrap();
        let root = d.path();
        std::fs::create_dir_all(root.join("bbb")).unwrap();
        std::fs::write(root.join("bbb/flow.toml"), "uuid=1").unwrap();
        std::fs::create_dir_all(root.join("aaa")).unwrap();
        std::fs::write(root.join("aaa/flow.toml"), "uuid=1").unwrap();
        std::fs::create_dir_all(root.join("nope")).unwrap(); // no flow.toml
        std::fs::write(root.join("common.toml"), "x=1").unwrap(); // not a dir

        let dirs = flow_dirs(root).unwrap();
        let names: Vec<_> = dirs
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["aaa", "bbb"]);
    }
}
```

- [ ] **Step 2: Run test to verify it compiles and passes**

Run: `cargo test --lib --no-default-features doctor::tests::flow_dirs`
Expected: PASS (enumeration logic + its test are added together in Step 1).

- [ ] **Step 3: Commit**

```bash
git add src/doctor/mod.rs
git commit -m "feat: add doctor flow-directory enumeration"
```

---

### Task 3: Parse checks

**Files:**
- Create: `src/doctor/checks.rs`
- Modify: `src/doctor/mod.rs` (add `pub mod checks;`)
- Test: co-located in `src/doctor/checks.rs`

- [ ] **Step 1: Create `src/doctor/checks.rs` with parse checks + failing tests**

```rust
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
        Ok(_) => vec![Finding::pass(&path, "flow.effective.toml parses as JobFlow")],
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
```

- [ ] **Step 2: Register the submodule**

In `src/doctor/mod.rs`, add below `pub mod report;`:

```rust
pub mod checks;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test --lib --no-default-features doctor::checks`
Expected: PASS (7 tests).

- [ ] **Step 4: Commit**

```bash
git add src/doctor/checks.rs src/doctor/mod.rs
git commit -m "feat: add doctor parse checks (flow/plan/effective/status)"
```

---

### Task 4: Structural checks

**Files:**
- Modify: `src/doctor/checks.rs`
- Test: co-located in `src/doctor/checks.rs`

- [ ] **Step 1: Append structural checks + failing tests to `src/doctor/checks.rs`**

Add these `use`s at the top of the file (with the others) and these functions after `check_status_files`:

```rust
use std::collections::BTreeSet;
use std::str::FromStr;

use crate::plan::ExperimentPlan;

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
            format!("uuid {} does not match directory name {dir_uuid}", flow.uuid),
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
```

Add these tests inside the existing `#[cfg(test)] mod tests` block:

```rust
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
        assert_eq!(check_uuid_matches_dir(&good, &flow)[0].severity, Severity::Pass);

        let bad = d.path().join("not-a-uuid");
        std::fs::create_dir_all(&bad).unwrap();
        let flow2 = write_flow_with(&bad, GOOD_FLOW);
        assert_eq!(check_uuid_matches_dir(&bad, &flow2)[0].severity, Severity::Fail);
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
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --lib --no-default-features doctor::checks`
Expected: PASS (10 tests total).

- [ ] **Step 3: Commit**

```bash
git add src/doctor/checks.rs
git commit -m "feat: add doctor structural checks (uuid/parents/plan-coverage)"
```

---

### Task 5: `run_doctor` orchestrator

**Files:**
- Modify: `src/doctor/mod.rs`, `src/lib.rs`
- Test: co-located in `src/doctor/mod.rs`

- [ ] **Step 1: Add scope + orchestrator + tests to `src/doctor/mod.rs`**

Add the following above the `#[cfg(test)]` block (and add the `use`s next to the existing ones):

```rust
use gaussian_job_shared::config::common::CommonConfig;

use crate::persistence::{read_common, synth_empty_common};
use report::DoctorReport;

/// What `run_doctor` validates.
#[derive(Debug, Clone)]
pub enum DoctorScope {
    /// Every `<root>/<dir>/` containing a `flow.toml`.
    All,
    /// A single flow directory `<root>/<uuid>/`.
    Flow(uuid::Uuid),
}

/// Validate `root` per `scope`. Reads `<root>/common.toml` once (or
/// synthesizes an empty one) and runs every check against each in-scope
/// flow directory. Pure: no stdout, no process exit — the caller decides.
pub fn run_doctor(root: &Path, scope: &DoctorScope) -> Result<DoctorReport, JobManagerError> {
    let mut report = DoctorReport::new();

    let common_path = root.join("common.toml");
    let common: CommonConfig = if common_path.exists() {
        match read_common(&common_path) {
            Ok(c) => {
                report.push(Finding::pass(&common_path, "common.toml parses as CommonConfig"));
                c
            }
            Err(e) => {
                report.push(Finding::fail(&common_path, e.to_string()));
                synth_empty_common()
            }
        }
    } else {
        report.push(Finding::pass(&common_path, "no common.toml (optional) — ok"));
        synth_empty_common()
    };

    let dirs = match scope {
        DoctorScope::All => flow_dirs(root)?,
        DoctorScope::Flow(u) => vec![root.join(u.to_string())],
    };

    for dir in dirs {
        let (flow, fs) = checks::check_flow(&dir, &common);
        report.extend(fs);
        report.extend(checks::check_plan(&dir));
        report.extend(checks::check_flow_effective(&dir));
        if let Some(flow) = flow {
            let ids: Vec<String> = flow.jobs.keys().map(|j| j.0.clone()).collect();
            report.extend(checks::check_status_files(&dir, ids.iter().map(String::as_str)));
            report.extend(checks::check_uuid_matches_dir(&dir, &flow));
            report.extend(checks::check_parents_resolve(&dir, &flow));
            report.extend(checks::check_plan_coverage(&dir, &flow));
        }
    }
    Ok(report)
}
```

In `src/lib.rs`, replace the doctor re-export line from Task 1:

```rust
pub use doctor::{DoctorReport, Finding, Severity};
```

with:

```rust
pub use doctor::{DoctorReport, DoctorScope, Finding, Severity, run_doctor};
```

- [ ] **Step 2: Add the orchestrator tests (extend `#[cfg(test)] mod tests`)**

```rust
    #[test]
    fn run_doctor_all_reports_pass_for_clean_tree() {
        let d = tempdir().unwrap();
        let root = d.path();
        let fdir = root.join("01999999-0000-7000-8000-000000000000");
        std::fs::create_dir_all(&fdir).unwrap();
        std::fs::write(
            fdir.join("flow.toml"),
            "uuid = \"01999999-0000-7000-8000-000000000000\"\n\
             created_at = \"2026-05-15T00:00:00Z\"\n\
             [jobs.opt]\nprogram = \"echo\"\nbody = \"x\\n\"\n\
             [jobs.opt.config]\npartition = \"long\"\n",
        )
        .unwrap();
        std::fs::write(fdir.join("plan.toml"), "[jobs.opt]\nnproc = 1\n").unwrap();

        let report = run_doctor(root, &DoctorScope::All).unwrap();
        assert!(!report.has_fail(), "report:\n{report}");
        assert!(report.count(Severity::Pass) >= 3);
    }

    #[test]
    fn run_doctor_flags_uuid_mismatch_as_fail() {
        let d = tempdir().unwrap();
        let root = d.path();
        let fdir = root.join("01999999-0000-7000-8000-000000000000");
        std::fs::create_dir_all(&fdir).unwrap();
        std::fs::write(
            fdir.join("flow.toml"),
            "uuid = \"01888888-0000-7000-8000-000000000000\"\n\
             created_at = \"2026-05-15T00:00:00Z\"\n\
             [jobs.opt]\nprogram = \"echo\"\nbody = \"x\\n\"\n\
             [jobs.opt.config]\npartition = \"long\"\n",
        )
        .unwrap();
        std::fs::write(fdir.join("plan.toml"), "[jobs.opt]\n").unwrap();

        let report = run_doctor(root, &DoctorScope::All).unwrap();
        assert!(report.has_fail(), "report:\n{report}");
    }
```

- [ ] **Step 3: Run the tests**

Run: `cargo test --lib --no-default-features doctor`
Expected: PASS (all doctor tests).

- [ ] **Step 4: Lint the `jm` deploy build (no-default-features)**

Run: `cargo clippy --no-default-features --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add src/doctor/mod.rs src/lib.rs
git commit -m "feat: add run_doctor orchestrator + DoctorScope"
```

---

### Task 6: `jm doctor` CLI subcommand

**Files:**
- Modify: `src/bin/jm.rs`
- Test: `tests/jm_doctor_cli.rs` (create)

- [ ] **Step 1: Add the `Doctor` variant + dispatch + handler in `src/bin/jm.rs`**

In `enum Cmd`, add this variant (after `Search`):

```rust
    /// Validate TOML files + structural invariants under --root.
    Doctor { target: Option<String> },
```

In `main()`'s `match cli.cmd { ... }`, add this arm:

```rust
        Cmd::Doctor { ref target } => {
            let root = resolve_root(&cli)?;
            cmd_doctor(&root, target.as_deref()).await
        }
```

Add this handler near `cmd_search`:

```rust
async fn cmd_doctor(root: &std::path::Path, target: Option<&str>) -> anyhow::Result<()> {
    use job_manager::doctor::{DoctorScope, run_doctor};

    let scope = match target {
        Some(t) => DoctorScope::Flow(parse_target(root, t)?),
        None => DoctorScope::All,
    };
    let report = run_doctor(root, &scope)?;
    print!("{report}");
    if report.has_fail() {
        anyhow::bail!(
            "doctor found {} error(s)",
            report.count(job_manager::Severity::Fail)
        );
    }
    Ok(())
}
```

- [ ] **Step 2: Write the failing CLI test**

Create `tests/jm_doctor_cli.rs`:

```rust
//! `jm doctor` CLI smoke test: clean tree exits 0, broken tree exits non-zero.

use assert_cmd::Command;
use predicates::str::contains;
use tempfile::tempdir;

fn write(p: &std::path::Path, body: &str) {
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(p, body).unwrap();
}

const FLOW: &str = "uuid = \"01999999-0000-7000-8000-000000000000\"\n\
created_at = \"2026-05-15T00:00:00Z\"\n\
[jobs.opt]\nprogram = \"echo\"\nbody = \"x\\n\"\n\
[jobs.opt.config]\npartition = \"long\"\n";

#[test]
fn doctor_clean_tree_exits_zero() {
    let d = tempdir().unwrap();
    let f = d.path().join("01999999-0000-7000-8000-000000000000");
    write(&f.join("flow.toml"), FLOW);
    write(&f.join("plan.toml"), "[jobs.opt]\nnproc = 1\n");

    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", d.path().to_str().unwrap(), "doctor"])
        .assert()
        .success()
        .stdout(contains("summary:"));
}

#[test]
fn doctor_broken_tree_exits_nonzero() {
    let d = tempdir().unwrap();
    let f = d.path().join("01999999-0000-7000-8000-000000000000");
    write(&f.join("flow.toml"), &format!("{FLOW}bogus = 1\n"));
    write(&f.join("plan.toml"), "[jobs.opt]\n");

    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", d.path().to_str().unwrap(), "doctor"])
        .assert()
        .failure()
        .stdout(contains("FAIL"));
}
```

- [ ] **Step 3: Run the CLI test**

Run: `cargo test --no-default-features --test jm_doctor_cli`
Expected: PASS (2 tests; `assert_cmd` rebuilds the `jm` bin automatically).

- [ ] **Step 4: Manual sanity**

Run: `cargo run --bin jm --no-default-features -- --root /tmp/nonexistent doctor; echo "exit=$?"`
Expected: a clear `resolve_root` error about the root not existing, non-zero exit.

- [ ] **Step 5: Commit**

```bash
git add src/bin/jm.rs tests/jm_doctor_cli.rs
git commit -m "feat: add jm doctor subcommand"
```

---

### Task 7: `examples/full/` exhaustive tree

**Files:**
- Create: `examples/full/common.toml`
- Create: `examples/full/01999999-0000-7000-8000-000000000000/flow.toml`
- Create: `examples/full/01999999-0000-7000-8000-000000000000/plan.toml`
- Create: `examples/full/01999999-0000-7000-8000-000000000000/.jm/flow.effective.toml`
- Create: `examples/full/01999999-0000-7000-8000-000000000000/.jm/opt/status.toml`
- Create: `examples/full/README.md`

- [ ] **Step 1: Create `examples/full/common.toml`**

```toml
# examples/full/common.toml — EXHAUSTIVE, VALID CommonConfig.
#
# Schema: gaussian_job_shared::config::CommonConfig (deny_unknown_fields)
#   [slurm_default] -> slurm_async_runner SlurmJobConfig (every field below)
#   [directories]   -> DirectoryConfig { project_root }
#
# Every user-settable [slurm_default] field is shown. `partition` is the
# only required field; the rest are optional and merged into each job's
# config (job value wins; common fills the gaps).

[slurm_default]
partition     = "long"
time_limit    = "00:10:00"            # HH:MM:SS (also "MM", "MM:SS", "D-H", "D-H:M", "D-H:M:S")
job_name      = "jm-full"
comment       = "job-manager full example"
log_stdout    = "/work/jm-full/%x.%j.out"   # %x=job_name %j=slurm jobid
log_stderr    = "/work/jm-full/%x.%j.err"
mail_user     = "user@example.com"
mail_types    = "BEGIN,END,FAIL"      # any of BEGIN,END,FAIL,REQUEUE,ALL
resource_spec = "p=4:t=8:c=8:m=8G"    # CPU form; GPU form is "g=1" (mutually exclusive)
array_spec    = "0-3%2"               # index range, %N caps concurrency
# dependency: a raw SLURM dependency string. job-manager normally manages
# inter-job dependencies via flow.toml `parents[]`, so leave this unset
# unless you need a manual SLURM dependency. Uncomment to use:
# dependency  = "afterok:200"

[directories]
project_root = "/work/jm-full"        # absolute; tilde / $HOME are NOT expanded
```

- [ ] **Step 2: Create `examples/full/01999999-0000-7000-8000-000000000000/flow.toml`**

```toml
# examples/full/<uuid>/flow.toml — EXHAUSTIVE, VALID JobFlow.
#
# Schema: gaussian_job_shared::entities::workflow::JobFlow
#   uuid          UUID v7 — MUST equal the parent directory name
#   created_at    RFC3339 UTC
#   tags          optional BTreeMap<String,String>
#   jobs.<JobId>  Job = JobSpec{program,config,body} (flattened) + parents[]
#
# Two jobs: `opt` (root) -> `freq` (afterok opt). `opt.config` shows every
# SlurmJobConfig field. partition is set explicitly so this file is valid
# standalone (a real flow.toml may omit it and inherit from common.toml).

uuid       = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"

[tags]
example = "full"
owner   = "demo"

# --- root job: every SlurmJobConfig field set ---
[jobs.opt]
program = "echo"
body    = "echo \"[opt] flow=$JM_FLOW_UUID job=$JM_JOB_ID\"\nsleep 1\n"

[jobs.opt.config]
partition     = "long"
time_limit    = "01:00:00"
job_name      = "jm-full-opt"
comment       = "optimization step"
log_stdout    = "/work/jm-full/opt.%j.out"
log_stderr    = "/work/jm-full/opt.%j.err"
mail_user     = "user@example.com"
mail_types    = "END,FAIL"
resource_spec = "p=1:t=8:c=8:m=8G"   # GPU alternative (do not combine): resource_spec = "g=1"
array_spec    = "0-1"
# dependency  = "afterok:12345"      # job-manager manages deps via parents[]; manual override only

# --- child job: depends on opt finishing exit 0 ---
[jobs.freq]
program = "echo"
body    = "echo \"[freq] flow=$JM_FLOW_UUID job=$JM_JOB_ID\"\nsleep 1\n"

[jobs.freq.config]
partition  = "long"
time_limit = "00:30:00"

# JobEdge.kind in { afterok, afterany, after, afternotok, aftercorr,
#                   afterburstbuffer, singleton }. afterok is the common one.
[[jobs.freq.parents]]
from = "opt"
kind = "afterok"
```

- [ ] **Step 3: Create `examples/full/01999999-0000-7000-8000-000000000000/plan.toml`**

```toml
# examples/full/<uuid>/plan.toml — ExperimentPlan.
#
# Schema: job_manager::plan::ExperimentPlan (deny_unknown_fields)
#   jobs.<JobId> : BTreeMap<String, toml::Value>  (arbitrary TOML values)
#
# Demonstrates every TOML value kind so you can see the flexibility.
# Every JobId in flow.toml has an entry here (jm doctor WARNs otherwise).

[jobs.opt]
note      = "string value"
nproc     = 8                        # integer
threshold = 1.5e-4                   # float
verbose   = true                     # boolean
route     = ["opt", "freq=noraman"]  # array
[jobs.opt.solvent]                   # nested table value
model = "pcm"
name  = "water"

[jobs.freq]
note  = "second step"
nproc = 8
```

- [ ] **Step 4: Create `examples/full/01999999-0000-7000-8000-000000000000/.jm/flow.effective.toml`**

```toml
# PROGRAM-WRITTEN — DO NOT EDIT.
#
# Materialized JobFlow snapshot (Cargo.lock analogue): every default from
# common.toml is baked in. job-manager writes this on `jm render`/`submit`
# and reads it on `tick`/`show` (no common.toml needed). Shown here so you
# know what the snapshot looks like; `jm doctor` parse-checks it.

uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"

[tags]
example = "full"
owner = "demo"

[jobs.opt]
program = "echo"
body = """
echo "[opt] flow=$JM_FLOW_UUID job=$JM_JOB_ID"
sleep 1
"""
parents = []

[jobs.opt.config]
partition = "long"
time_limit = "01:00:00"
job_name = "jm-full-opt"

[jobs.freq]
program = "echo"
body = """
echo "[freq] flow=$JM_FLOW_UUID job=$JM_JOB_ID"
sleep 1
"""

[jobs.freq.config]
partition = "long"
time_limit = "00:30:00"

[[jobs.freq.parents]]
from = "opt"
kind = "afterok"
```

- [ ] **Step 5: Create `examples/full/01999999-0000-7000-8000-000000000000/.jm/opt/status.toml`**

```toml
# PROGRAM-WRITTEN — DO NOT EDIT.
#
# Per-job runtime state. Schema: job_manager::job::JobRun (deny_unknown_fields)
#   lifecycle   queued | running | success | failed | skipped
#   updated_at  RFC3339 UTC
#   slurm_jobid optional u64 (omitted until submitted)
#   note        optional string
#   [slurm_status] optional A1 JobStatus snapshot { state, reason }

lifecycle = "running"
updated_at = "2026-05-15T00:05:00Z"
slurm_jobid = 12345
note = "submitted by jm submit"

[slurm_status]
state = "RUNNING"
reason = "None"
```

- [ ] **Step 6: Create `examples/full/README.md`**

````markdown
# `examples/full/` — exhaustive, valid TOML examples

Every file here parses cleanly (round-trips through serde) **and** shows
every user-settable field. It mirrors the real on-disk layout, so
`jm --root examples/full doctor` validates it (CI does this — see
`tests/doctor_examples.rs`).

```
examples/full/
  common.toml                                  # editable — CommonConfig
  01999999-0000-7000-8000-000000000000/
    flow.toml                                  # editable — JobFlow
    plan.toml                                  # editable — ExperimentPlan
    .jm/
      flow.effective.toml                      # PROGRAM-WRITTEN — do not edit
      opt/status.toml                          # PROGRAM-WRITTEN — do not edit
```

Editable = you author it. PROGRAM-WRITTEN = job-manager generates it; shown
for reference only. Field-by-field semantics: see
[`docs/toml-reference.md`](../../docs/toml-reference.md).
````

- [ ] **Step 7: Sanity-check via doctor**

Run: `cargo run --bin jm --no-default-features -- --root examples/full doctor; echo "exit=$?"`
Expected: report lines ending `summary: … 0 fail`, `exit=0`. A plan-coverage WARN is acceptable; only FAIL must be zero.

- [ ] **Step 8: Commit**

```bash
git add examples/full
git commit -m "docs: add examples/full exhaustive valid TOML tree"
```

---

### Task 8: Drift-guard integration test

**Files:**
- Create: `tests/doctor_examples.rs`

- [ ] **Step 1: Write the test**

```rust
//! Drift guard: `examples/full/` must stay valid. If an upstream D2/A1
//! struct changes incompatibly, `run_doctor` produces a FAIL and this
//! test (and `jm doctor`) goes red.

use job_manager::{DoctorScope, run_doctor};
use std::path::Path;

#[test]
fn examples_full_has_no_doctor_failures() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/full");
    let report = run_doctor(&root, &DoctorScope::All).expect("run_doctor should not error");
    assert!(!report.has_fail(), "examples/full has doctor FAILs:\n{report}");
}
```

- [ ] **Step 2: Run it**

Run: `cargo test --no-default-features --test doctor_examples`
Expected: PASS.

- [ ] **Step 3: Run the full CI gate**

Run:
```bash
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo clippy --no-default-features --all-targets -- -D warnings \
  && cargo test --all-features
```
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add tests/doctor_examples.rs
git commit -m "test: drift-guard examples/full via run_doctor"
```

---

### Task 9: `docs/toml-reference.md`

**Files:**
- Create: `docs/toml-reference.md`

- [ ] **Step 1: Create `docs/toml-reference.md` with the full content**

````markdown
# TOML File Reference

job-manager reads and writes five TOML files. **The authoritative schema
is the Rust serde structs** — run `cargo doc --no-deps --open` for field
docs. This page consolidates them. Every struct uses
`#[serde(deny_unknown_fields)]`, so a misspelled key is a hard parse
error. Validate a tree with **`jm --root <root> doctor`**.

## On-disk layout

```
<root>/
├── common.toml                         # optional, root-level (CommonConfig)
└── <flow_uuid>/
    ├── flow.toml                       # user-authored (JobFlow)
    ├── plan.toml                       # user-authored (ExperimentPlan)
    └── .jm/                            # program-managed (read-only to you)
        ├── flow.effective.toml         # materialized snapshot (JobFlow)
        └── <JobId>/status.toml         # per-job state (JobRun)
```

Exhaustive valid examples: [`examples/full/`](../examples/full/).

| File | Rust type | Source | Authored by |
|---|---|---|---|
| `common.toml` | `CommonConfig` | `gaussian_job_shared::config::common` | user (optional) |
| `flow.toml` | `JobFlow` | `gaussian_job_shared::entities::workflow` | user |
| `plan.toml` | `ExperimentPlan` | `job_manager::plan` | user |
| `.jm/flow.effective.toml` | `JobFlow` | same as flow.toml | program |
| `.jm/<JobId>/status.toml` | `JobRun` | `job_manager::job::run` | program |

## `common.toml` — `CommonConfig`

Optional. SLURM defaults merged into every job's `config` at submit time
(the job's own value wins; `common` fills gaps). `partition` set here is
injected into jobs that omit it.

| Key | Type | Required | Notes |
|---|---|---|---|
| `[slurm_default]` | `SlurmJobConfig` | yes | see [SlurmJobConfig](#slurmjobconfig) |
| `[directories].project_root` | path | yes | absolute; tilde/`$HOME` not expanded |

## `flow.toml` — `JobFlow`

| Key | Type | Required | Notes |
|---|---|---|---|
| `uuid` | UUID v7 string | yes | must equal the directory name |
| `created_at` | RFC3339 UTC | yes | e.g. `2026-05-15T00:00:00Z` |
| `[tags]` | map<string,string> | no | free-form metadata |
| `[jobs.<JobId>]` | `Job` | no | the DAG; key = stable JobId |

`Job` = `JobSpec` (flattened) + `parents`:

| Key | Type | Required | Notes |
|---|---|---|---|
| `program` | string | yes | e.g. `"g16"`, `"echo"` |
| `body` | string | yes | bash script body |
| `[jobs.<id>.config]` | `SlurmJobConfig` | yes\* | \*`partition` may be inherited from `common.toml` |
| `[[jobs.<id>.parents]]` | `JobEdge[]` | no | empty = root node |

`JobEdge`: `from` = a JobId key in this flow (FAIL if dangling);
`kind` ∈ `afterok afterany after afternotok aftercorr afterburstbuffer singleton`.

### SlurmJobConfig

Used by `[jobs.*.config]` and `common.toml [slurm_default]`.

| Key | Type | Required | Notes |
|---|---|---|---|
| `partition` | string | yes | inheritable from common.toml |
| `time_limit` | string | no | `HH:MM:SS`; also `MM`, `MM:SS`, `D-H`, `D-H:M`, `D-H:M:S` |
| `job_name` | string | no | |
| `comment` | string | no | |
| `log_stdout` | path | no | `%x`=job_name, `%j`=slurm jobid |
| `log_stderr` | path | no | |
| `mail_user` | string | no | email |
| `mail_types` | string | no | comma list of `BEGIN END FAIL REQUEUE ALL` |
| `resource_spec` | string | no | CPU `p=N:t=N:c=N:m=NG` **or** GPU `g=N` (mutually exclusive) |
| `array_spec` | string | no | `START-END[:STEP][%MAXCONC]`, comma-joined entries |
| `dependency` | string | no | raw SLURM dep; prefer `parents[]` |

### SLURM value grammars

- **time_limit**: `30` (30 min), `5:30` (5m30s), `12:34:56`, `1-0` (1 day),
  `2-3:30`, `3-12:00:00`. Serialized canonical `HH:MM:SS` (hours may exceed 23).
- **array_spec**: `0-15`, `0-15:4`, `0,6,16-32`, `0-15%4` (max 4 concurrent).
- **resource_spec**: CPU `p=4:t=8:c=8:m=8G` (each of p/t/c/m optional;
  memory suffix `K|M|G|T`, unitless = MiB); GPU `g=1`. CPU and GPU keys
  must not mix; zero counts rejected.
- **dependency**: `afterok:200`, `afterok:200:201`,
  `afterok:200,afterany:201` (AND), `afterok:200?afterany:201` (OR — do
  not mix `,` and `?`), `after:200+5` (`+min` only on `after`),
  `singleton` (no job ids).
- **mail_types**: `BEGIN,END,FAIL` — segments are case-sensitive UPPERCASE.
- **partition defaulting**: `flow.toml` may omit `[jobs.*.config].partition`;
  `read_flow` injects it from `common.toml [slurm_default].partition`. If
  neither supplies it, `jm doctor`/`submit` fails with `PartitionMissing`.

## `plan.toml` — `ExperimentPlan`

| Key | Type | Required | Notes |
|---|---|---|---|
| `[jobs.<JobId>]` | map<string, any TOML> | yes | arbitrary per-job render params |

Values may be string/int/float/bool/array/table. Every JobId in
`flow.toml` should have an entry (`jm doctor` WARNs on missing/extra).

## `.jm/flow.effective.toml` — `JobFlow`

Program-written materialized snapshot (Cargo.lock analogue): all
`common.toml` defaults baked in; readable without `common.toml`. Same
schema as `flow.toml`. **Do not edit.**

## `.jm/<JobId>/status.toml` — `JobRun`

Program-written per-job state. **Do not edit.**

| Key | Type | Required | Notes |
|---|---|---|---|
| `lifecycle` | enum | yes | `queued running success failed skipped` |
| `updated_at` | RFC3339 UTC | yes | |
| `slurm_jobid` | u64 | no | omitted until submitted |
| `note` | string | no | |
| `[slurm_status]` | `JobStatus` | no | `state` (UPPERCASE SLURM token, e.g. `PENDING`, `RUNNING`, `COMPLETED`, `OUT_OF_MEMORY`) + `reason` (PascalCase, e.g. `None`, `Priority`, `Dependency`; unknown → forward-compat `Other`) |

`lifecycle`: `Success|Failed|Skipped` are terminal (never overwritten by
`tick`). Pending is the *absence* of `status.toml` (no enum value).
````

- [ ] **Step 2: Verify fences balance**

Run: `grep -c '^```' docs/toml-reference.md`
Expected: an even number (all code fences closed).

- [ ] **Step 3: Commit**

```bash
git add docs/toml-reference.md
git commit -m "docs: add consolidated docs/toml-reference.md"
```

---

### Task 10: Cross-link the new docs

**Files:**
- Modify: `README.md`, `CLAUDE.md`, `docs/API.md`, `docs/development.md`, `docs/architecture.md`

For each file: read it first, locate the anchor by searching the given
text, and insert the exact snippet shown. Do not invent line numbers.

- [ ] **Step 1: README.md**

Find the section listing `jm` subcommands (search for `jm --root` or
`jm render`). Add this line to that list:

```
./target/debug/jm --root /work doctor [<flow_uuid>]   # validate TOML + structure
```

Find where the README links into `docs/` (search for `docs/API.md` or
`docs/architecture.md`) and add:

```
- [docs/toml-reference.md](docs/toml-reference.md) — every TOML file's format, field by field
```

- [ ] **Step 2: CLAUDE.md**

Find the fenced usage line (search for
`./target/debug/jm --root /work {render|submit`) and replace that single
line with:

```
./target/debug/jm --root /work {render|submit [--dry-run]|tick|show|doctor} <flow_uuid>
```

Immediately after that fenced block, add this paragraph:

```
`jm doctor` validates a `<root>` tree's TOML files + structural
invariants (uuid↔dir, parent edges, plan coverage). `examples/full/` is
an exhaustive valid tree, drift-guarded by `tests/doctor_examples.rs`.
Full TOML format reference: `docs/toml-reference.md`.
```

- [ ] **Step 3: docs/API.md**

Find the file/schema table (search for `JobFlow` or `status.toml`). Add
directly above that table:

```
> Full field-by-field TOML format: [toml-reference.md](toml-reference.md).
> Exhaustive valid examples: [`examples/full/`](../examples/full/).
> Validate a tree: `jm --root <root> doctor`.
```

- [ ] **Step 4: docs/development.md**

Find the testing/CI section (search for `cargo test`). Add:

```
- `cargo test --test doctor_examples` — drift-guards `examples/full/`
  (runs `jm doctor` logic over it). Also runnable as
  `cargo run --bin jm --no-default-features -- --root examples/full doctor`.
```

- [ ] **Step 5: docs/architecture.md**

Find the `status.toml` schema block (search for `lifecycle = "queued"`).
Add directly above it:

```
> Consolidated TOML format reference (all five files):
> [toml-reference.md](toml-reference.md).
```

- [ ] **Step 6: Verify**

Run:
```bash
for f in docs/toml-reference.md examples/full/README.md; do test -f "$f" && echo "OK $f"; done
grep -l "toml-reference" README.md CLAUDE.md docs/API.md docs/architecture.md
```
Expected: `OK` for both files; all four doc files listed by grep.

- [ ] **Step 7: Commit**

```bash
git add README.md CLAUDE.md docs/API.md docs/development.md docs/architecture.md
git commit -m "docs: cross-link toml-reference, examples/full, jm doctor"
```

---

## Self-Review

**Spec coverage:**
- Deliverable 1 (`docs/toml-reference.md`) → Task 9 + cross-links Task 10. ✓
- Deliverable 2 (`examples/full/`, valid+exhaustive, real layout, editable vs program-written) → Task 7. ✓
- Deliverable 3 (`jm doctor`: parse + structural checks, severity model, extensibility seam, reuse readers, no SLURM/fs/env checks) → Tasks 1–6; seam documented as a `//!` comment in `src/doctor/checks.rs` (Task 3 Step 1). ✓
- Drift guard (CI over `examples/full/`) → Task 8. ✓
- Doc updates (README/CLAUDE/API/development/architecture) → Task 10. ✓
- `jm` builds `--no-default-features` (SLURM node) → doctor is pure serde+fs; verified Task 5 Step 4 and Task 6/8 commands. ✓

**Placeholder scan:** No TBD/TODO. Every code step shows full code; every doc step shows full file content or the exact snippet + search anchor (doc files README/CLAUDE/API/development/architecture were not read in planning, so location is by search anchor with concrete content, not invented line numbers). ✓

**Type consistency:** `DoctorReport`/`Finding`/`Severity` (Task 1) used unchanged in Tasks 3–8. `run_doctor(&Path,&DoctorScope)->Result<DoctorReport,JobManagerError>` and `DoctorScope::{All,Flow(Uuid)}` (Task 5) used identically in Task 6 (`jm.rs`) and Task 8 (`tests/doctor_examples.rs`). `flow_dirs` (Task 2) consumed in Task 5. Check fns `check_flow/check_plan/check_flow_effective/check_status_files/check_uuid_matches_dir/check_parents_resolve/check_plan_coverage` defined Tasks 3–4, called identically in Task 5. Re-exports `job_manager::{run_doctor,DoctorScope,Severity}` consistent across `lib.rs`, `jm.rs`, integration test. `read_plan` reused in `check_plan_coverage` (no separate alias). ✓
