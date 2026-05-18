# `jm ls` Cross-Flow Status Listing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `jm ls jobs|flows|tree` — read-only cross-flow status listing with rich filtering — by wiring the existing `SearchFilter`/`matches()` into a new pure listing layer, and remove the half-built `jm search`.

**Architecture:** A new pure library module `src/listing.rs` owns the display type, row projection, aggregation, and string formatters (all unit-testable). `SearchFilter.status` is widened from `Option<Lifecycle>` to `BTreeSet<DisplayLifecycle>` for SLURM-style multi-state filtering. An async `collect()` reuses `walk_flows` + parallel `read_job_run`. `src/bin/jm.rs` becomes a thin CLI shell. No live SLURM; on-disk only.

**Tech Stack:** Rust (nightly, edition 2024), clap 4.6 derive, serde/serde_json, tokio, futures, rstest; PyO3 binding update + pyo3-stub-gen regen; uv/pytest smoke.

**Spec:** `docs/superpowers/specs/2026-05-16-jm-ls-commands-design.md`

---

## File Structure

- **Create** `src/listing.rs` — `DisplayLifecycle` (+ code/long parse & Display), `parse_status_set`, `JobRow`/`FlowRow` + JSON views, `FlowStatus` + `aggregate_flow_status`, pure formatters (`format_jobs_table`/`format_flows_table`/`*_json`/`format_tree`), `CollectedFlow`, async `collect`, projections (`job_rows`/`flow_rows`/`matched_flows`).
- **Create** `tests/integration_listing.rs` — on-disk end-to-end (no SLURM).
- **Modify** `src/lib.rs` — `pub mod listing;` + re-exports; reflect `SearchFilter` type change.
- **Modify** `src/search.rs` — `status: BTreeSet<DisplayLifecycle>`, `matches()` status clause, migrate existing tests.
- **Modify** `src/py_export/search.rs` — `status: Vec<String>`, `to_inner` parses via `listing::parse_status_set`.
- **Modify** `src/bin/jm.rs` — delete `Cmd::Search`/`cmd_search`; add `Cmd::Ls` + `LsView` + `FilterArgs`/`FmtArgs` + `cmd_ls_*`.
- **Modify** `python/job_manager/_job_manager_core/*.pyi` — regenerated (never hand-edited).
- **Modify** docs: `README.md`, `docs/API.md`, `docs/architecture.md`, `docs/development.md`, `CLAUDE.md`.

Reused as-is: `walk::walk_flows`, `persistence::{PathResolver, read_job_run, read_common, synth_empty_common}`, `flow::topological_order`, `bin/jm.rs::{resolve_root, parse_target, parse_tag}`.

---

## Task 1: `DisplayLifecycle` + status parsing (pure)

**Files:**
- Create: `src/listing.rs`
- Modify: `src/lib.rs` (add module + re-export)
- Test: `src/listing.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test**

Create `src/listing.rs` with only this content:

```rust
//! `jm ls` — pure projection/aggregation/formatting for cross-flow listing.

use std::collections::BTreeSet;

use crate::job::lifecycle::Lifecycle;

/// Display-time lifecycle: the 5 `Lifecycle` values plus `Pending`
/// (no `status.toml` on disk — not a real enum value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DisplayLifecycle {
    Pending,
    Real(Lifecycle),
}

impl DisplayLifecycle {
    /// Short code shown in the `ST` column.
    pub fn code(self) -> &'static str {
        match self {
            DisplayLifecycle::Pending => "PD",
            DisplayLifecycle::Real(Lifecycle::Queued) => "Q",
            DisplayLifecycle::Real(Lifecycle::Running) => "R",
            DisplayLifecycle::Real(Lifecycle::Success) => "OK",
            DisplayLifecycle::Real(Lifecycle::Failed) => "F",
            DisplayLifecycle::Real(Lifecycle::Skipped) => "SK",
        }
    }

    /// Long machine-readable name (used in `--json`).
    pub fn long(self) -> &'static str {
        match self {
            DisplayLifecycle::Pending => "pending",
            DisplayLifecycle::Real(Lifecycle::Queued) => "queued",
            DisplayLifecycle::Real(Lifecycle::Running) => "running",
            DisplayLifecycle::Real(Lifecycle::Success) => "success",
            DisplayLifecycle::Real(Lifecycle::Failed) => "failed",
            DisplayLifecycle::Real(Lifecycle::Skipped) => "skipped",
        }
    }

    /// Parse one token: short code or long name, case-insensitive.
    pub fn parse_token(s: &str) -> Result<DisplayLifecycle, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "pd" | "pending" => Ok(DisplayLifecycle::Pending),
            "q" | "queued" => Ok(DisplayLifecycle::Real(Lifecycle::Queued)),
            "r" | "running" => Ok(DisplayLifecycle::Real(Lifecycle::Running)),
            "ok" | "success" => Ok(DisplayLifecycle::Real(Lifecycle::Success)),
            "f" | "failed" => Ok(DisplayLifecycle::Real(Lifecycle::Failed)),
            "sk" | "skipped" => Ok(DisplayLifecycle::Real(Lifecycle::Skipped)),
            other => Err(format!(
                "unknown status {other:?} (expected one of \
                 pd,q,r,ok,f,sk / pending,queued,running,success,failed,skipped)"
            )),
        }
    }
}

/// Parse a comma-separated `--status` value into a set. Empty/blank input
/// yields an empty set (= no status filter). Whitespace around tokens is
/// trimmed; empty tokens (e.g. trailing comma) are ignored.
pub fn parse_status_set(csv: &str) -> Result<BTreeSet<DisplayLifecycle>, String> {
    let mut out = BTreeSet::new();
    for tok in csv.split(',') {
        if tok.trim().is_empty() {
            continue;
        }
        out.insert(DisplayLifecycle::parse_token(tok)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("PD", DisplayLifecycle::Pending)]
    #[case("pending", DisplayLifecycle::Pending)]
    #[case("R", DisplayLifecycle::Real(Lifecycle::Running))]
    #[case("Running", DisplayLifecycle::Real(Lifecycle::Running))]
    #[case("ok", DisplayLifecycle::Real(Lifecycle::Success))]
    #[case("SK", DisplayLifecycle::Real(Lifecycle::Skipped))]
    fn parse_token_accepts_code_and_long_case_insensitive(
        #[case] input: &str,
        #[case] expected: DisplayLifecycle,
    ) {
        assert_eq!(DisplayLifecycle::parse_token(input).unwrap(), expected);
    }

    #[test]
    fn parse_token_rejects_unknown() {
        let err = DisplayLifecycle::parse_token("xyz").unwrap_err();
        assert!(err.contains("unknown status"), "got: {err}");
    }

    #[test]
    fn code_and_long_round_trip_for_every_variant() {
        for dl in [
            DisplayLifecycle::Pending,
            DisplayLifecycle::Real(Lifecycle::Queued),
            DisplayLifecycle::Real(Lifecycle::Running),
            DisplayLifecycle::Real(Lifecycle::Success),
            DisplayLifecycle::Real(Lifecycle::Failed),
            DisplayLifecycle::Real(Lifecycle::Skipped),
        ] {
            assert_eq!(DisplayLifecycle::parse_token(dl.code()).unwrap(), dl);
            assert_eq!(DisplayLifecycle::parse_token(dl.long()).unwrap(), dl);
        }
    }

    #[test]
    fn parse_status_set_splits_csv_and_ignores_blanks() {
        let s = parse_status_set("running, F ,").unwrap();
        assert_eq!(s.len(), 2);
        assert!(s.contains(&DisplayLifecycle::Real(Lifecycle::Running)));
        assert!(s.contains(&DisplayLifecycle::Real(Lifecycle::Failed)));
        assert!(parse_status_set("").unwrap().is_empty());
        assert!(parse_status_set("  ").unwrap().is_empty());
    }

    #[test]
    fn parse_status_set_propagates_token_error() {
        assert!(parse_status_set("running,nope").is_err());
    }
}
```

Add to `src/lib.rs`: insert `pub mod listing;` between `pub mod jobid;` and `pub mod persistence;` (keep the existing alphabetical module list ordering). Add to the re-export block, immediately after the `pub use jobid::{...};` line:

```rust
pub use listing::{DisplayLifecycle, parse_status_set};
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --all-features listing::`
Expected: Either COMPILE ERROR (symbols newly added and not yet wired) or PASS. Because the implementation is included in Step 1, a clean PASS here is acceptable for this bootstrap task — proceed to Step 4.

- [ ] **Step 3: (implementation already included in Step 1)**

No additional code.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib --all-features listing::`
Expected: PASS (5 tests).
Run: `cargo build --no-default-features`
Expected: PASS (listing.rs must not depend on pyo3).

- [ ] **Step 5: Commit**

```bash
git add src/listing.rs src/lib.rs
git commit -m "feat(listing): add DisplayLifecycle + status CSV parsing"
```

---

## Task 2: Widen `SearchFilter.status` to a set + update `matches()`

**Files:**
- Modify: `src/search.rs` (struct field, `matches()` status clause, existing tests)
- Test: `src/search.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test**

In `src/search.rs`, append these tests inside `mod tests` (after `slurm_jobid_filter_matches_via_status_entry`):

```rust
    #[test]
    fn status_set_empty_matches_any() {
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let filt = SearchFilter::default();
        assert!(matches(&f, &id, &j, None, &filt));
    }

    #[test]
    fn status_set_or_matches_running_or_failed() {
        use crate::listing::DisplayLifecycle;
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let mut want = std::collections::BTreeSet::new();
        want.insert(DisplayLifecycle::Real(Lifecycle::Running));
        want.insert(DisplayLifecycle::Real(Lifecycle::Failed));
        let filt = SearchFilter { status: want, ..Default::default() };

        let running = JobRun {
            lifecycle: Lifecycle::Running,
            updated_at: Utc::now(),
            slurm_jobid: None,
            slurm_status: None,
            note: None,
        };
        assert!(matches(&f, &id, &j, Some(&running), &filt));

        let queued = JobRun { lifecycle: Lifecycle::Queued, ..running.clone() };
        assert!(!matches(&f, &id, &j, Some(&queued), &filt));
    }

    #[test]
    fn status_set_pending_matches_missing_status() {
        use crate::listing::DisplayLifecycle;
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let mut want = std::collections::BTreeSet::new();
        want.insert(DisplayLifecycle::Pending);
        let filt = SearchFilter { status: want, ..Default::default() };
        assert!(matches(&f, &id, &j, None, &filt)); // no JobRun == Pending
        let running = JobRun {
            lifecycle: Lifecycle::Running,
            updated_at: Utc::now(),
            slurm_jobid: None,
            slurm_status: None,
            note: None,
        };
        assert!(!matches(&f, &id, &j, Some(&running), &filt));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib --all-features search::tests::status_set`
Expected: COMPILE ERROR — `SearchFilter.status` is still `Option<Lifecycle>`; no `BTreeSet` field.

- [ ] **Step 3: Write minimal implementation**

In `src/search.rs`, replace the top imports with:

```rust
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, Program};

use crate::job::run::JobRun;
use crate::listing::DisplayLifecycle;
```

(`Lifecycle` is no longer used in non-test code. The `mod tests` block already references `Lifecycle`; if it does not resolve after this change, add `use crate::job::lifecycle::Lifecycle;` to the `mod tests` `use` lines.)

Change the struct field type (leave the other 7 fields unchanged):

```rust
    pub status: BTreeSet<DisplayLifecycle>,
```

Replace the status clause in `matches()` (the `if let Some(want) = f.status { match status { ... } }` block) with:

```rust
    if !f.status.is_empty() {
        let dl = match status {
            Some(jr) => DisplayLifecycle::Real(jr.lifecycle),
            None => DisplayLifecycle::Pending,
        };
        if !f.status.contains(&dl) {
            return false;
        }
    }
```

Migrate the existing `status_filter_requires_status_entry` test to the new type:

```rust
    #[test]
    fn status_filter_requires_status_entry() {
        use crate::listing::DisplayLifecycle;
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let mut want = std::collections::BTreeSet::new();
        want.insert(DisplayLifecycle::Real(Lifecycle::Queued));
        let filt = SearchFilter { status: want, ..Default::default() };
        assert!(!matches(&f, &id, &j, None, &filt));
        let entry = JobRun {
            lifecycle: Lifecycle::Queued,
            updated_at: Utc::now(),
            slurm_jobid: None,
            slurm_status: None,
            note: None,
        };
        assert!(matches(&f, &id, &j, Some(&entry), &filt));
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib --all-features search::`
Expected: PASS (all search tests incl. 3 new + migrated).

- [ ] **Step 5: Commit**

```bash
git add src/search.rs
git commit -m "feat(search)!: widen SearchFilter.status to BTreeSet<DisplayLifecycle>

BREAKING CHANGE: SearchFilter.status changes from Option<Lifecycle> to
BTreeSet<DisplayLifecycle> for SLURM-style multi-state filtering and a
first-class Pending (no status.toml)."
```

---

## Task 3: Update Python binding `PySearchFilter.status` → `list[str]`

**Files:**
- Modify: `src/py_export/search.rs`
- Modify: `python/job_manager/_job_manager_core/*.pyi` (regenerated)
- Create: `python/tests/test_search_filter_status.py`

- [ ] **Step 1: Write the failing test**

Create `python/tests/test_search_filter_status.py`:

```python
from job_manager._job_manager_core import SearchFilter


def test_search_filter_accepts_status_string_list():
    f = SearchFilter(status=["running", "F"])
    assert f.status == ["running", "F"]


def test_search_filter_status_defaults_empty():
    f = SearchFilter()
    assert f.status == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run maturin develop && uv run pytest python/tests/test_search_filter_status.py -v`
Expected: FAIL — current `status` is `Option[PyLifecycle]`; passing a list raises `TypeError`.

- [ ] **Step 3: Write minimal implementation**

In `src/py_export/search.rs`:

Change the struct field:

```rust
    pub status: Vec<String>,
```

Change the `#[new]` body parameter type and assignment (keep signature attribute as-is, `status=None` stays):

```rust
    fn new(
        program: Option<String>,
        tags: Option<HashMap<String, String>>,
        status: Option<Vec<String>>,
        flow_uuid_prefix: Option<String>,
        created_after: Option<DateTime<Utc>>,
        created_before: Option<DateTime<Utc>>,
        slurm_jobid: Option<u64>,
        job_id: Option<String>,
    ) -> Self {
        Self {
            program,
            tags: tags.unwrap_or_default(),
            status: status.unwrap_or_default(),
            flow_uuid_prefix,
            created_after,
            created_before,
            slurm_jobid,
            job_id,
        }
    }
```

Replace `to_inner` (it is `#[allow(dead_code)]` with no current callers; signature change is safe):

```rust
    // Used by py_export::walk and py_export::tick (Task 14); allow dead_code until then.
    #[allow(dead_code)]
    pub(crate) fn to_inner(&self) -> Result<Inner, String> {
        let mut status = std::collections::BTreeSet::new();
        for tok in &self.status {
            status.insert(crate::listing::DisplayLifecycle::parse_token(tok)?);
        }
        Ok(Inner {
            program: self
                .program
                .clone()
                .map(gaussian_job_shared::entities::workflow::Program::from),
            tags: self
                .tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
            status,
            flow_uuid_prefix: self.flow_uuid_prefix.clone(),
            created_after: self.created_after,
            created_before: self.created_before,
            slurm_jobid: self.slurm_jobid,
            job_id: self
                .job_id
                .clone()
                .map(gaussian_job_shared::entities::workflow::JobId::from),
        })
    }
```

Remove the now-unused `use crate::py_export::job::PyLifecycle;` import at the top of the file **only if** `PyLifecycle` is no longer referenced anywhere in `src/py_export/search.rs` (grep the file; if other references remain, keep the import).

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cargo run --bin stub_gen && uv run ruff format python/
uv run maturin develop
uv run pytest python/tests/test_search_filter_status.py -v
```
Expected: PASS (2 tests). `SearchFilter.status` in the `.pyi` now shows `list[builtins.str]`.

- [ ] **Step 5: Commit**

```bash
git add src/py_export/search.rs python/job_manager/_job_manager_core/ python/tests/test_search_filter_status.py
git commit -m "feat(py_export)!: PySearchFilter.status is now list[str]

BREAKING CHANGE: Python SearchFilter.status changes from Optional[Lifecycle]
to list[str] (short code or long name). Regenerated .pyi."
```

---

## Task 4: Row models + `aggregate_flow_status` (pure)

**Files:**
- Modify: `src/listing.rs` (append before `#[cfg(test)]`)
- Test: `src/listing.rs` `mod tests`

- [ ] **Step 1: Write the failing test**

Append to `src/listing.rs` (immediately before `#[cfg(test)] mod tests`):

```rust
use chrono::{DateTime, Utc};
use gaussian_job_shared::entities::workflow::{JobFlow, JobId};
use serde::Serialize;
use uuid::Uuid;

use crate::job::run::JobRun;
use crate::persistence::path::PathResolver;

/// One flow + its on-disk per-job status (`None` == Pending: no readable
/// `status.toml`). Produced by [`collect`] (Task 6).
#[derive(Debug)]
pub struct CollectedFlow {
    pub flow: JobFlow,
    pub statuses: std::collections::BTreeMap<JobId, Option<JobRun>>,
}

impl CollectedFlow {
    /// `DisplayLifecycle` for one job (`Pending` if no status).
    pub fn job_display(&self, job_id: &JobId) -> DisplayLifecycle {
        match self.statuses.get(job_id).and_then(|o| o.as_ref()) {
            Some(jr) => DisplayLifecycle::Real(jr.lifecycle),
            None => DisplayLifecycle::Pending,
        }
    }
}

/// Rolled-up flow status (priority order is fixed; see spec §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowStatus {
    Failed,
    Running,
    Queued,
    Done,
    Partial,
    Pending,
}

impl FlowStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            FlowStatus::Failed => "FAILED",
            FlowStatus::Running => "RUNNING",
            FlowStatus::Queued => "QUEUED",
            FlowStatus::Done => "DONE",
            FlowStatus::Partial => "PARTIAL",
            FlowStatus::Pending => "PENDING",
        }
    }
}

/// Aggregate a flow's per-job displays into one rolled-up status.
/// Priority: FAILED > RUNNING > QUEUED > DONE(all success) > PARTIAL
/// (>=1 skipped, all terminal) > PENDING (anything else / empty).
pub fn aggregate_flow_status(jobs: &[DisplayLifecycle]) -> FlowStatus {
    use crate::job::lifecycle::Lifecycle::{Failed, Queued, Running, Skipped, Success};
    if jobs.is_empty() {
        return FlowStatus::Pending;
    }
    if jobs.iter().any(|d| *d == DisplayLifecycle::Real(Failed)) {
        return FlowStatus::Failed;
    }
    if jobs.iter().any(|d| *d == DisplayLifecycle::Real(Running)) {
        return FlowStatus::Running;
    }
    if jobs.iter().any(|d| *d == DisplayLifecycle::Real(Queued)) {
        return FlowStatus::Queued;
    }
    if jobs.iter().all(|d| *d == DisplayLifecycle::Real(Success)) {
        return FlowStatus::Done;
    }
    let any_skipped = jobs.iter().any(|d| *d == DisplayLifecycle::Real(Skipped));
    let all_terminal = jobs.iter().all(|d| {
        matches!(
            d,
            DisplayLifecycle::Real(Success) | DisplayLifecycle::Real(Skipped)
        )
    });
    if any_skipped && all_terminal {
        FlowStatus::Partial
    } else {
        FlowStatus::Pending
    }
}

/// In-memory job row (canonical data; display/JSON views derived).
#[derive(Debug, Clone)]
pub struct JobRow {
    pub flow_uuid: Uuid,
    pub job_id: String,
    pub status: DisplayLifecycle,
    pub slurm_jobid: Option<u64>,
    pub program: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// In-memory flow row.
#[derive(Debug, Clone)]
pub struct FlowRow {
    pub flow_uuid: Uuid,
    pub total: usize,
    pub done: usize,
    pub status: FlowStatus,
    pub created_at: DateTime<Utc>,
}

/// `--json` view for a job row (full uuid, long status, RFC3339 times).
#[derive(Serialize)]
pub struct JobRowJson {
    pub flow: String,
    pub job: String,
    pub status: String,
    pub slurm_jobid: Option<u64>,
    pub program: String,
    pub updated_at: Option<String>,
    pub created_at: String,
}

impl From<&JobRow> for JobRowJson {
    fn from(r: &JobRow) -> Self {
        JobRowJson {
            flow: r.flow_uuid.to_string(),
            job: r.job_id.clone(),
            status: r.status.long().to_string(),
            slurm_jobid: r.slurm_jobid,
            program: r.program.clone(),
            updated_at: r.updated_at.map(|t| t.to_rfc3339()),
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

/// `--json` view for a flow row.
#[derive(Serialize)]
pub struct FlowRowJson {
    pub flow: String,
    pub total: usize,
    pub done: usize,
    pub status: String,
    pub created_at: String,
}

impl From<&FlowRow> for FlowRowJson {
    fn from(r: &FlowRow) -> Self {
        FlowRowJson {
            flow: r.flow_uuid.to_string(),
            total: r.total,
            done: r.done,
            status: r.status.as_str().to_string(),
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

// Keeps the `PathResolver` import live until Task 6 wires `collect`.
#[allow(unused_imports)]
use PathResolver as _PathResolverProbe;
```

Add aggregate tests inside `mod tests` (append):

```rust
    use crate::job::lifecycle::Lifecycle;

    fn dl(l: Lifecycle) -> DisplayLifecycle {
        DisplayLifecycle::Real(l)
    }

    #[rstest]
    #[case(vec![], FlowStatus::Pending)]
    #[case(vec![dl(Lifecycle::Success), dl(Lifecycle::Failed)], FlowStatus::Failed)]
    #[case(vec![dl(Lifecycle::Running), dl(Lifecycle::Queued)], FlowStatus::Running)]
    #[case(vec![dl(Lifecycle::Queued), dl(Lifecycle::Success)], FlowStatus::Queued)]
    #[case(vec![dl(Lifecycle::Success), dl(Lifecycle::Success)], FlowStatus::Done)]
    #[case(vec![dl(Lifecycle::Success), dl(Lifecycle::Skipped)], FlowStatus::Partial)]
    #[case(vec![dl(Lifecycle::Skipped), dl(Lifecycle::Skipped)], FlowStatus::Partial)]
    #[case(vec![DisplayLifecycle::Pending, dl(Lifecycle::Success)], FlowStatus::Pending)]
    #[case(vec![DisplayLifecycle::Pending, dl(Lifecycle::Skipped)], FlowStatus::Pending)]
    fn aggregate_flow_status_priority(
        #[case] jobs: Vec<DisplayLifecycle>,
        #[case] expected: FlowStatus,
    ) {
        assert_eq!(aggregate_flow_status(&jobs), expected);
    }
```

- [ ] **Step 2: Run test to verify it fails / compiles**

Run: `cargo test --lib --all-features listing::tests::aggregate_flow_status_priority`
Expected: PASS if it compiles (impl included). If COMPILE ERROR, fix imports per messages.

- [ ] **Step 3: (implementation included in Step 1)**

No extra code.

- [ ] **Step 4: Run full module + build**

Run: `cargo test --lib --all-features listing::`
Expected: PASS (Task 1 + Task 4 tests).
Run: `cargo build --no-default-features`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/listing.rs
git commit -m "feat(listing): add row models, JSON views, aggregate_flow_status"
```

---

## Task 5: Pure formatters — tables, JSON, tree

**Files:**
- Modify: `src/listing.rs` (append before `#[cfg(test)]`) + extend `src/lib.rs` re-export
- Test: `src/listing.rs` `mod tests`

- [ ] **Step 1: Write the failing test**

Append to `src/listing.rs` (before `#[cfg(test)]`):

```rust
fn pad(s: &str, w: usize) -> String {
    let mut out = String::from(s);
    while out.chars().count() < w {
        out.push(' ');
    }
    out
}

fn col_width<'a>(header: &str, cells: impl Iterator<Item = &'a str>) -> usize {
    cells.fold(header.chars().count(), |m, c| m.max(c.chars().count()))
}

fn render_row(cells: &[String], w: &[usize]) -> String {
    let mut line = String::new();
    for (i, c) in cells.iter().enumerate() {
        if i > 0 {
            line.push_str("  ");
        }
        if i == cells.len() - 1 {
            line.push_str(c); // last column unpadded
        } else {
            line.push_str(&pad(c, w[i]));
        }
    }
    line.push('\n');
    line
}

/// Render job rows as an aligned table. `FLOW` is the uuid's first 8
/// chars. Empty input + `no_header` => "".
pub fn format_jobs_table(rows: &[JobRow], no_header: bool) -> String {
    let cells: Vec<[String; 7]> = rows
        .iter()
        .map(|r| {
            [
                r.flow_uuid.to_string()[..8].to_string(),
                r.job_id.clone(),
                r.status.code().to_string(),
                r.slurm_jobid.map(|j| j.to_string()).unwrap_or_else(|| "-".into()),
                r.program.clone(),
                r.updated_at.map(|t| t.to_rfc3339()).unwrap_or_else(|| "-".into()),
                r.created_at.to_rfc3339(),
            ]
        })
        .collect();
    let hdr = ["FLOW", "JOB", "ST", "SLURM_ID", "PROGRAM", "UPDATED", "CREATED"];
    let w: Vec<usize> = (0..7)
        .map(|i| col_width(hdr[i], cells.iter().map(|c| c[i].as_str())))
        .collect();
    let mut out = String::new();
    if !no_header {
        out.push_str(&render_row(&hdr.map(String::from), &w));
    }
    for c in &cells {
        out.push_str(&render_row(c, &w));
    }
    out
}

/// Render flow rows as an aligned table.
pub fn format_flows_table(rows: &[FlowRow], no_header: bool) -> String {
    let cells: Vec<[String; 5]> = rows
        .iter()
        .map(|r| {
            [
                r.flow_uuid.to_string()[..8].to_string(),
                r.total.to_string(),
                r.done.to_string(),
                r.status.as_str().to_string(),
                r.created_at.to_rfc3339(),
            ]
        })
        .collect();
    let hdr = ["FLOW", "TOTAL", "DONE", "STATUS", "CREATED"];
    let w: Vec<usize> = (0..5)
        .map(|i| col_width(hdr[i], cells.iter().map(|c| c[i].as_str())))
        .collect();
    let mut out = String::new();
    if !no_header {
        out.push_str(&render_row(&hdr.map(String::from), &w));
    }
    for c in &cells {
        out.push_str(&render_row(c, &w));
    }
    out
}

/// Serialize job rows as a pretty JSON array.
pub fn format_jobs_json(rows: &[JobRow]) -> Result<String, serde_json::Error> {
    let v: Vec<JobRowJson> = rows.iter().map(JobRowJson::from).collect();
    serde_json::to_string_pretty(&v)
}

/// Serialize flow rows as a pretty JSON array.
pub fn format_flows_json(rows: &[FlowRow]) -> Result<String, serde_json::Error> {
    let v: Vec<FlowRowJson> = rows.iter().map(FlowRowJson::from).collect();
    serde_json::to_string_pretty(&v)
}

/// Render a forest of flows as a tree. Jobs are ordered topologically
/// (falls back to BTreeMap key order if the DAG has a cycle — `jm doctor`
/// reports cycles separately). Each child annotates its parent edges.
pub fn format_tree(flows: &[&CollectedFlow]) -> String {
    use crate::job::lifecycle::Lifecycle::Success;
    let mut out = String::new();
    for (fi, cf) in flows.iter().enumerate() {
        if fi > 0 {
            out.push('\n');
        }
        let displays: Vec<DisplayLifecycle> =
            cf.flow.jobs.keys().map(|k| cf.job_display(k)).collect();
        let agg = aggregate_flow_status(&displays);
        let ok = displays
            .iter()
            .filter(|d| **d == DisplayLifecycle::Real(Success))
            .count();
        out.push_str(&format!(
            "{}  ({} jobs · {} OK · {})\n",
            &cf.flow.uuid.to_string()[..8],
            cf.flow.jobs.len(),
            ok,
            agg.as_str()
        ));
        let order = crate::flow::topological_order(&cf.flow.jobs, cf.flow.uuid)
            .unwrap_or_else(|_| cf.flow.jobs.keys().cloned().collect());
        for (i, jid) in order.iter().enumerate() {
            let last = i == order.len() - 1;
            let branch = if last { "└─" } else { "├─" };
            let job = &cf.flow.jobs[jid];
            let edges = if job.parents.is_empty() {
                String::new()
            } else {
                let ps: Vec<String> = job
                    .parents
                    .iter()
                    .map(|e| format!("{:?} {}", e.kind, e.from.0))
                    .collect();
                format!("  ({})", ps.join(", "))
            };
            out.push_str(&format!(
                "{} {}  {}{}\n",
                branch,
                jid.0,
                cf.job_display(jid).code(),
                edges
            ));
        }
    }
    out
}
```

Extend the `src/lib.rs` listing re-export line (replace the Task 1 line):

```rust
pub use listing::{
    CollectedFlow, DisplayLifecycle, FlowRow, FlowStatus, JobRow, aggregate_flow_status,
    format_flows_json, format_flows_table, format_jobs_json, format_jobs_table, format_tree,
    parse_status_set,
};
```

Add formatter tests inside `mod tests` (append):

```rust
    use chrono::TimeZone;
    use std::collections::BTreeMap;

    fn sample_job_row() -> JobRow {
        JobRow {
            flow_uuid: Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap(),
            job_id: "step1".into(),
            status: DisplayLifecycle::Real(Lifecycle::Success),
            slurm_jobid: Some(120345),
            program: "g16".into(),
            updated_at: Some(Utc.with_ymd_and_hms(2026, 5, 16, 1, 2, 3).unwrap()),
            created_at: Utc.with_ymd_and_hms(2026, 5, 16, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn jobs_table_has_header_and_short_uuid_and_code() {
        let t = format_jobs_table(&[sample_job_row()], false);
        let mut lines = t.lines();
        assert!(lines.next().unwrap().starts_with("FLOW"));
        let row = lines.next().unwrap();
        assert!(row.contains("01997cdc"));
        assert!(!row.contains("01997cdc-0000"));
        assert!(row.contains("OK"));
    }

    #[test]
    fn jobs_table_no_header_drops_header() {
        let t = format_jobs_table(&[sample_job_row()], true);
        assert!(!t.contains("FLOW"));
        assert!(t.contains("01997cdc"));
    }

    #[test]
    fn jobs_table_empty_no_header_is_empty_string() {
        assert_eq!(format_jobs_table(&[], true), "");
    }

    #[test]
    fn jobs_json_uses_full_uuid_and_long_status() {
        let j = format_jobs_json(&[sample_job_row()]).unwrap();
        assert!(j.contains("01997cdc-0000-7000-8000-000000000000"));
        assert!(j.contains("\"status\": \"success\""));
        assert!(j.contains("\"slurm_jobid\": 120345"));
    }

    #[test]
    fn tree_renders_forest_with_edges_and_topo_order() {
        use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobSpec, Program};
        use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

        fn cfg() -> SlurmJobConfig {
            SlurmJobConfig {
                partition: "long".into(),
                time_limit: None,
                log_stdout: None,
                log_stderr: None,
                comment: None,
                job_name: None,
                array_spec: None,
                dependency: None,
                mail_user: None,
                mail_types: None,
                resource_spec: None,
            }
        }
        let mut jobs = BTreeMap::new();
        jobs.insert(
            JobId::from("step1"),
            Job {
                spec: JobSpec { program: Program::from("g16"), config: cfg(), body: "".into() },
                parents: vec![],
            },
        );
        jobs.insert(
            JobId::from("step2"),
            Job {
                spec: JobSpec { program: Program::from("g16"), config: cfg(), body: "".into() },
                parents: vec![JobEdge {
                    from: JobId::from("step1"),
                    kind: DependencyType::AfterOk,
                }],
            },
        );
        let flow = JobFlow {
            uuid: Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap(),
            created_at: Utc::now(),
            tags: BTreeMap::new(),
            jobs,
        };
        let mut statuses = BTreeMap::new();
        statuses.insert(
            JobId::from("step1"),
            Some(JobRun {
                lifecycle: Lifecycle::Success,
                updated_at: Utc::now(),
                slurm_jobid: Some(1),
                slurm_status: None,
                note: None,
            }),
        );
        statuses.insert(JobId::from("step2"), None);
        let cf = CollectedFlow { flow, statuses };
        let t = format_tree(&[&cf]);
        assert!(t.contains("01997cdc  (2 jobs"));
        assert!(t.contains("├─ step1  OK"));
        assert!(t.contains("└─ step2  PD"));
        assert!(t.contains("AfterOk step1"));
    }
```

- [ ] **Step 2: Run test to verify it fails / compiles**

Run: `cargo test --lib --all-features listing::tests::tree_renders_forest`
Expected: PASS if compiles (impl included). Fix any compile error from messages (note `JobEdge` path: `gaussian_job_shared::entities::workflow::JobEdge`).

- [ ] **Step 3: (implementation included in Step 1)**

No extra code.

- [ ] **Step 4: Run full module + build**

Run: `cargo test --lib --all-features listing::`
Expected: PASS (Task 1/4/5 tests).
Run: `cargo build --no-default-features`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/listing.rs src/lib.rs
git commit -m "feat(listing): add table/json/tree pure formatters"
```

---

## Task 6: Async `collect` + projections + integration test

**Files:**
- Modify: `src/listing.rs` (remove the `_PathResolverProbe` line; append `collect`/`job_rows`/`flow_rows`/`matched_flows`/`is_default_filter`)
- Modify: `src/lib.rs` (extend re-export)
- Modify: `src/persistence/path.rs` (ensure `PathResolver: Clone`)
- Create: `tests/integration_listing.rs`

- [ ] **Step 1: Write the failing test**

Create `tests/integration_listing.rs`:

```rust
//! Integration: on-disk cross-flow listing (no live SLURM).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
use job_manager::job::lifecycle::Lifecycle;
use job_manager::job::run::JobRun;
use job_manager::listing::{DisplayLifecycle, collect, flow_rows, job_rows};
use job_manager::persistence::flow::write_flow;
use job_manager::persistence::job_run::write_job_run;
use job_manager::persistence::path::PathResolver;
use job_manager::search::SearchFilter;
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use tempfile::TempDir;
use uuid::Uuid;

fn common() -> Arc<CommonConfig> {
    Arc::new(CommonConfig {
        slurm_default: SlurmJobConfig {
            partition: "long".to_string(),
            time_limit: None,
            log_stdout: None,
            log_stderr: None,
            comment: None,
            job_name: None,
            array_spec: None,
            dependency: None,
            mail_user: None,
            mail_types: None,
            resource_spec: None,
        },
        directories: DirectoryConfig { project_root: PathBuf::from("/work") },
    })
}

fn cfg() -> SlurmJobConfig {
    SlurmJobConfig {
        partition: "long".into(),
        time_limit: None,
        log_stdout: None,
        log_stderr: None,
        comment: None,
        job_name: None,
        array_spec: None,
        dependency: None,
        mail_user: None,
        mail_types: None,
        resource_spec: None,
    }
}

fn flow_with_two_jobs(uuid: Uuid) -> JobFlow {
    let mut jobs = BTreeMap::new();
    for name in ["step1", "step2"] {
        jobs.insert(
            JobId::from(name),
            Job {
                spec: JobSpec { program: Program::from("g16"), config: cfg(), body: "".into() },
                parents: vec![],
            },
        );
    }
    JobFlow { uuid, created_at: Utc::now(), tags: BTreeMap::new(), jobs }
}

#[tokio::test]
async fn collect_filters_jobs_by_status_set() {
    let dir = TempDir::new().unwrap();
    let resolver = PathResolver::new(dir.path());
    let u = Uuid::now_v7();
    write_flow(&resolver.flow_toml(&u), &flow_with_two_jobs(u)).unwrap();
    write_job_run(
        &resolver.status_file(&u, &JobId::from("step1")),
        &JobRun {
            lifecycle: Lifecycle::Success,
            updated_at: Utc::now(),
            slurm_jobid: Some(42),
            slurm_status: None,
            note: None,
        },
    )
    .unwrap();

    let collected = collect(dir.path(), common(), &SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(collected.len(), 1);

    let all = job_rows(&collected, &SearchFilter::default(), None);
    assert_eq!(all.len(), 2);

    let mut want = std::collections::BTreeSet::new();
    want.insert(DisplayLifecycle::Real(Lifecycle::Success));
    let only_ok = job_rows(
        &collected,
        &SearchFilter { status: want, ..Default::default() },
        None,
    );
    assert_eq!(only_ok.len(), 1);
    assert_eq!(only_ok[0].job_id, "step1");
    assert_eq!(only_ok[0].slurm_jobid, Some(42));

    let frows = flow_rows(&collected, &SearchFilter::default(), None);
    assert_eq!(frows.len(), 1);
    assert_eq!(frows[0].total, 2);
    assert_eq!(frows[0].done, 1);
}

#[tokio::test]
async fn collect_sorts_newest_first_and_limit_applies() {
    let dir = TempDir::new().unwrap();
    let resolver = PathResolver::new(dir.path());
    for _ in 0..5 {
        let u = Uuid::now_v7();
        write_flow(&resolver.flow_toml(&u), &flow_with_two_jobs(u)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    let collected = collect(dir.path(), common(), &SearchFilter::default())
        .await
        .unwrap();
    let times: Vec<_> = collected.iter().map(|c| c.flow.created_at).collect();
    let mut sorted = times.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(times, sorted);

    let limited = flow_rows(&collected, &SearchFilter::default(), Some(2));
    assert_eq!(limited.len(), 2);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test integration_listing --all-features`
Expected: COMPILE ERROR — `collect`/`job_rows`/`flow_rows` not defined.

- [ ] **Step 3: Write minimal implementation**

In `src/listing.rs`, delete the temporary lines added in Task 4:

```rust
// Keeps the `PathResolver` import live until Task 6 wires `collect`.
#[allow(unused_imports)]
use PathResolver as _PathResolverProbe;
```

Append (before `#[cfg(test)]`):

```rust
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use gaussian_job_shared::config::common::CommonConfig;

use crate::concurrency::parallelism;
use crate::error::JobManagerError;
use crate::search::{SearchFilter, matches};

/// Walk every flow under `root`, read each job's on-disk status, and
/// return flows newest-first (`flow.created_at` desc). Read-only: no
/// SLURM, no `tick`. Missing/unreadable `status.toml` => `None`
/// (Pending). A flow whose `flow.toml` fails to parse is logged to
/// stderr and skipped (listing robustness; `jm doctor` is strict).
/// `filter` is intentionally unused here — filtering happens in the
/// pure projections so a single `collect` serves all three views.
pub async fn collect(
    root: &std::path::Path,
    common: Arc<CommonConfig>,
    _filter: &SearchFilter,
) -> Result<Vec<CollectedFlow>, JobManagerError> {
    let resolver = PathResolver::new(root);
    let flows_stream = crate::walk::walk_flows(root, common);
    let mut flows_stream = std::pin::pin!(flows_stream);
    let mut flows: Vec<JobFlow> = Vec::new();
    while let Some(item) = flows_stream.next().await {
        match item {
            Ok(f) => flows.push(f),
            Err(e) => eprintln!("jm ls: skipping unreadable flow: {e}"),
        }
    }

    let p = parallelism();
    let results: Vec<Result<CollectedFlow, JobManagerError>> = stream::iter(flows)
        .map(|flow| {
            let resolver = resolver.clone();
            async move {
                let uuid = flow.uuid;
                let job_ids: Vec<JobId> = flow.jobs.keys().cloned().collect();
                let mut statuses = std::collections::BTreeMap::new();
                for jid in job_ids {
                    let path = resolver.status_file(&uuid, &jid);
                    let run = if path.exists() {
                        let p2 = path.clone();
                        match tokio::task::spawn_blocking(move || {
                            crate::persistence::read_job_run(&p2)
                        })
                        .await
                        {
                            Ok(Ok(jr)) => Some(jr),
                            Ok(Err(e)) => {
                                eprintln!(
                                    "jm ls: unreadable status {} ({e}); treating as pending",
                                    path.display()
                                );
                                None
                            }
                            Err(join) => {
                                return Err(JobManagerError::JoinFailed {
                                    op: "read_job_run",
                                    source: join,
                                });
                            }
                        }
                    } else {
                        None
                    };
                    statuses.insert(jid, run);
                }
                Ok(CollectedFlow { flow, statuses })
            }
        })
        .buffer_unordered(p)
        .collect::<Vec<_>>()
        .await;

    let mut collected: Vec<CollectedFlow> =
        results.into_iter().collect::<Result<Vec<_>, _>>()?;
    collected.sort_by(|a, b| b.flow.created_at.cmp(&a.flow.created_at));
    Ok(collected)
}

fn is_default_filter(f: &SearchFilter) -> bool {
    f.program.is_none()
        && f.tags.is_empty()
        && f.status.is_empty()
        && f.flow_uuid_prefix.is_none()
        && f.created_after.is_none()
        && f.created_before.is_none()
        && f.slurm_jobid.is_none()
        && f.job_id.is_none()
}

/// Project to job rows: jobs passing `filter`, input order (newest-first)
/// × topological job order. `limit` caps the row count.
pub fn job_rows(
    collected: &[CollectedFlow],
    filter: &SearchFilter,
    limit: Option<usize>,
) -> Vec<JobRow> {
    let mut out = Vec::new();
    for cf in collected {
        let order = crate::flow::topological_order(&cf.flow.jobs, cf.flow.uuid)
            .unwrap_or_else(|_| cf.flow.jobs.keys().cloned().collect());
        for jid in order {
            let job = &cf.flow.jobs[&jid];
            let status = cf.statuses.get(&jid).and_then(|o| o.as_ref());
            if !matches(&cf.flow, &jid, job, status, filter) {
                continue;
            }
            out.push(JobRow {
                flow_uuid: cf.flow.uuid,
                job_id: jid.0.clone(),
                status: cf.job_display(&jid),
                slurm_jobid: status.and_then(|s| s.slurm_jobid),
                program: job.spec.program.0.clone(),
                updated_at: status.map(|s| s.updated_at),
                created_at: cf.flow.created_at,
            });
            if let Some(n) = limit
                && out.len() >= n
            {
                return out;
            }
        }
    }
    out
}

/// Flows where **any** job passes `filter` (same inclusion rule as the
/// tree forest). Job-less flows are included only under the default
/// filter (no predicate can otherwise match zero jobs).
pub fn matched_flows<'a>(
    collected: &'a [CollectedFlow],
    filter: &SearchFilter,
    limit: Option<usize>,
) -> Vec<&'a CollectedFlow> {
    let mut out = Vec::new();
    for cf in collected {
        let include = if cf.flow.jobs.is_empty() {
            is_default_filter(filter)
        } else {
            cf.flow.jobs.iter().any(|(jid, job)| {
                let status = cf.statuses.get(jid).and_then(|o| o.as_ref());
                matches(&cf.flow, jid, job, status, filter)
            })
        };
        if include {
            out.push(cf);
            if let Some(n) = limit
                && out.len() >= n
            {
                return out;
            }
        }
    }
    out
}

/// Project matched flows to flow rows.
pub fn flow_rows(
    collected: &[CollectedFlow],
    filter: &SearchFilter,
    limit: Option<usize>,
) -> Vec<FlowRow> {
    use crate::job::lifecycle::Lifecycle::Success;
    matched_flows(collected, filter, limit)
        .into_iter()
        .map(|cf| {
            let displays: Vec<DisplayLifecycle> =
                cf.flow.jobs.keys().map(|k| cf.job_display(k)).collect();
            let done = displays
                .iter()
                .filter(|d| **d == DisplayLifecycle::Real(Success))
                .count();
            FlowRow {
                flow_uuid: cf.flow.uuid,
                total: cf.flow.jobs.len(),
                done,
                status: aggregate_flow_status(&displays),
                created_at: cf.flow.created_at,
            }
        })
        .collect()
}
```

Extend the `src/lib.rs` listing re-export (replace the Task 5 line):

```rust
pub use listing::{
    CollectedFlow, DisplayLifecycle, FlowRow, FlowStatus, JobRow, aggregate_flow_status,
    collect, flow_rows, format_flows_json, format_flows_table, format_jobs_json,
    format_jobs_table, format_tree, job_rows, matched_flows, parse_status_set,
};
```

Ensure `PathResolver` is `Clone`. Open `src/persistence/path.rs`; if its `struct PathResolver` lacks `#[derive(Clone)]`, add `Clone` to its derive list (it wraps a `PathBuf` — `Clone` is trivial and safe).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test integration_listing --all-features`
Expected: PASS (2 tests).
Run: `cargo test --lib --all-features listing::`
Expected: PASS (all unit tests still green).
Run: `cargo build --no-default-features`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/listing.rs src/lib.rs tests/integration_listing.rs src/persistence/path.rs
git commit -m "feat(listing): add async collect + job/flow projections"
```

---

## Task 7: Wire CLI — remove `jm search`, add `jm ls`

**Files:**
- Modify: `src/bin/jm.rs` (delete `Cmd::Search` + `cmd_search`; add `Cmd::Ls`, `LsView`, `FilterArgs`, `FmtArgs`, `cmd_ls`, `build_filter`)
- Test: `src/bin/jm.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test**

Append to `src/bin/jm.rs` `mod tests`:

```rust
    #[test]
    fn cli_parses_ls_jobs_with_filters() {
        let cli = Cli::try_parse_from([
            "jm", "--root", "/tmp/x", "ls", "jobs", "--status", "running,F",
            "--program", "g16", "--no-header",
        ])
        .expect("parse ls jobs");
        match cli.cmd {
            Cmd::Ls { view: LsView::Jobs { filter, fmt } } => {
                assert_eq!(filter.status.as_deref(), Some("running,F"));
                assert_eq!(filter.program.as_deref(), Some("g16"));
                assert!(fmt.no_header);
                assert!(!fmt.json);
            }
            _ => panic!("expected ls jobs"),
        }
    }

    #[test]
    fn cli_parses_ls_tree_optional_target() {
        let cli = Cli::try_parse_from(["jm", "--root", "/tmp/x", "ls", "tree"])
            .expect("parse ls tree");
        match cli.cmd {
            Cmd::Ls { view: LsView::Tree { target, .. } } => assert!(target.is_none()),
            _ => panic!("expected ls tree"),
        }
    }

    #[test]
    fn build_filter_parses_status_tag_dates() {
        let fa = FilterArgs {
            program: Some("g16".into()),
            tag: vec!["env=prod".into()],
            status: Some("ok,running".into()),
            flow: Some("0199".into()),
            created_after: Some("2026-05-16T00:00:00Z".into()),
            created_before: None,
            slurm_jobid: Some(42),
            job: Some("step1".into()),
            limit: Some(10),
        };
        let f = build_filter(&fa).expect("build_filter ok");
        assert_eq!(f.status.len(), 2);
        assert_eq!(f.tags.get("env").map(String::as_str), Some("prod"));
        assert!(f.created_after.is_some());
        assert_eq!(f.slurm_jobid, Some(42));
    }

    #[test]
    fn build_filter_rejects_bad_status() {
        let fa = FilterArgs {
            program: None, tag: vec![], status: Some("nope".into()),
            flow: None, created_after: None, created_before: None,
            slurm_jobid: None, job: None, limit: None,
        };
        assert!(build_filter(&fa).is_err());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin jm --all-features -- cli_parses_ls`
Expected: COMPILE ERROR — `Cmd::Ls`/`LsView`/`FilterArgs`/`build_filter` undefined.

- [ ] **Step 3: Write minimal implementation**

In `src/bin/jm.rs`:

Add to imports (after `use clap::{Parser, Subcommand};`):

```rust
use clap::Args;
```

Delete this variant from `enum Cmd`:

```rust
    /// Cross-flow search.
    Search {
        #[arg(long)]
        program: Option<String>,
    },
```

Add to `enum Cmd` (after `Doctor`):

```rust
    /// Cross-flow status listing (read-only; no SLURM).
    Ls {
        #[command(subcommand)]
        view: LsView,
    },
```

Add supporting types after `enum Cmd`:

```rust
#[derive(Subcommand)]
enum LsView {
    /// One row per job across all flows.
    Jobs {
        #[command(flatten)]
        filter: FilterArgs,
        #[command(flatten)]
        fmt: FmtArgs,
    },
    /// One row per flow (aggregated status).
    Flows {
        #[command(flatten)]
        filter: FilterArgs,
        #[command(flatten)]
        fmt: FmtArgs,
    },
    /// Flow → job tree. No arg = all flows; FLOW_UUID = that flow only.
    Tree {
        target: Option<String>,
        #[command(flatten)]
        filter: FilterArgs,
    },
}

#[derive(Args, Debug)]
struct FilterArgs {
    #[arg(long)]
    program: Option<String>,
    /// Repeatable KEY=VALUE; all must match.
    #[arg(long = "tag", value_name = "KEY=VALUE")]
    tag: Vec<String>,
    /// Comma-separated: pd,q,r,ok,f,sk or long names (case-insensitive).
    #[arg(long)]
    status: Option<String>,
    /// flow uuid prefix (case-insensitive).
    #[arg(long)]
    flow: Option<String>,
    #[arg(long)]
    created_after: Option<String>,
    #[arg(long)]
    created_before: Option<String>,
    #[arg(long)]
    slurm_jobid: Option<u64>,
    #[arg(long)]
    job: Option<String>,
    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Args, Debug)]
struct FmtArgs {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    no_header: bool,
}

fn build_filter(a: &FilterArgs) -> anyhow::Result<job_manager::SearchFilter> {
    use gaussian_job_shared::entities::workflow::{JobId, Program};

    let mut tags = std::collections::BTreeMap::new();
    for raw in &a.tag {
        let (k, v) = parse_tag(raw)?;
        tags.insert(k, v);
    }
    let status = match &a.status {
        Some(s) => job_manager::parse_status_set(s).map_err(|e| anyhow::anyhow!(e))?,
        None => std::collections::BTreeSet::new(),
    };
    let parse_dt = |s: &str| -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
        Ok(chrono::DateTime::parse_from_rfc3339(s)
            .map_err(|e| anyhow::anyhow!("invalid RFC3339 datetime {s:?}: {e}"))?
            .with_timezone(&chrono::Utc))
    };
    Ok(job_manager::SearchFilter {
        program: a.program.clone().map(Program::from),
        tags,
        status,
        flow_uuid_prefix: a.flow.clone(),
        created_after: a.created_after.as_deref().map(parse_dt).transpose()?,
        created_before: a.created_before.as_deref().map(parse_dt).transpose()?,
        slurm_jobid: a.slurm_jobid,
        job_id: a.job.clone().map(JobId::from),
    })
}
```

In `main()`'s match, delete the `Cmd::Search { ref program } => { ... }` arm and add:

```rust
        Cmd::Ls { ref view } => {
            let root = resolve_root(&cli)?;
            cmd_ls(&root, view).await
        }
```

Delete the entire `async fn cmd_search(...)` function. Add `cmd_ls` where `cmd_search` was:

```rust
async fn cmd_ls(root: &std::path::Path, view: &LsView) -> anyhow::Result<()> {
    use job_manager::persistence::{PathResolver, read_common};
    use std::sync::Arc;

    let resolver = PathResolver::new(root);
    let common_path = resolver.common_toml();
    let common = if common_path.exists() {
        read_common(&common_path)?
    } else {
        job_manager::persistence::synth_empty_common()
    };
    let common = Arc::new(common);

    match view {
        LsView::Jobs { filter, fmt } => {
            let f = build_filter(filter)?;
            let collected = job_manager::listing::collect(root, common, &f).await?;
            let rows = job_manager::listing::job_rows(&collected, &f, filter.limit);
            if fmt.json {
                println!("{}", job_manager::listing::format_jobs_json(&rows)?);
            } else {
                print!(
                    "{}",
                    job_manager::listing::format_jobs_table(&rows, fmt.no_header)
                );
            }
        }
        LsView::Flows { filter, fmt } => {
            let f = build_filter(filter)?;
            let collected = job_manager::listing::collect(root, common, &f).await?;
            let rows = job_manager::listing::flow_rows(&collected, &f, filter.limit);
            if fmt.json {
                println!("{}", job_manager::listing::format_flows_json(&rows)?);
            } else {
                print!(
                    "{}",
                    job_manager::listing::format_flows_table(&rows, fmt.no_header)
                );
            }
        }
        LsView::Tree { target, filter } => {
            let f = build_filter(filter)?;
            let collected = job_manager::listing::collect(root, common, &f).await?;
            let selected: Vec<&job_manager::listing::CollectedFlow> = match target {
                Some(t) => {
                    let uuid = parse_target(root, t)?;
                    collected.iter().filter(|c| c.flow.uuid == uuid).collect()
                }
                None => job_manager::listing::matched_flows(&collected, &f, filter.limit),
            };
            print!("{}", job_manager::listing::format_tree(&selected));
        }
    }
    Ok(())
}
```

Removing `cmd_search` also removes its function-local `use futures::StreamExt;` / `use job_manager::walk::walk_flows;`, so no top-level unused imports are introduced. If clippy still flags an unused import, delete it.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin jm --all-features`
Expected: PASS (new ls tests + existing jm tests).
Run: `cargo build --bin jm --no-default-features`
Expected: PASS (jm still builds without pyo3/libpython).

Optional manual smoke:
```bash
./target/debug/jm --root examples/full ls flows
./target/debug/jm --root examples/full ls jobs --status ok --json
./target/debug/jm --root examples/full ls tree
```
Expected: tables/JSON/tree print without error.

- [ ] **Step 5: Commit**

```bash
git add src/bin/jm.rs
git commit -m "feat(jm)!: replace 'jm search' with 'jm ls jobs|flows|tree'

BREAKING CHANGE: 'jm search' is removed. 'jm ls jobs' supersedes it with
the full SearchFilter surface."
```

---

## Task 8: Docs + full CI gate

**Files:**
- Modify: `README.md`, `docs/API.md`, `docs/architecture.md`, `docs/development.md`, `CLAUDE.md`

- [ ] **Step 1: Update docs**

- `README.md`: replace any `jm ... search --program <P>` example with:
  ```
  jm --root /work ls jobs --program g16 --status running,failed
  jm --root /work ls flows
  jm --root /work ls tree <flow_uuid>
  ```
  Add: "`jm ls` is read-only (no SLURM); run `jm tick` first to reconcile state."
- `docs/API.md`: update `SearchFilter` Rust section → `status: BTreeSet<DisplayLifecycle>` (was `Option<Lifecycle>`); Python section → `status: list[str]`. Add a "Listing" subsection documenting `listing::{collect, job_rows, flow_rows, matched_flows, format_jobs_table, format_flows_table, format_jobs_json, format_flows_json, format_tree, aggregate_flow_status}` and the `DisplayLifecycle` code/long table: PD/pending, Q/queued, R/running, OK/success, F/failed, SK/skipped.
- `docs/architecture.md`: add to the cheatsheet a read-only path: `jm ls → walk_flows + read_job_run (no SLURM) → search::matches → listing::format_*`. Note `jm search` was removed (superseded by `jm ls jobs`).
- `docs/development.md`: replace the `jm ... search --program g16` line in the jm CLI block with `jm ... ls jobs --program g16`; add `ls flows` and `ls tree` to the subcommand list.
- `CLAUDE.md`: in "Common commands", change the subcommand list `{render|submit [--dry-run]|tick|show|doctor}` to `{render|submit [--dry-run]|tick|show|doctor|ls {jobs|flows|tree}}`, and replace `./target/debug/jm --root /work search --program g16` with `./target/debug/jm --root /work ls jobs --program g16 --status running,failed`.

- [ ] **Step 2: Run the full CI gate**

Run:
```bash
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo test --all-features \
  && uv run pytest python/tests -v
```
Expected: ALL PASS. If `cargo fmt --check` fails, run `cargo fmt` and re-run. Fix any clippy unused-import warnings from the `cmd_search` removal.

- [ ] **Step 3: Verify `.pyi` has no drift**

Run:
```bash
cargo run --bin stub_gen && uv run ruff format python/
git status --porcelain python/job_manager/_job_manager_core/
```
Expected: no output. If drifted, `git add python/job_manager/_job_manager_core/*.pyi` and include in the commit.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/API.md docs/architecture.md docs/development.md CLAUDE.md python/job_manager/_job_manager_core/
git commit -m "docs: replace jm search with jm ls; document listing API"
```

- [ ] **Step 5: Final verification**

Run:
```bash
cargo build --bin jm --no-default-features
cargo llvm-cov --all-features --fail-under-lines 80
```
Expected: jm builds without libpython; coverage ≥ 80%. If below, add unit tests for uncovered branches (e.g. `matched_flows` job-less-flow path; `format_flows_table` empty-with-header; `is_default_filter` false case).

---

## Self-Review

**1. Spec coverage**

| Spec § | Covered by |
|---|---|
| §2 G1 (3 subcommands) | Task 7 (`Cmd::Ls`/`LsView`) |
| §2 G2 (shared filters) | Task 7 (`FilterArgs`/`build_filter`) |
| §2 G3 (`--status` multi → set) | Task 1 (`parse_status_set`), Task 2 (field), Task 3 (Python) |
| §2 G4 (table/json/no-header; tree fixed) | Task 5 (formatters), Task 7 (`FmtArgs`; `Tree` has none) |
| §2 G5 (pure testable core, thin CLI) | Tasks 1/4/5/6 + Task 7 thin shell |
| §2 G6 (remove search, keep show) | Task 7 (delete `Cmd::Search`/`cmd_search`; `show` untouched) |
| §2 G7 (read-only, no SLURM) | Task 6 (`collect` walks + reads only) |
| §4 short codes | Task 1 (`code`/`long`/`parse_token`) |
| §5.1 job columns | Task 4 (`JobRow`), Task 5 (`format_jobs_table`) |
| §5.2 flow columns + aggregation | Task 4 (`aggregate_flow_status` + matrix) |
| §5.3 output modes | Task 5 (`format_*_table`/`_json`, `no_header`) |
| §5.4 newest-first + limit | Task 6 (`collect` sort; `*_rows` limit) + tests |
| §6 tree forest/single + edges + topo | Task 5 (`format_tree`), Task 7 (`target` select) |
| §7 type change + matches() + Python | Tasks 2 & 3 |
| §8 module layout + dataflow | Tasks 1/4/5/6/7 file map |
| §9 tests | each Task's tests + `tests/integration_listing.rs` |
| §10 docs/.pyi | Task 8 |
| §11 codes/json names/glyph | Locked: PD/Q/R/OK/F/SK; JSON field names in `JobRowJson`/`FlowRowJson`; Unicode `├─/└─` |

No gaps.

**2. Placeholder scan:** No "TBD/TODO/handle edge cases/similar to Task N". Every code step has full code; every run step has an exact command + expected outcome.

**3. Type consistency:** `DisplayLifecycle` (Task 1) used identically in Tasks 2/4/5/6/7. `SearchFilter.status: BTreeSet<DisplayLifecycle>` (Task 2) matches `parse_status_set` return (Task 1), `build_filter` (Task 7), tests (Task 6). `CollectedFlow`/`JobRow`/`FlowRow`/`FlowStatus` defined Task 4, consumed unchanged Tasks 5/6/7. `collect`/`job_rows`/`flow_rows`/`matched_flows` signatures in Task 6 match Task 7 call sites. `FilterArgs`/`FmtArgs`/`build_filter`/`LsView`/`Cmd::Ls` defined and used consistently in Task 7. `format_tree(&[&CollectedFlow])` consistent between Task 5 def and Task 7 call. `src/lib.rs` re-export grows monotonically (Task 1 → 5 → 6) with no renames.

Plan is internally consistent.
