//! `jm ls` — pure projection/aggregation/formatting for cross-flow listing.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use gaussian_job_shared::entities::workflow::{JobFlow, JobId};
use serde::Serialize;
use uuid::Uuid;

use crate::job::lifecycle::Lifecycle;
use crate::job::run::JobRun;
use crate::persistence::path::PathResolver;

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

/// One flow + its on-disk per-job status (`None` == Pending: no readable
/// `status.toml`). Produced by `collect` (added in a later task).
#[derive(Debug)]
pub struct CollectedFlow {
    pub flow: JobFlow,
    pub statuses: BTreeMap<JobId, Option<JobRun>>,
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
    /// UPPERCASE label for the flow STATUS column and `--json` output.
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
    if jobs.contains(&DisplayLifecycle::Real(Failed)) {
        return FlowStatus::Failed;
    }
    if jobs.contains(&DisplayLifecycle::Real(Running)) {
        return FlowStatus::Running;
    }
    if jobs.contains(&DisplayLifecycle::Real(Queued)) {
        return FlowStatus::Queued;
    }
    if jobs.iter().all(|d| *d == DisplayLifecycle::Real(Success)) {
        return FlowStatus::Done;
    }
    let any_skipped = jobs.contains(&DisplayLifecycle::Real(Skipped));
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("PD", DisplayLifecycle::Pending)]
    #[case("pending", DisplayLifecycle::Pending)]
    #[case("Q", DisplayLifecycle::Real(Lifecycle::Queued))]
    #[case("F", DisplayLifecycle::Real(Lifecycle::Failed))]
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
}
