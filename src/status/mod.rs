//! Per-Job runtime status (queued/running/done/failed) and its TOML form.
//!
//! Status is **not** stored inside `JobFlow` to keep the D2 schema
//! unchanged. Each Job's status lives in
//! `<root>/<flow_uuid>/<JobId>/.status.toml` (dot-prefixed so it does not
//! collide with SLURM outputs or user-authored job input files). Resolve
//! that path via `PathResolver::status_file`.
//!
//! `StatusEntry.lifecycle` is the user-visible aggregated state (the
//! 4-state model). `StatusEntry.slurm_status` keeps the raw A1 (state,
//! reason) pair so callers can render scheduler-side details (e.g.
//! `OUT_OF_MEMORY/OutOfMemory`) when explaining a failure.

pub mod io;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use slurm_async_runner::JobStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PerJobStatus {
    Queued,
    Running,
    Done,
    Failed,
}

impl PerJobStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, PerJobStatus::Done | PerJobStatus::Failed)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusEntry {
    /// User-facing aggregated lifecycle.
    pub lifecycle: PerJobStatus,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slurm_jobid: Option<u64>,
    /// Raw SLURM `(state, reason)` pair last seen for this jobid. None
    /// until the first successful `tick`. A1 `JobStatus` already implements
    /// Serialize/Deserialize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slurm_status: Option<JobStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use slurm_async_runner::{JobReason, JobState};

    #[test]
    fn lifecycle_lowercase_serialization() {
        let s = toml::to_string(&StatusEntry {
            lifecycle: PerJobStatus::Queued,
            updated_at: chrono::Utc::now(),
            slurm_jobid: Some(42),
            slurm_status: None,
            note: None,
        })
        .unwrap();
        assert!(s.contains(r#"lifecycle = "queued""#), "actual: {s}");
        assert!(s.contains("slurm_jobid = 42"));
        assert!(!s.contains("note"), "None should be skipped: {s}");
        assert!(!s.contains("slurm_status"), "None should be skipped: {s}");
    }

    #[test]
    fn slurm_status_roundtrips_through_toml() {
        let e = StatusEntry {
            lifecycle: PerJobStatus::Failed,
            updated_at: chrono::Utc::now(),
            slurm_jobid: Some(99),
            slurm_status: Some(JobStatus::with_reason(
                JobState::OutOfMemory,
                JobReason::OutOfMemory,
            )),
            note: Some("synced: failed-terminal".into()),
        };
        let s = toml::to_string(&e).unwrap();
        let back: StatusEntry = toml::from_str(&s).unwrap();
        assert_eq!(back.lifecycle, e.lifecycle);
        assert_eq!(
            back.slurm_status.as_ref().unwrap().state,
            JobState::OutOfMemory
        );
        assert_eq!(
            back.slurm_status.as_ref().unwrap().reason,
            JobReason::OutOfMemory
        );
    }

    #[test]
    fn status_is_terminal_only_for_done_and_failed() {
        assert!(PerJobStatus::Done.is_terminal());
        assert!(PerJobStatus::Failed.is_terminal());
        assert!(!PerJobStatus::Queued.is_terminal());
        assert!(!PerJobStatus::Running.is_terminal());
    }
}
