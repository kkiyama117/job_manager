//! SLURM ↔ local status reconciliation.
//!
//! Step 1 of two: a pure decision function. Step 2 (orchestrator) lands
//! in the next task.

use slurm_async_runner::{JobState, JobStatus};

use crate::status::PerJobStatus;

/// Extension trait that exposes the `is_terminal() && !Completed`
/// derived predicate. A1 already publishes `is_terminal()` and
/// `is_running()`, but not this one — it is specific to this crate's
/// "Completed != Done" distinction.
pub trait JobStateExt {
    fn is_failed_terminal(&self) -> bool;
}

impl JobStateExt for JobState {
    fn is_failed_terminal(&self) -> bool {
        self.is_terminal() && !matches!(self, JobState::Completed)
    }
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub new: Option<PerJobStatus>,
    pub note: String,
}

/// Decide the next local lifecycle given the previous one and the
/// freshly-queried SLURM (state, reason). Returns `Decision { new, note }`
/// where `new == prev` means no-op (no write needed).
///
/// Invariants (spec §5):
/// 1. Never writes `Done` (post.bash is the sole authority).
/// 2. Never overwrites terminal local lifecycle.
/// 3. SLURM `is_failed_terminal()` → local `Failed` when not already terminal.
/// 4. SLURM `Unknown` + non-terminal local → no-op + warning (orphan).
/// 5. SLURM `Completed` + non-terminal local → no-op + warning.
pub fn decide_transition(prev: Option<PerJobStatus>, slurm: Option<JobStatus>) -> Decision {
    use PerJobStatus::*;

    let Some(status) = slurm else {
        return Decision {
            new: prev,
            note: "no slurm_jobid".to_string(),
        };
    };
    let state = status.state;
    let reason = status.reason.as_str();

    // ---- terminal failure ----
    if state.is_failed_terminal() {
        return match prev {
            Some(Done) => Decision {
                new: Some(Done),
                note: format!("warning: SLURM {state} but local done"),
            },
            Some(Failed) => Decision {
                new: Some(Failed),
                note: format!("unchanged: failed ({state}/{reason})"),
            },
            _ => Decision {
                new: Some(Failed),
                note: format!("synced: failed-terminal {state}/{reason}"),
            },
        };
    }

    // ---- terminal success ----
    if matches!(state, JobState::Completed) {
        return match prev {
            Some(Done) => Decision {
                new: Some(Done),
                note: "unchanged: done".to_string(),
            },
            Some(Failed) => Decision {
                new: Some(Failed),
                note: "warning: SLURM completed but local failed".to_string(),
            },
            _ => Decision {
                new: prev,
                note: "warning: SLURM completed but post.bash has not written done".to_string(),
            },
        };
    }

    // ---- alive / progressing on the cluster ----
    let is_alive = matches!(
        state,
        JobState::Running
            | JobState::Completing
            | JobState::Resizing
            | JobState::Signaling
            | JobState::StageOut
            | JobState::Suspended
    );
    if is_alive {
        return match prev {
            Some(Done) | Some(Failed) => Decision {
                new: prev,
                note: format!("warning: SLURM {state} but local terminal"),
            },
            Some(Running) => Decision {
                new: prev,
                note: format!("unchanged: running ({state})"),
            },
            _ => Decision {
                new: Some(Running),
                note: format!("promoted to running ({state})"),
            },
        };
    }

    // ---- queued / configuring / requeued / hold (not yet executed) ----
    let is_pending = matches!(
        state,
        JobState::Pending
            | JobState::Configuring
            | JobState::Requeued
            | JobState::RequeueFed
            | JobState::RequeueHold
            | JobState::ResvDelHold
            | JobState::Stopped
    );
    if is_pending {
        return match prev {
            Some(Running) => Decision {
                new: Some(Queued),
                note: format!("warning: regressed running->queued ({state})"),
            },
            _ => Decision {
                new: Some(Queued),
                note: format!("synced: pending ({state}/{reason})"),
            },
        };
    }

    // ---- Unknown (jobid expired / forward-compat) ----
    match prev {
        Some(Done) | Some(Failed) => Decision {
            new: prev,
            note: "unchanged: jobid expired".to_string(),
        },
        _ => Decision {
            new: prev,
            note: "orphan: jobid not found; manual investigation needed".to_string(),
        },
    }
}

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::Utc;
use futures::stream::{self, StreamExt};
use gaussian_job_shared::entities::workflow::JobId;
use uuid::Uuid;

use crate::concurrency::parallelism;
use crate::path::PathResolver;
use crate::slurm_facade::SlurmFacade;
use crate::status::{
    StatusEntry,
    io::{read_status, write_status},
};

#[derive(Debug, Clone)]
pub struct TickResult {
    pub flow_uuid: Uuid,
    pub job_id: JobId,
    pub previous: Option<PerJobStatus>,
    pub new: Option<PerJobStatus>,
    /// Raw SLURM `(state, reason)` last seen for this jobid. None when the
    /// batch query did not include it (jobid expired) or when the SLURM
    /// query itself failed.
    pub slurm_status: Option<JobStatus>,
    pub queried_jobid: Option<u64>,
    pub note: String,
}

/// Per-target blocking work: read local status, decide, write if changed.
/// Runs inside `spawn_blocking` so the async executor stays free.
///
/// Persistence rule: write whenever the lifecycle changes OR the raw
/// SLURM `(state, reason)` differs from what was previously persisted.
/// The second clause keeps `slurm_status` fresh on no-op lifecycle
/// transitions (e.g., `Failed → Failed` with a new failure reason).
fn process_one(
    flow_uuid: Uuid,
    job_id: JobId,
    slurm_jobid: u64,
    slurm_status: Option<JobStatus>,
    status_path: PathBuf,
) -> TickResult {
    let prev_entry = read_status(&status_path).ok();
    let prev = prev_entry.as_ref().map(|e| e.lifecycle);
    let prev_slurm = prev_entry.as_ref().and_then(|e| e.slurm_status.as_ref());

    let decision = decide_transition(prev, slurm_status.clone());
    let mut note = decision.note.clone();

    let target_lifecycle = decision.new.or(prev);
    let lifecycle_changed = decision.new.is_some() && decision.new != prev;
    let slurm_status_changed = slurm_status.as_ref() != prev_slurm;
    let needs_write = target_lifecycle.is_some() && (lifecycle_changed || slurm_status_changed);

    if needs_write && let Some(lifecycle) = target_lifecycle {
        let entry = StatusEntry {
            lifecycle,
            updated_at: Utc::now(),
            slurm_jobid: Some(slurm_jobid),
            slurm_status: slurm_status.clone(),
            note: Some(decision.note.clone()),
        };
        if let Err(e) = write_status(&status_path, &entry) {
            note = format!("status write failed: {e}");
            return TickResult {
                flow_uuid,
                job_id,
                previous: prev,
                new: prev, // rolled back
                slurm_status,
                queried_jobid: Some(slurm_jobid),
                note,
            };
        }
    }

    TickResult {
        flow_uuid,
        job_id,
        previous: prev,
        new: decision.new,
        slurm_status,
        queried_jobid: Some(slurm_jobid),
        note,
    }
}

/// For each target `(flow_uuid, job_id, slurm_jobid)`:
/// 1. Batch-query SLURM via `slurm`.
/// 2. Read local status (None if missing).
/// 3. Decide transition over the full `JobStatus`.
/// 4. Write back if changed.
///
/// Per-target read/decide/write runs on `spawn_blocking`, dispatched
/// concurrently via `buffer_unordered` (mirrors `walk::walk_flows`).
/// Output order is non-deterministic; callers that need input-order
/// pairing should key by `(flow_uuid, job_id)`.
pub async fn tick_many(
    targets: &[(Uuid, JobId, u64)],
    slurm: &dyn SlurmFacade,
    resolver: &PathResolver,
) -> Vec<TickResult> {
    let jobids: Vec<u64> = targets.iter().map(|(_, _, j)| *j).collect();
    let states: HashMap<u64, JobStatus> = match slurm.query_states_batch(&jobids).await {
        Ok(m) => m,
        Err(e) => {
            return targets
                .iter()
                .map(|(uuid, jid, slurm_jobid)| TickResult {
                    flow_uuid: *uuid,
                    job_id: jid.clone(),
                    previous: None,
                    new: None,
                    slurm_status: None,
                    queried_jobid: Some(*slurm_jobid),
                    note: format!("slurm batch query failed: {e}"),
                })
                .collect();
        }
    };

    // Collect owned tuples so each task has 'static lifetime independent
    // of the input slice.
    let owned: Vec<(Uuid, JobId, u64, Option<JobStatus>, PathBuf)> = targets
        .iter()
        .map(|(flow_uuid, job_id, slurm_jobid)| {
            let status_path = resolver.status_file(flow_uuid, job_id);
            let slurm_status = states.get(slurm_jobid).cloned();
            (
                *flow_uuid,
                job_id.clone(),
                *slurm_jobid,
                slurm_status,
                status_path,
            )
        })
        .collect();

    let tasks = owned.into_iter().map(
        |(flow_uuid, job_id, slurm_jobid, slurm_status, status_path)| {
            let job_id_fallback = job_id.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    process_one(flow_uuid, job_id, slurm_jobid, slurm_status, status_path)
                })
                .await
                .unwrap_or_else(|e| TickResult {
                    flow_uuid,
                    job_id: job_id_fallback,
                    previous: None,
                    new: None,
                    slurm_status: None,
                    queried_jobid: Some(slurm_jobid),
                    note: format!("spawn_blocking join error: {e}"),
                })
            }
        },
    );

    stream::iter(tasks)
        .buffer_unordered(parallelism())
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use slurm_async_runner::{JobReason, JobStatus};

    fn js(state: JobState) -> Option<JobStatus> {
        Some(JobStatus::new(state))
    }

    #[rstest]
    // (prev, slurm, expected_new, note_substring)
    #[case(None, None, None, "no slurm_jobid")]
    #[case(None, js(JobState::Pending), Some(PerJobStatus::Queued), "synced")]
    #[case(
        Some(PerJobStatus::Running),
        js(JobState::Pending),
        Some(PerJobStatus::Queued),
        "regressed"
    )]
    #[case(
        Some(PerJobStatus::Queued),
        js(JobState::Running),
        Some(PerJobStatus::Running),
        "promoted"
    )]
    #[case(
        Some(PerJobStatus::Done),
        js(JobState::Failed),
        Some(PerJobStatus::Done),
        "warning"
    )]
    #[case(
        Some(PerJobStatus::Running),
        js(JobState::Failed),
        Some(PerJobStatus::Failed),
        "synced"
    )]
    #[case(
        Some(PerJobStatus::Running),
        js(JobState::Completed),
        Some(PerJobStatus::Running),
        "post.bash"
    )]
    #[case(
        Some(PerJobStatus::Done),
        js(JobState::Completed),
        Some(PerJobStatus::Done),
        "unchanged"
    )]
    #[case(
        Some(PerJobStatus::Queued),
        js(JobState::Unknown),
        Some(PerJobStatus::Queued),
        "orphan"
    )]
    fn transition_matrix(
        #[case] prev: Option<PerJobStatus>,
        #[case] slurm: Option<JobStatus>,
        #[case] expected: Option<PerJobStatus>,
        #[case] note_substr: &str,
    ) {
        let d = decide_transition(prev, slurm);
        assert_eq!(d.new, expected, "note was: {}", d.note);
        assert!(
            d.note.contains(note_substr),
            "note `{}` missing `{note_substr}`",
            d.note
        );
    }

    #[test]
    fn is_failed_terminal_separates_completed_from_others() {
        assert!(!JobState::Completed.is_failed_terminal());
        assert!(JobState::Failed.is_failed_terminal());
        assert!(JobState::OutOfMemory.is_failed_terminal());
        assert!(!JobState::Running.is_failed_terminal());
        assert!(!JobState::Pending.is_failed_terminal());
    }

    #[test]
    fn failure_reason_is_surfaced_in_note() {
        let status = JobStatus::with_reason(JobState::OutOfMemory, JobReason::OutOfMemory);
        let d = decide_transition(Some(PerJobStatus::Running), Some(status));
        assert_eq!(d.new, Some(PerJobStatus::Failed));
        assert!(d.note.contains("OUT_OF_MEMORY"), "note: {}", d.note);
        assert!(d.note.contains("OutOfMemory"), "note: {}", d.note);
    }

    use crate::slurm_facade::InMemorySlurmFacade;
    use crate::status::io::{read_status, write_status};
    use tempfile::TempDir;

    #[tokio::test]
    async fn tick_many_writes_running_for_pending_to_running() {
        let dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(dir.path());
        let flow_uuid = Uuid::now_v7();
        let job_id = JobId::from("g16");

        // Seed local lifecycle = Queued
        let initial = StatusEntry {
            lifecycle: PerJobStatus::Queued,
            updated_at: Utc::now(),
            slurm_jobid: Some(99),
            slurm_status: None,
            note: None,
        };
        write_status(&resolver.status_file(&flow_uuid, &job_id), &initial).unwrap();

        let mut m = HashMap::new();
        m.insert(99u64, JobStatus::new(JobState::Running));
        let slurm = InMemorySlurmFacade::new(m);

        let results = tick_many(&[(flow_uuid, job_id.clone(), 99)], &slurm, &resolver).await;
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.previous, Some(PerJobStatus::Queued));
        assert_eq!(r.new, Some(PerJobStatus::Running));
        assert!(r.slurm_status.is_some());

        let back = read_status(&resolver.status_file(&flow_uuid, &job_id)).unwrap();
        assert_eq!(back.lifecycle, PerJobStatus::Running);
        // Raw SLURM status is now persisted, not just lifecycle.
        assert_eq!(back.slurm_status.as_ref().unwrap().state, JobState::Running);
    }

    #[tokio::test]
    async fn tick_many_refreshes_slurm_status_on_failed_to_failed() {
        let dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(dir.path());
        let flow_uuid = Uuid::now_v7();
        let job_id = JobId::from("g16");

        // Seed: already Failed with OOM reason
        let initial = StatusEntry {
            lifecycle: PerJobStatus::Failed,
            updated_at: Utc::now(),
            slurm_jobid: Some(55),
            slurm_status: Some(JobStatus::with_reason(
                JobState::OutOfMemory,
                JobReason::OutOfMemory,
            )),
            note: Some("synced: failed-terminal".into()),
        };
        write_status(&resolver.status_file(&flow_uuid, &job_id), &initial).unwrap();

        // SLURM now reports a different failed-terminal: Timeout.
        let mut m = HashMap::new();
        m.insert(
            55u64,
            JobStatus::with_reason(JobState::Timeout, JobReason::TimeLimit),
        );
        let slurm = InMemorySlurmFacade::new(m);

        let results = tick_many(&[(flow_uuid, job_id.clone(), 55)], &slurm, &resolver).await;
        assert_eq!(results[0].previous, Some(PerJobStatus::Failed));
        assert_eq!(results[0].new, Some(PerJobStatus::Failed));

        // File is refreshed even though lifecycle did not change.
        let back = read_status(&resolver.status_file(&flow_uuid, &job_id)).unwrap();
        assert_eq!(back.lifecycle, PerJobStatus::Failed);
        let s = back.slurm_status.as_ref().unwrap();
        assert_eq!(s.state, JobState::Timeout);
        assert_eq!(s.reason, JobReason::TimeLimit);
    }

    #[tokio::test]
    async fn tick_many_no_op_when_slurm_unknown_and_local_running() {
        let dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(dir.path());
        let flow_uuid = Uuid::now_v7();
        let job_id = JobId::from("g16");
        let initial = StatusEntry {
            lifecycle: PerJobStatus::Running,
            updated_at: Utc::now(),
            slurm_jobid: Some(77),
            slurm_status: None,
            note: None,
        };
        write_status(&resolver.status_file(&flow_uuid, &job_id), &initial).unwrap();

        let slurm = InMemorySlurmFacade::new(HashMap::new()); // no entry for 77
        let results = tick_many(&[(flow_uuid, job_id.clone(), 77)], &slurm, &resolver).await;
        assert_eq!(results[0].previous, Some(PerJobStatus::Running));
        assert_eq!(results[0].new, Some(PerJobStatus::Running));
        assert!(results[0].slurm_status.is_none());
    }
}
