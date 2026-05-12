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
}
