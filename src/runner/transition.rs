//! State transition decisions for a single JobRun based on
//! current lifecycle, latest SLURM query, and parent lifecycles.

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::JobId;
use slurm_async_runner::JobStatus;

use crate::job::lifecycle::Lifecycle;

#[derive(Debug)]
pub enum Decision {
    NoChange,
    Transition {
        from: Lifecycle,
        to: Lifecycle,
        slurm_status: Option<JobStatus>,
    },
    SkipDueToParent {
        parent: JobId,
    },
}

pub struct TickResult {
    pub transitions: BTreeMap<JobId, Decision>,
}

pub fn decide_transition(
    current: Lifecycle,
    query: Option<&JobStatus>,
    parents: &[(JobId, Lifecycle)],
) -> Decision {
    if current.is_terminal() {
        return Decision::NoChange;
    }
    if matches!(current, Lifecycle::Queued)
        && let Some((culprit, _)) = parents
            .iter()
            .find(|(_, l)| matches!(l, Lifecycle::Failed | Lifecycle::Skipped))
    {
        return Decision::SkipDueToParent {
            parent: culprit.clone(),
        };
    }
    match query {
        None => Decision::NoChange,
        Some(status) => {
            use slurm_async_runner::JobState;
            // Exhaustive over every variant A1 currently exposes. If A1 adds
            // a new state, clippy + rustc will flag this match as
            // non-exhaustive and the maintainer must consciously bucket the
            // new state — much safer than the previous `_ => current`
            // wildcard which silently treated unknown states as NoChange.
            let next = match status.state {
                // Queued / not progressing: still waiting for CPU.
                JobState::Pending
                | JobState::Configuring
                | JobState::Requeued
                | JobState::RequeueFed
                | JobState::RequeueHold
                | JobState::ResvDelHold
                | JobState::Suspended
                | JobState::Stopped => Lifecycle::Queued,

                // Alive / progressing: job is on the node, even if SLURM
                // is currently cleaning up or signaling.
                JobState::Running
                | JobState::Completing
                | JobState::Resizing
                | JobState::Signaling
                | JobState::StageOut => Lifecycle::Running,

                // Terminal success: only `Completed` means exit 0.
                JobState::Completed => Lifecycle::Success,

                // Terminal failure: every flavor SLURM offers, including
                // BootFail/Preempted/Revoked/SpecialExit/Deadline that the
                // old wildcard silently dropped.
                JobState::BootFail
                | JobState::Cancelled
                | JobState::Deadline
                | JobState::Failed
                | JobState::NodeFail
                | JobState::OutOfMemory
                | JobState::Preempted
                | JobState::Revoked
                | JobState::SpecialExit
                | JobState::Timeout => Lifecycle::Failed,

                // Sentinel: keep current — parse failure should not flip
                // a job's lifecycle on its own. Caller can inspect
                // `slurm_status` to surface the raw string.
                JobState::Unknown => current,
            };
            if next == current {
                Decision::NoChange
            } else {
                Decision::Transition {
                    from: current,
                    to: next,
                    slurm_status: Some(status.clone()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent(id: &str, l: Lifecycle) -> (JobId, Lifecycle) {
        (JobId(id.to_string()), l)
    }

    #[test]
    fn skip_when_parent_failed_and_current_queued() {
        let parents = vec![parent("p1", Lifecycle::Failed)];
        let decision = decide_transition(Lifecycle::Queued, None, &parents);
        match decision {
            Decision::SkipDueToParent { parent } => assert_eq!(parent.0, "p1"),
            other => panic!("expected SkipDueToParent, got {other:?}"),
        }
    }

    #[test]
    fn skip_when_parent_skipped_and_current_queued() {
        let parents = vec![parent("p1", Lifecycle::Skipped)];
        let decision = decide_transition(Lifecycle::Queued, None, &parents);
        match decision {
            Decision::SkipDueToParent { parent } => assert_eq!(parent.0, "p1"),
            other => panic!("expected SkipDueToParent, got {other:?}"),
        }
    }

    #[test]
    fn skip_records_first_failed_parent_when_multiple() {
        let parents = vec![
            parent("ok", Lifecycle::Success),
            parent("bad", Lifecycle::Failed),
            parent("also_bad", Lifecycle::Failed),
        ];
        let decision = decide_transition(Lifecycle::Queued, None, &parents);
        match decision {
            Decision::SkipDueToParent { parent } => assert_eq!(parent.0, "bad"),
            other => panic!("expected SkipDueToParent, got {other:?}"),
        }
    }

    #[test]
    fn no_change_when_all_parents_success_and_no_query() {
        let parents = vec![parent("p1", Lifecycle::Success)];
        let decision = decide_transition(Lifecycle::Queued, None, &parents);
        assert!(matches!(decision, Decision::NoChange));
    }

    #[test]
    fn terminal_returns_no_change() {
        let decision = decide_transition(Lifecycle::Success, None, &[]);
        assert!(matches!(decision, Decision::NoChange));
    }

    #[test]
    fn boot_fail_transitions_to_failed() {
        // Previously the wildcard arm dropped BootFail to NoChange. With
        // the exhaustive match, every terminal-failure variant maps to
        // Lifecycle::Failed.
        use slurm_async_runner::{JobState, JobStatus};
        let status = JobStatus::new(JobState::BootFail);
        let decision = decide_transition(Lifecycle::Queued, Some(&status), &[]);
        match decision {
            Decision::Transition { to, .. } => assert_eq!(to, Lifecycle::Failed),
            other => panic!("expected Transition to Failed, got {other:?}"),
        }
    }

    #[test]
    fn completing_transitions_queued_to_running() {
        // Same fix: SLURM's transient `Completing` (process exit cleanup)
        // means the job did run, so we should reflect Running rather than
        // silently keeping it Queued.
        use slurm_async_runner::{JobState, JobStatus};
        let status = JobStatus::new(JobState::Completing);
        let decision = decide_transition(Lifecycle::Queued, Some(&status), &[]);
        match decision {
            Decision::Transition { to, .. } => assert_eq!(to, Lifecycle::Running),
            other => panic!("expected Transition to Running, got {other:?}"),
        }
    }

    #[test]
    fn unknown_keeps_current_lifecycle() {
        // `Unknown` is the sentinel for parse failure; we must not flip a
        // job's lifecycle based on a state we couldn't decode.
        use slurm_async_runner::{JobState, JobStatus};
        let status = JobStatus::new(JobState::Unknown);
        let decision = decide_transition(Lifecycle::Running, Some(&status), &[]);
        assert!(matches!(decision, Decision::NoChange));
    }
}
