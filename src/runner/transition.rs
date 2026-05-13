//! State transition decisions for a single JobRun based on
//! current lifecycle, latest SLURM query, and parent lifecycles.

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::JobId;
use slurm_async_runner::JobStatus;

use crate::job::lifecycle::Lifecycle;

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
    parent_lifecycles: &[Lifecycle],
) -> Decision {
    if current.is_terminal() {
        return Decision::NoChange;
    }
    if matches!(current, Lifecycle::Queued)
        && parent_lifecycles
            .iter()
            .any(|p| matches!(p, Lifecycle::Failed | Lifecycle::Skipped))
    {
        return Decision::SkipDueToParent {
            parent: JobId("<unknown>".to_string()),
        };
    }
    match query {
        None => Decision::NoChange,
        Some(status) => {
            use slurm_async_runner::JobState;
            let next = match status.state {
                JobState::Pending => Lifecycle::Queued,
                JobState::Running => Lifecycle::Running,
                JobState::Completed => Lifecycle::Success,
                JobState::Failed
                | JobState::Timeout
                | JobState::OutOfMemory
                | JobState::NodeFail
                | JobState::Cancelled => Lifecycle::Failed,
                _ => current,
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

    #[test]
    fn skip_when_parent_failed_and_current_queued() {
        let decision = decide_transition(Lifecycle::Queued, None, &[Lifecycle::Failed]);
        assert!(matches!(decision, Decision::SkipDueToParent { .. }));
    }

    #[test]
    fn skip_when_parent_skipped_and_current_queued() {
        let decision = decide_transition(Lifecycle::Queued, None, &[Lifecycle::Skipped]);
        assert!(matches!(decision, Decision::SkipDueToParent { .. }));
    }

    #[test]
    fn no_change_when_all_parents_success_and_no_query() {
        let decision = decide_transition(Lifecycle::Queued, None, &[Lifecycle::Success]);
        assert!(matches!(decision, Decision::NoChange));
    }

    #[test]
    fn terminal_returns_no_change() {
        let decision = decide_transition(Lifecycle::Success, None, &[]);
        assert!(matches!(decision, Decision::NoChange));
    }
}
