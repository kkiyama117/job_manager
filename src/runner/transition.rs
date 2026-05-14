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
}
