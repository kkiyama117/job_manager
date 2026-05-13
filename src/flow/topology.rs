//! Kahn's algorithm: topological sort with cycle detection.

use std::collections::{BTreeMap, VecDeque};

use gaussian_job_shared::entities::workflow::{Job, JobId};

use crate::error::JobManagerError;

pub fn topological_order(
    jobs: &BTreeMap<JobId, Job>,
    flow_uuid: uuid::Uuid,
) -> Result<Vec<JobId>, JobManagerError> {
    let mut indeg: BTreeMap<JobId, usize> = jobs
        .iter()
        .map(|(jid, job)| (jid.clone(), job.parents.len()))
        .collect();

    let mut queue: VecDeque<JobId> = indeg
        .iter()
        .filter_map(|(k, v)| if *v == 0 { Some(k.clone()) } else { None })
        .collect();

    let mut order = Vec::with_capacity(jobs.len());

    while let Some(jid) = queue.pop_front() {
        order.push(jid.clone());
        for (other_jid, other_job) in jobs {
            if other_job.parents.iter().any(|e| e.from == jid)
                && let Some(c) = indeg.get_mut(other_jid)
                && *c > 0
            {
                *c -= 1;
                if *c == 0 {
                    queue.push_back(other_jid.clone());
                }
            }
        }
    }

    if order.len() != jobs.len() {
        return Err(JobManagerError::DependencyCycle { flow: flow_uuid });
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::{JobEdge, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

    fn empty_spec() -> JobSpec {
        JobSpec {
            program: Program("dummy".to_string()),
            body: String::new(),
            config: SlurmJobConfig {
                partition: "p".to_string(),
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
        }
    }

    fn job_with_parents(parents: Vec<JobEdge>) -> Job {
        Job {
            spec: empty_spec(),
            parents,
        }
    }

    #[test]
    fn linear_chain_a_b_c() {
        let a = JobId("a".to_string());
        let b = JobId("b".to_string());
        let c = JobId("c".to_string());
        let mut jobs = BTreeMap::new();
        jobs.insert(a.clone(), job_with_parents(vec![]));
        jobs.insert(
            b.clone(),
            job_with_parents(vec![JobEdge {
                from: a.clone(),
                kind: DependencyType::AfterOk,
            }]),
        );
        jobs.insert(
            c.clone(),
            job_with_parents(vec![JobEdge {
                from: b.clone(),
                kind: DependencyType::AfterOk,
            }]),
        );

        let order = topological_order(&jobs, uuid::Uuid::nil()).unwrap();
        assert_eq!(order, vec![a, b, c]);
    }

    #[test]
    fn cycle_detected() {
        let a = JobId("a".to_string());
        let b = JobId("b".to_string());
        let mut jobs = BTreeMap::new();
        jobs.insert(
            a.clone(),
            job_with_parents(vec![JobEdge {
                from: b.clone(),
                kind: DependencyType::AfterOk,
            }]),
        );
        jobs.insert(
            b.clone(),
            job_with_parents(vec![JobEdge {
                from: a.clone(),
                kind: DependencyType::AfterOk,
            }]),
        );

        let result = topological_order(&jobs, uuid::Uuid::nil());
        assert!(matches!(
            result,
            Err(JobManagerError::DependencyCycle { .. })
        ));
    }
}
