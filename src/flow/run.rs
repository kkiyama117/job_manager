//! FlowRun — aggregate of flow.toml + plan.toml + optional common.toml.

use std::collections::BTreeMap;

use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::{JobEdge, JobFlow, JobId};
use slurm_async_runner::entities::slurm::SlurmJobConfig;

use crate::error::JobManagerError;
use crate::flow::topology;
use crate::persistence::common::merge_with_defaults;
use crate::plan::ExperimentPlan;

pub struct FlowRun {
    pub flow_uuid: uuid::Uuid,
    pub flow: JobFlow,
    pub plan: ExperimentPlan,
    pub common: Option<CommonConfig>,
}

impl FlowRun {
    pub fn read(
        resolver: &crate::persistence::PathResolver,
        flow_uuid: uuid::Uuid,
    ) -> Result<Self, JobManagerError> {
        let flow = crate::persistence::read_flow(&resolver.flow_toml(&flow_uuid))?;
        let plan = crate::persistence::read_plan(&resolver.plan_toml(&flow_uuid))?;
        let common_path = resolver.common_toml();
        let common = if common_path.exists() {
            Some(crate::persistence::read_common(&common_path)?)
        } else {
            None
        };
        Ok(Self {
            flow_uuid,
            flow,
            plan,
            common,
        })
    }

    pub fn topological_order(&self) -> Result<Vec<JobId>, JobManagerError> {
        topology::topological_order(&self.flow.jobs, self.flow_uuid)
    }

    pub fn parents_of(&self, jid: &JobId) -> &[JobEdge] {
        self.flow
            .jobs
            .get(jid)
            .map(|job| job.parents.as_slice())
            .unwrap_or(&[])
    }

    pub fn params_of(
        &self,
        jid: &JobId,
    ) -> Result<&BTreeMap<String, toml::Value>, JobManagerError> {
        self.plan
            .jobs
            .get(jid)
            .ok_or_else(|| JobManagerError::MissingPlanEntry {
                flow: self.flow_uuid,
                job: jid.clone(),
            })
    }

    pub fn effective_config(&self, jid: &JobId) -> Result<SlurmJobConfig, JobManagerError> {
        let job = self
            .flow
            .jobs
            .get(jid)
            .ok_or_else(|| JobManagerError::MissingPlanEntry {
                flow: self.flow_uuid,
                job: jid.clone(),
            })?;
        Ok(match &self.common {
            Some(c) => merge_with_defaults(c, &job.spec.config),
            None => job.spec.config.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobId, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

    fn empty_spec(partition: &str) -> JobSpec {
        JobSpec {
            program: Program("dummy".to_string()),
            body: String::new(),
            config: SlurmJobConfig {
                partition: partition.to_string(),
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

    pub(crate) fn fr_with_2_jobs() -> FlowRun {
        let a = JobId("a".to_string());
        let b = JobId("b".to_string());

        let mut jobs: BTreeMap<JobId, Job> = BTreeMap::new();
        jobs.insert(
            a.clone(),
            Job {
                spec: empty_spec(""),
                parents: vec![],
            },
        );
        jobs.insert(
            b.clone(),
            Job {
                spec: empty_spec("short"),
                parents: vec![JobEdge {
                    from: a.clone(),
                    kind: DependencyType::AfterOk,
                }],
            },
        );

        let mut plan_jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>> = BTreeMap::new();
        plan_jobs.insert(a, BTreeMap::new());
        plan_jobs.insert(b, BTreeMap::new());

        FlowRun {
            flow_uuid: uuid::Uuid::nil(),
            flow: JobFlow {
                uuid: uuid::Uuid::nil(),
                created_at: chrono::Utc::now(),
                tags: BTreeMap::new(),
                jobs,
            },
            plan: ExperimentPlan { jobs: plan_jobs },
            common: None,
        }
    }

    #[test]
    fn topological_order_returns_a_then_b() {
        let fr = fr_with_2_jobs();
        let order = fr.topological_order().unwrap();
        assert_eq!(order, vec![JobId("a".to_string()), JobId("b".to_string())]);
    }

    #[test]
    fn parents_of_b_is_a() {
        let fr = fr_with_2_jobs();
        let p = fr.parents_of(&JobId("b".to_string()));
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].from, JobId("a".to_string()));
    }

    #[test]
    fn params_of_missing_returns_error() {
        let fr = fr_with_2_jobs();
        let result = fr.params_of(&JobId("nope".to_string()));
        assert!(matches!(
            result,
            Err(JobManagerError::MissingPlanEntry { .. })
        ));
    }

    #[test]
    fn effective_config_without_common_returns_spec_config() {
        let fr = fr_with_2_jobs();
        let cfg = fr.effective_config(&JobId("b".to_string())).unwrap();
        assert_eq!(cfg.partition, "short");
    }

    #[test]
    fn read_constructs_from_disk_with_common() {
        use crate::persistence::{PathResolver, write_common, write_flow, write_plan};
        use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
        use std::path::PathBuf;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path());
        let fr_src = fr_with_2_jobs();
        let uuid = uuid::Uuid::nil();

        let common = CommonConfig {
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
            directories: DirectoryConfig {
                project_root: PathBuf::from(dir.path()),
            },
        };
        write_common(&resolver.common_toml(), &common).unwrap();
        write_flow(&resolver.flow_toml(&uuid), &fr_src.flow).unwrap();
        write_plan(&resolver.plan_toml(&uuid), &fr_src.plan).unwrap();

        let fr = FlowRun::read(&resolver, uuid).unwrap();
        assert_eq!(fr.flow_uuid, uuid);
        assert!(fr.common.is_some());
        assert_eq!(fr.flow.jobs.len(), 2);
    }

    #[test]
    fn read_works_without_common_toml() {
        use crate::persistence::{PathResolver, write_flow, write_plan};
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path());
        let fr_src = fr_with_2_jobs();
        let uuid = uuid::Uuid::nil();

        write_flow(&resolver.flow_toml(&uuid), &fr_src.flow).unwrap();
        write_plan(&resolver.plan_toml(&uuid), &fr_src.plan).unwrap();

        let fr = FlowRun::read(&resolver, uuid).unwrap();
        assert!(fr.common.is_none());
    }
}
