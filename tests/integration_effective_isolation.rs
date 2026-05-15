//! Verify that `.flow.effective.toml` makes `tick` / `show` independent of
//! `common.toml` — the snapshot is self-contained.

use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
use job_manager::flow::FlowRun;
use job_manager::persistence::{PathResolver, write_flow, write_flow_effective, write_plan};
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::tempdir;
use uuid::Uuid;

fn sample_common() -> CommonConfig {
    CommonConfig {
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
            project_root: PathBuf::from("/work"),
        },
    }
}

fn sample_flow(uuid: Uuid) -> JobFlow {
    let mut jobs = BTreeMap::new();
    jobs.insert(
        JobId::from("opt"),
        Job {
            spec: JobSpec {
                program: Program::from("echo"),
                config: SlurmJobConfig {
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
                body: "true\n".to_string(),
            },
            parents: vec![],
        },
    );
    JobFlow {
        uuid,
        created_at: chrono::Utc::now(),
        tags: BTreeMap::new(),
        jobs,
    }
}

#[test]
fn load_effective_works_after_common_is_removed() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let uuid = Uuid::nil();

    let common = sample_common();
    let common_path = resolver.common_toml();
    std::fs::write(&common_path, toml::to_string(&common).unwrap()).unwrap();
    let flow = sample_flow(uuid);
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    let plan = job_manager::ExperimentPlan {
        jobs: {
            let mut m = BTreeMap::new();
            m.insert(JobId::from("opt"), BTreeMap::new());
            m
        },
    };
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();
    write_flow_effective(&resolver.flow_effective_toml(&uuid), &flow).unwrap();

    // Now nuke common.toml. load_effective should still work.
    std::fs::remove_file(&common_path).unwrap();

    let fr = FlowRun::load_effective(&resolver, uuid).unwrap();
    assert_eq!(fr.flow_uuid, uuid);
    assert_eq!(fr.flow.jobs.len(), 1);
    assert_eq!(
        fr.flow.jobs[&JobId::from("opt")].spec.config.partition,
        "long"
    );
}

#[test]
fn load_effective_fails_when_snapshot_missing() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let uuid = Uuid::nil();

    let plan = job_manager::ExperimentPlan {
        jobs: BTreeMap::new(),
    };
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let err = FlowRun::load_effective(&resolver, uuid).unwrap_err();
    assert!(matches!(
        err,
        job_manager::JobManagerError::SnapshotMissing { .. }
    ));
}
