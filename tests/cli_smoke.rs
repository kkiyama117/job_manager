//! jm CLI smoke tests.

use assert_cmd::Command;
use predicates::prelude::*;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[test]
fn jm_help_runs() {
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--help");
    cmd.assert().success();
}

#[test]
fn jm_render_writes_batch_bash() {
    use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
    use job_manager::persistence::{PathResolver, write_flow, write_plan};
    use job_manager::plan::ExperimentPlan;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;

    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let uuid = uuid::Uuid::new_v4();
    let jid = JobId("a".to_string());
    let mut jobs = BTreeMap::new();
    jobs.insert(
        jid.clone(),
        Job {
            spec: JobSpec {
                program: Program("g16".to_string()),
                body: "echo hi".to_string(),
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
            },
            parents: vec![],
        },
    );
    let flow = JobFlow {
        uuid,
        created_at: chrono::Utc::now(),
        tags: BTreeMap::new(),
        jobs,
    };
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();

    let mut plan_jobs = BTreeMap::new();
    plan_jobs.insert(jid.clone(), BTreeMap::new());
    let plan = ExperimentPlan { jobs: plan_jobs };
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root")
        .arg(dir.path())
        .arg("render")
        .arg(uuid.to_string());
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("rendered"));

    assert!(resolver.batch_bash(&uuid, &jid).exists());
}
