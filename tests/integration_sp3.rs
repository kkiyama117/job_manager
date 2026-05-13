//! SP-3 re-arch integration tests using MockExecutor + InMemoryQuerier.

use job_manager::flow::FlowRun;
use job_manager::job::Lifecycle;
use job_manager::persistence::{PathResolver, read_job_run, write_flow, write_plan};
use job_manager::plan::ExperimentPlan;
use job_manager::runner::flow::FlowRunner;
use job_manager::slurm::executor::MockExecutor;
use job_manager::slurm::querier::InMemoryQuerier;
use std::collections::{BTreeMap, HashMap};
use tempfile::tempdir;

fn build_2_job_flow() -> (
    uuid::Uuid,
    gaussian_job_shared::entities::workflow::JobFlow,
    ExperimentPlan,
) {
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobFlow, JobId, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

    let a = JobId("a".to_string());
    let b = JobId("b".to_string());
    let spec = JobSpec {
        program: Program("g16".to_string()),
        body: "echo hello".to_string(),
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
    };
    let mut jobs = BTreeMap::new();
    jobs.insert(
        a.clone(),
        Job {
            spec: spec.clone(),
            parents: vec![],
        },
    );
    jobs.insert(
        b.clone(),
        Job {
            spec: spec.clone(),
            parents: vec![JobEdge {
                from: a.clone(),
                kind: DependencyType::AfterOk,
            }],
        },
    );

    let uuid = uuid::Uuid::new_v4();
    let flow = JobFlow {
        uuid,
        created_at: chrono::Utc::now(),
        tags: BTreeMap::new(),
        jobs,
    };

    let mut plan_jobs = BTreeMap::new();
    plan_jobs.insert(a, BTreeMap::new());
    plan_jobs.insert(b, BTreeMap::new());
    let plan = ExperimentPlan { jobs: plan_jobs };

    (uuid, flow, plan)
}

#[tokio::test]
async fn submit_writes_batch_bash_and_status_in_topo_order() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let (uuid, flow, plan) = build_2_job_flow();
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    let exec = MockExecutor::new(vec![100, 200]);
    let querier = InMemoryQuerier::new(HashMap::new());
    let runner = FlowRunner::new(Box::new(exec), Box::new(querier), &resolver);

    let result = runner.submit(&fr, false).await.unwrap();
    assert_eq!(result.len(), 2);

    for jid in fr.flow.jobs.keys() {
        let p = resolver.batch_bash(&uuid, jid);
        assert!(p.exists(), "missing batch.bash for {jid:?}");
    }

    for jid in fr.flow.jobs.keys() {
        let s = resolver.status_file(&uuid, jid);
        let entry = read_job_run(&s).unwrap();
        assert_eq!(entry.lifecycle, Lifecycle::Queued);
        assert!(entry.slurm_jobid.is_some());
    }
}

#[tokio::test]
async fn submit_dry_run_writes_batch_bash_but_does_not_call_executor() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let (uuid, flow, plan) = build_2_job_flow();
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    let exec = MockExecutor::new(vec![]); // empty — error if called
    let querier = InMemoryQuerier::new(HashMap::new());
    let runner = FlowRunner::new(Box::new(exec), Box::new(querier), &resolver);

    let result = runner.submit(&fr, true).await.unwrap();
    assert!(result.is_empty(), "dry_run should not record jobids");

    for jid in fr.flow.jobs.keys() {
        assert!(resolver.batch_bash(&uuid, jid).exists());
        assert!(!resolver.status_file(&uuid, jid).exists());
    }
}

#[tokio::test]
async fn tick_marks_child_skipped_when_parent_failed() {
    use gaussian_job_shared::entities::workflow::JobId;
    use slurm_async_runner::{JobState, JobStatus};

    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let (uuid, flow, plan) = build_2_job_flow();
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    let exec = MockExecutor::new(vec![100, 200]);
    let mut q = HashMap::new();
    q.insert(
        100,
        JobStatus {
            state: JobState::Failed,
            ..Default::default()
        },
    );
    q.insert(
        200,
        JobStatus {
            state: JobState::Pending,
            ..Default::default()
        },
    );
    let querier = InMemoryQuerier::new(q);
    let runner = FlowRunner::new(Box::new(exec), Box::new(querier), &resolver);

    runner.submit(&fr, false).await.unwrap();
    runner.tick(&fr).await.unwrap();

    let a_run = read_job_run(&resolver.status_file(&uuid, &JobId("a".to_string()))).unwrap();
    assert_eq!(a_run.lifecycle, Lifecycle::Failed);
    let b_run = read_job_run(&resolver.status_file(&uuid, &JobId("b".to_string()))).unwrap();
    assert_eq!(b_run.lifecycle, Lifecycle::Skipped);
}
