//! Integration: on-disk cross-flow listing (no live SLURM).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
use job_manager::job::lifecycle::Lifecycle;
use job_manager::job::run::JobRun;
use job_manager::listing::{DisplayLifecycle, collect, flow_rows, job_rows};
use job_manager::persistence::PathResolver;
use job_manager::persistence::write_flow;
use job_manager::persistence::write_job_run;
use job_manager::search::SearchFilter;
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use tempfile::TempDir;
use uuid::Uuid;

fn common() -> Arc<CommonConfig> {
    Arc::new(CommonConfig {
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
    })
}

fn cfg() -> SlurmJobConfig {
    SlurmJobConfig {
        partition: "long".into(),
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
    }
}

fn flow_with_two_jobs(uuid: Uuid) -> JobFlow {
    let mut jobs = BTreeMap::new();
    for name in ["step1", "step2"] {
        jobs.insert(
            JobId::from(name),
            Job {
                spec: JobSpec {
                    program: Program::from("g16"),
                    config: cfg(),
                    body: "".into(),
                },
                parents: vec![],
            },
        );
    }
    JobFlow {
        uuid,
        created_at: Utc::now(),
        tags: BTreeMap::new(),
        jobs,
    }
}

#[tokio::test]
async fn collect_filters_jobs_by_status_set() {
    let dir = TempDir::new().unwrap();
    let resolver = PathResolver::new(dir.path());
    let u = Uuid::now_v7();
    write_flow(&resolver.flow_toml(&u), &flow_with_two_jobs(u)).unwrap();
    write_job_run(
        &resolver.status_file(&u, &JobId::from("step1")),
        &JobRun {
            lifecycle: Lifecycle::Success,
            updated_at: Utc::now(),
            slurm_jobid: Some(42),
            slurm_status: None,
            note: None,
        },
    )
    .unwrap();

    let collected = collect(dir.path(), common(), &SearchFilter::default())
        .await
        .unwrap();
    assert_eq!(collected.len(), 1);

    let all = job_rows(&collected, &SearchFilter::default(), None);
    assert_eq!(all.len(), 2);

    let mut want = std::collections::BTreeSet::new();
    want.insert(DisplayLifecycle::Real(Lifecycle::Success));
    let only_ok = job_rows(
        &collected,
        &SearchFilter {
            status: want,
            ..Default::default()
        },
        None,
    );
    assert_eq!(only_ok.len(), 1);
    assert_eq!(only_ok[0].job_id, "step1");
    assert_eq!(only_ok[0].slurm_jobid, Some(42));

    let frows = flow_rows(&collected, &SearchFilter::default(), None);
    assert_eq!(frows.len(), 1);
    assert_eq!(frows[0].total, 2);
    assert_eq!(frows[0].done, 1);
}

#[tokio::test]
async fn collect_sorts_newest_first_and_limit_applies() {
    use chrono::Duration;
    let dir = TempDir::new().unwrap();
    let resolver = PathResolver::new(dir.path());
    let base = chrono::Utc::now();
    for i in 0..5 {
        let u = Uuid::now_v7();
        let mut flow = flow_with_two_jobs(u);
        flow.created_at = base + Duration::seconds(i as i64);
        write_flow(&resolver.flow_toml(&u), &flow).unwrap();
    }
    let collected = collect(dir.path(), common(), &SearchFilter::default())
        .await
        .unwrap();
    let times: Vec<_> = collected.iter().map(|c| c.flow.created_at).collect();
    let mut sorted = times.clone();
    sorted.sort_by(|a, b| b.cmp(a));
    assert_eq!(times, sorted);

    let limited = flow_rows(&collected, &SearchFilter::default(), Some(2));
    assert_eq!(limited.len(), 2);
}
