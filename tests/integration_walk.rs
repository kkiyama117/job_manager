//! Integration: 100-flow walk completes under 1s.

use std::collections::BTreeMap;
use std::time::Instant;

use chrono::Utc;
use futures::StreamExt;
use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
use gaussian_job_shared::entities::workflow::JobFlow;
use job_manager::persistence::flow::write_flow;
use job_manager::walk::walk_flows;
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

fn sample_common_arc() -> Arc<CommonConfig> {
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

fn empty_flow(uuid: Uuid) -> JobFlow {
    JobFlow {
        uuid,
        created_at: Utc::now(),
        tags: BTreeMap::new(),
        jobs: BTreeMap::new(),
    }
}

#[tokio::test]
async fn walks_100_flows_under_one_second() {
    let dir = TempDir::new().unwrap();
    let mut written = Vec::new();
    for _ in 0..100 {
        let u = Uuid::now_v7();
        let f = empty_flow(u);
        let p = dir.path().join(u.to_string()).join("flow.toml");
        write_flow(&p, &f).unwrap();
        written.push(u);
    }

    let start = Instant::now();
    let stream = walk_flows(dir.path(), sample_common_arc());
    let collected: Vec<_> = stream.collect().await;
    let elapsed = start.elapsed();

    assert_eq!(collected.iter().filter(|r| r.is_ok()).count(), 100);
    assert!(elapsed.as_secs() < 1, "walk took {elapsed:?}");
}
