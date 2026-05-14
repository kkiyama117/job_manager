//! Integration: 100-flow walk completes under 1s.

use std::collections::BTreeMap;
use std::time::Instant;

use chrono::Utc;
use futures::StreamExt;
use gaussian_job_shared::entities::workflow::JobFlow;
use job_manager::persistence::flow::write_flow;
use job_manager::walk::walk_flows;
use tempfile::TempDir;
use uuid::Uuid;

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
    let stream = walk_flows(dir.path());
    let collected: Vec<_> = stream.collect().await;
    let elapsed = start.elapsed();

    assert_eq!(collected.iter().filter(|r| r.is_ok()).count(), 100);
    assert!(elapsed.as_secs() < 1, "walk took {elapsed:?}");
}
