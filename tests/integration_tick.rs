//! Integration: tick_many drives status writes via MockSlurmFacade.

use std::collections::HashMap;

use chrono::Utc;
use gaussian_job_shared::entities::workflow::JobId;
use job_manager::path::PathResolver;
use job_manager::slurm_facade::MockSlurmFacade;
use job_manager::status::{
    PerJobStatus, StatusEntry,
    io::{read_status, write_status},
};
use job_manager::tick::tick_many;
use slurm_async_runner::{JobState, JobStatus};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn three_targets_tick_independently() {
    let dir = TempDir::new().unwrap();
    let resolver = PathResolver::new(dir.path());

    let triples: Vec<(Uuid, JobId, u64)> = (0..3)
        .map(|i| {
            (
                Uuid::now_v7(),
                JobId::from(format!("job{i}").as_str()),
                i as u64 + 100,
            )
        })
        .collect();

    for (uuid, jid, sid) in &triples {
        write_status(
            &resolver.status_file(uuid, jid),
            &StatusEntry {
                lifecycle: PerJobStatus::Queued,
                updated_at: Utc::now(),
                slurm_jobid: Some(*sid),
                slurm_status: None,
                note: None,
            },
        )
        .unwrap();
    }

    let mut responses = HashMap::new();
    responses.insert(100u64, JobStatus::new(JobState::Running));
    responses.insert(101u64, JobStatus::new(JobState::Failed));
    // 102 left unset → SLURM Unknown for this jobid

    let slurm = MockSlurmFacade::new(responses);

    let results = tick_many(&triples, &slurm, &resolver).await;
    assert_eq!(results.len(), 3);

    let s0 = read_status(&resolver.status_file(&triples[0].0, &triples[0].1)).unwrap();
    let s1 = read_status(&resolver.status_file(&triples[1].0, &triples[1].1)).unwrap();
    let s2 = read_status(&resolver.status_file(&triples[2].0, &triples[2].1)).unwrap();

    assert_eq!(s0.lifecycle, PerJobStatus::Running);
    assert_eq!(s1.lifecycle, PerJobStatus::Failed);
    assert_eq!(s2.lifecycle, PerJobStatus::Queued); // unchanged (orphan)
    // Raw SLURM status is preserved (Running/Failed for s0/s1; None for s2).
    assert_eq!(s0.slurm_status.as_ref().unwrap().state, JobState::Running);
    assert_eq!(s1.slurm_status.as_ref().unwrap().state, JobState::Failed);
    assert!(s2.slurm_status.is_none());
}
