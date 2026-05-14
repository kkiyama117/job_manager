//! SLURM query abstraction with both live (`SlurmQuerier`) and offline
//! (`InMemoryQuerier`) concrete impls.
//!
//! `Querier::query` returns `HashMap<u64, JobStatus>`,
//! transparently mirroring A1's `SlurmManager::query_job_states_batch`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use slurm_async_runner::{JobStatus, SlurmManager};

use crate::error::JobManagerError;

#[async_trait]
pub trait Querier: Send + Sync {
    async fn query(&self, jobids: &[u64]) -> Result<HashMap<u64, JobStatus>, JobManagerError>;
}

/// A1-backed concrete `Querier`.
pub struct SlurmQuerier {
    manager: Arc<SlurmManager>,
}

impl SlurmQuerier {
    pub fn new(manager: Arc<SlurmManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl Querier for SlurmQuerier {
    async fn query(&self, jobids: &[u64]) -> Result<HashMap<u64, JobStatus>, JobManagerError> {
        self.manager
            .query_job_states_batch(jobids)
            .await
            .map_err(|e| JobManagerError::Slurm(e.to_string()))
    }
}

/// Pre-populated in-memory `Querier`. Returns the configured
/// `responses` map verbatim. Useful for tests, dry-runs, and replay
/// against captured `sacct` snapshots — anywhere a live SLURM query is
/// unavailable or undesirable.
pub struct InMemoryQuerier {
    pub responses: HashMap<u64, JobStatus>,
}

impl InMemoryQuerier {
    pub fn new(responses: HashMap<u64, JobStatus>) -> Self {
        Self { responses }
    }
}

#[async_trait]
impl Querier for InMemoryQuerier {
    async fn query(&self, jobids: &[u64]) -> Result<HashMap<u64, JobStatus>, JobManagerError> {
        let mut out = HashMap::new();
        for &j in jobids {
            if let Some(s) = self.responses.get(&j) {
                out.insert(j, s.clone());
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slurm_async_runner::JobState;

    #[tokio::test]
    async fn in_memory_returns_configured_states_for_known_jobids() {
        let mut m = HashMap::new();
        let status = JobStatus {
            state: JobState::Running,
            ..Default::default()
        };
        m.insert(10u64, status);
        let querier = InMemoryQuerier::new(m);
        let r = querier.query(&[10, 11]).await.unwrap();
        assert_eq!(r.len(), 1);
        assert!(matches!(r.get(&10).unwrap().state, JobState::Running));
    }
}
