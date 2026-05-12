//! Mockable SLURM query abstraction.
//!
//! `SlurmFacade::query_states_batch` returns `HashMap<u64, JobStatus>`,
//! transparently mirroring A1's `SlurmManager::query_job_states_batch`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use slurm_async_runner::{JobStatus, SlurmManager};

use crate::error::JobManagerError;

#[async_trait]
pub trait SlurmFacade: Send + Sync {
    async fn query_states_batch(
        &self,
        jobids: &[u64],
    ) -> Result<HashMap<u64, JobStatus>, JobManagerError>;
}

/// A1-backed concrete `SlurmFacade`.
pub struct A1SlurmFacade {
    manager: Arc<SlurmManager>,
}

impl A1SlurmFacade {
    pub fn new(manager: Arc<SlurmManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl SlurmFacade for A1SlurmFacade {
    async fn query_states_batch(
        &self,
        jobids: &[u64],
    ) -> Result<HashMap<u64, JobStatus>, JobManagerError> {
        self.manager
            .query_job_states_batch(jobids)
            .await
            .map_err(|e| JobManagerError::Slurm(e.to_string()))
    }
}

/// Hand-rolled mock for tests. Returns the configured map verbatim.
pub struct MockSlurmFacade {
    pub responses: HashMap<u64, JobStatus>,
}

impl MockSlurmFacade {
    pub fn new(responses: HashMap<u64, JobStatus>) -> Self {
        Self { responses }
    }
}

#[async_trait]
impl SlurmFacade for MockSlurmFacade {
    async fn query_states_batch(
        &self,
        jobids: &[u64],
    ) -> Result<HashMap<u64, JobStatus>, JobManagerError> {
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
    async fn mock_returns_configured_states_for_known_jobids() {
        let mut m = HashMap::new();
        let mut status = JobStatus::default();
        status.state = JobState::Running;
        m.insert(10u64, status);
        let mock = MockSlurmFacade::new(m);
        let r = mock.query_states_batch(&[10, 11]).await.unwrap();
        assert_eq!(r.len(), 1);
        assert!(matches!(r.get(&10).unwrap().state, JobState::Running));
    }
}
