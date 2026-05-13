//! Executor trait — abstraction over sbatch submission.

use async_trait::async_trait;
use slurm_async_runner::{SbatchCmd, SbatchManager};

use crate::error::JobManagerError;

/// Abstraction over sbatch submission.
///
/// `SbatchExecutor` is the production implementation; `DryRunExecutor` (D.2)
/// and `MockExecutor` (D.3) are added in subsequent tasks.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError>;
}

/// Production executor: wraps A1 `SbatchManager.spawn().await`.
pub struct SbatchExecutor;

#[async_trait]
impl Executor for SbatchExecutor {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError> {
        let manager = SbatchManager::new(cmd);
        let handle = manager
            .spawn()
            .await
            .map_err(|e| JobManagerError::SubmitFailed {
                source: anyhow::anyhow!(e),
            })?;
        handle.jobid().ok_or_else(|| JobManagerError::SubmitFailed {
            source: anyhow::anyhow!("sbatch returned no jobid"),
        })
    }
}
