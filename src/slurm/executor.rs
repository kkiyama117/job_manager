//! Executor trait — abstraction over sbatch submission.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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

/// `jm submit --dry-run` 用。決定的な fake jobid を返す。
pub struct DryRunExecutor;

#[async_trait]
impl Executor for DryRunExecutor {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError> {
        let mut h = DefaultHasher::new();
        cmd.script.hash(&mut h);
        Ok(1 + (h.finish() % 9_999_999))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slurm_async_runner::SbatchCmd;
    use std::path::PathBuf;

    #[tokio::test]
    async fn dry_run_returns_deterministic_jobid() {
        let exec = DryRunExecutor;
        let j1 = exec
            .submit(SbatchCmd::new(PathBuf::from("/tmp/a.sh")))
            .await
            .unwrap();
        let j2 = exec
            .submit(SbatchCmd::new(PathBuf::from("/tmp/a.sh")))
            .await
            .unwrap();
        let j3 = exec
            .submit(SbatchCmd::new(PathBuf::from("/tmp/b.sh")))
            .await
            .unwrap();

        assert_eq!(j1, j2, "same script => same fake jobid");
        assert_ne!(j1, j3, "different script => different jobid");
    }
}
