//! Executor trait — abstraction over sbatch submission.

use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

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

/// Mock executor for integration tests — returns pre-recorded jobids in order and logs calls.
pub struct MockExecutor {
    recordings: Mutex<VecDeque<u64>>,
    calls_log: Mutex<Vec<SbatchCmd>>,
}

impl MockExecutor {
    pub fn new(recordings: Vec<u64>) -> Self {
        Self {
            recordings: Mutex::new(recordings.into_iter().collect()),
            calls_log: Mutex::new(Vec::new()),
        }
    }

    pub fn calls(&self) -> Vec<SbatchCmd> {
        self.calls_log.lock().unwrap().clone()
    }
}

#[async_trait]
impl Executor for MockExecutor {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError> {
        self.calls_log.lock().unwrap().push(cmd.clone());
        self.recordings
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| JobManagerError::SubmitFailed {
                source: anyhow::anyhow!("MockExecutor recordings exhausted"),
            })
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

    #[tokio::test]
    async fn mock_returns_recorded_jobids_in_order() {
        let exec = MockExecutor::new(vec![100, 200, 300]);
        assert_eq!(
            exec.submit(SbatchCmd::new(PathBuf::from("/tmp/a.sh")))
                .await
                .unwrap(),
            100
        );
        assert_eq!(
            exec.submit(SbatchCmd::new(PathBuf::from("/tmp/b.sh")))
                .await
                .unwrap(),
            200
        );
        assert_eq!(
            exec.submit(SbatchCmd::new(PathBuf::from("/tmp/c.sh")))
                .await
                .unwrap(),
            300
        );
        assert_eq!(exec.calls().len(), 3);
    }

    #[tokio::test]
    async fn mock_errors_when_exhausted() {
        let exec = MockExecutor::new(vec![100]);
        let _ = exec
            .submit(SbatchCmd::new(PathBuf::from("/x")))
            .await
            .unwrap();
        let result = exec.submit(SbatchCmd::new(PathBuf::from("/y"))).await;
        assert!(result.is_err());
    }
}
