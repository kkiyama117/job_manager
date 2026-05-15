//! Python wrapper for `FlowRunner::submit` (async).

use std::collections::HashMap;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use slurm_async_runner::SlurmManager;

use crate::flow::FlowRun;
use crate::persistence::PathResolver;
use crate::runner::flow::FlowRunner;
use crate::slurm::executor::{DryRunExecutor, Executor, SbatchExecutor};
use crate::slurm::querier::{InMemoryQuerier, Querier, SlurmQuerier};

/// Submit a flow.
///
/// - `root`: project root (where `<uuid>/flow.toml` and `plan.toml` live).
/// - `flow_uuid`: target flow UUID (string form).
/// - `dry_run`: when `True`, only render `batch.bash` files; do not call sbatch.
///
/// Returns a `dict[str, int]` mapping `JobId` → SLURM jobid for the jobs
/// that were submitted. Empty when `dry_run=True`.
///
/// Not annotated with `#[pyfunction]` directly — the `#[pymodule]` in
/// `py_export/mod.rs` re-declares the pyfunction wrapper to avoid duplicate
/// stub-gen registrations.
pub fn submit_flow<'py>(
    py: Python<'py>,
    root: std::path::PathBuf,
    flow_uuid: String,
    dry_run: bool,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let resolver = PathResolver::new(root);
        // uuid::Error is not a JobManagerError variant; surface as
        // PyValueError directly.
        let uuid = uuid::Uuid::parse_str(&flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let fr = FlowRun::read(&resolver, uuid)?;
        // Pair executor and querier consistently: dry-run stays purely
        // offline; production talks to the real sbatch + sacct via A1.
        let (exec, querier): (Box<dyn Executor>, Box<dyn Querier>) = if dry_run {
            (
                Box::new(DryRunExecutor),
                Box::new(InMemoryQuerier::new(HashMap::new())),
            )
        } else {
            (
                Box::new(SbatchExecutor),
                Box::new(SlurmQuerier::new(Arc::new(SlurmManager::default()))),
            )
        };
        let runner = FlowRunner::new(exec, querier, &resolver);
        let result = runner.submit(&fr, dry_run).await?;
        let py_dict: HashMap<String, u64> = result.into_iter().map(|(k, v)| (k.0, v)).collect();
        Ok(py_dict)
    })
}
