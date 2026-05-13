//! Python wrapper for `FlowRunner::submit` (async).

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::gen_stub_pyfunction;

use crate::flow::FlowRun;
use crate::persistence::PathResolver;
use crate::runner::flow::FlowRunner;
use crate::slurm::executor::{DryRunExecutor, Executor, SbatchExecutor};
use crate::slurm::querier::InMemoryQuerier;

/// Submit a flow.
///
/// - `root`: project root (where `<uuid>/flow.toml` and `plan.toml` live).
/// - `flow_uuid`: target flow UUID (string form).
/// - `dry_run`: when `True`, only render `batch.bash` files; do not call sbatch.
///
/// Returns a `dict[str, int]` mapping `JobId` → SLURM jobid for the jobs
/// that were submitted. Empty when `dry_run=True`.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (root, flow_uuid, dry_run = false))]
pub fn submit_flow<'py>(
    py: Python<'py>,
    root: std::path::PathBuf,
    flow_uuid: String,
    dry_run: bool,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let resolver = PathResolver::new(root);
        let uuid = uuid::Uuid::parse_str(&flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let fr = FlowRun::read(&resolver, uuid)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let exec: Box<dyn Executor> = if dry_run {
            Box::new(DryRunExecutor)
        } else {
            Box::new(SbatchExecutor)
        };
        let runner = FlowRunner::new(
            exec,
            Box::new(InMemoryQuerier::new(HashMap::new())),
            &resolver,
        );
        let result = runner
            .submit(&fr, dry_run)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let py_dict: HashMap<String, u64> = result.into_iter().map(|(k, v)| (k.0, v)).collect();
        Ok(py_dict)
    })
}
