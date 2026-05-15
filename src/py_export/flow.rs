//! Python wrapper for `FlowRun`.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::flow::FlowRun;
use crate::persistence::PathResolver;

/// Python-facing wrapper for `FlowRun`. Read-only — mutation happens in
/// Rust via `FlowRunner`. `frozen` enforces that on the Python side.
#[gen_stub_pyclass]
#[pyclass(name = "FlowRun", module = "job_manager._job_manager_core", frozen)]
pub struct PyFlowRun {
    pub(crate) inner: FlowRun,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFlowRun {
    /// Read `flow.toml` + `plan.toml` (+ optional `common.toml`) from `root`
    /// and return a `FlowRun` for `flow_uuid`.
    #[staticmethod]
    pub fn read(root: std::path::PathBuf, flow_uuid: &str) -> PyResult<Self> {
        let resolver = PathResolver::new(root);
        // uuid::Error is not a JobManagerError variant; surface as
        // PyValueError directly.
        let uuid = uuid::Uuid::parse_str(flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let inner = FlowRun::read(&resolver, uuid)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn flow_uuid(&self) -> String {
        self.inner.flow_uuid.to_string()
    }

    fn __repr__(&self) -> String {
        format!(
            "FlowRun(flow_uuid={}, job_count={})",
            self.inner.flow_uuid,
            self.inner.flow.jobs.len(),
        )
    }
}
