//! Python wrapper for `FlowRun`.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::flow::FlowRun;
use crate::persistence::PathResolver;

#[gen_stub_pyclass]
#[pyclass(name = "FlowRun", module = "job_manager._job_manager_core")]
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
        let uuid = uuid::Uuid::parse_str(flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let inner = FlowRun::read(&resolver, uuid)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn flow_uuid(&self) -> String {
        self.inner.flow_uuid.to_string()
    }
}
