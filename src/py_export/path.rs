use std::path::PathBuf;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::path::PathResolver as Inner;

#[gen_stub_pyclass]
#[pyclass(name = "PathResolver", frozen)]
pub struct PyPathResolver {
    pub inner: Arc<Inner>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPathResolver {
    #[new]
    fn new(root: PathBuf) -> Self {
        Self {
            inner: Arc::new(Inner::new(root)),
        }
    }

    fn root(&self) -> PathBuf {
        self.inner.root().to_path_buf()
    }

    fn flow_dir(&self, flow_uuid: &str) -> PyResult<PathBuf> {
        let u = uuid::Uuid::parse_str(flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad uuid: {e}")))?;
        Ok(self.inner.flow_dir(&u))
    }

    fn flow_toml(&self, flow_uuid: &str) -> PyResult<PathBuf> {
        let u = uuid::Uuid::parse_str(flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad uuid: {e}")))?;
        Ok(self.inner.flow_toml(&u))
    }

    fn status_file(&self, flow_uuid: &str, job_id: &str) -> PyResult<PathBuf> {
        let u = uuid::Uuid::parse_str(flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad uuid: {e}")))?;
        Ok(self.inner.status_file(
            &u,
            &gaussian_job_shared::entities::workflow::JobId::from(job_id),
        ))
    }
}
