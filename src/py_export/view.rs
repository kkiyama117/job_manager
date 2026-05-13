use std::path::PathBuf;
use std::sync::Arc;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::persistence::flow::read_flow;
use crate::persistence::path::PathResolver;
use crate::py_export::path::PyPathResolver;
use crate::status::io::read_status;
use crate::view::CalcView;

#[gen_stub_pyclass]
#[pyclass(name = "CalcView")]
pub struct PyCalcView {
    flow_uuid: uuid::Uuid,
    job_id: gaussian_job_shared::entities::workflow::JobId,
    resolver: Arc<PathResolver>,
    flow: gaussian_job_shared::entities::workflow::JobFlow,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCalcView {
    #[new]
    fn new(resolver: &PyPathResolver, flow_uuid: &str, job_id: &str) -> PyResult<Self> {
        let uuid = uuid::Uuid::parse_str(flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad uuid: {e}")))?;
        let flow = read_flow(&resolver.inner.flow_toml(&uuid))?;
        let jid = gaussian_job_shared::entities::workflow::JobId::from(job_id);
        if !flow.jobs.contains_key(&jid) {
            return Err(crate::error::JobManagerError::JobNotFound {
                flow: uuid,
                job: jid,
            }
            .into());
        }
        Ok(Self {
            flow_uuid: uuid,
            job_id: jid,
            resolver: resolver.inner.clone(),
            flow,
        })
    }

    fn job_dir(&self) -> PathBuf {
        self.resolver.job_dir(&self.flow_uuid, &self.job_id)
    }

    fn status_path(&self) -> PathBuf {
        self.resolver.status_file(&self.flow_uuid, &self.job_id)
    }

    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let entry = read_status(&self.status_path())?;
        let d = pyo3::types::PyDict::new(py);
        d.set_item("lifecycle", format!("{:?}", entry.lifecycle).to_lowercase())?;
        d.set_item("updated_at", entry.updated_at.to_rfc3339())?;
        d.set_item("slurm_jobid", entry.slurm_jobid)?;
        // Raw SLURM (state, reason); pythonized so Python sees a dict
        // with `state = "RUNNING"`/`reason = "Priority"` style values.
        d.set_item(
            "slurm_status",
            match &entry.slurm_status {
                Some(s) => pythonize::pythonize(py, s)?,
                None => py.None().into_bound(py),
            },
        )?;
        d.set_item("note", entry.note)?;
        Ok(d)
    }

    fn files(&self) -> PyResult<Vec<PathBuf>> {
        let view = CalcView::new(&self.flow, self.job_id.clone(), self.resolver.as_ref())?;
        Ok(view.files()?)
    }
}
