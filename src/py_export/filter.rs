use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

use crate::filter::SearchFilter as Inner;
use crate::py_export::status::PyPerJobStatus;

#[gen_stub_pyclass]
#[pyclass(name = "SearchFilter", get_all, set_all, from_py_object)]
#[derive(Clone, Default)]
pub struct PySearchFilter {
    pub program: Option<String>,
    pub tags: HashMap<String, String>,
    pub status: Option<PyPerJobStatus>,
    pub flow_uuid_prefix: Option<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub slurm_jobid: Option<u64>,
    pub job_id: Option<String>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySearchFilter {
    #[new]
    #[pyo3(signature = (
        program=None,
        tags=None,
        status=None,
        flow_uuid_prefix=None,
        created_after=None,
        created_before=None,
        slurm_jobid=None,
        job_id=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        program: Option<String>,
        tags: Option<HashMap<String, String>>,
        status: Option<PyPerJobStatus>,
        flow_uuid_prefix: Option<String>,
        created_after: Option<DateTime<Utc>>,
        created_before: Option<DateTime<Utc>>,
        slurm_jobid: Option<u64>,
        job_id: Option<String>,
    ) -> Self {
        Self {
            program,
            tags: tags.unwrap_or_default(),
            status,
            flow_uuid_prefix,
            created_after,
            created_before,
            slurm_jobid,
            job_id,
        }
    }
}

impl PySearchFilter {
    // Used by py_export::walk and py_export::tick (Task 14); allow dead_code until then.
    #[allow(dead_code)]
    pub(crate) fn to_inner(&self) -> Inner {
        Inner {
            program: self
                .program
                .clone()
                .map(gaussian_job_shared::entities::workflow::Program::from),
            tags: self
                .tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
            status: self.status.map(Into::into),
            flow_uuid_prefix: self.flow_uuid_prefix.clone(),
            created_after: self.created_after,
            created_before: self.created_before,
            slurm_jobid: self.slurm_jobid,
            job_id: self
                .job_id
                .clone()
                .map(gaussian_job_shared::entities::workflow::JobId::from),
        }
    }
}
