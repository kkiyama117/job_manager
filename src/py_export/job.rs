use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

use crate::job::lifecycle::Lifecycle as Inner;
use crate::job::run::JobRun;
use crate::persistence::job_run as job_run_io;

#[gen_stub_pyclass_enum]
#[pyclass(eq, eq_int, name = "Lifecycle", from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyLifecycle {
    Queued,
    Running,
    Success,
    Failed,
    Skipped,
}

impl From<PyLifecycle> for Inner {
    fn from(v: PyLifecycle) -> Inner {
        match v {
            PyLifecycle::Queued => Inner::Queued,
            PyLifecycle::Running => Inner::Running,
            PyLifecycle::Success => Inner::Success,
            PyLifecycle::Failed => Inner::Failed,
            PyLifecycle::Skipped => Inner::Skipped,
        }
    }
}

impl From<Inner> for PyLifecycle {
    fn from(v: Inner) -> Self {
        match v {
            Inner::Queued => Self::Queued,
            Inner::Running => Self::Running,
            Inner::Success => Self::Success,
            Inner::Failed => Self::Failed,
            Inner::Skipped => Self::Skipped,
        }
    }
}

/// Python-facing wrapper for `JobRun` (旧 `StatusEntry`).
///
/// Read-only view. Mutation is performed in Rust via `FlowRunner::submit`/`tick`.
/// `frozen` makes that read-only intent enforceable by PyO3.
#[gen_stub_pyclass]
#[pyclass(
    name = "JobRun",
    module = "job_manager._job_manager_core",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyJobRun {
    pub(crate) inner: JobRun,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyJobRun {
    #[getter]
    fn lifecycle(&self) -> PyLifecycle {
        self.inner.lifecycle.into()
    }

    #[getter]
    fn slurm_jobid(&self) -> Option<u64> {
        self.inner.slurm_jobid
    }

    #[getter]
    fn note(&self) -> Option<String> {
        self.inner.note.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "JobRun(lifecycle={:?}, slurm_jobid={:?})",
            self.inner.lifecycle, self.inner.slurm_jobid,
        )
    }
}

pub(crate) fn read_job_run(path: PathBuf) -> PyResult<PyJobRun> {
    let inner = job_run_io::read_job_run(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    Ok(PyJobRun { inner })
}

pub(crate) fn write_job_run(path: PathBuf, run: PyJobRun) -> PyResult<()> {
    job_run_io::write_job_run(&path, &run.inner)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}
