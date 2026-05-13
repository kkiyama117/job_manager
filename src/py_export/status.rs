use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyclass_enum;

use crate::job::lifecycle::Lifecycle as Inner;

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
