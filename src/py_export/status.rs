use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyclass_enum;

use crate::status::PerJobStatus as Inner;

#[gen_stub_pyclass_enum]
#[pyclass(eq, eq_int, name = "PerJobStatus", from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyPerJobStatus {
    Queued,
    Running,
    Done,
    Failed,
}

impl From<PyPerJobStatus> for Inner {
    fn from(v: PyPerJobStatus) -> Inner {
        match v {
            PyPerJobStatus::Queued => Inner::Queued,
            PyPerJobStatus::Running => Inner::Running,
            PyPerJobStatus::Done => Inner::Done,
            PyPerJobStatus::Failed => Inner::Failed,
        }
    }
}

impl From<Inner> for PyPerJobStatus {
    fn from(v: Inner) -> Self {
        match v {
            Inner::Queued => Self::Queued,
            Inner::Running => Self::Running,
            Inner::Done => Self::Done,
            Inner::Failed => Self::Failed,
        }
    }
}
