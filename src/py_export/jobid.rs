//! Python 公開: jobid helpers.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::jobid;

pub(crate) fn validate_step_id(s: &str) -> PyResult<String> {
    jobid::validate_step_id(s)
        .map(|x| x.to_string())
        .map_err(PyErr::from)
}

pub(crate) fn validate_job_id(s: &str) -> PyResult<String> {
    jobid::validate_job_id(s)
        .map(|x| x.to_string())
        .map_err(PyErr::from)
}

pub(crate) fn build_job_id(source_step_id: &str, axis_combo: Vec<(String, usize)>) -> String {
    let refs: Vec<(&str, usize)> = axis_combo.iter().map(|(s, i)| (s.as_str(), *i)).collect();
    jobid::build_job_id(source_step_id, &refs)
}

pub(crate) fn parse_job_id<'py>(py: Python<'py>, s: &str) -> PyResult<Bound<'py, PyDict>> {
    let parts = jobid::parse_job_id(s).map_err(PyErr::from)?;
    let dict = PyDict::new(py);
    dict.set_item("source_step_id", parts.source_step_id)?;
    let pylist = PyList::new(
        py,
        parts.axis_combo.iter().map(|(k, v)| (k.to_string(), *v)),
    )?;
    dict.set_item("axis_combo", pylist)?;
    Ok(dict)
}
