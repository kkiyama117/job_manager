use pyo3::{
    PyErr,
    exceptions::{PyRuntimeError, PyValueError},
};

use crate::error::{JobManagerError, SchemaParseError};

impl From<SchemaParseError> for PyErr {
    fn from(value: SchemaParseError) -> Self {
        PyValueError::new_err(value.to_string())
    }
}

impl From<JobManagerError> for PyErr {
    fn from(value: JobManagerError) -> Self {
        PyRuntimeError::new_err(value.to_string())
    }
}
