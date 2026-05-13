use pyo3::{
    PyErr,
    exceptions::{PyFileNotFoundError, PyRuntimeError, PyValueError},
};

use crate::error::{JobManagerError, SchemaParseError};

impl From<SchemaParseError> for PyErr {
    fn from(value: SchemaParseError) -> Self {
        PyValueError::new_err(value.to_string())
    }
}

impl From<JobManagerError> for PyErr {
    fn from(value: JobManagerError) -> Self {
        match &value {
            JobManagerError::FlowNotFound { .. }
            | JobManagerError::JobNotFound { .. }
            | JobManagerError::StatusNotFound { .. } => {
                PyFileNotFoundError::new_err(value.to_string())
            }
            JobManagerError::TomlParse { .. }
            | JobManagerError::TomlSer(_)
            | JobManagerError::InvalidStepId(_)
            | JobManagerError::InvalidJobId(_)
            | JobManagerError::ReservedJobId(_)
            | JobManagerError::JobIdParseError { .. } => PyValueError::new_err(value.to_string()),
            _ => PyRuntimeError::new_err(value.to_string()),
        }
    }
}
