//! Crate-level error types. Mirrors the slurm_async_runner skeleton:
//! one library error enum plus a parse error variant, both wired to
//! Python exceptions in `crate::py_export::error`.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum JobManagerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Error)]
pub enum SchemaParseError {
    #[error("parse error: {0}")]
    Invalid(String),
}
