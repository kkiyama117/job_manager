//! `job_manager` data-layer crate.
//!
//! See `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`.

mod concurrency;

pub mod error;
pub mod filter;
pub mod flow_io;
pub mod jobid;
pub mod persistence;
pub mod plan;
pub mod slurm_facade;
pub mod status;
pub mod tick;
pub mod view;
pub mod walk;

pub use error::{JobManagerError, SchemaParseError};
pub use filter::{SearchFilter, matches};
pub use flow_io::{read_flow, write_flow};
pub use jobid::{JobIdParts, build_job_id, parse_job_id, validate_job_id, validate_step_id};
pub use persistence::PathResolver;
pub use plan::ExperimentPlan;
pub use plan::io::{read_plan, write_plan};
pub use slurm_facade::{A1SlurmFacade, InMemorySlurmFacade, SlurmFacade};
pub use status::{PerJobStatus, StatusEntry};
pub use tick::{Decision, TickResult, decide_transition, tick_many};
pub use view::CalcView;
pub use walk::walk_flows;

#[cfg(feature = "pyo3")]
pub mod py_export;
#[cfg(feature = "pyo3")]
pub use py_export::stub_info;
