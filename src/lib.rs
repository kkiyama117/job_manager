//! `job_manager` data-layer crate.
//!
//! See `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`.

mod concurrency;

pub mod error;
pub mod job;
pub mod jobid;
pub mod persistence;
pub mod plan;
pub mod runner;
pub mod search;
pub mod slurm;
pub mod view;
pub mod walk;

pub use error::{JobManagerError, SchemaParseError};
pub use job::{JobRun, Lifecycle};
pub use jobid::{JobIdParts, build_job_id, parse_job_id, validate_job_id, validate_step_id};
pub use persistence::{
    PathResolver, read_common, read_flow, read_job_run, read_plan, write_common, write_flow,
    write_job_run, write_plan,
};
pub use plan::ExperimentPlan;
pub use runner::{Decision, TickResult, decide_transition, tick_many};
pub use search::{SearchFilter, matches};
pub use slurm::{InMemoryQuerier, Querier, SlurmQuerier};
pub use view::CalcView;
pub use walk::walk_flows;

#[cfg(feature = "pyo3")]
pub mod py_export;
#[cfg(feature = "pyo3")]
pub use py_export::stub_info;
