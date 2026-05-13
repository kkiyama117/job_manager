//! `job_manager` data-layer crate.
//!
//! See `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`.

mod concurrency;

pub mod error;
pub mod filter;
pub mod job;
pub mod jobid;
pub mod persistence;
pub mod plan;
pub mod slurm_facade;
pub mod tick;
pub mod view;
pub mod walk;

pub use error::{JobManagerError, SchemaParseError};
pub use filter::{SearchFilter, matches};
pub use job::{JobRun, Lifecycle};
pub use jobid::{JobIdParts, build_job_id, parse_job_id, validate_job_id, validate_step_id};
pub use persistence::{
    PathResolver, read_flow, read_job_run, read_plan, write_flow, write_job_run, write_plan,
};
pub use plan::ExperimentPlan;
pub use slurm_facade::{A1SlurmFacade, InMemorySlurmFacade, SlurmFacade};
pub use tick::{Decision, TickResult, decide_transition, tick_many};
pub use view::CalcView;
pub use walk::walk_flows;

#[cfg(feature = "pyo3")]
pub mod py_export;
#[cfg(feature = "pyo3")]
pub use py_export::stub_info;
