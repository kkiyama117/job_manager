//! `job_manager` data-layer crate.
//!
//! See `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`.

mod concurrency;

pub mod doctor;
pub mod error;
pub mod flow;
pub mod job;
pub mod jobid;
pub mod persistence;
pub mod plan;
pub mod render;
pub mod runner;
pub mod search;
pub mod slurm;
pub mod view;
pub mod walk;

pub use doctor::{DoctorReport, DoctorScope, Finding, Severity, run_doctor};
pub use error::{JobManagerError, SchemaParseError};
pub use flow::FlowRun;
pub use job::{JobRun, Lifecycle};
pub use jobid::{JobIdParts, build_job_id, parse_job_id, validate_job_id, validate_step_id};
pub use persistence::{
    PathResolver, merge_with_defaults, read_common, read_flow, read_flow_effective, read_job_run,
    read_plan, synth_empty_common, write_common, write_flow, write_flow_effective, write_job_run,
    write_plan,
};
pub use plan::ExperimentPlan;
pub use render::render_batch_bash;
pub use runner::{Decision, FlowRunner, TickResult, decide_transition};
pub use search::{SearchFilter, matches};
pub use slurm::executor::{DryRunExecutor, Executor, MockExecutor, SbatchExecutor};
pub use slurm::{InMemoryQuerier, Querier, SlurmQuerier};
pub use view::CalcView;
pub use walk::walk_flows;

#[cfg(feature = "pyo3")]
pub mod py_export;
#[cfg(feature = "pyo3")]
pub use py_export::stub_info;
