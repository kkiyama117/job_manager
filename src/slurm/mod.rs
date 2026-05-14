//! SLURM-facing modules — every contact point with A1 lives here.
//!
//! `executor` (sbatch submit) and `querier` (sacct query) separated by 2-way responsibility.

pub mod dependency;
pub mod executor;
pub mod querier;

pub use executor::{DryRunExecutor, Executor, MockExecutor, SbatchExecutor};
pub use querier::{InMemoryQuerier, Querier, SlurmQuerier};
