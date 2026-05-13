//! SLURM-facing modules — every contact point with A1 lives here.
//!
//! `executor` (sbatch submit) and `querier` (sacct query) separated by 2-way responsibility.

pub mod executor;
pub mod querier;

pub use executor::{Executor, SbatchExecutor};
pub use querier::{InMemoryQuerier, Querier, SlurmQuerier};
