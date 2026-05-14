//! Persistence layer — all TOML file I/O lives here.
//!
//! Submodules are organized by file kind (one TOML schema per submodule).

pub mod common;
pub mod flow;
pub mod job_run;
pub mod path;
pub mod plan;

pub use common::{merge_with_defaults, read_common, write_common};
pub use flow::{read_flow, write_flow};
pub use job_run::{read_job_run, write_job_run};
pub use path::PathResolver;
pub use plan::{read_plan, write_plan};
