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

/// Build a tmp-file extension that survives concurrent writers within the
/// same process. PID alone collides when the same process writes the same
/// path from two threads simultaneously; appending nanos + thread id makes
/// collisions astronomically unlikely without pulling in a uuid dependency.
///
/// Format: `toml.<pid>.<nanos>.<tid>.tmp`
pub(crate) fn tmp_extension() -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tid = format!("{:?}", std::thread::current().id());
    let tid_short: String = tid.chars().filter(|c| c.is_ascii_digit()).take(8).collect();
    format!("toml.{pid}.{nanos}.{tid_short}.tmp")
}
