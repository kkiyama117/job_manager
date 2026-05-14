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

/// Atomic write helper: ensures parent dir, picks a unique tmp path,
/// writes the body, fsyncs the tmp, renames over `path`, and cleans up
/// the tmp on any failure.
///
/// The fsync guards against kernel panic / power loss leaving a
/// zero-length file at `path`. Callers should still treat rename failure
/// as fatal — the source data is in memory anyway.
pub(crate) fn atomic_write(
    path: &std::path::Path,
    body: &[u8],
) -> Result<(), crate::error::JobManagerError> {
    use crate::error::JobManagerError;
    use std::io::Write;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    let tmp = path.with_extension(tmp_extension());

    let write_and_sync = || -> Result<(), JobManagerError> {
        let mut f = std::fs::File::create(&tmp).map_err(|source| JobManagerError::Io {
            path: tmp.clone(),
            source,
        })?;
        f.write_all(body).map_err(|source| JobManagerError::Io {
            path: tmp.clone(),
            source,
        })?;
        f.sync_all().map_err(|source| JobManagerError::Io {
            path: tmp.clone(),
            source,
        })?;
        Ok(())
    };

    let result = write_and_sync().and_then(|()| {
        std::fs::rename(&tmp, path).map_err(|source| JobManagerError::Io {
            path: path.to_path_buf(),
            source,
        })
    });

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}
