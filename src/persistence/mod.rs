//! Persistence layer — all TOML file I/O lives here.
//!
//! Submodules are organized by file kind (one TOML schema per submodule).
//!
//! All readers route through `read_toml_string()`, which enforces an
//! upper bound on file size so a malicious or runaway producer cannot
//! exhaust memory by handing us a multi-gigabyte TOML file.

pub mod common;
pub mod flow;
pub mod job_run;
pub mod path;
pub mod plan;

pub use common::{merge_with_defaults, read_common, write_common};
pub use flow::{read_flow, read_flow_effective, write_flow, write_flow_effective};
pub use job_run::{read_job_run, write_job_run};
pub use path::PathResolver;
pub use plan::{read_plan, write_plan};

/// Default upper bound on TOML file size (10 MiB). TOML at this scale
/// is already a code smell (large embedded blobs belong elsewhere) and
/// the limit guards against DoS via crafted files.
pub(crate) const DEFAULT_MAX_TOML_SIZE: u64 = 10 * 1024 * 1024;

/// Effective TOML size limit. `JM_MAX_TOML_SIZE` (positive integer in
/// bytes) overrides the default; otherwise `DEFAULT_MAX_TOML_SIZE`.
pub(crate) fn max_toml_size() -> u64 {
    if let Ok(s) = std::env::var("JM_MAX_TOML_SIZE")
        && let Ok(n) = s.parse::<u64>()
        && n > 0
    {
        return n;
    }
    DEFAULT_MAX_TOML_SIZE
}

/// Read a TOML file as a string, enforcing the size limit before
/// reading. Returns `JobManagerError::FileTooLarge` if the file exceeds
/// the limit, `JobManagerError::Io` for any I/O error.
pub(crate) fn read_toml_string(
    path: &std::path::Path,
) -> Result<String, crate::error::JobManagerError> {
    use crate::error::JobManagerError;
    let metadata = std::fs::metadata(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let limit = max_toml_size();
    if metadata.len() > limit {
        return Err(JobManagerError::FileTooLarge {
            path: path.to_path_buf(),
            size: metadata.len(),
            limit,
        });
    }
    std::fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::JobManagerError;
    use tempfile::tempdir;

    #[test]
    fn read_toml_string_rejects_file_larger_than_default_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("big.toml");
        // Write a file 1 byte over the default 10 MiB limit. Use sparse
        // padding so the test still finishes quickly even on slow disks.
        let body = "x".repeat((DEFAULT_MAX_TOML_SIZE + 1) as usize);
        std::fs::write(&path, body).unwrap();
        let err = read_toml_string(&path).unwrap_err();
        match err {
            JobManagerError::FileTooLarge { size, limit, .. } => {
                assert!(size > limit);
                assert_eq!(limit, DEFAULT_MAX_TOML_SIZE);
            }
            other => panic!("expected FileTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn read_toml_string_accepts_file_at_default_limit() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("at_limit.toml");
        // Exactly at the limit (no overflow).
        let body = "x".repeat(DEFAULT_MAX_TOML_SIZE as usize);
        std::fs::write(&path, &body).unwrap();
        let out = read_toml_string(&path).expect("file at limit should succeed");
        assert_eq!(out.len(), body.len());
    }
}
