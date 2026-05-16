//! `jm doctor` — validate a `<root>` tree's TOML files and structural
//! invariants before `render`/`submit`.

pub mod checks;
pub mod report;

pub use report::{DoctorReport, Finding, Severity};

use std::path::{Path, PathBuf};

use crate::error::JobManagerError;

/// Return every immediate `<root>/<dir>/` that contains a `flow.toml`,
/// sorted by directory name for deterministic output. The directory name
/// is *not* validated as a UUID here — that is a structural check.
pub fn flow_dirs(root: &Path) -> Result<Vec<PathBuf>, JobManagerError> {
    let mut out = Vec::new();
    let rd = std::fs::read_dir(root).map_err(|source| JobManagerError::Io {
        path: root.to_path_buf(),
        source,
    })?;
    for entry in rd {
        let entry = entry.map_err(|source| JobManagerError::Io {
            path: root.to_path_buf(),
            source,
        })?;
        let p = entry.path();
        if p.is_dir() && p.join("flow.toml").is_file() {
            out.push(p);
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn flow_dirs_lists_only_dirs_with_flow_toml_sorted() {
        let d = tempdir().unwrap();
        let root = d.path();
        std::fs::create_dir_all(root.join("bbb")).unwrap();
        std::fs::write(root.join("bbb/flow.toml"), "uuid=1").unwrap();
        std::fs::create_dir_all(root.join("aaa")).unwrap();
        std::fs::write(root.join("aaa/flow.toml"), "uuid=1").unwrap();
        std::fs::create_dir_all(root.join("nope")).unwrap(); // no flow.toml
        std::fs::write(root.join("common.toml"), "x=1").unwrap(); // not a dir

        let dirs = flow_dirs(root).unwrap();
        let names: Vec<_> = dirs
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["aaa", "bbb"]);
    }
}
