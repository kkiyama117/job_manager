//! `jm doctor` — validate a `<root>` tree's TOML files and structural
//! invariants before `render`/`submit`.

pub mod checks;
pub mod report;

pub use report::{DoctorReport, Finding, Severity};

use std::path::{Path, PathBuf};

use gaussian_job_shared::config::common::CommonConfig;

use crate::error::JobManagerError;
use crate::persistence::{read_common, synth_empty_common};

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

/// What `run_doctor` validates.
#[derive(Debug, Clone)]
pub enum DoctorScope {
    /// Every `<root>/<dir>/` containing a `flow.toml`.
    All,
    /// A single flow directory `<root>/<uuid>/`.
    Flow(uuid::Uuid),
}

/// Validate `root` per `scope`. Reads `<root>/common.toml` once (or
/// synthesizes an empty one) and runs every check against each in-scope
/// flow directory. Pure: no stdout, no process exit — the caller decides.
pub fn run_doctor(root: &Path, scope: &DoctorScope) -> Result<DoctorReport, JobManagerError> {
    let mut report = DoctorReport::new();

    let common_path = root.join("common.toml");
    let common: CommonConfig = if common_path.exists() {
        match read_common(&common_path) {
            Ok(c) => {
                report.push(Finding::pass(
                    &common_path,
                    "common.toml parses as CommonConfig",
                ));
                c
            }
            Err(e) => {
                report.push(Finding::fail(&common_path, e.to_string()));
                synth_empty_common()
            }
        }
    } else {
        report.push(Finding::pass(
            &common_path,
            "no common.toml (optional) — ok",
        ));
        synth_empty_common()
    };

    let dirs = match scope {
        DoctorScope::All => flow_dirs(root)?,
        DoctorScope::Flow(u) => vec![root.join(u.to_string())],
    };

    for dir in dirs {
        let (flow, fs) = checks::check_flow(&dir, &common);
        report.extend(fs);
        report.extend(checks::check_plan(&dir));
        report.extend(checks::check_flow_effective(&dir));
        if let Some(flow) = flow {
            let ids: Vec<String> = flow.jobs.keys().map(|j| j.0.clone()).collect();
            report.extend(checks::check_status_files(
                &dir,
                ids.iter().map(String::as_str),
            ));
            report.extend(checks::check_uuid_matches_dir(&dir, &flow));
            report.extend(checks::check_parents_resolve(&dir, &flow));
            report.extend(checks::check_plan_coverage(&dir, &flow));
        }
    }
    Ok(report)
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

    #[test]
    fn run_doctor_all_reports_pass_for_clean_tree() {
        let d = tempdir().unwrap();
        let root = d.path();
        let fdir = root.join("01999999-0000-7000-8000-000000000000");
        std::fs::create_dir_all(&fdir).unwrap();
        std::fs::write(
            fdir.join("flow.toml"),
            "uuid = \"01999999-0000-7000-8000-000000000000\"\n\
             created_at = \"2026-05-15T00:00:00Z\"\n\
             [jobs.opt]\nprogram = \"echo\"\nbody = \"x\\n\"\n\
             [jobs.opt.config]\npartition = \"long\"\n",
        )
        .unwrap();
        std::fs::write(fdir.join("plan.toml"), "[jobs.opt]\nnproc = 1\n").unwrap();

        let report = run_doctor(root, &DoctorScope::All).unwrap();
        assert!(!report.has_fail(), "report:\n{report}");
        assert!(report.count(Severity::Pass) >= 3);
    }

    #[test]
    fn run_doctor_flags_uuid_mismatch_as_fail() {
        let d = tempdir().unwrap();
        let root = d.path();
        let fdir = root.join("01999999-0000-7000-8000-000000000000");
        std::fs::create_dir_all(&fdir).unwrap();
        std::fs::write(
            fdir.join("flow.toml"),
            "uuid = \"01888888-0000-7000-8000-000000000000\"\n\
             created_at = \"2026-05-15T00:00:00Z\"\n\
             [jobs.opt]\nprogram = \"echo\"\nbody = \"x\\n\"\n\
             [jobs.opt.config]\npartition = \"long\"\n",
        )
        .unwrap();
        std::fs::write(fdir.join("plan.toml"), "[jobs.opt]\n").unwrap();

        let report = run_doctor(root, &DoctorScope::All).unwrap();
        assert!(report.has_fail(), "report:\n{report}");
    }
}
