//! Doctor findings & aggregated report.
//!
//! A `Finding` is one check outcome at one path. `DoctorReport` collects
//! them and renders a stable, greppable summary. `Severity::Fail` is the
//! only level that makes `jm doctor` exit non-zero.

use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Pass,
    Warn,
    Fail,
}

impl Severity {
    fn label(self) -> &'static str {
        match self {
            Severity::Pass => "PASS",
            Severity::Warn => "WARN",
            Severity::Fail => "FAIL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Finding {
    pub severity: Severity,
    pub path: PathBuf,
    pub message: String,
}

impl Finding {
    pub fn pass(path: &Path, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Pass,
            path: path.to_path_buf(),
            message: message.into(),
        }
    }
    pub fn warn(path: &Path, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warn,
            path: path.to_path_buf(),
            message: message.into(),
        }
    }
    pub fn fail(path: &Path, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Fail,
            path: path.to_path_buf(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DoctorReport {
    pub findings: Vec<Finding>,
}

impl DoctorReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, f: Finding) {
        self.findings.push(f);
    }

    pub fn extend(&mut self, fs: impl IntoIterator<Item = Finding>) {
        self.findings.extend(fs);
    }

    pub fn count(&self, sev: Severity) -> usize {
        self.findings.iter().filter(|f| f.severity == sev).count()
    }

    /// True iff at least one `Fail` finding exists.
    pub fn has_fail(&self) -> bool {
        self.count(Severity::Fail) > 0
    }
}

impl fmt::Display for DoctorReport {
    fn fmt(&self, fm: &mut fmt::Formatter<'_>) -> fmt::Result {
        for f in &self.findings {
            writeln!(
                fm,
                "{:<4}  {}  {}",
                f.severity.label(),
                f.path.display(),
                f.message
            )?;
        }
        writeln!(
            fm,
            "summary: {} pass, {} warn, {} fail",
            self.count(Severity::Pass),
            self.count(Severity::Warn),
            self.count(Severity::Fail),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn has_fail_only_when_a_fail_finding_present() {
        let mut r = DoctorReport::new();
        r.push(Finding::pass(Path::new("/a"), "ok"));
        r.push(Finding::warn(Path::new("/b"), "meh"));
        assert!(!r.has_fail());
        r.push(Finding::fail(Path::new("/c"), "boom"));
        assert!(r.has_fail());
        assert_eq!(r.count(Severity::Pass), 1);
        assert_eq!(r.count(Severity::Warn), 1);
        assert_eq!(r.count(Severity::Fail), 1);
    }

    #[test]
    fn display_renders_summary_line() {
        let mut r = DoctorReport::new();
        r.push(Finding::fail(Path::new("/x"), "bad"));
        let s = r.to_string();
        assert!(s.contains("FAIL  /x  bad"), "got: {s}");
        assert!(s.contains("summary: 0 pass, 0 warn, 1 fail"), "got: {s}");
    }
}
