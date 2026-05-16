//! Shared SLURM log-path validation.
//!
//! `sbatch` does **not** create the `--output`/`--error` parent
//! directory; if it is missing, SLURM fails to open the file and the job
//! dies as `FAILED` before its body runs — with no useful sacct reason.
//! Both the `jm doctor` advisory check and the `jm submit` fail-fast
//! preflight use these helpers so their semantics never drift apart.

use std::path::{Path, PathBuf};

use slurm_async_runner::entities::slurm::SlurmJobConfig;

/// Deepest ancestor of `p`'s parent directory that contains no `%`
/// (SLURM filename token) component. `sbatch` expands `%x`/`%j` only when
/// it creates the file, so only the token-free prefix is a real
/// directory we can stat. Examples: `/a/b/logs/%x.%j.out` → `/a/b/logs`;
/// `/a/%j/x.out` → `/a`; `x.out` → `""`.
pub fn token_free_log_dir(p: &Path) -> PathBuf {
    let parent = p.parent().unwrap_or_else(|| Path::new(""));
    let mut acc = PathBuf::new();
    for comp in parent.components() {
        if comp.as_os_str().to_string_lossy().contains('%') {
            break;
        }
        acc.push(comp);
    }
    acc
}

/// For each of `cfg`'s `log_stdout`/`log_stderr` whose parent directory
/// does **not** exist, return `(field_name, resolved_dir)`.
///
/// Relative log dirs are resolved against `root` (the most stable
/// reference callers have; `sbatch` itself resolves them against the
/// `jm submit` cwd, which is not known here). A filename-only value has
/// no directory component and is skipped (the file lands in a dir that,
/// by construction, exists).
pub fn missing_log_dirs(root: &Path, cfg: &SlurmJobConfig) -> Vec<(&'static str, PathBuf)> {
    let mut out = Vec::new();
    for (field, val) in [
        ("log_stdout", cfg.log_stdout.as_deref()),
        ("log_stderr", cfg.log_stderr.as_deref()),
    ] {
        let Some(p) = val else { continue };
        let dir = token_free_log_dir(p);
        if dir.as_os_str().is_empty() {
            continue;
        }
        let resolved = if dir.is_absolute() {
            dir
        } else {
            root.join(&dir)
        };
        if !resolved.exists() {
            out.push((field, resolved));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn cfg_with(stdout: Option<&str>, stderr: Option<&str>) -> SlurmJobConfig {
        let mut c = crate::persistence::synth_empty_common().slurm_default;
        c.partition = "long".to_string();
        c.log_stdout = stdout.map(PathBuf::from);
        c.log_stderr = stderr.map(PathBuf::from);
        c
    }

    #[test]
    fn token_free_strips_tokens_to_real_dir() {
        assert_eq!(
            token_free_log_dir(Path::new("/a/b/logs/%x.%j.out")),
            PathBuf::from("/a/b/logs")
        );
        assert_eq!(
            token_free_log_dir(Path::new("/a/%j/x.out")),
            PathBuf::from("/a")
        );
        assert!(
            token_free_log_dir(Path::new("x.out"))
                .as_os_str()
                .is_empty()
        );
    }

    #[test]
    fn absolute_missing_dir_is_reported() {
        let d = tempdir().unwrap();
        let missing = d.path().join("no_such/logs/%j.out");
        let cfg = cfg_with(Some(missing.to_str().unwrap()), None);
        let got = missing_log_dirs(d.path(), &cfg);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "log_stdout");
        assert_eq!(got[0].1, d.path().join("no_such/logs"));
    }

    #[test]
    fn relative_is_resolved_against_root() {
        let d = tempdir().unwrap();
        // missing under root → reported with the root-joined path
        let cfg = cfg_with(Some("rel/logs/%j.out"), None);
        let got = missing_log_dirs(d.path(), &cfg);
        assert_eq!(got, vec![("log_stdout", d.path().join("rel/logs"))]);

        // exists under root → not reported
        std::fs::create_dir_all(d.path().join("rel/logs")).unwrap();
        assert!(missing_log_dirs(d.path(), &cfg).is_empty());
    }

    #[test]
    fn existing_and_unset_dirs_are_not_reported() {
        let d = tempdir().unwrap();
        // log_stdout points at the tempdir itself (exists); log_stderr unset
        let ok = d.path().join("%j.out");
        let cfg = cfg_with(Some(ok.to_str().unwrap()), None);
        assert!(missing_log_dirs(d.path(), &cfg).is_empty());
    }

    #[test]
    fn both_fields_reported_independently() {
        let d = tempdir().unwrap();
        let so = d.path().join("a/%j.out");
        let se = d.path().join("b/%j.err");
        let cfg = cfg_with(Some(so.to_str().unwrap()), Some(se.to_str().unwrap()));
        let got = missing_log_dirs(d.path(), &cfg);
        assert_eq!(got.len(), 2);
        assert!(got.iter().any(|(f, _)| *f == "log_stdout"));
        assert!(got.iter().any(|(f, _)| *f == "log_stderr"));
    }
}
