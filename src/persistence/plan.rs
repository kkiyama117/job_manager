//! plan.toml の atomic rename I/O (SP-1 の flow_io と同じパターン)。

use std::path::Path;

use crate::error::JobManagerError;
use crate::plan::ExperimentPlan;

/// Read an `ExperimentPlan` from a TOML file at `path`.
#[must_use = "read_plan returns the parsed ExperimentPlan; ignoring it drops the data"]
pub fn read_plan(path: &Path) -> Result<ExperimentPlan, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|e| JobManagerError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    toml::from_str(&text).map_err(|e| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Write `plan` to `path` atomically (write to `<path>.tmp` then rename).
/// Creates parent directories if missing (対称: `flow_io::write_flow`)。
pub fn write_plan(path: &Path, plan: &ExperimentPlan) -> Result<(), JobManagerError> {
    // M-4: write_flow と同じく親 dir を自動作成し、呼び側の create_dir_all 重複を解消。
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| JobManagerError::Io {
            path: parent.to_path_buf(),
            source: e,
        })?;
    }
    let text = toml::to_string_pretty(plan)?;
    // Suffix tmp file name with PID so two processes writing the same path
    // in parallel don't trample each other's intermediate state.
    let tmp = path.with_extension(format!("toml.{}.tmp", std::process::id()));
    let result = std::fs::write(&tmp, text)
        .map_err(|e| JobManagerError::Io {
            path: tmp.clone(),
            source: e,
        })
        .and_then(|()| {
            std::fs::rename(&tmp, path).map_err(|e| JobManagerError::Io {
                path: path.to_path_buf(),
                source: e,
            })
        });
    if result.is_err() {
        // L-3: write/rename どちらの失敗でも tmp が残る可能性があるため best-effort で削除。
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::JobId;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn sample_plan() -> ExperimentPlan {
        let mut params = BTreeMap::new();
        params.insert("route".into(), toml::Value::String("# B3LYP".into()));
        params.insert("nproc".into(), toml::Value::Integer(16));
        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("opt__c=0"), params);
        ExperimentPlan { jobs }
    }

    #[test]
    fn round_trip_preserves_params() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.toml");
        let p = sample_plan();
        write_plan(&path, &p).unwrap();
        let back = read_plan(&path).unwrap();
        assert_eq!(back.jobs.len(), 1);
        let params = &back.jobs[&JobId::from("opt__c=0")];
        assert_eq!(params.get("route").unwrap().as_str().unwrap(), "# B3LYP");
        assert_eq!(params.get("nproc").unwrap().as_integer().unwrap(), 16);
    }

    #[test]
    fn read_missing_returns_io_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope.toml");
        let e = read_plan(&path).unwrap_err();
        assert!(matches!(e, JobManagerError::Io { .. }));
    }

    #[test]
    fn round_trip_preserves_multiple_jobs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.toml");
        let mut jobs = BTreeMap::new();
        for i in 0..3 {
            let jid = JobId::from(format!("opt__c={i}"));
            let mut params = BTreeMap::new();
            params.insert("idx".into(), toml::Value::Integer(i as i64));
            jobs.insert(jid, params);
        }
        let p = ExperimentPlan { jobs };
        write_plan(&path, &p).unwrap();
        let back = read_plan(&path).unwrap();
        assert_eq!(back.jobs.len(), 3);
    }

    #[test]
    fn deny_unknown_fields_rejects_extra_top_level() {
        let bad = r##"
extra = "field"

[jobs."opt"]
route = "# x"
"##;
        let result: Result<ExperimentPlan, _> = toml::from_str(bad);
        assert!(result.is_err());
    }

    #[test]
    fn write_creates_parent_dirs() {
        // M-4: write_flow と対称に、親ディレクトリを自動作成する。
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a/b/c");
        let path = nested.join("plan.toml");
        let p = sample_plan();
        write_plan(&path, &p).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn write_plan_cleans_up_tmp_on_rename_failure() {
        // L-3: rename 失敗時に .toml.<pid>.tmp が残らないことを検証。
        // target が既存ディレクトリだと rename(file, dir) は失敗するので、それで誘発する。
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.toml");
        std::fs::create_dir_all(&path).unwrap();
        let p = sample_plan();
        let result = write_plan(&path, &p);
        assert!(result.is_err());
        let leaks: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| {
                let p = e.ok()?.path();
                let is_tmp = p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|n| n.ends_with(".tmp"));
                if is_tmp { Some(p) } else { None }
            })
            .collect();
        assert!(leaks.is_empty(), "tmp leaked: {leaks:?}");
    }

    #[test]
    fn atomic_rename_replaces_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.toml");
        std::fs::write(&path, "existing = 1").unwrap();
        let p = sample_plan();
        write_plan(&path, &p).unwrap();
        let back = read_plan(&path).unwrap();
        assert_eq!(back.jobs.len(), 1);
    }
}
