//! Path resolution for the `<root>/<flow_uuid>/...` layout.
//!
//! Layout invariant:
//!
//! ```text
//! <root>/                      <- PathResolver.root
//! └── <flow_uuid>/             <- flow_dir(&flow.uuid)
//!     ├── flow.toml            <- user-authored JobFlow TOML
//!     ├── plan.toml            <- user-authored ExperimentPlan TOML
//!     └── .jm/                 <- jm_dir(&flow.uuid); program-managed
//!         ├── flow.effective.toml  <- materialized snapshot
//!         └── <JobId>/         <- job_dir(&flow.uuid, &job_id)
//!             ├── batch.bash   <- rendered SBATCH script
//!             ├── status.toml  <- per-Job status (this crate, atomic write)
//!             └── slurm-*.out/err  <- SLURM stdout/stderr
//! ```
//!
//! Pure: no filesystem I/O. Just deterministic path string composition.

use std::path::{Path, PathBuf};

use gaussian_job_shared::entities::workflow::JobId;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PathResolver {
    root: PathBuf,
}

impl PathResolver {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `<root>/<flow_uuid>/`. This is the canonical on-disk folder for a
    /// `JobFlow`; D2 no longer stores it as a field, so callers should
    /// always derive it from the flow's `uuid` via this resolver.
    pub fn flow_dir(&self, flow_uuid: &Uuid) -> PathBuf {
        self.root.join(flow_uuid.to_string())
    }

    pub fn flow_toml(&self, flow_uuid: &Uuid) -> PathBuf {
        self.flow_dir(flow_uuid).join("flow.toml")
    }

    pub fn plan_toml(&self, flow_uuid: &Uuid) -> PathBuf {
        self.flow_dir(flow_uuid).join("plan.toml")
    }

    /// 将来、ユーザーが experiment authoring の Python script を flow dir に保存
    /// したい場合の慣用 path。SP-2 では使わない (SP-2 は experiment.toml DSL を
    /// 実装しないため)。
    pub fn experiment_toml(&self, flow_uuid: &Uuid) -> PathBuf {
        self.flow_dir(flow_uuid).join("experiment.toml")
    }

    /// `<flow_dir>/.jm/` — hidden subdirectory holding all program-managed
    /// files (snapshot, batch.bash, status, slurm-*.out/err). User-authored
    /// `flow.toml` and `plan.toml` live one level up.
    pub fn jm_dir(&self, flow_uuid: &Uuid) -> PathBuf {
        self.flow_dir(flow_uuid).join(".jm")
    }

    /// `<flow_dir>/.jm/flow.effective.toml` — materialized snapshot of the
    /// JobFlow (all defaults resolved). Written by `submit`/`render`, read
    /// by `tick`/`show` (common 不要).
    pub fn flow_effective_toml(&self, flow_uuid: &Uuid) -> PathBuf {
        self.jm_dir(flow_uuid).join("flow.effective.toml")
    }

    /// `<flow_dir>/.jm/<JobId>/` — D2's per-Job folder, now nested under
    /// the program-managed `.jm/` directory.
    pub fn job_dir(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
        self.jm_dir(flow_uuid).join(&job_id.0)
    }

    /// `<job_dir>/status.toml` — owned by job-manager. No dot prefix since
    /// `.jm/` already hides the whole tree from casual `ls`.
    pub fn status_file(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
        self.job_dir(flow_uuid, job_id).join("status.toml")
    }

    pub fn common_toml(&self) -> PathBuf {
        self.root.join("common.toml")
    }

    /// `<job_dir>/batch.bash` — the rendered batch script for submission.
    pub fn batch_bash(&self, flow_uuid: &Uuid, jid: &JobId) -> PathBuf {
        self.job_dir(flow_uuid, jid).join("batch.bash")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn sample_uuid() -> Uuid {
        Uuid::from_str("01997cdc-0000-7000-8000-000000000000").unwrap()
    }

    #[test]
    fn flow_dir_joins_uuid_under_root() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        assert_eq!(r.flow_dir(&u), PathBuf::from(format!("/work/{u}")));
    }

    #[test]
    fn flow_toml_appends_flow_toml_filename() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        assert_eq!(
            r.flow_toml(&u),
            PathBuf::from(format!("/work/{u}/flow.toml"))
        );
    }

    #[test]
    fn job_dir_under_jm_subdir() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        let j = JobId::from("post");
        assert_eq!(
            r.job_dir(&u, &j),
            PathBuf::from(format!("/work/{u}/.jm/post"))
        );
    }

    #[test]
    fn status_file_lives_inside_jm_job_dir_without_dot_prefix() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        let j = JobId::from("g16");
        assert_eq!(
            r.status_file(&u, &j),
            PathBuf::from(format!("/work/{u}/.jm/g16/status.toml"))
        );
    }

    #[test]
    fn plan_toml_path_under_flow_dir() {
        let r = PathResolver::new(PathBuf::from("/root"));
        let uuid = Uuid::parse_str("0193a8c0-0000-7000-8000-000000000000").unwrap();
        let p = r.plan_toml(&uuid);
        assert!(p.ends_with("plan.toml"));
        assert!(p.starts_with("/root"));
    }

    #[test]
    fn experiment_toml_path_under_flow_dir() {
        let r = PathResolver::new(PathBuf::from("/root"));
        let uuid = Uuid::nil();
        let p = r.experiment_toml(&uuid);
        assert!(p.ends_with("experiment.toml"));
    }

    #[test]
    fn common_toml_returns_root_common_toml() {
        let r = PathResolver::new("/work");
        assert_eq!(
            r.common_toml(),
            std::path::PathBuf::from("/work/common.toml")
        );
    }

    #[test]
    fn batch_bash_returns_jm_job_dir_batch_bash() {
        let r = PathResolver::new("/work");
        let uuid = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
        let jid = JobId("opt__a=0".to_string());
        let p = r.batch_bash(&uuid, &jid);
        assert!(
            p.ends_with("01997cdc-0000-7000-8000-000000000000/.jm/opt__a=0/batch.bash"),
            "p = {}",
            p.display()
        );
    }

    #[test]
    fn flow_effective_toml_lives_under_jm_dir() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        assert_eq!(
            r.flow_effective_toml(&u),
            PathBuf::from(format!("/work/{u}/.jm/flow.effective.toml"))
        );
    }

    #[test]
    fn jm_dir_returns_flow_dir_dot_jm() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        assert_eq!(r.jm_dir(&u), PathBuf::from(format!("/work/{u}/.jm")));
    }
}
