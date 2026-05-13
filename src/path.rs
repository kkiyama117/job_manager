//! Path resolution for the `<root>/<flow_uuid>/<JobId>/...` layout.
//!
//! Layout invariant:
//!
//! ```text
//! <root>/                  <- PathResolver.root
//! └── <flow_uuid>/         <- flow_dir(&flow.uuid)
//!     ├── flow.toml        <- JobFlow TOML
//!     └── <JobId>/         <- job_dir(&flow.uuid, &job_id)
//!         └── .status.toml <- per-Job status (this crate, atomic write)
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

    /// `<flow_dir>/<JobId>/` — D2's per-Job folder. SLURM stdout/stderr,
    /// rendered .bash, input files all live here. No `jobs/` middle layer.
    pub fn job_dir(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
        self.flow_dir(flow_uuid).join(&job_id.0)
    }

    /// `<job_dir>/.status.toml` — hidden file owned by job-manager.
    /// Dot-prefix keeps it from colliding with SLURM outputs like
    /// `slurm-<jobid>.out` and from grammar-layer files like `input.gjf`.
    pub fn status_file(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
        self.job_dir(flow_uuid, job_id).join(".status.toml")
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
    fn job_dir_is_flow_dir_joined_with_job_id_no_jobs_layer() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        let j = JobId::from("post");
        assert_eq!(r.job_dir(&u, &j), PathBuf::from(format!("/work/{u}/post")));
    }

    #[test]
    fn status_file_lives_inside_job_dir_as_dot_status_toml() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        let j = JobId::from("g16");
        assert_eq!(
            r.status_file(&u, &j),
            PathBuf::from(format!("/work/{u}/g16/.status.toml"))
        );
    }
}
