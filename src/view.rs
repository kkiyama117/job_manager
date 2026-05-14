//! Per-Job facade: paths + status getter.

use std::path::PathBuf;

use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId};

use crate::error::JobManagerError;
use crate::job::run::JobRun;
use crate::persistence::job_run::read_job_run;
use crate::persistence::path::PathResolver;

#[derive(Debug)]
pub struct CalcView<'a> {
    pub flow: &'a JobFlow,
    pub job_id: JobId,
    resolver: &'a PathResolver,
}

impl<'a> CalcView<'a> {
    pub fn new(
        flow: &'a JobFlow,
        job_id: JobId,
        resolver: &'a PathResolver,
    ) -> Result<Self, JobManagerError> {
        if !flow.jobs.contains_key(&job_id) {
            return Err(JobManagerError::JobNotFound {
                flow: flow.uuid,
                job: job_id,
            });
        }
        Ok(Self {
            flow,
            job_id,
            resolver,
        })
    }

    pub fn job(&self) -> &'a Job {
        self.flow
            .jobs
            .get(&self.job_id)
            .expect("constructor enforces presence")
    }

    pub fn status(&self) -> Result<JobRun, JobManagerError> {
        read_job_run(&self.resolver.status_file(&self.flow.uuid, &self.job_id))
    }

    pub fn job_dir(&self) -> PathBuf {
        self.resolver.job_dir(&self.flow.uuid, &self.job_id)
    }

    pub fn status_path(&self) -> PathBuf {
        self.resolver.status_file(&self.flow.uuid, &self.job_id)
    }

    /// List files directly under `job_dir()`, **excluding dot-prefixed
    /// hidden files** (so `.status.toml` does not leak into the user-
    /// facing input/output listing). Returns `Ok(vec![])` if the dir does
    /// not exist. Order: sorted by filename.
    ///
    /// Individual `DirEntry` errors (e.g. permission denied on a single
    /// file) are logged at WARN and the entry is skipped; an unreadable
    /// directory still surfaces as `Err`.
    pub fn files(&self) -> Result<Vec<PathBuf>, JobManagerError> {
        let d = self.job_dir();
        if !d.exists() {
            return Ok(vec![]);
        }
        let mut out: Vec<PathBuf> = std::fs::read_dir(&d)
            .map_err(|source| JobManagerError::Io {
                path: d.clone(),
                source,
            })?
            .filter_map(|entry| match entry {
                Ok(e) => Some(e),
                Err(err) => {
                    log::warn!("skipping dir entry in {}: {err}", d.display());
                    None
                }
            })
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| !n.starts_with('.'))
                    .unwrap_or(true)
            })
            .collect();
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobSpec, Program};
    use slurm_async_runner::entities::slurm::SlurmJobConfig;
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn cfg() -> SlurmJobConfig {
        SlurmJobConfig {
            partition: "long".into(),
            time_limit: None,
            log_stdout: None,
            log_stderr: None,
            comment: None,
            job_name: None,
            array_spec: None,
            dependency: None,
            mail_user: None,
            mail_types: None,
            resource_spec: None,
        }
    }

    fn single_job_flow(uuid: Uuid) -> JobFlow {
        let mut jobs = BTreeMap::new();
        jobs.insert(
            JobId::from("g16"),
            Job {
                spec: JobSpec {
                    program: Program::from("g16"),
                    config: cfg(),
                    body: "".into(),
                },
                parents: vec![],
            },
        );
        JobFlow {
            uuid,
            created_at: Utc::now(),
            tags: BTreeMap::new(),
            jobs,
        }
    }

    #[test]
    fn rejects_unknown_job_id() {
        let dir = TempDir::new().unwrap();
        let r = PathResolver::new(dir.path());
        let f = single_job_flow(Uuid::now_v7());
        let err = CalcView::new(&f, JobId::from("nope"), &r).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn paths_use_resolver_layout() {
        let dir = TempDir::new().unwrap();
        let r = PathResolver::new(dir.path());
        let uuid = Uuid::now_v7();
        let f = single_job_flow(uuid);
        let v = CalcView::new(&f, JobId::from("g16"), &r).unwrap();
        assert_eq!(v.job_dir(), r.job_dir(&uuid, &JobId::from("g16")));
        assert_eq!(v.status_path(), r.status_file(&uuid, &JobId::from("g16")));
    }

    #[test]
    fn files_returns_empty_when_dir_missing() {
        let dir = TempDir::new().unwrap();
        let r = PathResolver::new(dir.path());
        let f = single_job_flow(Uuid::now_v7());
        let v = CalcView::new(&f, JobId::from("g16"), &r).unwrap();
        assert!(v.files().unwrap().is_empty());
    }

    #[test]
    fn status_reads_from_resolver_path() {
        let dir = TempDir::new().unwrap();
        let r = PathResolver::new(dir.path());
        let uuid = Uuid::now_v7();
        let f = single_job_flow(uuid);
        let run = crate::job::run::JobRun {
            lifecycle: crate::job::lifecycle::Lifecycle::Queued,
            updated_at: Utc::now(),
            slurm_jobid: Some(7),
            slurm_status: None,
            note: None,
        };
        crate::persistence::job_run::write_job_run(
            &r.status_file(&uuid, &JobId::from("g16")),
            &run,
        )
        .unwrap();
        let v = CalcView::new(&f, JobId::from("g16"), &r).unwrap();
        let got = v.status().unwrap();
        assert_eq!(got.lifecycle, crate::job::lifecycle::Lifecycle::Queued);
    }
}
