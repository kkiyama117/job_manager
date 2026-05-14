//! FlowRunner — orchestrates submit / tick / render_only for a FlowRun.
//!
//! Responsibilities:
//! - `submit`: topological order → per-job render batch.bash → (if !dry_run)
//!   build SbatchCmd + dependency → Executor::submit → write JobRun(Queued)
//! - `tick`: read all .status.toml → filter non-terminal → query SLURM →
//!   decide_transition (passing parent lifecycles) → write updated JobRun
//! - `render_only`: same as submit render step but never calls executor

use std::collections::BTreeMap;
use std::path::PathBuf;

use gaussian_job_shared::entities::workflow::JobId;

use crate::error::JobManagerError;
use crate::flow::FlowRun;
use crate::job::Lifecycle;
use crate::job::run::JobRun;
use crate::jobid::parse_job_id;
use crate::persistence::{PathResolver, read_job_run, write_job_run};
use crate::render::render_batch_bash;
use crate::runner::transition::{Decision, TickResult, decide_transition};
use crate::slurm::dependency;
use crate::slurm::executor::Executor;
use crate::slurm::querier::Querier;
use slurm_async_runner::SbatchCmd;

/// Run the synchronous `write_job_run` on a blocking-pool thread so we don't
/// stall the tokio runtime. `path` and `run` are owned to satisfy `'static`.
async fn async_write_job_run(path: PathBuf, run: JobRun) -> Result<(), JobManagerError> {
    tokio::task::spawn_blocking(move || write_job_run(&path, &run))
        .await
        .map_err(|e| JobManagerError::Other(format!("write_job_run blocking task: {e}")))?
}

/// Async counterpart to the synchronous `read_job_run`.
async fn async_read_job_run(path: PathBuf) -> Result<JobRun, JobManagerError> {
    tokio::task::spawn_blocking(move || read_job_run(&path))
        .await
        .map_err(|e| JobManagerError::Other(format!("read_job_run blocking task: {e}")))?
}

/// Write `batch.bash` atomically: open a PID-suffixed tmp file in the
/// same directory, write the body, **fsync**, restrict permissions to
/// owner-only on Unix, then rename over `path`. Prevents (a) a window in
/// which another process can race-read or modify the script between the
/// initial write and `sbatch` picking it up, and (b) a zero-length file
/// surviving a kernel panic / power loss between write and rename.
async fn atomic_write_batch_bash(
    path: &std::path::Path,
    body: &[u8],
) -> Result<(), JobManagerError> {
    use tokio::io::AsyncWriteExt;

    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| JobManagerError::Other(format!("invalid batch path: {}", path.display())))?;
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    // Use the same pid + nanos + thread-id suffix scheme as persistence/
    // so concurrent writers in the same process can't trample each other.
    let suffix = crate::persistence::tmp_extension();
    let tmp = parent.join(format!(".{file_name}.{suffix}"));

    let io_err = |source| JobManagerError::Io {
        path: tmp.clone(),
        source,
    };

    let write_and_sync = async {
        let mut f = tokio::fs::File::create(&tmp).await.map_err(io_err)?;
        f.write_all(body).await.map_err(io_err)?;
        f.sync_all().await.map_err(io_err)?;
        Ok::<(), JobManagerError>(())
    };

    if let Err(e) = write_and_sync.await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // chmod 0600 is a defense-in-depth measure to keep secrets in the
        // rendered script from being world-readable. On some filesystems
        // (overlayfs / certain NFS / FAT-on-Linux / sandboxed CI) the call
        // can fail with EPERM even though the file is fine. Treat that as
        // a recoverable warning rather than aborting the whole submit:
        // the security loss is small (script lives at the default mode)
        // and the alternative is an unrunnable workflow.
        if let Err(e) =
            tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).await
        {
            eprintln!(
                "warning: chmod 0600 failed on {} ({e}); continuing with default permissions. \
                 If the filesystem cannot represent Unix permissions this is expected. \
                 The script may be readable by other users on this host.",
                tmp.display()
            );
        }
    }

    if let Err(e) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(JobManagerError::Io {
            path: path.to_path_buf(),
            source: e,
        });
    }
    Ok(())
}

/// FlowRunner — owns a boxed `Executor` and `Querier`, and coordinates the
/// submit / tick / render_only operations for a given `FlowRun`.
pub struct FlowRunner<'r> {
    executor: Box<dyn Executor>,
    querier: Box<dyn Querier>,
    resolver: &'r PathResolver,
}

impl<'r> FlowRunner<'r> {
    pub fn new(
        executor: Box<dyn Executor>,
        querier: Box<dyn Querier>,
        resolver: &'r PathResolver,
    ) -> Self {
        Self {
            executor,
            querier,
            resolver,
        }
    }

    /// Render batch.bash for every job (topological order), then optionally
    /// submit each job via the executor and write a `.status.toml`.
    ///
    /// Returns a map from `JobId` to the submitted SLURM jobid.
    /// When `dry_run` is `true`, returns an empty map and does NOT call
    /// `executor.submit` — only the batch.bash files are written.
    pub async fn submit(
        &self,
        fr: &FlowRun,
        dry_run: bool,
    ) -> Result<BTreeMap<JobId, u64>, JobManagerError> {
        let order = fr.topological_order()?;
        let mut submitted: BTreeMap<JobId, u64> = BTreeMap::new();

        // Defensive preseed: if a parent's status.toml already exists from
        // a previous run, populate `submitted` with its recorded slurm_jobid
        // so `dependency::build` still resolves the dependency even when the
        // parent is missing from the current topo order (e.g., removed from
        // flow.toml between runs) or when a future revision adds skip logic.
        let mut candidate_jids: std::collections::BTreeSet<JobId> =
            fr.flow.jobs.keys().cloned().collect();
        for job in fr.flow.jobs.values() {
            for edge in &job.parents {
                candidate_jids.insert(edge.from.clone());
            }
        }
        for jid in &candidate_jids {
            let path = self.resolver.status_file(&fr.flow_uuid, jid);
            if tokio::fs::try_exists(&path).await.unwrap_or(false)
                && let Ok(run) = async_read_job_run(path).await
                && let Some(jobid) = run.slurm_jobid
            {
                submitted.insert(jid.clone(), jobid);
            }
        }

        for jid in &order {
            // --- render batch.bash ---
            let params = fr.params_of(jid)?;
            let parts = parse_job_id(&jid.0).map_err(|e| JobManagerError::RenderError {
                job: jid.clone(),
                reason: e.to_string(),
            })?;
            let job = fr
                .flow
                .jobs
                .get(jid)
                .ok_or_else(|| JobManagerError::JobNotFound {
                    flow: fr.flow_uuid,
                    job: jid.clone(),
                })?;
            let script_content =
                render_batch_bash(&fr.flow_uuid, jid, &parts, params, &job.spec.body);

            let batch_path = self.resolver.batch_bash(&fr.flow_uuid, jid);
            if let Some(parent) = batch_path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| JobManagerError::Io {
                        path: parent.to_path_buf(),
                        source: e,
                    })?;
            }
            atomic_write_batch_bash(&batch_path, script_content.as_bytes()).await?;

            if dry_run {
                continue;
            }

            // --- build SbatchCmd ---
            let config = fr.effective_config(jid)?;
            let mut cmd = SbatchCmd::new(batch_path.clone());
            cmd.partition = Some(config.partition.clone());
            cmd.time_limit = config.time_limit;
            cmd.rsc = config.resource_spec.clone();
            cmd.output = config
                .log_stdout
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned());
            cmd.error = config
                .log_stderr
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned());
            cmd.job_name = config.job_name.clone();
            cmd.array_spec = config.array_spec.clone();
            cmd.mail_user = config.mail_user.clone();
            cmd.mail_types = config.mail_types.clone();
            cmd.comment = config.comment.clone();

            // Build dependency from parents + already-submitted jobids
            let parents = fr.parents_of(jid);
            let dep = dependency::build(parents, &submitted, jid)?;
            cmd.dependency = dep;

            // --- submit ---
            let slurm_jobid = self.executor.submit(cmd).await?;
            submitted.insert(jid.clone(), slurm_jobid);

            // --- write .status.toml ---
            let run = JobRun {
                lifecycle: Lifecycle::Queued,
                updated_at: chrono::Utc::now(),
                slurm_jobid: Some(slurm_jobid),
                slurm_status: None,
                note: None,
            };
            let status_path = self.resolver.status_file(&fr.flow_uuid, jid);
            async_write_job_run(status_path, run).await?;
        }

        Ok(submitted)
    }

    /// Read all `.status.toml` files, query SLURM for non-terminal jobs,
    /// apply `decide_transition` (with parent lifecycles), and write back
    /// any updated states.
    ///
    /// Returns a `TickResult` whose `transitions` map records every
    /// evaluated decision (including `NoChange`) for non-terminal jobs.
    /// Terminal jobs and jobs without a `.status.toml` file are skipped
    /// and not recorded.
    pub async fn tick(&self, fr: &FlowRun) -> Result<TickResult, JobManagerError> {
        let order = fr.topological_order()?;

        // --- collect current lifecycles ---
        let mut current: BTreeMap<JobId, JobRun> = BTreeMap::new();
        for jid in &order {
            let path = self.resolver.status_file(&fr.flow_uuid, jid);
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                let run = async_read_job_run(path).await?;
                current.insert(jid.clone(), run);
            }
        }

        // --- gather non-terminal jobids for SLURM query ---
        let jobids_to_query: Vec<u64> = current
            .values()
            .filter(|r| !r.lifecycle.is_terminal())
            .filter_map(|r| r.slurm_jobid)
            .collect();

        let slurm_statuses = if jobids_to_query.is_empty() {
            Default::default()
        } else {
            self.querier.query(&jobids_to_query).await?
        };

        // --- apply transitions in topological order ---
        let mut transitions: BTreeMap<JobId, Decision> = BTreeMap::new();
        for jid in &order {
            let run = match current.get(jid) {
                Some(r) => r,
                None => continue,
            };

            if run.lifecycle.is_terminal() {
                continue;
            }

            // Collect parent (JobId, Lifecycle) pairs so SkipDueToParent
            // can carry the actual culprit instead of a placeholder.
            let parents_with_lifecycle: Vec<(JobId, Lifecycle)> = fr
                .parents_of(jid)
                .iter()
                .filter_map(|edge| {
                    current
                        .get(&edge.from)
                        .map(|r| (edge.from.clone(), r.lifecycle))
                })
                .collect();

            let slurm_status = run.slurm_jobid.and_then(|id| slurm_statuses.get(&id));

            let decision = decide_transition(run.lifecycle, slurm_status, &parents_with_lifecycle);

            let new_lifecycle = match &decision {
                Decision::NoChange => {
                    transitions.insert(jid.clone(), decision);
                    continue;
                }
                Decision::Transition { to, .. } => *to,
                Decision::SkipDueToParent { .. } => Lifecycle::Skipped,
            };

            let new_slurm_status = match &decision {
                Decision::Transition { slurm_status, .. } => slurm_status.clone(),
                _ => None,
            };

            let updated = JobRun {
                lifecycle: new_lifecycle,
                updated_at: chrono::Utc::now(),
                slurm_jobid: run.slurm_jobid,
                slurm_status: new_slurm_status,
                note: run.note.clone(),
            };
            let path = self.resolver.status_file(&fr.flow_uuid, jid);
            async_write_job_run(path, updated.clone()).await?;

            transitions.insert(jid.clone(), decision);

            // Update local cache so subsequent jobs in topo order see the
            // updated lifecycle when computing their parent_lifecycles.
            current.insert(jid.clone(), updated);
        }

        Ok(TickResult { transitions })
    }

    /// Render batch.bash for every job without submitting.
    /// Equivalent to `submit(fr, true)`.
    pub async fn render_only(&self, fr: &FlowRun) -> Result<(), JobManagerError> {
        self.submit(fr, true).await?;
        Ok(())
    }
}
