//! On-disk flow collection + filter projections for `jm ls`.
//!
//! Read-only: walks flows, reads each job's `status.toml`, and projects
//! the result into job/flow rows. No SLURM, no `tick`.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::{JobFlow, JobId};

use crate::concurrency::parallelism;
use crate::error::JobManagerError;
use crate::persistence::path::PathResolver;
use crate::search::{SearchFilter, matches};

use super::{CollectedFlow, DisplayLifecycle, FlowRow, JobRow, aggregate_flow_status};

/// Walk every flow under `root`, read each job's on-disk status, and
/// return flows newest-first (`flow.created_at` desc). Read-only: no
/// SLURM, no `tick`. Missing/unreadable `status.toml` => `None`
/// (Pending). A flow whose `flow.toml` fails to parse is logged to
/// stderr and skipped (listing robustness; `jm doctor` is strict).
/// `filter` is intentionally unused here — filtering happens in the
/// pure projections so a single `collect` serves all three views.
pub async fn collect(
    root: &std::path::Path,
    common: Arc<CommonConfig>,
    _filter: &SearchFilter,
) -> Result<Vec<CollectedFlow>, JobManagerError> {
    let resolver = PathResolver::new(root);
    let flows_stream = crate::walk::walk_flows(root, common);
    let mut flows_stream = std::pin::pin!(flows_stream);
    let mut flows: Vec<JobFlow> = Vec::new();
    while let Some(item) = flows_stream.next().await {
        match item {
            Ok(f) => flows.push(f),
            // CLI-facing diagnostic: jm installs no tracing subscriber, so write to stderr directly.
            Err(e) => eprintln!("jm ls: skipping unreadable flow: {e}"),
        }
    }

    let p = parallelism();
    let results: Vec<Result<CollectedFlow, JobManagerError>> = stream::iter(flows)
        .map(|flow| {
            let resolver = resolver.clone();
            async move {
                let uuid = flow.uuid;
                let job_ids: Vec<JobId> = flow.jobs.keys().cloned().collect();
                let mut statuses = BTreeMap::new();
                for jid in job_ids {
                    let path = resolver.status_file(&uuid, &jid);
                    let run = if path.exists() {
                        let p2 = path.clone();
                        match tokio::task::spawn_blocking(move || {
                            crate::persistence::read_job_run(&p2)
                        })
                        .await
                        {
                            Ok(Ok(jr)) => Some(jr),
                            Ok(Err(e)) => {
                                eprintln!(
                                    "jm ls: unreadable status {} ({e}); treating as pending",
                                    path.display()
                                );
                                None
                            }
                            Err(join) => {
                                return Err(JobManagerError::JoinFailed {
                                    op: "read_job_run",
                                    source: join,
                                });
                            }
                        }
                    } else {
                        None
                    };
                    statuses.insert(jid, run);
                }
                Ok(CollectedFlow { flow, statuses })
            }
        })
        .buffer_unordered(p)
        .collect::<Vec<_>>()
        .await;

    let mut collected: Vec<CollectedFlow> = results.into_iter().collect::<Result<Vec<_>, _>>()?;
    collected.sort_by_key(|b| std::cmp::Reverse(b.flow.created_at));
    Ok(collected)
}

fn is_default_filter(f: &SearchFilter) -> bool {
    f.program.is_none()
        && f.tags.is_empty()
        && f.status.is_empty()
        && f.flow_uuid_prefix.is_none()
        && f.created_after.is_none()
        && f.created_before.is_none()
        && f.slurm_jobid.is_none()
        && f.job_id.is_none()
}

/// Project to job rows: jobs passing `filter`, input order (newest-first)
/// x topological job order. `limit` caps the row count.
pub fn job_rows(
    collected: &[CollectedFlow],
    filter: &SearchFilter,
    limit: Option<usize>,
) -> Vec<JobRow> {
    let mut out = Vec::new();
    for cf in collected {
        let order = cf.topo_or_key_order();
        for jid in order {
            let job = &cf.flow.jobs[&jid];
            let status = cf.statuses.get(&jid).and_then(|o| o.as_ref());
            if !matches(&cf.flow, &jid, job, status, filter) {
                continue;
            }
            out.push(JobRow {
                flow_uuid: cf.flow.uuid,
                job_id: jid.0.clone(),
                status: cf.job_display(&jid),
                slurm_jobid: status.and_then(|s| s.slurm_jobid),
                program: job.spec.program.0.clone(),
                updated_at: status.map(|s| s.updated_at),
                created_at: cf.flow.created_at,
            });
            if let Some(n) = limit
                && out.len() >= n
            {
                return out;
            }
        }
    }
    out
}

/// Flows where **any** job passes `filter` (same inclusion rule as the
/// tree forest). Job-less flows are included only under the default
/// filter (no predicate can otherwise match zero jobs).
pub fn matched_flows<'a>(
    collected: &'a [CollectedFlow],
    filter: &SearchFilter,
    limit: Option<usize>,
) -> Vec<&'a CollectedFlow> {
    let mut out = Vec::new();
    for cf in collected {
        let include = if cf.flow.jobs.is_empty() {
            is_default_filter(filter)
        } else {
            cf.flow.jobs.iter().any(|(jid, job)| {
                let status = cf.statuses.get(jid).and_then(|o| o.as_ref());
                matches(&cf.flow, jid, job, status, filter)
            })
        };
        if include {
            out.push(cf);
            if let Some(n) = limit
                && out.len() >= n
            {
                return out;
            }
        }
    }
    out
}

/// Project matched flows to flow rows.
pub fn flow_rows(
    collected: &[CollectedFlow],
    filter: &SearchFilter,
    limit: Option<usize>,
) -> Vec<FlowRow> {
    use crate::job::lifecycle::Lifecycle::Success;
    matched_flows(collected, filter, limit)
        .into_iter()
        .map(|cf| {
            let displays: Vec<DisplayLifecycle> =
                cf.flow.jobs.keys().map(|k| cf.job_display(k)).collect();
            let done = displays
                .iter()
                .filter(|d| **d == DisplayLifecycle::Real(Success))
                .count();
            FlowRow {
                flow_uuid: cf.flow.uuid,
                total: cf.flow.jobs.len(),
                done,
                status: aggregate_flow_status(&displays),
                created_at: cf.flow.created_at,
            }
        })
        .collect()
}
