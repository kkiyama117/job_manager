//! `jm ls` — pure projection/aggregation/formatting for cross-flow listing.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use gaussian_job_shared::entities::workflow::{JobFlow, JobId};
use serde::Serialize;
use uuid::Uuid;

use crate::job::lifecycle::Lifecycle;
use crate::job::run::JobRun;
use crate::persistence::path::PathResolver;

/// Display-time lifecycle: the 5 `Lifecycle` values plus `Pending`
/// (no `status.toml` on disk — not a real enum value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DisplayLifecycle {
    Pending,
    Real(Lifecycle),
}

impl DisplayLifecycle {
    /// Short code shown in the `ST` column.
    pub fn code(self) -> &'static str {
        match self {
            DisplayLifecycle::Pending => "PD",
            DisplayLifecycle::Real(Lifecycle::Queued) => "Q",
            DisplayLifecycle::Real(Lifecycle::Running) => "R",
            DisplayLifecycle::Real(Lifecycle::Success) => "OK",
            DisplayLifecycle::Real(Lifecycle::Failed) => "F",
            DisplayLifecycle::Real(Lifecycle::Skipped) => "SK",
        }
    }

    /// Long machine-readable name (used in `--json`).
    pub fn long(self) -> &'static str {
        match self {
            DisplayLifecycle::Pending => "pending",
            DisplayLifecycle::Real(Lifecycle::Queued) => "queued",
            DisplayLifecycle::Real(Lifecycle::Running) => "running",
            DisplayLifecycle::Real(Lifecycle::Success) => "success",
            DisplayLifecycle::Real(Lifecycle::Failed) => "failed",
            DisplayLifecycle::Real(Lifecycle::Skipped) => "skipped",
        }
    }

    /// Parse one token: short code or long name, case-insensitive.
    pub fn parse_token(s: &str) -> Result<DisplayLifecycle, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "pd" | "pending" => Ok(DisplayLifecycle::Pending),
            "q" | "queued" => Ok(DisplayLifecycle::Real(Lifecycle::Queued)),
            "r" | "running" => Ok(DisplayLifecycle::Real(Lifecycle::Running)),
            "ok" | "success" => Ok(DisplayLifecycle::Real(Lifecycle::Success)),
            "f" | "failed" => Ok(DisplayLifecycle::Real(Lifecycle::Failed)),
            "sk" | "skipped" => Ok(DisplayLifecycle::Real(Lifecycle::Skipped)),
            other => Err(format!(
                "unknown status {other:?} (expected one of \
                 pd,q,r,ok,f,sk / pending,queued,running,success,failed,skipped)"
            )),
        }
    }
}

/// Parse a comma-separated `--status` value into a set. Empty/blank input
/// yields an empty set (= no status filter). Whitespace around tokens is
/// trimmed; empty tokens (e.g. trailing comma) are ignored.
pub fn parse_status_set(csv: &str) -> Result<BTreeSet<DisplayLifecycle>, String> {
    let mut out = BTreeSet::new();
    for tok in csv.split(',') {
        if tok.trim().is_empty() {
            continue;
        }
        out.insert(DisplayLifecycle::parse_token(tok)?);
    }
    Ok(out)
}

/// One flow + its on-disk per-job status (`None` == Pending: no readable
/// `status.toml`). Produced by `collect` (added in a later task).
#[derive(Debug)]
pub struct CollectedFlow {
    pub flow: JobFlow,
    pub statuses: BTreeMap<JobId, Option<JobRun>>,
}

impl CollectedFlow {
    /// `DisplayLifecycle` for one job (`Pending` if no status).
    pub fn job_display(&self, job_id: &JobId) -> DisplayLifecycle {
        match self.statuses.get(job_id).and_then(|o| o.as_ref()) {
            Some(jr) => DisplayLifecycle::Real(jr.lifecycle),
            None => DisplayLifecycle::Pending,
        }
    }

    /// Job ids in topological order, falling back to `BTreeMap` key order
    /// if the DAG has a cycle (`jm doctor` reports cycles separately).
    pub fn topo_or_key_order(&self) -> Vec<JobId> {
        crate::flow::topological_order(&self.flow.jobs, self.flow.uuid)
            .unwrap_or_else(|_| self.flow.jobs.keys().cloned().collect())
    }
}

/// Rolled-up flow status (priority order is fixed; see spec §5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowStatus {
    Failed,
    Running,
    Queued,
    Done,
    Partial,
    Pending,
}

impl FlowStatus {
    /// UPPERCASE label for the flow STATUS column and `--json` output.
    pub fn as_str(self) -> &'static str {
        match self {
            FlowStatus::Failed => "FAILED",
            FlowStatus::Running => "RUNNING",
            FlowStatus::Queued => "QUEUED",
            FlowStatus::Done => "DONE",
            FlowStatus::Partial => "PARTIAL",
            FlowStatus::Pending => "PENDING",
        }
    }
}

/// Aggregate a flow's per-job displays into one rolled-up status.
/// Priority: FAILED > RUNNING > QUEUED > DONE(all success) > PARTIAL
/// (>=1 skipped, all terminal) > PENDING (anything else / empty).
pub fn aggregate_flow_status(jobs: &[DisplayLifecycle]) -> FlowStatus {
    use crate::job::lifecycle::Lifecycle::{Failed, Queued, Running, Skipped, Success};
    if jobs.is_empty() {
        return FlowStatus::Pending;
    }
    if jobs.contains(&DisplayLifecycle::Real(Failed)) {
        return FlowStatus::Failed;
    }
    if jobs.contains(&DisplayLifecycle::Real(Running)) {
        return FlowStatus::Running;
    }
    if jobs.contains(&DisplayLifecycle::Real(Queued)) {
        return FlowStatus::Queued;
    }
    if jobs.iter().all(|d| *d == DisplayLifecycle::Real(Success)) {
        return FlowStatus::Done;
    }
    let any_skipped = jobs.contains(&DisplayLifecycle::Real(Skipped));
    let all_terminal = jobs.iter().all(|d| {
        matches!(
            d,
            DisplayLifecycle::Real(Success) | DisplayLifecycle::Real(Skipped)
        )
    });
    if any_skipped && all_terminal {
        FlowStatus::Partial
    } else {
        FlowStatus::Pending
    }
}

/// In-memory job row (canonical data; display/JSON views derived).
#[derive(Debug, Clone)]
pub struct JobRow {
    pub flow_uuid: Uuid,
    pub job_id: String,
    pub status: DisplayLifecycle,
    pub slurm_jobid: Option<u64>,
    pub program: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// In-memory flow row.
#[derive(Debug, Clone)]
pub struct FlowRow {
    pub flow_uuid: Uuid,
    pub total: usize,
    pub done: usize,
    pub status: FlowStatus,
    pub created_at: DateTime<Utc>,
}

/// `--json` view for a job row (full uuid, long status, RFC3339 times).
#[derive(Serialize)]
pub struct JobRowJson {
    pub flow: String,
    pub job: String,
    pub status: String,
    pub slurm_jobid: Option<u64>,
    pub program: String,
    pub updated_at: Option<String>,
    pub created_at: String,
}

impl From<&JobRow> for JobRowJson {
    fn from(r: &JobRow) -> Self {
        JobRowJson {
            flow: r.flow_uuid.to_string(),
            job: r.job_id.clone(),
            status: r.status.long().to_string(),
            slurm_jobid: r.slurm_jobid,
            program: r.program.clone(),
            updated_at: r.updated_at.map(|t| t.to_rfc3339()),
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

/// `--json` view for a flow row.
#[derive(Serialize)]
pub struct FlowRowJson {
    pub flow: String,
    pub total: usize,
    pub done: usize,
    pub status: String,
    pub created_at: String,
}

impl From<&FlowRow> for FlowRowJson {
    fn from(r: &FlowRow) -> Self {
        FlowRowJson {
            flow: r.flow_uuid.to_string(),
            total: r.total,
            done: r.done,
            status: r.status.as_str().to_string(),
            created_at: r.created_at.to_rfc3339(),
        }
    }
}

fn pad(s: &str, w: usize) -> String {
    let len = s.chars().count();
    if len >= w {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len() + (w - len));
    out.push_str(s);
    out.push_str(&" ".repeat(w - len));
    out
}

fn col_width<'a>(header: &str, cells: impl Iterator<Item = &'a str>) -> usize {
    cells.fold(header.chars().count(), |m, c| m.max(c.chars().count()))
}

fn render_row(cells: &[String], w: &[usize]) -> String {
    debug_assert_eq!(w.len(), cells.len(), "width vector must match cell count");
    let mut line = String::new();
    for (i, c) in cells.iter().enumerate() {
        if i > 0 {
            line.push_str("  ");
        }
        if i == cells.len() - 1 {
            line.push_str(c); // last column unpadded
        } else {
            line.push_str(&pad(c, w[i]));
        }
    }
    line.push('\n');
    line
}

/// Render job rows as an aligned table. `FLOW` is the uuid's first 8
/// chars. Empty input + `no_header` => "".
pub fn format_jobs_table(rows: &[JobRow], no_header: bool) -> String {
    let cells: Vec<[String; 7]> = rows
        .iter()
        .map(|r| {
            [
                r.flow_uuid.to_string()[..8].to_string(),
                r.job_id.clone(),
                r.status.code().to_string(),
                r.slurm_jobid
                    .map(|j| j.to_string())
                    .unwrap_or_else(|| "-".into()),
                r.program.clone(),
                r.updated_at
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_else(|| "-".into()),
                r.created_at.to_rfc3339(),
            ]
        })
        .collect();
    let hdr = [
        "FLOW", "JOB", "ST", "SLURM_ID", "PROGRAM", "UPDATED", "CREATED",
    ];
    let w: Vec<usize> = hdr
        .iter()
        .enumerate()
        .map(|(i, h)| col_width(h, cells.iter().map(|c| c[i].as_str())))
        .collect();
    let mut out = String::new();
    if !no_header {
        out.push_str(&render_row(&hdr.map(String::from), &w));
    }
    for c in &cells {
        out.push_str(&render_row(c, &w));
    }
    out
}

/// Render flow rows as an aligned table.
pub fn format_flows_table(rows: &[FlowRow], no_header: bool) -> String {
    let cells: Vec<[String; 5]> = rows
        .iter()
        .map(|r| {
            [
                r.flow_uuid.to_string()[..8].to_string(),
                r.total.to_string(),
                r.done.to_string(),
                r.status.as_str().to_string(),
                r.created_at.to_rfc3339(),
            ]
        })
        .collect();
    let hdr = ["FLOW", "TOTAL", "DONE", "STATUS", "CREATED"];
    let w: Vec<usize> = hdr
        .iter()
        .enumerate()
        .map(|(i, h)| col_width(h, cells.iter().map(|c| c[i].as_str())))
        .collect();
    let mut out = String::new();
    if !no_header {
        out.push_str(&render_row(&hdr.map(String::from), &w));
    }
    for c in &cells {
        out.push_str(&render_row(c, &w));
    }
    out
}

/// Serialize job rows as a pretty JSON array.
pub fn format_jobs_json(rows: &[JobRow]) -> Result<String, serde_json::Error> {
    let v: Vec<JobRowJson> = rows.iter().map(JobRowJson::from).collect();
    serde_json::to_string_pretty(&v)
}

/// Serialize flow rows as a pretty JSON array.
pub fn format_flows_json(rows: &[FlowRow]) -> Result<String, serde_json::Error> {
    let v: Vec<FlowRowJson> = rows.iter().map(FlowRowJson::from).collect();
    serde_json::to_string_pretty(&v)
}

/// Render a forest of flows as a tree. Jobs are ordered topologically
/// (falls back to BTreeMap key order if the DAG has a cycle — `jm doctor`
/// reports cycles separately). Each child annotates its parent edges.
pub fn format_tree(flows: &[&CollectedFlow]) -> String {
    use crate::job::lifecycle::Lifecycle::Success;
    let mut out = String::new();
    for (fi, cf) in flows.iter().enumerate() {
        if fi > 0 {
            out.push('\n');
        }
        let displays: Vec<DisplayLifecycle> =
            cf.flow.jobs.keys().map(|k| cf.job_display(k)).collect();
        let agg = aggregate_flow_status(&displays);
        let ok = displays
            .iter()
            .filter(|d| **d == DisplayLifecycle::Real(Success))
            .count();
        out.push_str(&format!(
            "{}  ({} jobs · {} OK · {})\n",
            &cf.flow.uuid.to_string()[..8],
            cf.flow.jobs.len(),
            ok,
            agg.as_str()
        ));
        let order = cf.topo_or_key_order();
        for (i, jid) in order.iter().enumerate() {
            let last = i == order.len() - 1;
            let branch = if last { "└─" } else { "├─" };
            let job = &cf.flow.jobs[jid];
            let edges = if job.parents.is_empty() {
                String::new()
            } else {
                let ps: Vec<String> = job
                    .parents
                    .iter()
                    .map(|e| format!("{:?} {}", e.kind, e.from.0))
                    .collect();
                format!("  ({})", ps.join(", "))
            };
            out.push_str(&format!(
                "{} {}  {}{}\n",
                branch,
                jid.0,
                cf.job_display(jid).code(),
                edges
            ));
        }
    }
    out
}

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use gaussian_job_shared::config::common::CommonConfig;

use crate::concurrency::parallelism;
use crate::error::JobManagerError;
use crate::search::{SearchFilter, matches};

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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("PD", DisplayLifecycle::Pending)]
    #[case("pending", DisplayLifecycle::Pending)]
    #[case("Q", DisplayLifecycle::Real(Lifecycle::Queued))]
    #[case("F", DisplayLifecycle::Real(Lifecycle::Failed))]
    #[case("R", DisplayLifecycle::Real(Lifecycle::Running))]
    #[case("Running", DisplayLifecycle::Real(Lifecycle::Running))]
    #[case("ok", DisplayLifecycle::Real(Lifecycle::Success))]
    #[case("SK", DisplayLifecycle::Real(Lifecycle::Skipped))]
    fn parse_token_accepts_code_and_long_case_insensitive(
        #[case] input: &str,
        #[case] expected: DisplayLifecycle,
    ) {
        assert_eq!(DisplayLifecycle::parse_token(input).unwrap(), expected);
    }

    #[test]
    fn parse_token_rejects_unknown() {
        let err = DisplayLifecycle::parse_token("xyz").unwrap_err();
        assert!(err.contains("unknown status"), "got: {err}");
    }

    #[test]
    fn code_and_long_round_trip_for_every_variant() {
        for dl in [
            DisplayLifecycle::Pending,
            DisplayLifecycle::Real(Lifecycle::Queued),
            DisplayLifecycle::Real(Lifecycle::Running),
            DisplayLifecycle::Real(Lifecycle::Success),
            DisplayLifecycle::Real(Lifecycle::Failed),
            DisplayLifecycle::Real(Lifecycle::Skipped),
        ] {
            assert_eq!(DisplayLifecycle::parse_token(dl.code()).unwrap(), dl);
            assert_eq!(DisplayLifecycle::parse_token(dl.long()).unwrap(), dl);
        }
    }

    #[test]
    fn parse_status_set_splits_csv_and_ignores_blanks() {
        let s = parse_status_set("running, F ,").unwrap();
        assert_eq!(s.len(), 2);
        assert!(s.contains(&DisplayLifecycle::Real(Lifecycle::Running)));
        assert!(s.contains(&DisplayLifecycle::Real(Lifecycle::Failed)));
        assert!(parse_status_set("").unwrap().is_empty());
        assert!(parse_status_set("  ").unwrap().is_empty());
    }

    #[test]
    fn parse_status_set_propagates_token_error() {
        assert!(parse_status_set("running,nope").is_err());
    }

    fn dl(l: Lifecycle) -> DisplayLifecycle {
        DisplayLifecycle::Real(l)
    }

    #[rstest]
    #[case(vec![], FlowStatus::Pending)]
    #[case(vec![dl(Lifecycle::Success), dl(Lifecycle::Failed)], FlowStatus::Failed)]
    #[case(vec![dl(Lifecycle::Running), dl(Lifecycle::Queued)], FlowStatus::Running)]
    #[case(vec![dl(Lifecycle::Queued), dl(Lifecycle::Success)], FlowStatus::Queued)]
    #[case(vec![dl(Lifecycle::Success), dl(Lifecycle::Success)], FlowStatus::Done)]
    #[case(vec![dl(Lifecycle::Success), dl(Lifecycle::Skipped)], FlowStatus::Partial)]
    #[case(vec![dl(Lifecycle::Skipped), dl(Lifecycle::Skipped)], FlowStatus::Partial)]
    #[case(vec![DisplayLifecycle::Pending, dl(Lifecycle::Success)], FlowStatus::Pending)]
    #[case(vec![DisplayLifecycle::Pending, dl(Lifecycle::Skipped)], FlowStatus::Pending)]
    fn aggregate_flow_status_priority(
        #[case] jobs: Vec<DisplayLifecycle>,
        #[case] expected: FlowStatus,
    ) {
        assert_eq!(aggregate_flow_status(&jobs), expected);
    }

    use chrono::TimeZone;

    fn sample_job_row() -> JobRow {
        JobRow {
            flow_uuid: Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap(),
            job_id: "step1".into(),
            status: DisplayLifecycle::Real(Lifecycle::Success),
            slurm_jobid: Some(120345),
            program: "g16".into(),
            updated_at: Some(Utc.with_ymd_and_hms(2026, 5, 16, 1, 2, 3).unwrap()),
            created_at: Utc.with_ymd_and_hms(2026, 5, 16, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn jobs_table_has_header_and_short_uuid_and_code() {
        let t = format_jobs_table(&[sample_job_row()], false);
        let mut lines = t.lines();
        assert!(lines.next().unwrap().starts_with("FLOW"));
        let row = lines.next().unwrap();
        assert!(row.contains("01997cdc"));
        assert!(!row.contains("01997cdc-0000"));
        assert!(row.contains("OK"));
    }

    #[test]
    fn jobs_table_no_header_drops_header() {
        let t = format_jobs_table(&[sample_job_row()], true);
        assert!(!t.contains("FLOW"));
        assert!(t.contains("01997cdc"));
    }

    #[test]
    fn jobs_table_empty_no_header_is_empty_string() {
        assert_eq!(format_jobs_table(&[], true), "");
    }

    #[test]
    fn jobs_json_uses_full_uuid_and_long_status() {
        let j = format_jobs_json(&[sample_job_row()]).unwrap();
        assert!(j.contains("01997cdc-0000-7000-8000-000000000000"));
        assert!(j.contains("\"status\": \"success\""));
        assert!(j.contains("\"slurm_jobid\": 120345"));
    }

    #[test]
    fn tree_renders_forest_with_edges_and_topo_order() {
        use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobSpec, Program};
        use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

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
        let mut jobs = BTreeMap::new();
        jobs.insert(
            JobId::from("step1"),
            Job {
                spec: JobSpec {
                    program: Program::from("g16"),
                    config: cfg(),
                    body: "".into(),
                },
                parents: vec![],
            },
        );
        jobs.insert(
            JobId::from("step2"),
            Job {
                spec: JobSpec {
                    program: Program::from("g16"),
                    config: cfg(),
                    body: "".into(),
                },
                parents: vec![JobEdge {
                    from: JobId::from("step1"),
                    kind: DependencyType::AfterOk,
                }],
            },
        );
        let flow = JobFlow {
            uuid: Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap(),
            created_at: Utc::now(),
            tags: BTreeMap::new(),
            jobs,
        };
        let mut statuses = BTreeMap::new();
        statuses.insert(
            JobId::from("step1"),
            Some(JobRun {
                lifecycle: Lifecycle::Success,
                updated_at: Utc::now(),
                slurm_jobid: Some(1),
                slurm_status: None,
                note: None,
            }),
        );
        statuses.insert(JobId::from("step2"), None);
        let cf = CollectedFlow { flow, statuses };
        let t = format_tree(&[&cf]);
        assert!(t.contains("01997cdc  (2 jobs"));
        assert!(t.contains("├─ step1  OK"));
        assert!(t.contains("└─ step2  PD"));
        assert!(t.contains("AfterOk step1"));
    }

    fn sample_flow_row() -> FlowRow {
        FlowRow {
            flow_uuid: Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap(),
            total: 3,
            done: 2,
            status: FlowStatus::Running,
            created_at: Utc.with_ymd_and_hms(2026, 5, 16, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn flows_table_has_header_and_short_uuid_and_status() {
        let t = format_flows_table(&[sample_flow_row()], false);
        let mut lines = t.lines();
        let hdr = lines.next().unwrap();
        assert!(hdr.starts_with("FLOW"));
        assert!(hdr.contains("TOTAL"));
        assert!(hdr.contains("DONE"));
        assert!(hdr.contains("STATUS"));
        let row = lines.next().unwrap();
        assert!(row.contains("01997cdc"));
        assert!(!row.contains("01997cdc-0000"));
        assert!(row.contains("RUNNING"));
    }

    #[test]
    fn flows_table_empty_no_header_is_empty_string() {
        assert_eq!(format_flows_table(&[], true), "");
    }

    #[test]
    fn flows_json_uses_full_uuid_and_status_string() {
        let j = format_flows_json(&[sample_flow_row()]).unwrap();
        assert!(j.contains("01997cdc-0000-7000-8000-000000000000"));
        assert!(j.contains("\"status\": \"RUNNING\""));
        assert!(j.contains("\"total\": 3"));
        assert!(j.contains("\"done\": 2"));
    }

    #[test]
    fn jobs_table_multi_row_alignment_spans_widest_cell() {
        let mut wide = sample_job_row();
        wide.job_id = "a_very_long_job_identifier".into();
        let narrow = sample_job_row(); // job_id = "step1"
        let t = format_jobs_table(&[wide, narrow], false);
        let lines: Vec<&str> = t.lines().collect();
        // header + 2 rows
        assert_eq!(lines.len(), 3);
        // the JOB column must be wide enough that the short row's JOB cell is
        // space-padded to the long id's width (i.e. the column aligns).
        let long_line = lines[1];
        let short_line = lines[2];
        assert!(long_line.contains("a_very_long_job_identifier"));
        // After the JOB column the next column (ST) should start at the same
        // byte offset on both data rows because of padding.
        let st_idx_long = long_line.find("  OK").unwrap();
        let st_idx_short = short_line.find("  OK").unwrap();
        assert_eq!(
            st_idx_long, st_idx_short,
            "JOB column not aligned across rows"
        );
    }

    #[test]
    fn tree_two_flows_separated_by_blank_line() {
        use std::collections::BTreeMap as TreeBTreeMap;
        fn empty_flow(uuid_last: &str) -> CollectedFlow {
            let uuid =
                Uuid::parse_str(&format!("01997cdc-0000-7000-8000-0000000000{uuid_last}")).unwrap();
            CollectedFlow {
                flow: JobFlow {
                    uuid,
                    created_at: Utc::now(),
                    tags: TreeBTreeMap::new(),
                    jobs: TreeBTreeMap::new(),
                },
                statuses: TreeBTreeMap::new(),
            }
        }
        let a = empty_flow("01");
        let b = empty_flow("02");
        let t = format_tree(&[&a, &b]);
        // two flow headers, separated by exactly one blank line between them
        assert!(t.contains("\n\n"), "expected a blank line between flows");
        assert_eq!(t.matches("(0 jobs").count(), 2);
    }
}
