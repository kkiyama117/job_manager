//! Aligned-table / JSON / tree renderers for `jm ls` rows.

use serde::Serialize;

use super::{CollectedFlow, DisplayLifecycle, FlowRow, JobRow, aggregate_flow_status};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::lifecycle::Lifecycle;
    use crate::job::run::JobRun;
    use crate::listing::FlowStatus;
    use chrono::{TimeZone, Utc};
    use gaussian_job_shared::entities::workflow::{JobFlow, JobId};
    use std::collections::BTreeMap;
    use uuid::Uuid;

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
