//! `SearchFilter` and the pure `matches()` predicate.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, Program};

use crate::status::{PerJobStatus, StatusEntry};

#[derive(Debug, Clone, Default)]
pub struct SearchFilter {
    pub program: Option<Program>,
    pub tags: BTreeMap<String, String>,
    pub status: Option<PerJobStatus>,
    pub flow_uuid_prefix: Option<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub slurm_jobid: Option<u64>,
    pub job_id: Option<JobId>,
}

/// All filters AND.
pub fn matches(
    flow: &JobFlow,
    job_id: &JobId,
    job: &Job,
    status: Option<&StatusEntry>,
    f: &SearchFilter,
) -> bool {
    if let Some(ref p) = f.program
        && &job.spec.program != p
    {
        return false;
    }
    if !f.tags.is_empty() {
        for (k, v) in &f.tags {
            match flow.tags.get(k) {
                Some(existing) if existing == v => {}
                _ => return false,
            }
        }
    }
    if let Some(want) = f.status {
        match status {
            Some(e) if e.lifecycle == want => {}
            _ => return false,
        }
    }
    if let Some(ref prefix) = f.flow_uuid_prefix
        && !flow.uuid.to_string().starts_with(&prefix.to_lowercase())
    {
        return false;
    }
    if let Some(after) = f.created_after
        && flow.created_at < after
    {
        return false;
    }
    if let Some(before) = f.created_before
        && flow.created_at > before
    {
        return false;
    }
    if let Some(want_jobid) = f.slurm_jobid {
        match status {
            Some(e) if e.slurm_jobid == Some(want_jobid) => {}
            _ => return false,
        }
    }
    if let Some(ref want_job) = f.job_id
        && job_id != want_job
    {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
    use rstest::rstest;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
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

    fn make_flow(uuid: Uuid, tags: BTreeMap<String, String>) -> (JobFlow, JobId, Job) {
        let job = Job {
            spec: JobSpec {
                program: Program::from("g16"),
                config: cfg(),
                body: "".into(),
            },
            parents: vec![],
        };
        let id = JobId::from("g16");
        let mut jobs = BTreeMap::new();
        jobs.insert(id.clone(), job.clone());
        let flow = JobFlow {
            uuid,
            created_at: Utc::now(),
            work_dir: PathBuf::from("/tmp"),
            tags,
            jobs,
        };
        (flow, id, job)
    }

    #[test]
    fn empty_filter_matches_everything() {
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let filt = SearchFilter::default();
        assert!(matches(&f, &id, &j, None, &filt));
    }

    #[test]
    fn program_filter_rejects_mismatch() {
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let filt = SearchFilter {
            program: Some(Program::from("post")),
            ..Default::default()
        };
        assert!(!matches(&f, &id, &j, None, &filt));
    }

    #[test]
    fn tag_filter_requires_all_tags_match() {
        let mut tags = BTreeMap::new();
        tags.insert("project".into(), "smoke".into());
        let (f, id, j) = make_flow(Uuid::now_v7(), tags);
        let mut want = BTreeMap::new();
        want.insert("project".into(), "smoke".into());
        let filt = SearchFilter {
            tags: want.clone(),
            ..Default::default()
        };
        assert!(matches(&f, &id, &j, None, &filt));
        want.insert("missing".into(), "x".into());
        let filt = SearchFilter {
            tags: want,
            ..Default::default()
        };
        assert!(!matches(&f, &id, &j, None, &filt));
    }

    #[test]
    fn status_filter_requires_status_entry() {
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let filt = SearchFilter {
            status: Some(PerJobStatus::Queued),
            ..Default::default()
        };
        assert!(!matches(&f, &id, &j, None, &filt));
        let entry = StatusEntry {
            lifecycle: PerJobStatus::Queued,
            updated_at: Utc::now(),
            slurm_jobid: None,
            slurm_status: None,
            note: None,
        };
        assert!(matches(&f, &id, &j, Some(&entry), &filt));
    }

    #[rstest]
    #[case("0199", true)]
    #[case("DOES_NOT_MATCH", false)]
    fn uuid_prefix_filter_is_case_insensitive(#[case] prefix: &str, #[case] expected: bool) {
        let uuid = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
        let (f, id, j) = make_flow(uuid, BTreeMap::new());
        let filt = SearchFilter {
            flow_uuid_prefix: Some(prefix.into()),
            ..Default::default()
        };
        assert_eq!(matches(&f, &id, &j, None, &filt), expected);
    }

    #[test]
    fn slurm_jobid_filter_matches_via_status_entry() {
        let (f, id, j) = make_flow(Uuid::now_v7(), BTreeMap::new());
        let entry = StatusEntry {
            lifecycle: PerJobStatus::Running,
            updated_at: Utc::now(),
            slurm_jobid: Some(9999),
            slurm_status: None,
            note: None,
        };
        let filt = SearchFilter {
            slurm_jobid: Some(9999),
            ..Default::default()
        };
        assert!(matches(&f, &id, &j, Some(&entry), &filt));
        let filt_no = SearchFilter {
            slurm_jobid: Some(1),
            ..Default::default()
        };
        assert!(!matches(&f, &id, &j, Some(&entry), &filt_no));
    }
}
