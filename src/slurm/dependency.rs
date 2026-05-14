//! Build A1 `SlurmDependency` from JobEdge[] + submitted jobids.

use std::collections::BTreeMap;
use std::str::FromStr;

use gaussian_job_shared::entities::workflow::{JobEdge, JobId};
use slurm_async_runner::entities::slurm::{DependencyType, SlurmDependency};

use crate::error::JobManagerError;

pub fn build(
    parents: &[JobEdge],
    submitted: &BTreeMap<JobId, u64>,
    job: &JobId,
) -> Result<Option<SlurmDependency>, JobManagerError> {
    let pairs: Vec<(u64, DependencyType)> = parents
        .iter()
        .filter_map(|e| submitted.get(&e.from).map(|j| (*j, e.kind)))
        .collect();
    if pairs.is_empty() {
        return Ok(None);
    }
    let s = pairs
        .iter()
        .map(|(jid, kind)| format!("{kind}:{jid}"))
        .collect::<Vec<_>>()
        .join(",");
    let dep = SlurmDependency::from_str(&s).map_err(|e| JobManagerError::SubmitFailed {
        source: anyhow::anyhow!("dependency parse failed for {job}: {e}"),
    })?;
    Ok(Some(dep))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_none_when_no_parents_submitted() {
        let result = build(&[], &BTreeMap::new(), &JobId("child".into())).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn afterok_single_parent() {
        let p_jid = JobId("parent".into());
        let parents = vec![JobEdge {
            from: p_jid.clone(),
            kind: DependencyType::AfterOk,
        }];
        let mut submitted = BTreeMap::new();
        submitted.insert(p_jid, 12345);
        let result = build(&parents, &submitted, &JobId("child".into())).unwrap();
        assert!(result.is_some());
        let s = format!("{}", result.unwrap());
        assert!(s.contains("afterok:12345"), "got: {s}");
    }

    #[test]
    fn multi_parents_joined_by_comma() {
        let p1 = JobId("p1".into());
        let p2 = JobId("p2".into());
        let parents = vec![
            JobEdge {
                from: p1.clone(),
                kind: DependencyType::AfterOk,
            },
            JobEdge {
                from: p2.clone(),
                kind: DependencyType::AfterAny,
            },
        ];
        let mut submitted = BTreeMap::new();
        submitted.insert(p1, 100);
        submitted.insert(p2, 200);
        let result = build(&parents, &submitted, &JobId("child".into())).unwrap();
        let s = format!("{}", result.unwrap());
        assert!(s.contains("afterok:100"));
        assert!(s.contains("afterany:200"));
    }
}
