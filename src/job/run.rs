//! JobRun — per-job runtime state (旧 StatusEntry の置換、Airflow TaskInstance 相当)。

use crate::job::lifecycle::Lifecycle;
use slurm_async_runner::JobStatus;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRun {
    pub lifecycle: Lifecycle,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slurm_jobid: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slurm_status: Option<JobStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample() -> JobRun {
        JobRun {
            lifecycle: Lifecycle::Queued,
            updated_at: chrono::Utc
                .with_ymd_and_hms(2026, 5, 13, 12, 34, 56)
                .unwrap(),
            slurm_jobid: Some(12345),
            slurm_status: None,
            note: None,
        }
    }

    #[test]
    fn toml_round_trip() {
        let original = sample();
        let toml_str = toml::to_string(&original).unwrap();
        let restored: JobRun = toml::from_str(&toml_str).unwrap();
        assert_eq!(restored.lifecycle, original.lifecycle);
        assert_eq!(restored.slurm_jobid, original.slurm_jobid);
    }

    #[test]
    fn toml_uses_snake_case_lifecycle() {
        let s = toml::to_string(&sample()).unwrap();
        assert!(s.contains("lifecycle = \"queued\""), "got: {s}");
    }

    #[test]
    fn deny_unknown_fields_rejects_extra() {
        let bad = r#"
lifecycle = "queued"
updated_at = "2026-05-13T12:34:56Z"
extra = 1
"#;
        let result: Result<JobRun, _> = toml::from_str(bad);
        assert!(result.is_err());
    }
}
