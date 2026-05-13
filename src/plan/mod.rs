//! Experiment plan sidecar — per-job params 永続化 (SP-3 が bash render で使う)。

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::JobId;

pub mod io;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[must_use]
pub struct ExperimentPlan {
    /// Map key は D2 `JobId` newtype。value は任意の TOML 値。
    pub jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>,
}
