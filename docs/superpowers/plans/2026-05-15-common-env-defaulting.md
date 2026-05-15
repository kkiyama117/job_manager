# common.toml defaulting + `.jm/` program subdir — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `flow.toml` の `partition` 重複記述を解消し、program-managed ファイルを `<flow_uuid>/.jm/` 配下に集約する。materialized snapshot を `.flow.effective.toml` として program 側で書き、tick/show は common 不要で動くようにする。

**Architecture:** Route 1 = TOML preparse 層を `read_flow` に追加し、partition 欠損なら common から inject。`<flow_uuid>/.jm/` 配下に program 出力を集約 (Cargo.lock パターン)。read API は `read_flow` (common 要) と `read_flow_effective` (snapshot, common 不要) の 2 系統。

**Tech Stack:** Rust nightly edition 2024 + PyO3 + maturin、tokio、toml、thiserror。

**Reference:** `docs/superpowers/specs/2026-05-15-common-env-defaulting-design.md`

---

## Prerequisites

- [ ] **P-1**: このブランチに develop を merge して examples/ と thought docs を取り込む

```bash
git fetch origin
git merge origin/develop --no-edit
# conflict があれば解消（典型的には docs/superpowers/specs の追加のみで衝突しない）
```

- [ ] **P-2**: ビルド環境確認

```bash
uv sync
uv run maturin develop
cargo check --all-features
```

Expected: いずれもエラーなし

---

## Task 1: Add new error variants

**Files:**
- Modify: `src/error.rs`

- [ ] **Step 1.1: Write the failing test** at end of `src/error.rs` `mod tests`:

```rust
    #[test]
    fn partition_missing_carries_job_id() {
        let err = JobManagerError::PartitionMissing {
            job: gaussian_job_shared::entities::workflow::JobId("opt".to_string()),
        };
        let msg = err.to_string();
        assert!(msg.contains("opt"), "msg = {msg}");
        assert!(msg.contains("partition"), "msg = {msg}");
    }

    #[test]
    fn snapshot_missing_carries_path_and_uuid() {
        let err = JobManagerError::SnapshotMissing {
            path: PathBuf::from("/work/abc/.jm/flow.effective.toml"),
            uuid: "01999999-0000-7000-8000-000000000000".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("/work/abc/.jm/flow.effective.toml"), "msg = {msg}");
        assert!(msg.contains("jm render"), "msg should hint at render: {msg}");
    }

    #[test]
    fn root_inference_failed_carries_path() {
        let err = JobManagerError::RootInferenceFailed {
            path: PathBuf::from("/tmp/x.toml"),
        };
        assert!(err.to_string().contains("/tmp/x.toml"));
    }
```

- [ ] **Step 1.2: Run tests to verify they fail**

```bash
cargo test --lib --all-features error:: -- --nocapture
```

Expected: 3 tests FAIL with "no variant or associated item named ..."

- [ ] **Step 1.3: Add the new variants** to `src/error.rs`'s `JobManagerError` enum (insert before `Other`):

```rust
    #[error(
        "partition is required but missing: job={job} has no partition and common.toml [slurm_default] has no partition either"
    )]
    PartitionMissing {
        job: gaussian_job_shared::entities::workflow::JobId,
    },

    #[error(
        "effective snapshot missing at {path} (uuid={uuid}): run `jm render <uuid>` first to materialize"
    )]
    SnapshotMissing { path: PathBuf, uuid: String },

    #[error("cannot infer root from flow.toml path {path}: expected <root>/<flow_uuid>/flow.toml layout")]
    RootInferenceFailed { path: PathBuf },
```

- [ ] **Step 1.4: Run tests to verify they pass**

```bash
cargo test --lib --all-features error:: -- --nocapture
```

Expected: all error:: tests PASS

- [ ] **Step 1.5: Commit**

```bash
git add src/error.rs
git commit -m "feat(error): add PartitionMissing/SnapshotMissing/RootInferenceFailed variants"
```

---

## Task 2: PathResolver — `.jm/` layout

**Files:**
- Modify: `src/persistence/path.rs`

- [ ] **Step 2.1: Update existing tests for new layout** in `src/persistence/path.rs` `mod tests` — replace the `job_dir_is_flow_dir_joined_with_job_id_no_jobs_layer`, `status_file_lives_inside_job_dir_as_dot_status_toml`, `batch_bash_returns_job_dir_batch_bash` test bodies:

```rust
    #[test]
    fn job_dir_under_jm_subdir() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        let j = JobId::from("post");
        assert_eq!(
            r.job_dir(&u, &j),
            PathBuf::from(format!("/work/{u}/.jm/post"))
        );
    }

    #[test]
    fn status_file_lives_inside_jm_job_dir_without_dot_prefix() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        let j = JobId::from("g16");
        assert_eq!(
            r.status_file(&u, &j),
            PathBuf::from(format!("/work/{u}/.jm/g16/status.toml"))
        );
    }

    #[test]
    fn batch_bash_returns_jm_job_dir_batch_bash() {
        let r = PathResolver::new("/work");
        let uuid = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
        let jid = JobId("opt__a=0".to_string());
        let p = r.batch_bash(&uuid, &jid);
        assert!(
            p.ends_with("01997cdc-0000-7000-8000-000000000000/.jm/opt__a=0/batch.bash"),
            "p = {}",
            p.display()
        );
    }

    #[test]
    fn flow_effective_toml_lives_under_jm_dir() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        assert_eq!(
            r.flow_effective_toml(&u),
            PathBuf::from(format!("/work/{u}/.jm/flow.effective.toml"))
        );
    }

    #[test]
    fn jm_dir_returns_flow_dir_dot_jm() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        assert_eq!(r.jm_dir(&u), PathBuf::from(format!("/work/{u}/.jm")));
    }
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
cargo test --lib --all-features persistence::path:: -- --nocapture
```

Expected: 5 tests FAIL (3 layout mismatch, 2 method not found)

- [ ] **Step 2.3: Update `PathResolver` impl** in `src/persistence/path.rs` — replace `job_dir`, `status_file`, `batch_bash` bodies + add `jm_dir`, `flow_effective_toml`:

```rust
    /// `<flow_dir>/.jm/` — hidden subdirectory holding all program-managed
    /// files (snapshot, batch.bash, status, slurm-*.out/err). User-authored
    /// `flow.toml` and `plan.toml` live one level up.
    pub fn jm_dir(&self, flow_uuid: &Uuid) -> PathBuf {
        self.flow_dir(flow_uuid).join(".jm")
    }

    /// `<flow_dir>/.jm/flow.effective.toml` — materialized snapshot of the
    /// JobFlow (all defaults resolved). Written by `submit`/`render`, read
    /// by `tick`/`show` (common 不要)。
    pub fn flow_effective_toml(&self, flow_uuid: &Uuid) -> PathBuf {
        self.jm_dir(flow_uuid).join("flow.effective.toml")
    }

    /// `<flow_dir>/.jm/<JobId>/` — D2's per-Job folder, now nested under
    /// the program-managed `.jm/` directory.
    pub fn job_dir(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
        self.jm_dir(flow_uuid).join(&job_id.0)
    }

    /// `<job_dir>/status.toml` — owned by job-manager. No dot prefix since
    /// `.jm/` already hides the whole tree from casual `ls`.
    pub fn status_file(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
        self.job_dir(flow_uuid, job_id).join("status.toml")
    }
```

Leave `flow_dir`, `flow_toml`, `plan_toml`, `common_toml`, `experiment_toml`, `batch_bash` (already derives from `job_dir`) untouched in signature — `batch_bash` will cascade correctly through the new `job_dir`.

Also update the doc comment block at top of file (lines 1-13) to reflect new layout:

```rust
//! Path resolution for the `<root>/<flow_uuid>/...` layout.
//!
//! Layout invariant:
//!
//! ```text
//! <root>/                      <- PathResolver.root
//! └── <flow_uuid>/             <- flow_dir(&flow.uuid)
//!     ├── flow.toml            <- user-authored JobFlow TOML
//!     ├── plan.toml            <- user-authored ExperimentPlan TOML
//!     └── .jm/                 <- jm_dir(&flow.uuid); program-managed
//!         ├── flow.effective.toml  <- materialized snapshot
//!         └── <JobId>/         <- job_dir(&flow.uuid, &job_id)
//!             ├── batch.bash   <- rendered SBATCH script
//!             ├── status.toml  <- per-Job status (this crate, atomic write)
//!             └── slurm-*.out/err  <- SLURM stdout/stderr
//! ```
//!
//! Pure: no filesystem I/O. Just deterministic path string composition.
```

- [ ] **Step 2.4: Run tests to verify they pass**

```bash
cargo test --lib --all-features persistence::path:: -- --nocapture
```

Expected: all `persistence::path::tests::*` PASS

- [ ] **Step 2.5: Commit**

```bash
git add src/persistence/path.rs
git commit -m "feat(path): relocate program-managed files under <flow_uuid>/.jm/"
```

---

## Task 3: `inject_partition_defaults` helper

**Files:**
- Modify: `src/persistence/flow.rs`

- [ ] **Step 3.1: Write failing tests** — add to `src/persistence/flow.rs` `mod tests` (before existing tests):

```rust
    #[test]
    fn inject_adds_partition_when_missing_in_flow() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "long");
    }

    #[test]
    fn inject_keeps_partition_when_already_set_in_flow() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
partition = "short"
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "short", "explicit flow partition must win over common");
    }

    #[test]
    fn inject_creates_missing_config_table() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "long");
    }

    #[test]
    fn inject_returns_partition_missing_when_both_missing() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
"#,
        )
        .unwrap();
        let err = super::inject_partition_defaults(&mut v, None).unwrap_err();
        match err {
            JobManagerError::PartitionMissing { job } => assert_eq!(job.0, "opt"),
            other => panic!("expected PartitionMissing, got {other:?}"),
        }
    }

    #[test]
    fn inject_idempotent_on_already_injected_table() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.opt]
program = "echo"
body = "true"
[jobs.opt.config]
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        let p = v["jobs"]["opt"]["config"]["partition"].as_str().unwrap();
        assert_eq!(p, "long");
    }

    #[test]
    fn inject_handles_multiple_jobs_mixed() {
        let mut v: toml::Value = toml::from_str(
            r#"
uuid = "01999999-0000-7000-8000-000000000000"
created_at = "2026-05-15T00:00:00Z"
[jobs.a]
program = "echo"
body = "true"
[jobs.a.config]
partition = "short"
[jobs.b]
program = "echo"
body = "true"
[jobs.b.config]
"#,
        )
        .unwrap();
        super::inject_partition_defaults(&mut v, Some("long")).unwrap();
        assert_eq!(v["jobs"]["a"]["config"]["partition"].as_str().unwrap(), "short");
        assert_eq!(v["jobs"]["b"]["config"]["partition"].as_str().unwrap(), "long");
    }
```

- [ ] **Step 3.2: Run tests to verify they fail**

```bash
cargo test --lib --all-features persistence::flow::tests::inject -- --nocapture
```

Expected: 6 tests FAIL with "no function `inject_partition_defaults`"

- [ ] **Step 3.3: Add `inject_partition_defaults`** to `src/persistence/flow.rs` (after `use` block, before `read_flow`):

```rust
use gaussian_job_shared::entities::workflow::JobId;

/// Walk `[jobs.*.config]` tables, ensuring each has a `partition` key.
/// Missing tables are created. Missing partition entries are filled from
/// `common_partition` (passed as `Option<&str>` so the caller can express
/// "common has no partition either"). Returns `PartitionMissing { job }`
/// if both flow and common lack a partition for some job.
fn inject_partition_defaults(
    v: &mut toml::Value,
    common_partition: Option<&str>,
) -> Result<(), JobManagerError> {
    let jobs = match v.get_mut("jobs").and_then(|j| j.as_table_mut()) {
        Some(t) => t,
        None => return Ok(()), // no [jobs] table — let downstream serde report it
    };

    for (job_id_str, job_val) in jobs.iter_mut() {
        let job_t = match job_val.as_table_mut() {
            Some(t) => t,
            None => continue, // malformed; serde will complain
        };

        let cfg = job_t
            .entry("config")
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
        let cfg_t = match cfg.as_table_mut() {
            Some(t) => t,
            None => continue,
        };

        if cfg_t.contains_key("partition") {
            continue;
        }

        match common_partition {
            Some(p) => {
                cfg_t.insert("partition".to_string(), toml::Value::String(p.to_string()));
            }
            None => {
                return Err(JobManagerError::PartitionMissing {
                    job: JobId(job_id_str.clone()),
                });
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 3.4: Run tests to verify they pass**

```bash
cargo test --lib --all-features persistence::flow::tests::inject -- --nocapture
```

Expected: 6 inject_* tests PASS

- [ ] **Step 3.5: Commit**

```bash
git add src/persistence/flow.rs
git commit -m "feat(persistence): add inject_partition_defaults TOML preparse helper"
```

---

## Task 4: `read_flow` signature: take `&CommonConfig`

**Files:**
- Modify: `src/persistence/flow.rs`
- Modify: `src/persistence/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/walk.rs`
- Modify: `src/flow/run.rs` (call site)
- Modify: `src/persistence/flow.rs` tests
- Modify: `src/walk.rs` tests

- [ ] **Step 4.1: Update `read_flow` signature** in `src/persistence/flow.rs`:

```rust
use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::{JobFlow, JobId};

/// Read a `JobFlow` from a TOML file at `path`, materializing it with
/// `common` defaults (notably injecting `partition` from `common.slurm_default`
/// when omitted in the flow.toml). Returns `PartitionMissing { job }` if any
/// job lacks a partition and common has none either.
pub fn read_flow(path: &Path, common: &CommonConfig) -> Result<JobFlow, JobManagerError> {
    let text = super::read_toml_string(path)?;
    let mut v: toml::Value = toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })?;
    let common_partition = if common.slurm_default.partition.is_empty() {
        None
    } else {
        Some(common.slurm_default.partition.as_str())
    };
    inject_partition_defaults(&mut v, common_partition)?;
    v.try_into().map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}
```

- [ ] **Step 4.2: Update the existing test fixtures** in `src/persistence/flow.rs` `mod tests`:

```rust
    fn sample_common() -> gaussian_job_shared::config::common::CommonConfig {
        use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
        use std::path::PathBuf;
        CommonConfig {
            slurm_default: SlurmJobConfig {
                partition: "long".to_string(),
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
            },
            directories: DirectoryConfig {
                project_root: PathBuf::from("/work"),
            },
        }
    }
```

Then replace `read_flow(&path).unwrap()` calls in existing tests with `read_flow(&path, &sample_common()).unwrap()`.

Specifically `roundtrip_write_read_recovers_jobflow` and `read_missing_file_returns_io_error_with_path` both need this update.

- [ ] **Step 4.3: Update `src/walk.rs`** — `walk_flows` needs to accept common. Change function signature:

```rust
use gaussian_job_shared::config::common::CommonConfig;
use std::sync::Arc;

pub fn walk_flows(
    root: &Path,
    common: Arc<CommonConfig>,
) -> impl Stream<Item = Result<JobFlow, JobManagerError>> + Send + 'static {
    let root = root.to_path_buf();
    let parallelism = parallelism();
    stream! {
        let paths = match candidate_paths(&root) {
            Ok(p) => p,
            Err(e) => {
                yield Err(e);
                return;
            }
        };
        let body = stream::iter(paths)
            .map(move |p| {
                let common = Arc::clone(&common);
                async move {
                    tokio::task::spawn_blocking(move || read_flow(&p, &common))
                        .await
                        .map_err(|e| JobManagerError::Other(format!("spawn_blocking join: {e}")))?
                }
            })
            .buffer_unordered(parallelism);
        let mut body = std::pin::pin!(body);
        while let Some(r) = body.next().await {
            yield r;
        }
    }
}
```

- [ ] **Step 4.4: Update `src/walk.rs` tests** to construct and pass common:

```rust
    fn sample_common_arc() -> std::sync::Arc<gaussian_job_shared::config::common::CommonConfig> {
        use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
        use slurm_async_runner::entities::slurm::SlurmJobConfig;
        use std::path::PathBuf;
        std::sync::Arc::new(CommonConfig {
            slurm_default: SlurmJobConfig {
                partition: "long".to_string(),
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
            },
            directories: DirectoryConfig {
                project_root: PathBuf::from("/work"),
            },
        })
    }
```

Update all `walk_flows(dir.path())` → `walk_flows(dir.path(), sample_common_arc())`.

- [ ] **Step 4.5: Update `src/flow/run.rs`** — `FlowRun::read` now passes common through:

```rust
    pub fn read(
        resolver: &crate::persistence::PathResolver,
        flow_uuid: uuid::Uuid,
    ) -> Result<Self, JobManagerError> {
        let plan = crate::persistence::read_plan(&resolver.plan_toml(&flow_uuid))?;
        let common_path = resolver.common_toml();
        let common = if common_path.exists() {
            Some(crate::persistence::read_common(&common_path)?)
        } else {
            None
        };
        // read_flow needs a common; if user didn't provide one, fall back to a
        // synthetic default that simply requires partition to be present in
        // each [jobs.*.config].
        let synth_common;
        let common_for_read = match &common {
            Some(c) => c,
            None => {
                use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
                use slurm_async_runner::entities::slurm::SlurmJobConfig;
                synth_common = CommonConfig {
                    slurm_default: SlurmJobConfig {
                        partition: String::new(),
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
                    },
                    directories: DirectoryConfig {
                        project_root: std::path::PathBuf::from("."),
                    },
                };
                &synth_common
            }
        };
        let flow = crate::persistence::read_flow(&resolver.flow_toml(&flow_uuid), common_for_read)?;
        Ok(Self {
            flow_uuid,
            flow,
            plan,
            common,
        })
    }
```

- [ ] **Step 4.6: Build to flush other compile errors**

```bash
cargo check --all-features 2>&1 | head -30
```

Expected: compiles, or shows other call sites needing update (`src/py_export/*`, integration tests). Note them; we fix in later tasks.

- [ ] **Step 4.7: Run unit tests (skip py_export and tests that need integration setup)**

```bash
cargo test --lib --all-features persistence:: -- --nocapture
```

Expected: persistence layer tests PASS.

- [ ] **Step 4.8: Commit**

```bash
git add src/persistence/flow.rs src/walk.rs src/flow/run.rs
git commit -m "refactor(persistence): read_flow takes &CommonConfig for partition defaulting"
```

---

## Task 5: `merge_with_defaults` — drop `is_empty()` partition branch

**Files:**
- Modify: `src/persistence/common.rs`

- [ ] **Step 5.1: Update test expectations** — replace `merge_uses_common_default_when_override_partition_is_empty` body in `src/persistence/common.rs` `mod tests`:

```rust
    #[test]
    fn merge_preserves_explicit_empty_partition_in_override() {
        // After F2, partition is materialized at read_flow time. By the time
        // merge_with_defaults runs, partition is whatever read_flow put there.
        // An explicit "" is preserved verbatim — sbatch will reject it.
        let common = sample();
        let override_cfg = SlurmJobConfig {
            partition: "".to_string(),
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
        };
        let merged = merge_with_defaults(&common, &override_cfg);
        assert_eq!(merged.partition, "");
    }
```

- [ ] **Step 5.2: Update `merge_with_defaults` impl**:

```rust
/// Merge `override_` on top of `common.slurm_default`.
///
/// Partition is **not** filled from common here — `read_flow`'s TOML
/// preparse step (`inject_partition_defaults`) guarantees it is already
/// materialized when this function runs. We simply forward `override_.partition`
/// as-is. The other Option<T> fields fall back to common when None.
pub fn merge_with_defaults(common: &CommonConfig, override_: &SlurmJobConfig) -> SlurmJobConfig {
    let base = &common.slurm_default;
    SlurmJobConfig {
        partition: override_.partition.clone(),
        time_limit: override_.time_limit.or(base.time_limit),
        log_stdout: override_
            .log_stdout
            .clone()
            .or_else(|| base.log_stdout.clone()),
        log_stderr: override_
            .log_stderr
            .clone()
            .or_else(|| base.log_stderr.clone()),
        comment: override_.comment.clone().or_else(|| base.comment.clone()),
        job_name: override_.job_name.clone().or_else(|| base.job_name.clone()),
        array_spec: override_
            .array_spec
            .clone()
            .or_else(|| base.array_spec.clone()),
        dependency: override_
            .dependency
            .clone()
            .or_else(|| base.dependency.clone()),
        mail_user: override_
            .mail_user
            .clone()
            .or_else(|| base.mail_user.clone()),
        mail_types: override_
            .mail_types
            .clone()
            .or_else(|| base.mail_types.clone()),
        resource_spec: override_
            .resource_spec
            .clone()
            .or_else(|| base.resource_spec.clone()),
    }
}
```

- [ ] **Step 5.3: Run tests**

```bash
cargo test --lib --all-features persistence::common -- --nocapture
```

Expected: All `persistence::common::tests::*` PASS. `merge_uses_common_default_when_override_partition_is_empty` is gone, replaced.

- [ ] **Step 5.4: Commit**

```bash
git add src/persistence/common.rs
git commit -m "refactor(merge): drop is_empty() partition branch (now guaranteed by preparse)"
```

---

## Task 6: `read_flow_effective` + `write_flow_effective`

**Files:**
- Modify: `src/persistence/flow.rs`
- Modify: `src/persistence/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 6.1: Write failing tests** — add to `src/persistence/flow.rs` `mod tests`:

```rust
    #[test]
    fn write_then_read_effective_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flow.effective.toml");
        let original = sample_flow();
        write_flow_effective(&path, &original).unwrap();
        let back = read_flow_effective(&path).unwrap();
        assert_eq!(back.uuid, original.uuid);
        assert_eq!(back.jobs.len(), 2);
    }

    #[test]
    fn read_effective_missing_file_returns_snapshot_missing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(".jm").join("flow.effective.toml");
        let err = read_flow_effective(&path).unwrap_err();
        match err {
            JobManagerError::SnapshotMissing { path: p, .. } => {
                assert_eq!(p, path);
            }
            other => panic!("expected SnapshotMissing, got {other:?}"),
        }
    }

    #[test]
    fn read_effective_parse_error_returns_toml_parse() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("flow.effective.toml");
        std::fs::write(&path, "this is = not = valid toml").unwrap();
        let err = read_flow_effective(&path).unwrap_err();
        assert!(matches!(err, JobManagerError::TomlParse { .. }), "got {err:?}");
    }
```

- [ ] **Step 6.2: Run tests to verify they fail**

```bash
cargo test --lib --all-features persistence::flow::tests::write_then_read_effective_roundtrip persistence::flow::tests::read_effective -- --nocapture
```

Expected: 3 tests FAIL with "no function `read_flow_effective` / `write_flow_effective`"

- [ ] **Step 6.3: Add the two new functions** to `src/persistence/flow.rs` (after `write_flow`):

```rust
/// Read a materialized snapshot. Unlike `read_flow`, this does not need a
/// `CommonConfig` — the snapshot has every default already baked in. If
/// the file is absent, returns `SnapshotMissing` with a hint pointing the
/// caller at `jm render <uuid>`.
pub fn read_flow_effective(path: &Path) -> Result<JobFlow, JobManagerError> {
    if !path.exists() {
        // Try to extract the uuid from the path: <root>/<uuid>/.jm/flow.effective.toml
        let uuid_hint = path
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or("<unknown>")
            .to_string();
        return Err(JobManagerError::SnapshotMissing {
            path: path.to_path_buf(),
            uuid: uuid_hint,
        });
    }
    let text = super::read_toml_string(path)?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

/// Write a materialized snapshot atomically. Creates `<flow_dir>/.jm/`
/// (and intermediate dirs) if missing.
pub fn write_flow_effective(path: &Path, flow: &JobFlow) -> Result<(), JobManagerError> {
    let body = toml::to_string_pretty(flow)?;
    super::atomic_write(path, body.as_bytes())
}
```

- [ ] **Step 6.4: Re-export from `src/persistence/mod.rs`**:

```rust
pub use flow::{read_flow, read_flow_effective, write_flow, write_flow_effective};
```

- [ ] **Step 6.5: Re-export from `src/lib.rs`** — update the `pub use persistence::{...}` line:

```rust
pub use persistence::{
    PathResolver, merge_with_defaults, read_common, read_flow, read_flow_effective,
    read_job_run, read_plan, write_common, write_flow, write_flow_effective, write_job_run,
    write_plan,
};
```

- [ ] **Step 6.6: Run tests to verify they pass**

```bash
cargo test --lib --all-features persistence::flow -- --nocapture
```

Expected: all `persistence::flow::tests::*` PASS

- [ ] **Step 6.7: Commit**

```bash
git add src/persistence/flow.rs src/persistence/mod.rs src/lib.rs
git commit -m "feat(persistence): add read_flow_effective / write_flow_effective"
```

---

## Task 7: `FlowRun::load_effective`

**Files:**
- Modify: `src/flow/run.rs`

- [ ] **Step 7.1: Write failing test** — add to `src/flow/run.rs` `mod tests`:

```rust
    #[test]
    fn load_effective_returns_snapshot_missing_when_absent() {
        use crate::persistence::PathResolver;
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path());
        let uuid = uuid::Uuid::nil();
        let err = FlowRun::load_effective(&resolver, uuid).unwrap_err();
        assert!(matches!(err, JobManagerError::SnapshotMissing { .. }));
    }

    #[test]
    fn load_effective_reads_snapshot_without_common() {
        use crate::persistence::{PathResolver, write_flow_effective, write_plan};
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path());
        let fr_src = fr_with_2_jobs();
        let uuid = uuid::Uuid::nil();

        write_flow_effective(&resolver.flow_effective_toml(&uuid), &fr_src.flow).unwrap();
        write_plan(&resolver.plan_toml(&uuid), &fr_src.plan).unwrap();

        let fr = FlowRun::load_effective(&resolver, uuid).unwrap();
        assert_eq!(fr.flow_uuid, uuid);
        assert!(fr.common.is_none(), "load_effective never reads common");
        assert_eq!(fr.flow.jobs.len(), 2);
    }
```

- [ ] **Step 7.2: Run tests to verify they fail**

```bash
cargo test --lib --all-features flow::run::tests::load_effective -- --nocapture
```

Expected: 2 tests FAIL with "no function `load_effective`"

- [ ] **Step 7.3: Implement `load_effective`** in `src/flow/run.rs` `impl FlowRun`:

```rust
    /// Load FlowRun from a materialized snapshot — used by tick/show paths
    /// that don't need `common.toml`. The `.flow.effective.toml` snapshot
    /// has every default already resolved.
    pub fn load_effective(
        resolver: &crate::persistence::PathResolver,
        flow_uuid: uuid::Uuid,
    ) -> Result<Self, JobManagerError> {
        let plan = crate::persistence::read_plan(&resolver.plan_toml(&flow_uuid))?;
        let flow = crate::persistence::read_flow_effective(
            &resolver.flow_effective_toml(&flow_uuid),
        )?;
        Ok(Self {
            flow_uuid,
            flow,
            plan,
            common: None,
        })
    }
```

- [ ] **Step 7.4: Run tests to verify they pass**

```bash
cargo test --lib --all-features flow::run::tests::load_effective -- --nocapture
```

Expected: 2 tests PASS

- [ ] **Step 7.5: Commit**

```bash
git add src/flow/run.rs
git commit -m "feat(flow): add FlowRun::load_effective for snapshot-driven paths"
```

---

## Task 8: `FlowRunner::submit` writes snapshot before render loop

**Files:**
- Modify: `src/runner/flow.rs`

- [ ] **Step 8.1: Write failing test** — add to `src/runner/flow.rs` (create `mod tests` at bottom if missing):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::flow::run::tests::fr_with_2_jobs;
    use crate::persistence::PathResolver;
    use crate::slurm::executor::MockExecutor;
    use crate::slurm::querier::InMemoryQuerier;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn submit_writes_effective_snapshot_dry_run() {
        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path());
        let fr = {
            let mut fr = fr_with_2_jobs();
            // Make sure both jobs have a real partition so render doesn't bail.
            fr.flow.jobs.values_mut().for_each(|j| {
                j.spec.config.partition = "long".to_string();
            });
            fr
        };
        let runner = FlowRunner::new(
            Box::new(MockExecutor::new()),
            Box::new(InMemoryQuerier::new(HashMap::new())),
            &resolver,
        );
        runner.submit(&fr, true).await.unwrap();
        let snap = resolver.flow_effective_toml(&fr.flow_uuid);
        assert!(snap.exists(), "snapshot not written at {}", snap.display());
    }
}
```

NOTE: `fr_with_2_jobs` is `pub(crate)` in `src/flow/run.rs` test module. If visibility is lower, raise it.

- [ ] **Step 8.2: Run test to verify it fails**

```bash
cargo test --lib --all-features runner::flow::tests::submit_writes_effective_snapshot_dry_run -- --nocapture
```

Expected: FAIL (snapshot file does not exist)

- [ ] **Step 8.3: Add snapshot write at start of `submit`** in `src/runner/flow.rs`:

Find `pub async fn submit` (~line 141), insert this AFTER the `order` is computed but BEFORE the candidate_jids loop:

```rust
        // Materialize snapshot before any render/submit work, so tick/show
        // can run later without re-reading common.toml.
        let eff_path = self.resolver.flow_effective_toml(&fr.flow_uuid);
        if let Some(parent) = eff_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| JobManagerError::Io {
                    path: parent.to_path_buf(),
                    source: e,
                })?;
        }
        tokio::task::spawn_blocking({
            let path = eff_path.clone();
            let flow = fr.flow.clone();
            move || crate::persistence::write_flow_effective(&path, &flow)
        })
        .await
        .map_err(|e| JobManagerError::Other(format!("write_flow_effective join: {e}")))??;
```

- [ ] **Step 8.4: Run tests to verify they pass**

```bash
cargo test --lib --all-features runner::flow:: -- --nocapture
```

Expected: PASS. Existing runner tests still pass (they happen to also hit this path now).

- [ ] **Step 8.5: Commit**

```bash
git add src/runner/flow.rs
git commit -m "feat(runner): write .flow.effective.toml at the start of submit/render"
```

---

## Task 9: `FlowRunner::tick` reads snapshot — verify

`FlowRunner::tick(&self, fr: &FlowRun)` takes a `&FlowRun`. The change here is at the **caller** layer — `tick` itself uses `fr.flow` and `fr.parents_of()`, which work the same whether `fr` came from `FlowRun::read` or `FlowRun::load_effective`. The CLI/test caller must call `FlowRun::load_effective` before passing `fr` to `tick`. We make that swap in Task 10 (CLI). The runner itself stays put.

- [ ] **Step 9.1: Add a verification test** — add to `src/runner/flow.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn tick_works_on_load_effective_fr() {
        use crate::flow::FlowRun;
        use crate::persistence::{PathResolver, write_flow_effective, write_plan};

        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path());
        let fr_src = {
            let mut fr = fr_with_2_jobs();
            fr.flow.jobs.values_mut().for_each(|j| {
                j.spec.config.partition = "long".to_string();
            });
            fr
        };
        let uuid = fr_src.flow_uuid;
        write_flow_effective(&resolver.flow_effective_toml(&uuid), &fr_src.flow).unwrap();
        write_plan(&resolver.plan_toml(&uuid), &fr_src.plan).unwrap();

        let fr = FlowRun::load_effective(&resolver, uuid).unwrap();
        let runner = FlowRunner::new(
            Box::new(MockExecutor::new()),
            Box::new(InMemoryQuerier::new(HashMap::new())),
            &resolver,
        );
        let result = runner.tick(&fr).await.unwrap();
        // No status files exist yet → transitions map should be empty.
        assert!(result.transitions.is_empty());
    }
```

- [ ] **Step 9.2: Run test**

```bash
cargo test --lib --all-features runner::flow::tests::tick_works_on_load_effective_fr -- --nocapture
```

Expected: PASS without further code changes.

- [ ] **Step 9.3: Commit**

```bash
git add src/runner/flow.rs
git commit -m "test(runner): verify tick works against snapshot-derived FlowRun"
```

---

## Task 10: CLI — `jm render --effective-only` flag + use `load_effective` for tick/show

**Files:**
- Modify: `src/bin/jm.rs`
- Modify (if needed): `tests/cli_smoke.rs`

- [ ] **Step 10.1: Update `Cmd::Render` enum variant** in `src/bin/jm.rs`:

```rust
    /// Render batch.bash + .flow.effective.toml. With --effective-only the
    /// batch.bash files are NOT touched, only the snapshot is refreshed.
    Render {
        target: String,
        #[arg(long)]
        effective_only: bool,
    },
```

- [ ] **Step 10.2: Update `Cmd::Render` dispatch** in `main`:

```rust
        Cmd::Render {
            ref target,
            effective_only,
        } => {
            let root = resolve_root(&cli)?;
            cmd_render(&root, target, effective_only).await
        }
```

- [ ] **Step 10.3: Update `cmd_render`**:

```rust
async fn cmd_render(
    root: &std::path::Path,
    target: &str,
    effective_only: bool,
) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::{PathResolver, write_flow_effective};
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::DryRunExecutor;
    use job_manager::slurm::querier::InMemoryQuerier;
    use std::collections::HashMap;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;

    if effective_only {
        let path = resolver.flow_effective_toml(&uuid);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        write_flow_effective(&path, &fr.flow)?;
        println!("updated .flow.effective.toml for {}", uuid);
        return Ok(());
    }

    let runner = FlowRunner::new(
        Box::new(DryRunExecutor),
        Box::new(InMemoryQuerier::new(HashMap::new())),
        &resolver,
    );
    runner.render_only(&fr).await?;
    println!("rendered {} jobs in {}", fr.flow.jobs.len(), uuid);
    Ok(())
}
```

- [ ] **Step 10.4: Update `cmd_tick` and `cmd_show`** to use `load_effective`:

```rust
async fn cmd_show(root: &std::path::Path, target: &str) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::{PathResolver, read_job_run};

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::load_effective(&resolver, uuid)?;
    println!("flow {} ({} jobs)", uuid, fr.flow.jobs.len());
    for jid in fr.flow.jobs.keys() {
        let p = resolver.status_file(&uuid, jid);
        let label = if p.exists() {
            let r = read_job_run(&p)?;
            match r.slurm_jobid {
                Some(j) => format!("{:?} (slurm_jobid={j})", r.lifecycle),
                None => format!("{:?}", r.lifecycle),
            }
        } else {
            "<pending>".to_string()
        };
        println!("  {}  {}", jid.0, label);
    }
    Ok(())
}

async fn cmd_tick(root: &std::path::Path, target: &str) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::DryRunExecutor;
    use job_manager::slurm::querier::SlurmQuerier;
    use slurm_async_runner::SlurmManager;
    use std::sync::Arc;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::load_effective(&resolver, uuid)?;
    let manager = Arc::new(SlurmManager::default());
    let querier = SlurmQuerier::new(manager);
    let runner = FlowRunner::new(Box::new(DryRunExecutor), Box::new(querier), &resolver);
    let result = runner.tick(&fr).await?;
    println!(
        "tick complete: {} transitions evaluated",
        result.transitions.len()
    );
    Ok(())
}
```

- [ ] **Step 10.5: Update `cmd_search`** — `walk_flows` now takes `Arc<CommonConfig>`:

```rust
async fn cmd_search(root: &std::path::Path, program: Option<&str>) -> anyhow::Result<()> {
    use futures::StreamExt;
    use job_manager::persistence::{PathResolver, read_common};
    use job_manager::walk::walk_flows;
    use std::sync::Arc;

    let resolver = PathResolver::new(root);
    let common_path = resolver.common_toml();
    let common = if common_path.exists() {
        read_common(&common_path)?
    } else {
        use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
        use slurm_async_runner::entities::slurm::SlurmJobConfig;
        CommonConfig {
            slurm_default: SlurmJobConfig {
                partition: String::new(),
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
            },
            directories: DirectoryConfig {
                project_root: std::path::PathBuf::from("."),
            },
        }
    };

    let s = walk_flows(root, Arc::new(common));
    let mut s = std::pin::pin!(s);
    while let Some(item) = s.next().await {
        let flow = item?;
        if let Some(p) = program
            && !flow.jobs.values().any(|j| j.spec.program.0 == p)
        {
            continue;
        }
        println!("{}\t{}", flow.uuid, flow.created_at);
    }
    Ok(())
}
```

- [ ] **Step 10.6: Build to confirm**

```bash
cargo build --bin jm --no-default-features 2>&1 | tail -20
```

Expected: builds clean

- [ ] **Step 10.7: Smoke test via cli_smoke**

```bash
cargo test --test cli_smoke --all-features -- --nocapture 2>&1 | tail -40
```

If failures appear because assertions about output text changed, update `tests/cli_smoke.rs` minimally to match the new flag list. Re-run.

- [ ] **Step 10.8: Commit**

```bash
git add src/bin/jm.rs tests/cli_smoke.rs
git commit -m "feat(cli): jm render --effective-only + tick/show use load_effective"
```

---

## Task 11: PyO3 wrapper — root inference for `read_flow`

**Files:**
- Modify: `src/py_export/persistence.rs`
- Modify: `src/py_export/mod.rs`

- [ ] **Step 11.1: Update `src/py_export/persistence.rs`**:

```rust
//! Python wrappers for the persistence layer (`common.toml`, `flow.toml`).

use pyo3::prelude::*;

use crate::error::JobManagerError;
use crate::persistence::common::{
    read_common as inner_read_common, write_common as inner_write_common,
};
use crate::persistence::flow::{
    read_flow as inner_read_flow, read_flow_effective as inner_read_flow_effective,
    write_flow as inner_write_flow,
};
use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::JobFlow;

/// Infer the `<root>` path from a `<root>/<flow_uuid>/flow.toml` path so we
/// can locate `<root>/common.toml`. Returns RootInferenceFailed if the path
/// is shorter than `<root>/<flow_uuid>/flow.toml`.
fn infer_root_common(path: &std::path::Path) -> Result<std::path::PathBuf, JobManagerError> {
    path.parent()
        .and_then(|flow_dir| flow_dir.parent())
        .map(|root| root.join("common.toml"))
        .ok_or_else(|| JobManagerError::RootInferenceFailed {
            path: path.to_path_buf(),
        })
}

pub fn read_common(path: std::path::PathBuf) -> PyResult<String> {
    let cc = inner_read_common(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&cc).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

pub fn write_common(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
    let cc: CommonConfig = toml::from_str(toml_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    inner_write_common(&path, &cc)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

pub fn read_flow(path: std::path::PathBuf) -> PyResult<String> {
    let common_path = infer_root_common(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    let common = if common_path.exists() {
        inner_read_common(&common_path)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
    } else {
        use gaussian_job_shared::config::common::DirectoryConfig;
        use slurm_async_runner::entities::slurm::SlurmJobConfig;
        CommonConfig {
            slurm_default: SlurmJobConfig {
                partition: String::new(),
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
            },
            directories: DirectoryConfig {
                project_root: std::path::PathBuf::from("."),
            },
        }
    };
    let fl = inner_read_flow(&path, &common)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&fl).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

pub fn write_flow(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
    let fl: JobFlow = toml::from_str(toml_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    inner_write_flow(&path, &fl)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

/// Read a materialized snapshot. No common.toml required.
pub fn read_flow_effective(path: std::path::PathBuf) -> PyResult<String> {
    let fl = inner_read_flow_effective(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&fl).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}
```

- [ ] **Step 11.2: Register the new pyfunction** in `src/py_export/mod.rs` — add after the existing `read_flow` registration (around line 138):

```rust
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn read_flow_effective(path: std::path::PathBuf) -> PyResult<String> {
        super::persistence::read_flow_effective(path)
    }
```

- [ ] **Step 11.3: Build the extension**

```bash
cargo build --all-features 2>&1 | tail -20
```

Expected: clean build

- [ ] **Step 11.4: Commit**

```bash
git add src/py_export/persistence.rs src/py_export/mod.rs
git commit -m "feat(py): read_flow infers common from path; add read_flow_effective"
```

---

## Task 12: Python re-export + regenerate `.pyi`

**Files:**
- Modify: `python/job_manager/__init__.py`
- Regenerated: `python/job_manager/_job_manager_core/__init__.pyi`

- [ ] **Step 12.1: Update `python/job_manager/__init__.py`** — add `read_flow_effective`:

Find the `read_flow = _core.read_flow` line and add right after:

```python
read_flow_effective = _core.read_flow_effective
```

Then add `"read_flow_effective"` to the `__all__` list.

- [ ] **Step 12.2: Regenerate `.pyi`**

```bash
cargo run --bin stub_gen
uv run ruff format python/
```

Expected: `python/job_manager/_job_manager_core/__init__.pyi` updated with `read_flow_effective` def.

- [ ] **Step 12.3: Rebuild Python extension**

```bash
uv run maturin develop
```

Expected: clean build

- [ ] **Step 12.4: Smoke-check Python import**

```bash
uv run python -c "from job_manager import read_flow, read_flow_effective, write_flow; print('OK')"
```

Expected: `OK`

- [ ] **Step 12.5: Commit**

```bash
git add python/job_manager/__init__.py python/job_manager/_job_manager_core/__init__.pyi
git commit -m "feat(py): re-export read_flow_effective and regenerate .pyi"
```

---

## Task 13: Integration test — `.jm/` isolation

**Files:**
- Create: `tests/integration_effective_isolation.rs`

- [ ] **Step 13.1: Create the test**

```rust
//! Verify that `.flow.effective.toml` makes `tick` / `show` independent of
//! `common.toml` — the snapshot is self-contained.

use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
use job_manager::flow::FlowRun;
use job_manager::persistence::{
    PathResolver, write_flow, write_flow_effective, write_plan,
};
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::tempdir;
use uuid::Uuid;

fn sample_common() -> CommonConfig {
    CommonConfig {
        slurm_default: SlurmJobConfig {
            partition: "long".to_string(),
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
        },
        directories: DirectoryConfig {
            project_root: PathBuf::from("/work"),
        },
    }
}

fn sample_flow(uuid: Uuid) -> JobFlow {
    let mut jobs = BTreeMap::new();
    jobs.insert(
        JobId::from("opt"),
        Job {
            spec: JobSpec {
                program: Program::from("echo"),
                config: SlurmJobConfig {
                    partition: "long".to_string(),
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
                },
                body: "true\n".to_string(),
            },
            parents: vec![],
        },
    );
    JobFlow {
        uuid,
        created_at: chrono::Utc::now(),
        tags: BTreeMap::new(),
        jobs,
    }
}

#[test]
fn load_effective_works_after_common_is_removed() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let uuid = Uuid::nil();

    let common = sample_common();
    let common_path = resolver.common_toml();
    std::fs::write(&common_path, toml::to_string(&common).unwrap()).unwrap();
    let flow = sample_flow(uuid);
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    let plan = job_manager::ExperimentPlan {
        jobs: {
            let mut m = BTreeMap::new();
            m.insert(JobId::from("opt"), BTreeMap::new());
            m
        },
    };
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();
    write_flow_effective(&resolver.flow_effective_toml(&uuid), &flow).unwrap();

    // Now nuke common.toml. load_effective should still work.
    std::fs::remove_file(&common_path).unwrap();

    let fr = FlowRun::load_effective(&resolver, uuid).unwrap();
    assert_eq!(fr.flow_uuid, uuid);
    assert_eq!(fr.flow.jobs.len(), 1);
    assert_eq!(
        fr.flow.jobs[&JobId::from("opt")].spec.config.partition,
        "long"
    );
}

#[test]
fn load_effective_fails_when_snapshot_missing() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let uuid = Uuid::nil();

    let plan = job_manager::ExperimentPlan {
        jobs: BTreeMap::new(),
    };
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let err = FlowRun::load_effective(&resolver, uuid).unwrap_err();
    assert!(matches!(
        err,
        job_manager::JobManagerError::SnapshotMissing { .. }
    ));
}
```

- [ ] **Step 13.2: Run the integration test**

```bash
cargo test --test integration_effective_isolation --all-features -- --nocapture
```

Expected: 2 tests PASS

- [ ] **Step 13.3: Commit**

```bash
git add tests/integration_effective_isolation.rs
git commit -m "test(integration): snapshot is self-contained; load_effective enforces presence"
```

---

## Task 14: Update existing integration tests

**Files:**
- Modify: `tests/integration_sp3.rs`
- Modify: `tests/integration_walk.rs`
- Modify: `tests/integration_plan.rs` (if it calls read_flow)

- [ ] **Step 14.1: Build to find call sites needing update**

```bash
cargo test --tests --all-features --no-run 2>&1 | head -40
```

Expected: error list naming `read_flow` and `walk_flows` call sites that haven't been updated.

- [ ] **Step 14.2: For each error**, add common (or `Arc<CommonConfig>` for walk_flows) at the call site. Pattern:

```rust
// Before:
let flow = job_manager::read_flow(&path)?;
// After:
let common = /* construct or load */;
let flow = job_manager::read_flow(&path, &common)?;

// Before:
let s = walk_flows(&path);
// After:
let s = walk_flows(&path, std::sync::Arc::new(common.clone()));
```

Reuse the `sample_common` helper pattern from Task 13. If many tests use it, extract into `tests/common/mod.rs`.

- [ ] **Step 14.3: Run integration tests**

```bash
cargo test --tests --all-features 2>&1 | tail -40
```

Expected: all tests PASS

- [ ] **Step 14.4: Commit**

```bash
git add tests/
git commit -m "test: update integration tests for read_flow(path, &common) and walk_flows(path, common)"
```

---

## Task 15: Python smoke test

**Files:**
- Create: `python/tests/test_read_flow_effective.py`

- [ ] **Step 15.1: Write the test**

```python
"""Smoke test: roundtrip via write_flow_effective ↔ read_flow_effective."""
from __future__ import annotations

import subprocess
from pathlib import Path

import job_manager


def test_read_flow_effective_after_render(tmp_path: Path) -> None:
    uuid = "01999999-0000-7000-8000-000000000000"

    common_toml = """
[slurm_default]
partition = "long"

[directories]
project_root = "/tmp/jm-test"
"""
    (tmp_path / "common.toml").write_text(common_toml.strip() + "\n")

    flow_dir = tmp_path / uuid
    flow_dir.mkdir()
    flow_toml = f"""
uuid = "{uuid}"
created_at = "2026-05-15T00:00:00Z"

[jobs.opt]
program = "echo"
body = "true\\n"
# partition は省略、common.toml の "long" が inject される
"""
    (flow_dir / "flow.toml").write_text(flow_toml.strip() + "\n")

    plan_toml = """
[jobs.opt]
"""
    (flow_dir / "plan.toml").write_text(plan_toml.strip() + "\n")

    repo_root = Path(__file__).resolve().parents[2]
    cmd = [
        "cargo",
        "run",
        "--bin",
        "jm",
        "--no-default-features",
        "--quiet",
        "--",
        "--root",
        str(tmp_path),
        "render",
        uuid,
        "--effective-only",
    ]
    r = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
    assert r.returncode == 0, f"jm render failed: stderr={r.stderr}"

    eff_path = flow_dir / ".jm" / "flow.effective.toml"
    assert eff_path.exists(), f"snapshot not written at {eff_path}"

    body = job_manager.read_flow_effective(str(eff_path))
    assert uuid in body
    assert 'partition = "long"' in body, f"expected default partition baked in: {body}"
```

- [ ] **Step 15.2: Run the test**

```bash
uv run pytest python/tests/test_read_flow_effective.py -v
```

Expected: PASS. If cargo run fails because of a missing toolchain step, ensure `cargo build --bin jm --no-default-features` works in isolation first.

- [ ] **Step 15.3: Commit**

```bash
git add python/tests/test_read_flow_effective.py
git commit -m "test(py): smoke test read_flow_effective after jm render --effective-only"
```

---

## Task 16: Update `examples/simple/inputs/`

**Files:**
- Modify: `examples/simple/inputs/01999999-0000-7000-8000-000000000000/flow.toml`

- [ ] **Step 16.1: Remove partition lines** in `flow.toml`. The current file has two `[jobs.*.config]` blocks each with `partition = "REPLACE_ME"`. After F2, the per-job `partition` is no longer required — common's value flows through.

Replace:

```toml
[jobs.opt.config]
partition = "REPLACE_ME"   # ← merge_with_defaults takes this over common.toml when non-empty; rewrite both before submit
```

with:

```toml
[jobs.opt.config]
# partition は common.toml の [slurm_default] から自動で補完される。
# 個別に上書きしたい場合のみここで partition = "..." を指定。
```

Same for `[jobs.freq.config]`.

- [ ] **Step 16.2: Sanity-check by parsing** — note: `common.toml` still has `partition = "REPLACE_ME"`. preparse treats `REPLACE_ME` as a non-empty string and injects it as-is. The point of this step is to verify the **TOML parse + materialize pipeline succeeds without partition lines in flow.toml**.

```bash
cargo run --bin jm --no-default-features --quiet -- \
    --root examples/simple/inputs \
    render 01999999-0000-7000-8000-000000000000 \
    --effective-only
```

Expected: command succeeds. `examples/simple/inputs/<uuid>/.jm/flow.effective.toml` exists and contains `partition = "REPLACE_ME"` for both jobs (inherited from common).

- [ ] **Step 16.3: Re-generate outputs/** (dry-run snapshot for committed example)

```bash
rm -rf examples/simple/outputs
cargo run --bin jm --no-default-features --quiet -- \
    --root examples/simple/inputs \
    submit 01999999-0000-7000-8000-000000000000 \
    --dry-run
```

This writes `examples/simple/inputs/<uuid>/.jm/{flow.effective.toml,<JobId>/batch.bash}`.

- [ ] **Step 16.4: Move artifacts into the committed `outputs/` snapshot path**:

```bash
mkdir -p examples/simple/outputs/01999999-0000-7000-8000-000000000000/.jm
cp -r examples/simple/inputs/01999999-0000-7000-8000-000000000000/.jm/* \
      examples/simple/outputs/01999999-0000-7000-8000-000000000000/.jm/
rm -rf examples/simple/inputs/01999999-0000-7000-8000-000000000000/.jm
```

- [ ] **Step 16.5: Inspect**

```bash
find examples/simple/outputs -type f
```

Expected output:
```
examples/simple/outputs/01999999-0000-7000-8000-000000000000/.jm/flow.effective.toml
examples/simple/outputs/01999999-0000-7000-8000-000000000000/.jm/opt/batch.bash
examples/simple/outputs/01999999-0000-7000-8000-000000000000/.jm/freq/batch.bash
```

- [ ] **Step 16.6: Commit**

```bash
git add examples/simple
git commit -m "docs(examples/simple): omit per-job partition; outputs/ uses .jm/ layout"
```

---

## Task 17: Update `examples/sweep/PLAN.md`

**Files:**
- Modify: `examples/sweep/PLAN.md`

- [ ] **Step 17.1: Update "File layout"** — replace the existing tree with:

```text
examples/sweep/
├── PLAN.md
├── README.md
├── author.py
│
├── inputs/                                            ← success variant
│   ├── common.toml
│   └── 0199999a-0000-7000-8000-000000000000/
│       ├── flow.toml                                  ← 7 jobs, 6 edges (partition omitted)
│       └── plan.toml
│
├── inputs-fail/                                       ← failure variant
│   ├── common.toml
│   └── 0199999a-0000-7000-8000-000000000001/
│       ├── flow.toml                                  ← opt__compound=1 body changed
│       └── plan.toml
│
├── outputs/                                           ← success snapshot
│   └── 0199999a-0000-7000-8000-000000000000/
│       └── .jm/                                       ← program-managed (hidden)
│           ├── flow.effective.toml
│           ├── prep/batch.bash
│           ├── opt__compound=0/batch.bash
│           ├── opt__compound=1/batch.bash
│           ├── opt__compound=2/batch.bash
│           ├── freq__compound=0/batch.bash
│           ├── freq__compound=1/batch.bash
│           └── freq__compound=2/batch.bash
│
└── outputs-fail/                                      ← failure snapshot
    └── 0199999a-0000-7000-8000-000000000001/
        └── .jm/
            ├── flow.effective.toml
            ├── prep/batch.bash
            ├── opt__compound={0,1,2}/batch.bash
            └── freq__compound={0,1,2}/batch.bash
```

- [ ] **Step 17.2: Add a note in the "common.toml" section** about partition defaulting:

```markdown
## common.toml

Same shape as `examples/simple/inputs/common.toml`: two `REPLACE_ME`
sentinels (`partition`, `project_root`), `time_limit = "00:10:00"`,
`job_name = "jm-sweep"`. **`partition` here flows through to every job's
`[jobs.*.config]` automatically (F2 defaulting), so per-job `[jobs.X.config]`
blocks can omit `partition`.**
```

- [ ] **Step 17.3: Update the Expected `.status.toml` outcomes** section: paths under `.jm/`:

Replace mentions like `<uuid>/freq__compound=1/.status.toml` with `<uuid>/.jm/freq__compound=1/status.toml` (note also no leading `.` on the filename).

- [ ] **Step 17.4: Update Q2 note** referring to `slurm-<jobid>.out` companion location: `.jm/<JobId>/slurm-*.out`.

- [ ] **Step 17.5: Commit**

```bash
git add examples/sweep/PLAN.md
git commit -m "docs(examples/sweep): update layout for .jm/ + omit per-job partition"
```

---

## Task 18: Update `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 18.1: Locate the layout description** in `CLAUDE.md`, currently:

```
- `PathResolver` is the single source of truth for paths. On-disk: `<root>/<flow_uuid>/{flow,plan}.toml` + `<root>/<flow_uuid>/<JobId>/{batch.bash, .status.toml, slurm-*.out/err}` + optional root-level `<root>/common.toml` (per-flow common is **not** supported).
```

Replace with:

```
- `PathResolver` is the single source of truth for paths. On-disk:
  `<root>/<flow_uuid>/{flow,plan}.toml` are user-authored;
  `<root>/<flow_uuid>/.jm/flow.effective.toml` is the program-written
  materialized snapshot; `<root>/<flow_uuid>/.jm/<JobId>/{batch.bash, status.toml, slurm-*.out/err}`
  are per-job program-managed artifacts; `<root>/common.toml` is the optional root-level common
  (per-flow common is **not** supported).
  **`flow.toml` is read-only user input from job-manager's perspective**; the program writes only
  under `.jm/`. The `--effective-only` mode of `jm render` regenerates the snapshot without
  re-rendering batch.bash.
```

- [ ] **Step 18.2: Add a note about partition defaulting**, as a new subsection (place it near the existing "Architecture cheatsheet" or "Out of scope" — wherever flow naturally fits):

```markdown
## common.toml defaulting

`flow.toml` may omit `[jobs.*.config] partition`; the value flows in from
`common.toml [slurm_default] partition` at `read_flow` time (TOML preparse).
A1's `SlurmJobConfig::partition` remains a required field of the type;
the omission is only at the on-disk TOML layer. See
`docs/superpowers/specs/2026-05-15-common-env-defaulting-design.md`.
```

- [ ] **Step 18.3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(CLAUDE): reflect .jm/ layout and common.toml partition defaulting"
```

---

## Task 19: Update `docs/architecture.md`

**Files:**
- Modify: `docs/architecture.md`

- [ ] **Step 19.1: Add Airflow/Prefect mapping** — append a section near the existing vocabulary mapping (or create one if absent):

```markdown
## common.toml as Pool template (Airflow / Prefect mapping)

| job-manager | Airflow | Prefect |
|---|---|---|
| `common.toml [slurm_default]` | DAG `default_args` | Work Pool `base_job_template` + variables |
| `flow.toml [jobs.*.config]` | Operator kwargs (partial) | Deployment variables (per-task override) |
| `read_flow(path, &common)` | `apply_defaults` + DAG load | template render |
| `.flow.effective.toml` | (not保存される) | Deployment spec (Cargo.lock 相当) |

`flow.toml` の `partition` を省略すると `common.toml` の値が `read_flow` 段で TOML
preparse によって inject される。これは Airflow の `default_args` 継承、Prefect の
template render と同型の機構。
```

- [ ] **Step 19.2: Add a note about `.flow.effective.toml`** — short paragraph:

```markdown
## `.flow.effective.toml` — materialized snapshot

`<flow_uuid>/.jm/flow.effective.toml` は `jm submit` / `jm render` 時に書かれる
materialized snapshot で、Cargo.lock パターンに対応する。`flow.toml` (partial input)
→ `.flow.effective.toml` (full spec) は一方向変換。`tick` / `show` はこの snapshot
を読み、`common.toml` は不要。
```

- [ ] **Step 19.3: Commit**

```bash
git add docs/architecture.md
git commit -m "docs(architecture): document common.toml defaulting + .flow.effective.toml"
```

---

## Task 20: Update `docs/development.md`

**Files:**
- Modify: `docs/development.md`

- [ ] **Step 20.1: Update any reference to `<JobId>/batch.bash`** to `<JobId>/.jm/batch.bash`:

```bash
grep -n "<JobId>" docs/development.md
grep -n ".status.toml" docs/development.md
```

For each match referring to layout, update to the `.jm/` form.

- [ ] **Step 20.2: Add a section about `.gitignore` for `.jm/`**:

```markdown
## `.gitignore` for program-managed `.jm/`

If you want git to ignore `.jm/` per-flow but still commit `flow.toml` /
`plan.toml`, drop a `.gitignore` file inside each flow_dir:

```
<root>/<flow_uuid>/.gitignore
```

containing just `.jm/`. Repo-root `.gitignore` cannot blanket-ignore `.jm/`
because `examples/*/outputs/<uuid>/.jm/` IS commit territory (it's the
snapshot we want shipped alongside the example).
```

- [ ] **Step 20.3: Commit**

```bash
git add docs/development.md
git commit -m "docs(development): reflect .jm/ layout and per-flow .gitignore strategy"
```

---

## Task 21: Verify SLURM `--output`/`--error` paths

**Files:**
- Inspect: `src/render/mod.rs`
- Inspect: `src/runner/flow.rs` (already touches the relevant fields)

- [ ] **Step 21.1: Search for hardcoded SLURM log paths**

```bash
grep -rn "slurm-.*\.out\|slurm-.*\.err\|--output\|--error" src/render/ src/runner/
```

Expected: paths come from `SlurmJobConfig.log_stdout` / `log_stderr` (configurable via `common.toml`) and the `SbatchCmd` builder. **If a hardcoded `<JobId>/slurm-*.out` template exists**, change it to `.jm/<JobId>/slurm-%j.out`.

- [ ] **Step 21.2: If any change was made**, write a regression test under `src/render/mod.rs` `mod tests` asserting the path appears under `.jm/<JobId>/`. Otherwise skip.

- [ ] **Step 21.3: Commit (if any change)**

```bash
git add src/render src/runner
git commit -m "fix(render): SLURM log default paths under .jm/<JobId>/"
```

---

## Task 22: Full CI gate

- [ ] **Step 22.1: Format check**

```bash
cargo fmt --check
```

Expected: clean (or run `cargo fmt` and re-commit)

- [ ] **Step 22.2: Clippy**

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: clean

- [ ] **Step 22.3: Full test run**

```bash
cargo test --all-features
```

Expected: all PASS

- [ ] **Step 22.4: Python tests**

```bash
uv run pytest python/tests -v
```

Expected: all PASS

- [ ] **Step 22.5: Coverage**

```bash
cargo llvm-cov --fail-under-lines 80
```

Expected: PASS (≥80%)

- [ ] **Step 22.6: stub_gen drift check**

```bash
cargo run --bin stub_gen
git diff --exit-code python/job_manager/_job_manager_core/*.pyi
```

Expected: no diff (the .pyi was already committed in Task 12)

- [ ] **Step 22.7: Final commit (if fmt/clippy fixes accumulated)**

```bash
git status
# if anything is staged for fmt/clippy reasons:
git commit -m "chore: cargo fmt / clippy fixes"
```

---

## Self-Review

**Spec coverage:**
- §3.1 file layout → Task 2 + Task 16/17
- §3.2 design principles → Task 8 (one-way), Task 7 (self-contained snapshot)
- §4.1 persistence functions → Tasks 3, 4, 5, 6
- §4.2 PathResolver → Task 2
- §4.3 Runner integration → Task 8
- §4.4 FlowRun loader → Task 4 (read), Task 7 (load_effective)
- §4.5 CLI → Task 10
- §4.6 SLURM output directives → Task 21
- §4.7 PyO3 → Tasks 11, 12
- §5 data flow → exercised by Tasks 13, 15 (integration + Python smoke)
- §6 error handling → Task 1 (variants) + assertions in Tasks 3, 6, 7, 13
- §7.1-7.3 testing → Tasks 3, 6, 7, 8, 9, 13, 14, 15
- §7.4 examples → Tasks 16, 17
- §7.5 docs → Tasks 18, 19, 20
- §7.6 CI gate → Task 22
- §8 breaking changes → naturally covered by Tasks 2, 4, 14
- §9 implementation order → mirrored

**Placeholder scan:** all steps contain concrete code, paths, and commands. No "TBD" / "implement later".

**Type consistency:** `JobManagerError` variants named identically across Task 1 and the rest. `read_flow(path, &CommonConfig)` signature is consistent in Tasks 4/11/13/14. `flow_effective_toml(uuid)` method name consistent in Tasks 2/6/7/8/10/11/13. `load_effective` consistent in Tasks 7/9/10/13.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-15-common-env-defaulting.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
