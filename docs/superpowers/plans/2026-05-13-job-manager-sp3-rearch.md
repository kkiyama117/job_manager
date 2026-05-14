# job-manager SP-3 Re-architecture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SP-1 / SP-2 で確立した data + plan layer を Airflow / Prefect 語彙に揃えて再構築し、`Executor` / `Querier` trait、`FlowRun` / `JobRun` / `Lifecycle` の新型、`FlowRunner` orchestrator、CLI `jm` を実装する。`.status.toml` schema を破壊的に書き換え、旧 `PerJobStatus / StatusEntry / SlurmFacade` を全削除する。

**Architecture:** A1 (`slurm-async-runner`) は完全不可侵、D2 (`gaussian-job-shared`) は Phase 0 PR で `CommonConfig` / `DirectoryConfig` に serde derives 追加のみ。job-manager 内部は `flow/`, `job/`, `slurm/`, `persistence/`, `render/`, `runner/` の 6 モジュールに再編。`Executor` trait で sbatch を抽象化し `--dry-run` / mock を自然に表現、`Lifecycle` 5 値に `Skipped` を追加して parent failed の伝播を表す。

**Tech Stack:** Rust 2024 (toml, serde, async-trait, tokio, thiserror, clap v4) / PyO3 0.28 / D2 (`gaussian_job_shared`) / A1 (`slurm_async_runner::SbatchManager` + `SlurmManager`).

**Spec reference:** `docs/superpowers/specs/2026-05-13-job-manager-sp3-rearch-design.md`

**Supersedes:** `docs/superpowers/plans/2026-05-13-job-manager-sp3.md` (v1, PR #10)

---

## File Structure

### D2 (Phase 0 PR — separate repo)

- Modify: `../gaussian-job-shared2/src/config/common.rs`
  - 役割: `CommonConfig` / `DirectoryConfig` に serde derives + `#[serde(deny_unknown_fields)]` 追加

### job-manager (本ブランチ `feat/sp3-submit-and-cli`)

| File | Status | 役割 |
|---|---|---|
| **Phase A: rename + move** |||
| `src/persistence/mod.rs` | CREATE | `path` / `flow` / `plan` / `common` / `job_run` を re-export |
| `src/persistence/path.rs` | MOVED from `src/path.rs` | `PathResolver` |
| `src/persistence/flow.rs` | MOVED from `src/flow_io.rs` | `read_flow` / `write_flow` |
| `src/persistence/plan.rs` | MOVED from `src/plan/io.rs` | `read_plan` / `write_plan` |
| `src/persistence/job_run.rs` | NEW (from `src/status/io.rs`) | `read_job_run` / `write_job_run` |
| `src/job/mod.rs` | NEW (from `src/status/mod.rs`) | `Lifecycle` + `JobRun` re-export |
| `src/job/lifecycle.rs` | NEW (replaces `src/status::PerJobStatus`) | `Lifecycle` enum (5 値) |
| `src/job/run.rs` | NEW (replaces `src/status::StatusEntry`) | `JobRun` struct |
| `src/slurm/mod.rs` | CREATE | `executor` / `querier` / `dependency` re-export |
| `src/slurm/querier.rs` | MOVED from `src/slurm_facade.rs` | `Querier` trait + `SlurmQuerier` / `InMemoryQuerier` |
| `src/search.rs` | MOVED from `src/filter.rs` | `SearchFilter::matches` |
| `src/runner/mod.rs` | CREATE | `flow` / `transition` re-export |
| `src/runner/transition.rs` | MOVED from `src/tick.rs` | `Decision` / `TickResult` / `decide_transition` |
| `src/plan/mod.rs` | MODIFY | `ExperimentPlan` のみ残す (`io.rs` は persistence に移動済み) |
| `src/lib.rs` | MODIFY | re-export を全面置換 (約 20 シンボル) |
| `src/py_export/status.rs` → `src/py_export/job.rs` | RENAME + MODIFY | `JobRun` / `Lifecycle` |
| `src/py_export/filter.rs` → `src/py_export/search.rs` | RENAME | `SearchFilter` |
| `src/py_export/tick.rs` → `src/py_export/transition.rs` | RENAME + MODIFY | `Decision` / `TickResult` |
| `src/py_export/mod.rs` | MODIFY | submodule registration を新名に追従 |
| **Phase B: common** |||
| `src/persistence/common.rs` | CREATE | `read_common` / `write_common` + `merge_with_defaults` |
| `src/persistence/mod.rs` | MODIFY | `pub mod common` 追加 |
| `src/persistence/path.rs` | MODIFY | `PathResolver::common_toml()` getter 追加 |
| **Phase C: render** |||
| `src/render/mod.rs` | CREATE | `render_batch_bash` / `sanitize_var_name` / `quote_for_bash` |
| `src/persistence/path.rs` | MODIFY | `PathResolver::batch_bash()` getter 追加 |
| **Phase D: slurm submit infra** |||
| `src/slurm/executor.rs` | CREATE | `Executor` trait + `SbatchExecutor` / `DryRunExecutor` / `MockExecutor` |
| `src/slurm/dependency.rs` | CREATE | `build(parents, submitted, jid)` → `SlurmDependency` |
| `src/error.rs` | MODIFY | 新 variants 追加 |
| **Phase E: runner** |||
| `src/flow/mod.rs` | CREATE | `FlowRun` / `topology` re-export |
| `src/flow/topology.rs` | CREATE | Kahn's algorithm + cycle detection |
| `src/flow/run.rs` | CREATE | `FlowRun` struct + 関連 methods |
| `src/runner/flow.rs` | CREATE | `FlowRunner` struct + `submit` / `tick` / `render_only` |
| `src/runner/transition.rs` | MODIFY | `decide_transition` に `parent_lifecycles` 引数追加 |
| **Phase F: CLI** |||
| `Cargo.toml` | MODIFY | clap dep + `[[bin]] jm` |
| `src/bin/jm.rs` | CREATE | clap CLI + 5 subcommands |
| `tests/cli_smoke.rs` | CREATE | CLI smoke tests |
| **Phase G: Python** |||
| `src/py_export/flow.rs` | CREATE | `FlowRun` pyclass |
| `src/py_export/render.rs` | CREATE | `render_batch_bash` pyfunction |
| `src/py_export/runner.rs` | CREATE | `submit_flow` async pyfunction |
| `src/py_export/persistence.rs` | CREATE | `read_common` / `write_common` |
| `src/py_export/mod.rs` | MODIFY | 全 submodule 再登録 |
| `python/job_manager/__init__.py` | MODIFY | 新型を re-export |
| `python/tests/test_*.py` | MODIFY/CREATE | 全テストを新型対応 |
| **Integration** |||
| `tests/integration_sp3.rs` | CREATE | 12-job sample + MockExecutor + tick (InMemoryQuerier) |

---

## Phase 0 — D2 PR: `CommonConfig` serde derives

D2 (`gaussian-job-shared2`) repo で `CommonConfig` + `DirectoryConfig` に serde derive + `#[serde(deny_unknown_fields)]` を追加する PR を出す。**この PR が merge されるまで job-manager 側はビルドできない**ため必ず先行する。

**branch (D2 repo):** `feat/serde-common-config`

### Task 0.1: D2 `CommonConfig` / `DirectoryConfig` に serde derives

**Files:**
- Modify: `../gaussian-job-shared2/src/config/common.rs`

- [ ] **Step 1: 現状確認**

```bash
cat ../gaussian-job-shared2/src/config/common.rs
```

期待: `#[derive(Debug, Clone)]` のみ (serde derive なし)。

- [ ] **Step 2: ファイルを書き換える**

`../gaussian-job-shared2/src/config/common.rs` 全置換:

```rust
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use std::path::PathBuf;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommonConfig {
    pub slurm_default: SlurmJobConfig,
    pub directories: DirectoryConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryConfig {
    pub project_root: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;

    fn sample() -> CommonConfig {
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

    #[test]
    fn serde_round_trip() {
        let original = sample();
        let toml_str = toml::to_string(&original).unwrap();
        let restored: CommonConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(restored.slurm_default.partition, original.slurm_default.partition);
        assert_eq!(restored.directories.project_root, original.directories.project_root);
    }

    #[test]
    fn deny_unknown_fields_rejects_extra_top_level() {
        let bad = r#"
[slurm_default]
partition = "long"

[directories]
project_root = "/work"

[bogus]
key = "value"
"#;
        let result: Result<CommonConfig, _> = toml::from_str(bad);
        assert!(result.is_err());
    }
}
```

- [ ] **Step 3: D2 ビルド・テスト**

```bash
cd ../gaussian-job-shared2
cargo test --all-features
```

期待: 全 PASS、新 2 テストも含まれる。

- [ ] **Step 4: コミット (D2 repo)**

```bash
cd ../gaussian-job-shared2
git add src/config/common.rs
git commit -m "feat: derive Serialize/Deserialize for CommonConfig and DirectoryConfig

job-manager SP-3 で <root>/common.toml として TOML 永続化するため
serde derive と #[serde(deny_unknown_fields)] を追加。

newtype 不可侵原則と無関係 (struct shape は変えていない)。
既存 consumer (job-manager 以外) に互換性破壊なし — 追加のみ。"
```

- [ ] **Step 5: D2 PR を出して merge**

`gh pr create` で PR を作成、CI green を確認、merge。**この後 job-manager 側に戻る**。

```bash
cd ../job-manager
```

---

## Phase A — Rename + Move

既存 SP-1/SP-2 モジュールを spec §2.2 の新構成に再配置する。**`.status.toml` schema 書き換えは A.5 で実施**、これ以外は構造変更のみで挙動は同じ。各タスクは独立コミット可能。

### Task A.1: Cargo.toml に clap 依存と `jm` bin entry を追加 (Phase F 前準備)

**Files:**
- Modify: `Cargo.toml`
- Create: `src/bin/jm.rs` (stub)

- [ ] **Step 1: 現状確認**

```bash
grep -A2 "\[\[bin\]\]" Cargo.toml
```

期待: `stub_gen` の 1 entry のみ。

- [ ] **Step 2: `Cargo.toml` に追加**

`[[bin]] name = "stub_gen"` ブロックの直後に追加:

```toml
[[bin]]
name = "jm"
path = "src/bin/jm.rs"
```

`[dependencies]` 末尾に追加:

```toml
clap = { version = "4", features = ["derive"] }
```

- [ ] **Step 3: 一時的に `src/bin/jm.rs` をスタブで作成**

`src/bin/jm.rs`:

```rust
fn main() {
    eprintln!("jm CLI: not implemented yet (Phase F)");
    std::process::exit(2);
}
```

- [ ] **Step 4: ビルド確認**

```bash
cargo build --bin jm
```

期待: 成功、`jm` 実行ファイル生成。

- [ ] **Step 5: コミット**

```bash
git add Cargo.toml src/bin/jm.rs
git commit -m "chore(sp3): add clap dep and jm bin entry (Phase F prep)

Phase F で実装する jm CLI のために clap v4 依存と [[bin]] entry を
先行追加。バイナリ自体は Phase F.1 まで stub のまま。"
```

---

### Task A.2: `src/path.rs` → `src/persistence/path.rs`

**Files:**
- Create: `src/persistence/mod.rs`
- Move: `src/path.rs` → `src/persistence/path.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: ファイル移動 + ディレクトリ作成**

```bash
mkdir -p src/persistence
git mv src/path.rs src/persistence/path.rs
```

- [ ] **Step 2: `src/persistence/mod.rs` を新規作成**

```rust
//! Persistence layer — all TOML file I/O lives here.
//!
//! Submodules are organized by file kind (one TOML schema per submodule).

pub mod path;

pub use path::PathResolver;
```

- [ ] **Step 3: `src/lib.rs` 更新**

旧 `pub mod path; pub use path::PathResolver;` を以下に置換:

```rust
pub mod persistence;
pub use persistence::PathResolver;
```

- [ ] **Step 4: 全 `use crate::path::` を置換**

```bash
grep -rln "use crate::path::" src/ tests/ | xargs sed -i 's|use crate::path::|use crate::persistence::path::|g'
```

- [ ] **Step 5: テスト pass 確認**

```bash
cargo build && cargo test --all-features 2>&1 | tail -10
```

- [ ] **Step 6: コミット**

```bash
git add -A
git commit -m "refactor(sp3): move path.rs to persistence/path.rs

挙動は完全不変、import path だけ変わる。
crate::path::PathResolver → crate::persistence::path::PathResolver"
```

---

### Task A.3: `src/flow_io.rs` → `src/persistence/flow.rs`

**Files:**
- Move: `src/flow_io.rs` → `src/persistence/flow.rs`
- Modify: `src/persistence/mod.rs`, `src/lib.rs`

- [ ] **Step 1: 移動**

```bash
git mv src/flow_io.rs src/persistence/flow.rs
```

- [ ] **Step 2: `src/persistence/mod.rs` 更新**

```rust
pub mod path;
pub mod flow;

pub use path::PathResolver;
pub use flow::{read_flow, write_flow};
```

- [ ] **Step 3: `src/lib.rs` 更新**

旧 `pub mod flow_io; pub use flow_io::{read_flow, write_flow};` を以下に置換:

```rust
pub use persistence::{read_flow, write_flow};
```

- [ ] **Step 4: 全 `crate::flow_io` を置換**

```bash
grep -rln "crate::flow_io" src/ tests/ | xargs sed -i 's|crate::flow_io|crate::persistence::flow|g'
```

- [ ] **Step 5: テスト pass 確認**

```bash
cargo build && cargo test --all-features 2>&1 | tail -10
```

- [ ] **Step 6: コミット**

```bash
git add -A
git commit -m "refactor(sp3): move flow_io.rs to persistence/flow.rs

crate::flow_io::{read_flow, write_flow} → crate::persistence::flow::{...}
挙動不変。"
```

---

### Task A.4: `src/plan/io.rs` → `src/persistence/plan.rs`

**Files:**
- Move: `src/plan/io.rs` → `src/persistence/plan.rs`
- Modify: `src/plan/mod.rs` (io 参照削除)
- Modify: `src/persistence/mod.rs`, `src/lib.rs`

- [ ] **Step 1: 移動**

```bash
git mv src/plan/io.rs src/persistence/plan.rs
```

- [ ] **Step 2: `src/plan/mod.rs` から `pub mod io;` を削除**

```bash
sed -i '/pub mod io;/d' src/plan/mod.rs
```

- [ ] **Step 3: `src/persistence/mod.rs` 更新**

```rust
pub mod path;
pub mod flow;
pub mod plan;

pub use path::PathResolver;
pub use flow::{read_flow, write_flow};
pub use plan::{read_plan, write_plan};
```

- [ ] **Step 4: `src/lib.rs` 更新**

```rust
pub use persistence::{read_plan, write_plan};
```

旧 `pub use plan::io::{read_plan, write_plan};` を削除。

- [ ] **Step 5: 全 `crate::plan::io` を置換**

```bash
grep -rln "crate::plan::io" src/ tests/ | xargs sed -i 's|crate::plan::io|crate::persistence::plan|g'
```

- [ ] **Step 6: `src/persistence/plan.rs` 内の `super::ExperimentPlan` を `crate::plan::ExperimentPlan` に修正**

```bash
sed -i 's|use super::ExperimentPlan|use crate::plan::ExperimentPlan|g' src/persistence/plan.rs
```

- [ ] **Step 7: テスト pass 確認**

```bash
cargo build && cargo test --all-features 2>&1 | tail -10
```

- [ ] **Step 8: コミット**

```bash
git add -A
git commit -m "refactor(sp3): move plan/io.rs to persistence/plan.rs

plan/ module は ExperimentPlan struct のみ残す。
crate::plan::io::{read_plan, write_plan} → crate::persistence::plan::{...}"
```

---

### Task A.5: `status/` → `job/` + `persistence/job_run.rs` + Lifecycle / JobRun rename (BREAKING)

**この task が最も破壊的**。`PerJobStatus` → `Lifecycle` (5 値 snake_case)、`StatusEntry` → `JobRun`、`.status.toml` schema 書き換え。

**Files:**
- Create: `src/job/mod.rs`, `src/job/lifecycle.rs`, `src/job/run.rs`
- Create: `src/persistence/job_run.rs`
- Delete: `src/status/mod.rs`, `src/status/io.rs`
- Modify: `src/persistence/mod.rs`, `src/lib.rs`, 全 consumer

- [ ] **Step 1: 旧ファイル読み**

```bash
cat src/status/mod.rs && echo "---" && cat src/status/io.rs
```

`PerJobStatus` の値 (Queued / Running / Done / Failed) と `StatusEntry` の field を控える。

- [ ] **Step 2: `src/job/lifecycle.rs` 新規作成 (failing test 含む)**

`src/job/lifecycle.rs`:

```rust
//! Lifecycle — per-job state machine.
//!
//! Pending は enum value にしない (ファイル不在で表現)。

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    Queued,
    Running,
    Success,
    Failed,
    Skipped,
}

impl Lifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(self, Lifecycle::Success | Lifecycle::Failed | Lifecycle::Skipped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_snake_case() {
        assert_eq!(serde_json::to_string(&Lifecycle::Queued).unwrap(), "\"queued\"");
        assert_eq!(serde_json::to_string(&Lifecycle::Success).unwrap(), "\"success\"");
        assert_eq!(serde_json::to_string(&Lifecycle::Skipped).unwrap(), "\"skipped\"");
    }

    #[test]
    fn deserialize_rejects_pascal_case() {
        let result: Result<Lifecycle, _> = serde_json::from_str("\"Queued\"");
        assert!(result.is_err(), "PascalCase should be rejected");
    }

    #[test]
    fn is_terminal_marks_terminal_states() {
        assert!(!Lifecycle::Queued.is_terminal());
        assert!(!Lifecycle::Running.is_terminal());
        assert!(Lifecycle::Success.is_terminal());
        assert!(Lifecycle::Failed.is_terminal());
        assert!(Lifecycle::Skipped.is_terminal());
    }
}
```

- [ ] **Step 3: `src/job/run.rs` 新規作成**

`src/job/run.rs`:

```rust
//! JobRun — per-job runtime state (旧 StatusEntry の置換、Airflow TaskInstance 相当)。

use crate::job::lifecycle::Lifecycle;
use slurm_async_runner::JobStatus;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRun {
    pub lifecycle: Lifecycle,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub slurm_jobid: Option<u64>,
    pub slurm_status: Option<JobStatus>,
    pub note: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample() -> JobRun {
        JobRun {
            lifecycle: Lifecycle::Queued,
            updated_at: chrono::Utc.with_ymd_and_hms(2026, 5, 13, 12, 34, 56).unwrap(),
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
```

- [ ] **Step 4: `src/job/mod.rs` 新規作成**

```rust
pub mod lifecycle;
pub mod run;

pub use lifecycle::Lifecycle;
pub use run::JobRun;
```

- [ ] **Step 5: `src/persistence/job_run.rs` 新規作成**

```rust
//! `<job_dir>/.status.toml` の atomic read / write.

use std::fs;
use std::path::Path;

use crate::concurrency;
use crate::error::JobManagerError;
use crate::job::run::JobRun;

#[must_use = "read_job_run returns the parsed JobRun; ignoring it drops the data"]
pub fn read_job_run(path: &Path) -> Result<JobRun, JobManagerError> {
    let text = fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| JobManagerError::Toml {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_job_run(path: &Path, run: &JobRun) -> Result<(), JobManagerError> {
    let text = toml::to_string(run).map_err(|e| JobManagerError::Toml {
        path: path.to_path_buf(),
        source: toml::de::Error::custom(e.to_string()),
    })?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    concurrency::atomic_write(path, text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::lifecycle::Lifecycle;
    use tempfile::tempdir;

    #[test]
    fn round_trip_through_disk() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("sample/.status.toml");
        let run = JobRun {
            lifecycle: Lifecycle::Queued,
            updated_at: chrono::Utc::now(),
            slurm_jobid: Some(42),
            slurm_status: None,
            note: Some("hello".to_string()),
        };
        write_job_run(&p, &run).unwrap();
        let restored = read_job_run(&p).unwrap();
        assert_eq!(restored.lifecycle, Lifecycle::Queued);
        assert_eq!(restored.slurm_jobid, Some(42));
        assert_eq!(restored.note.as_deref(), Some("hello"));
    }
}
```

`toml::ser::Error` を `toml::de::Error::custom` で wrap する代わりに、`JobManagerError::Toml` の source 型が合わなければ `Serialize` variant を追加するか、既存の `Toml` variant を `anyhow::Error` source に変更する選択をする (実装者判断、最小変更を優先)。

- [ ] **Step 6: 旧 `src/status/` を削除**

```bash
git rm src/status/mod.rs src/status/io.rs
rmdir src/status 2>/dev/null || true
```

- [ ] **Step 7: `src/persistence/mod.rs` 更新**

```rust
pub mod path;
pub mod flow;
pub mod plan;
pub mod job_run;

pub use path::PathResolver;
pub use flow::{read_flow, write_flow};
pub use plan::{read_plan, write_plan};
pub use job_run::{read_job_run, write_job_run};
```

- [ ] **Step 8: `src/lib.rs` 更新**

```rust
// 削除
pub mod status;
pub use status::{PerJobStatus, StatusEntry};

// 追加
pub mod job;
pub use job::{Lifecycle, JobRun};
pub use persistence::{read_job_run, write_job_run};
```

- [ ] **Step 9: 全 consumer を rename**

```bash
grep -rln "PerJobStatus\|StatusEntry\|status::io\|crate::status" src/ tests/
```

各ヒットを書き換え:
- `PerJobStatus` → `Lifecycle`
- `PerJobStatus::Done` → `Lifecycle::Success` (★ 名前変更注意)
- `PerJobStatus::Queued` → `Lifecycle::Queued`
- `PerJobStatus::Running` → `Lifecycle::Running`
- `PerJobStatus::Failed` → `Lifecycle::Failed`
- `StatusEntry` → `JobRun`
- `crate::status::io::read_status` → `crate::persistence::job_run::read_job_run`
- `crate::status::io::write_status` → `crate::persistence::job_run::write_job_run`
- `use crate::status::` → 削除して必要な型を `use crate::job::` から取る

- [ ] **Step 10: テスト全 pass 確認**

```bash
cargo test --all-features 2>&1 | tail -30
```

期待: 全 pass。warning なし。失敗テストがあれば旧 PascalCase / 旧 `"Done"` 値を期待しているはず → snake_case + `"success"` に書き換え。

- [ ] **Step 11: コミット**

```bash
git add -A
git commit -m "refactor(sp3)!: rename status to job and PerJobStatus to Lifecycle

BREAKING:
- PerJobStatus (4 値) → Lifecycle (5 値): \"Done\" → \"success\",
  新規 \"skipped\" 追加 (parent failed の伝播用)
- StatusEntry → JobRun (Airflow TaskInstance 用語)
- .status.toml schema が snake_case に変わる (旧 PascalCase ファイル非互換、
  SP-3 リリース前のため migration 不要)
- crate::status::io → crate::persistence::job_run
- 旧 src/status/ 削除

依存テスト ~20 箇所を全て新名に書き換え。A1 (slurm-async-runner) 変更ゼロ。"
```

---

### Task A.6: `src/slurm_facade.rs` → `src/slurm/querier.rs` + rename

**Files:**
- Create: `src/slurm/mod.rs`
- Move: `src/slurm_facade.rs` → `src/slurm/querier.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: 移動 + rename**

```bash
mkdir -p src/slurm
git mv src/slurm_facade.rs src/slurm/querier.rs
```

- [ ] **Step 2: `src/slurm/querier.rs` 内で trait と struct を rename**

```bash
sed -i \
  -e 's|pub trait SlurmFacade|pub trait Querier|g' \
  -e 's|impl SlurmFacade for|impl Querier for|g' \
  -e 's|pub struct A1SlurmFacade|pub struct SlurmQuerier|g' \
  -e 's|impl A1SlurmFacade|impl SlurmQuerier|g' \
  -e 's|pub struct InMemorySlurmFacade|pub struct InMemoryQuerier|g' \
  -e 's|impl InMemorySlurmFacade|impl InMemoryQuerier|g' \
  -e 's|async fn query_states_batch|async fn query|g' \
  -e 's|\.query_states_batch(|.query(|g' \
  src/slurm/querier.rs
```

- [ ] **Step 3: `src/slurm/mod.rs` を新規作成**

```rust
//! SLURM-facing modules — every contact point with A1 lives here.
//!
//! `executor` (sbatch submit) と `querier` (sacct query) で 2 方向に分離。

pub mod querier;

pub use querier::{Querier, SlurmQuerier, InMemoryQuerier};
```

(executor / dependency は Phase D で追加)

- [ ] **Step 4: `src/lib.rs` 更新**

```rust
// 削除
pub mod slurm_facade;
pub use slurm_facade::{A1SlurmFacade, InMemorySlurmFacade, SlurmFacade};

// 追加
pub mod slurm;
pub use slurm::querier::{Querier, SlurmQuerier, InMemoryQuerier};
```

- [ ] **Step 5: 全消費箇所を rename**

```bash
grep -rln "SlurmFacade\|A1SlurmFacade\|InMemorySlurmFacade\|slurm_facade" src/ tests/
```

各ヒットを書き換え:
- `SlurmFacade` → `Querier`
- `A1SlurmFacade` → `SlurmQuerier`
- `InMemorySlurmFacade` → `InMemoryQuerier`
- `crate::slurm_facade::` → `crate::slurm::querier::`
- `.query_states_batch(` → `.query(`

- [ ] **Step 6: テスト全 pass 確認**

```bash
cargo test --all-features 2>&1 | tail -20
```

- [ ] **Step 7: コミット**

```bash
git add -A
git commit -m "refactor(sp3)!: rename slurm_facade to slurm/querier

BREAKING:
- SlurmFacade trait → Querier
- A1SlurmFacade → SlurmQuerier (A1 SlurmManager を wrap する事実を反映)
- InMemorySlurmFacade → InMemoryQuerier
- メソッド名 query_states_batch → query
- crate::slurm_facade → crate::slurm::querier

Phase D で追加する slurm::executor (Sbatch submit 側) と対をなす設計。
ロジックは完全不変、rename のみ。"
```

---

### Task A.7: `src/filter.rs` → `src/search.rs`

**Files:**
- Move: `src/filter.rs` → `src/search.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: 移動**

```bash
git mv src/filter.rs src/search.rs
```

- [ ] **Step 2: `src/lib.rs` 更新**

```rust
// 旧
pub mod filter;
pub use filter::{SearchFilter, matches};

// 新
pub mod search;
pub use search::{SearchFilter, matches};
```

- [ ] **Step 3: 全 `use crate::filter` を置換**

```bash
grep -rln "crate::filter" src/ tests/ | xargs sed -i 's|crate::filter|crate::search|g'
```

- [ ] **Step 4: テスト pass 確認**

```bash
cargo test --all-features 2>&1 | tail -10
```

- [ ] **Step 5: コミット**

```bash
git add -A
git commit -m "refactor(sp3): rename filter.rs to search.rs

filter は実装語、search が役割語。挙動不変、rename のみ。"
```

---

### Task A.8: `src/tick.rs` → `src/runner/transition.rs`

`tick_many` は Phase E で `FlowRunner::tick` に統合される。Phase A.8 では `decide_transition` / `Decision` / `TickResult` を `transition.rs` に移動し、`tick_many` も一時的にそのまま残す。

**Files:**
- Create: `src/runner/mod.rs`
- Move: `src/tick.rs` → `src/runner/transition.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: 移動**

```bash
mkdir -p src/runner
git mv src/tick.rs src/runner/transition.rs
```

- [ ] **Step 2: `src/runner/transition.rs` 内で `PerJobStatus` → `Lifecycle` 置換**

```bash
sed -i \
  -e 's|PerJobStatus|Lifecycle|g' \
  -e 's|use crate::status::|use crate::job::|g' \
  -e 's|Lifecycle::Done|Lifecycle::Success|g' \
  src/runner/transition.rs
```

- [ ] **Step 3: `src/runner/mod.rs` 新規作成**

```rust
//! Orchestration layer.

pub mod transition;

pub use transition::{Decision, TickResult, decide_transition, tick_many};
```

(`flow` は Phase E で追加)

- [ ] **Step 4: `src/lib.rs` 更新**

```rust
// 旧
pub mod tick;
pub use tick::{Decision, TickResult, decide_transition, tick_many};

// 新
pub mod runner;
pub use runner::{Decision, TickResult, decide_transition, tick_many};
```

- [ ] **Step 5: 全 consumer を置換**

```bash
grep -rln "crate::tick" src/ tests/ | xargs sed -i 's|crate::tick|crate::runner::transition|g'
```

- [ ] **Step 6: テスト全 pass 確認**

```bash
cargo test --all-features 2>&1 | tail -20
```

期待: 全 pass。`decide_transition` の semantics は **変えていない**ので既存テストは pass する。

- [ ] **Step 7: コミット**

```bash
git add -A
git commit -m "refactor(sp3)!: move tick.rs to runner/transition.rs and rename to Lifecycle

BREAKING:
- crate::tick → crate::runner::transition
- 内部の PerJobStatus 参照を Lifecycle に置換 (Done → Success)

tick_many は Phase E で FlowRunner::tick に統合予定、本タスクでは一時的に残す。"
```

---

### Task A.9: `view.rs::CalcView` の再評価

**Files:**
- Read: `src/view.rs`
- Modify or Delete: `src/view.rs`

- [ ] **Step 1: 現状確認**

```bash
cat src/view.rs && echo "--- usage ---" && grep -rn "CalcView" src/ tests/ python/
```

- [ ] **Step 2: 判断ルール**

- 外部 (tests / py_export / Python) から使用ありなら **残す + 内部 PerJobStatus → Lifecycle 更新のみ**
- 使用無しなら削除候補

**Most likely:** `src/py_export/view.rs` で pyclass 化されているので残す。

- [ ] **Step 3 (残す場合): 内部 rename**

```bash
grep "PerJobStatus\|StatusEntry\|::Done" src/view.rs src/py_export/view.rs
```

ヒットがあれば各箇所で `PerJobStatus` → `Lifecycle`、`::Done` → `::Success` に書き換え。

- [ ] **Step 4: テスト pass 確認**

```bash
cargo test --all-features 2>&1 | tail -10
```

- [ ] **Step 5: コミット**

```bash
git add -A
git commit -m "refactor(sp3): update CalcView for Lifecycle rename

CalcView は Python API で使用されているため残す。
内部の PerJobStatus / Done 参照のみ Lifecycle / Success に置換。"
```

---

### Task A.10: `src/lib.rs` re-export sweep + Python `__init__.py` 暫定対応

**Files:**
- Modify: `src/lib.rs`, `python/job_manager/__init__.py`

- [ ] **Step 1: 旧名残検出**

```bash
grep -nE "PerJobStatus|StatusEntry|SlurmFacade|A1SlurmFacade|InMemorySlurmFacade|read_status|write_status|crate::status|crate::tick|crate::filter|crate::path|crate::flow_io|crate::slurm_facade" src/ tests/
```

期待: 0 ヒット。残っていれば書き換え。

- [ ] **Step 2: `python/job_manager/__init__.py` の旧 alias を全削除**

```bash
grep -n "PerJobStatus\|StatusEntry\|SlurmFacade\|read_status\|write_status\|A1SlurmFacade\|InMemorySlurmFacade" python/job_manager/__init__.py
```

ヒット箇所を新名に書き換え (Phase G.2 で完全対応するが、ここでビルドが通る程度に調整):
- `PerJobStatus` → `Lifecycle`
- `StatusEntry` → `JobRun`
- `read_status` → `read_job_run`
- `write_status` → `write_job_run`
- `SlurmFacade` → `Querier`
- `A1SlurmFacade` → `SlurmQuerier`
- `InMemorySlurmFacade` → `InMemoryQuerier`

- [ ] **Step 3: 全テスト + clippy + fmt 確認**

```bash
cargo test --all-features 2>&1 | tail -10
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -10
cargo fmt --check
```

- [ ] **Step 4: コミット**

```bash
git add -A
git commit -m "refactor(sp3): Phase A complete — final sweep

lib.rs の re-export が新モジュール構成の新名のみを参照、
Python __init__.py も新名に追従 (Phase G.2 で詳細補強)。"
```

---

## Phase B — common

### Task B.1: `persistence::common::{read_common, write_common}`

**Files:**
- Create: `src/persistence/common.rs`
- Modify: `src/persistence/mod.rs`, `src/lib.rs`

- [ ] **Step 1: failing test を書く**

`src/persistence/common.rs`:

```rust
//! <root>/common.toml read / write.

use std::fs;
use std::path::Path;

use gaussian_job_shared::config::common::CommonConfig;

use crate::concurrency;
use crate::error::JobManagerError;

#[must_use = "read_common returns the parsed CommonConfig; ignoring it drops the data"]
pub fn read_common(path: &Path) -> Result<CommonConfig, JobManagerError> {
    let text = fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| JobManagerError::Toml {
        path: path.to_path_buf(),
        source,
    })
}

pub fn write_common(path: &Path, common: &CommonConfig) -> Result<(), JobManagerError> {
    let text = toml::to_string(common).map_err(|e| JobManagerError::Toml {
        path: path.to_path_buf(),
        source: toml::de::Error::custom(e.to_string()),
    })?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    concurrency::atomic_write(path, text.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
    use slurm_async_runner::entities::slurm::SlurmJobConfig;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn sample() -> CommonConfig {
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

    #[test]
    fn round_trip_through_disk() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("common.toml");
        let original = sample();
        write_common(&p, &original).unwrap();
        let restored = read_common(&p).unwrap();
        assert_eq!(restored.slurm_default.partition, "long");
        assert_eq!(restored.directories.project_root, PathBuf::from("/work"));
    }

    #[test]
    fn read_missing_returns_io_error() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("nonexistent.toml");
        let result = read_common(&p);
        assert!(matches!(result, Err(JobManagerError::Io { .. })));
    }
}
```

- [ ] **Step 2: `src/persistence/mod.rs` 更新**

```rust
pub mod path;
pub mod flow;
pub mod plan;
pub mod job_run;
pub mod common;

pub use path::PathResolver;
pub use flow::{read_flow, write_flow};
pub use plan::{read_plan, write_plan};
pub use job_run::{read_job_run, write_job_run};
pub use common::{read_common, write_common};
```

- [ ] **Step 3: `src/lib.rs` 更新**

```rust
pub use persistence::{read_common, write_common};
```

- [ ] **Step 4: ビルド + テスト pass 確認**

```bash
cargo build --all-features
cargo test common --all-features 2>&1 | tail -10
```

期待: 2 テスト pass。**Phase 0 D2 PR が merge されている前提**で `CommonConfig` の serde derive が available。

- [ ] **Step 5: コミット**

```bash
git add -A
git commit -m "feat(sp3): add persistence::common (read_common / write_common)

D2 CommonConfig を Phase 0 PR の serde derives 経由で直接 read/write。
job-manager 側にラッパは作らない。"
```

---

### Task B.2: `merge_with_defaults` helper

**Files:**
- Modify: `src/persistence/common.rs`, `src/persistence/mod.rs`, `src/lib.rs`

- [ ] **Step 1: failing test を書く (既存 `mod tests` に追加)**

`src/persistence/common.rs` の `mod tests` 末尾に追加:

```rust
#[test]
fn merge_uses_common_default_when_override_partition_is_empty() {
    let common = sample();
    let override_cfg = SlurmJobConfig {
        partition: "".to_string(),
        time_limit: None, log_stdout: None, log_stderr: None,
        comment: None, job_name: None, array_spec: None,
        dependency: None, mail_user: None, mail_types: None,
        resource_spec: None,
    };
    let merged = merge_with_defaults(&common, &override_cfg);
    assert_eq!(merged.partition, "long");
}

#[test]
fn merge_keeps_override_partition_when_set() {
    let common = sample();
    let override_cfg = SlurmJobConfig {
        partition: "short".to_string(),
        time_limit: None, log_stdout: None, log_stderr: None,
        comment: None, job_name: None, array_spec: None,
        dependency: None, mail_user: None, mail_types: None,
        resource_spec: None,
    };
    let merged = merge_with_defaults(&common, &override_cfg);
    assert_eq!(merged.partition, "short");
}

#[test]
fn merge_uses_common_for_optional_field_when_override_is_none() {
    let mut common = sample();
    // common 側にだけ time_limit がある (実値は A1 JobTimeLimit::FromStr に従う)
    // テスト的には None override がスルーする確認だけ十分なので clone 経由で確認
    let override_cfg = SlurmJobConfig {
        partition: "short".to_string(),
        time_limit: None, log_stdout: None, log_stderr: None,
        comment: None, job_name: None, array_spec: None,
        dependency: None, mail_user: None, mail_types: None,
        resource_spec: None,
    };
    let merged = merge_with_defaults(&common, &override_cfg);
    assert!(merged.time_limit.is_none(), "common も None なので merge も None");
    common.slurm_default.time_limit = override_cfg.time_limit.clone(); // dummy
}
```

- [ ] **Step 2: テスト fail 確認**

```bash
cargo test merge --all-features 2>&1 | tail -10
```

期待: `merge_with_defaults` 未定義 → compile error。

- [ ] **Step 3: implement**

`src/persistence/common.rs` に追加:

```rust
use slurm_async_runner::entities::slurm::SlurmJobConfig;

/// Merge `override_` on top of `common.slurm_default`.
/// - Option<T>: override.or(common)
/// - partition (String): override if non-empty, else common
pub fn merge_with_defaults(common: &CommonConfig, override_: &SlurmJobConfig) -> SlurmJobConfig {
    let base = &common.slurm_default;
    SlurmJobConfig {
        partition: if override_.partition.is_empty() {
            base.partition.clone()
        } else {
            override_.partition.clone()
        },
        time_limit: override_.time_limit.clone().or_else(|| base.time_limit.clone()),
        log_stdout: override_.log_stdout.clone().or_else(|| base.log_stdout.clone()),
        log_stderr: override_.log_stderr.clone().or_else(|| base.log_stderr.clone()),
        comment: override_.comment.clone().or_else(|| base.comment.clone()),
        job_name: override_.job_name.clone().or_else(|| base.job_name.clone()),
        array_spec: override_.array_spec.clone().or_else(|| base.array_spec.clone()),
        dependency: override_.dependency.clone().or_else(|| base.dependency.clone()),
        mail_user: override_.mail_user.clone().or_else(|| base.mail_user.clone()),
        mail_types: override_.mail_types.clone().or_else(|| base.mail_types.clone()),
        resource_spec: override_.resource_spec.clone().or_else(|| base.resource_spec.clone()),
    }
}
```

- [ ] **Step 4: テスト pass 確認**

```bash
cargo test merge --all-features 2>&1 | tail -10
```

- [ ] **Step 5: re-export 追加**

`src/persistence/mod.rs`:

```rust
pub use common::{read_common, write_common, merge_with_defaults};
```

`src/lib.rs`:

```rust
pub use persistence::merge_with_defaults;
```

- [ ] **Step 6: コミット**

```bash
git add -A
git commit -m "feat(sp3): add merge_with_defaults helper for SlurmJobConfig

Option<T> fields は override が Some なら override、None なら common。
partition は String なので is_empty() で判定 (A1 不可侵で Option 化不可)。"
```

---

### Task B.3: `PathResolver::common_toml()` getter

**Files:**
- Modify: `src/persistence/path.rs`

- [ ] **Step 1: failing test**

`src/persistence/path.rs` の test module に追加:

```rust
#[test]
fn common_toml_returns_root_common_toml() {
    let r = PathResolver::new("/work");
    assert_eq!(r.common_toml(), std::path::PathBuf::from("/work/common.toml"));
}
```

- [ ] **Step 2: implement**

`src/persistence/path.rs` の `impl PathResolver`:

```rust
pub fn common_toml(&self) -> std::path::PathBuf {
    self.root.join("common.toml")
}
```

`PathResolver` に `pub fn root(&self) -> &std::path::Path` getter が無ければ追加:

```rust
pub fn root(&self) -> &std::path::Path {
    &self.root
}
```

- [ ] **Step 3: テスト pass 確認**

```bash
cargo test common_toml --all-features 2>&1 | tail -5
```

- [ ] **Step 4: コミット**

```bash
git add -A
git commit -m "feat(sp3): PathResolver::common_toml() + root() getter"
```

---

## Phase C — render

### Task C.1: `render::sanitize_var_name` + `render::quote_for_bash`

**Files:**
- Create: `src/render/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: failing test を含む module を作成**

`src/render/mod.rs`:

```rust
//! batch.bash render — env-export style with POSIX single-quote escaping.

/// Convert an axis name or param key into a bash-safe upper-case identifier.
pub fn sanitize_var_name(name: &str) -> String {
    let upper = name.to_ascii_uppercase();
    upper
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// POSIX-safe single-quote escape: `'` → `'\''`, then wrap in single quotes.
pub fn quote_for_bash(value: &str) -> String {
    let escaped = value.replace('\'', r"'\''");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_upper_cases_lowercase() {
        assert_eq!(sanitize_var_name("compound"), "COMPOUND");
    }

    #[test]
    fn sanitize_replaces_hyphen() {
        assert_eq!(sanitize_var_name("my-axis"), "MY_AXIS");
    }

    #[test]
    fn sanitize_replaces_dot() {
        assert_eq!(sanitize_var_name("ax.is"), "AX_IS");
    }

    #[test]
    fn quote_simple_value() {
        assert_eq!(quote_for_bash("hello"), "'hello'");
    }

    #[test]
    fn quote_escapes_single_quote() {
        assert_eq!(quote_for_bash("it's"), r"'it'\''s'");
    }

    #[test]
    fn quote_preserves_newline() {
        assert_eq!(quote_for_bash("a\nb"), "'a\nb'");
    }
}
```

`src/lib.rs` に追加:

```rust
pub mod render;
```

- [ ] **Step 2: テスト pass 確認**

```bash
cargo test render --all-features 2>&1 | tail -10
```

期待: 6 テスト pass。

- [ ] **Step 3: コミット**

```bash
git add -A
git commit -m "feat(sp3): add render::sanitize_var_name and render::quote_for_bash

sanitize_var_name: 任意の入力を [A-Z0-9_]+ にアップケース + 非英数字を _。
quote_for_bash: POSIX single-quote escape (内側の ' を '\\'' に)。"
```

---

### Task C.2: `render::render_batch_bash`

**Files:**
- Modify: `src/render/mod.rs`, `src/lib.rs`

- [ ] **Step 1: failing test を追加**

`src/render/mod.rs` の `mod tests` に追加:

```rust
#[test]
fn render_batch_bash_produces_expected_sections() {
    use crate::jobid::JobIdParts;
    use gaussian_job_shared::entities::workflow::JobId;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    let flow_uuid = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
    let jid = JobId("opt__compound=0__method=1".to_string());
    let parts = JobIdParts {
        source_step_id: "opt",
        axis_combo: vec![("compound", 0), ("method", 1)],
    };
    let mut params: BTreeMap<String, toml::Value> = BTreeMap::new();
    params.insert("route".to_string(), toml::Value::String("# B3LYP/6-31G*".to_string()));
    params.insert("nproc".to_string(), toml::Value::Integer(16));
    let body = "#!/bin/bash\necho hello";

    let out = render_batch_bash(&flow_uuid, &jid, &parts, &params, body);

    assert!(out.starts_with("#!/bin/bash"));
    assert!(out.contains("export JM_FLOW_UUID='01997cdc-0000-7000-8000-000000000000'"));
    assert!(out.contains("export JM_JOB_ID='opt__compound=0__method=1'"));
    assert!(out.contains("export JM_AXIS_COMPOUND='0'"));
    assert!(out.contains("export JM_AXIS_METHOD='1'"));
    assert!(out.contains("export JM_PARAM_ROUTE='# B3LYP/6-31G*'"));
    assert!(out.contains("export JM_PARAM_NPROC='16'"));
    assert!(out.contains(body));
}

#[test]
fn render_batch_bash_escapes_single_quote_in_param() {
    use crate::jobid::JobIdParts;
    use gaussian_job_shared::entities::workflow::JobId;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    let flow_uuid = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
    let jid = JobId("x__a=0".to_string());
    let parts = JobIdParts { source_step_id: "x", axis_combo: vec![("a", 0)] };
    let mut params: BTreeMap<String, toml::Value> = BTreeMap::new();
    params.insert("note".to_string(), toml::Value::String("it's working".to_string()));

    let out = render_batch_bash(&flow_uuid, &jid, &parts, &params, "");
    assert!(out.contains(r"export JM_PARAM_NOTE='it'\''s working'"), "got: {out}");
}
```

- [ ] **Step 2: テスト fail 確認**

```bash
cargo test render_batch_bash --all-features 2>&1 | tail -10
```

- [ ] **Step 3: implement**

`src/render/mod.rs` の top-level に追加:

```rust
use std::collections::BTreeMap;
use crate::jobid::JobIdParts;
use gaussian_job_shared::entities::workflow::JobId;
use uuid::Uuid;

pub fn render_batch_bash(
    flow_uuid: &Uuid,
    jid: &JobId,
    parts: &JobIdParts<'_>,
    params: &BTreeMap<String, toml::Value>,
    body: &str,
) -> String {
    let mut s = String::new();
    s.push_str("#!/bin/bash\n");
    s.push_str("# Generated by job_manager SP-3. Do not edit; regenerated on every `jm run`.\n");
    s.push_str("\n# --- job-manager runtime context ---\n");
    s.push_str(&format!("export JM_FLOW_UUID={}\n", quote_for_bash(&flow_uuid.to_string())));
    s.push_str(&format!("export JM_JOB_ID={}\n", quote_for_bash(&jid.0)));
    for (axis, idx) in &parts.axis_combo {
        let key = sanitize_var_name(axis);
        s.push_str(&format!("export JM_AXIS_{}={}\n", key, quote_for_bash(&idx.to_string())));
    }
    s.push_str("\n# --- plan.toml params ---\n");
    for (k, v) in params {
        let key = sanitize_var_name(k);
        let val = toml_value_to_string(v);
        s.push_str(&format!("export JM_PARAM_{}={}\n", key, quote_for_bash(&val)));
    }
    s.push_str("\n# --- user body (JobSpec.body) ---\n");
    s.push_str(body);
    if !body.ends_with('\n') {
        s.push('\n');
    }
    s
}

fn toml_value_to_string(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Array(a) => {
            let parts: Vec<String> = a.iter().map(toml_value_to_string).collect();
            parts.join(" ")
        }
        toml::Value::Datetime(d) => d.to_string(),
        toml::Value::Table(_) => format!("{v}"),
    }
}
```

- [ ] **Step 4: テスト pass 確認**

```bash
cargo test render_batch_bash --all-features 2>&1 | tail -10
```

- [ ] **Step 5: `src/lib.rs` re-export**

```rust
pub use render::render_batch_bash;
```

- [ ] **Step 6: コミット**

```bash
git add -A
git commit -m "feat(sp3): add render_batch_bash (env-export bash render)

JM_FLOW_UUID / JM_JOB_ID / JM_AXIS_* / JM_PARAM_* を quote_for_bash で
single-quote escape して書き出す。#SBATCH directives は batch.bash に
含めない (SbatchCmd CLI 引数で渡す設計)。"
```

---

### Task C.3: `PathResolver::batch_bash()` getter

**Files:**
- Modify: `src/persistence/path.rs`

- [ ] **Step 1: failing test**

```rust
#[test]
fn batch_bash_returns_job_dir_batch_bash() {
    use gaussian_job_shared::entities::workflow::JobId;
    use uuid::Uuid;

    let r = PathResolver::new("/work");
    let uuid = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
    let jid = JobId("opt__a=0".to_string());
    let p = r.batch_bash(&uuid, &jid);
    assert!(p.ends_with("01997cdc-0000-7000-8000-000000000000/opt__a=0/batch.bash"));
}
```

- [ ] **Step 2: implement**

```rust
pub fn batch_bash(
    &self,
    flow_uuid: &uuid::Uuid,
    jid: &gaussian_job_shared::entities::workflow::JobId,
) -> std::path::PathBuf {
    self.job_dir(flow_uuid, jid).join("batch.bash")
}
```

- [ ] **Step 3: テスト pass 確認 + コミット**

```bash
cargo test batch_bash --all-features 2>&1 | tail -5
git add -A
git commit -m "feat(sp3): PathResolver::batch_bash() getter"
```

---

## Phase D — Slurm submit infrastructure

### Task D.1: `Executor` trait + `SbatchExecutor`

**Files:**
- Create: `src/slurm/executor.rs`
- Modify: `src/slurm/mod.rs`, `src/error.rs`, `src/lib.rs`

- [ ] **Step 1: `JobManagerError::SubmitFailed` variant 追加**

`src/error.rs` の enum に:

```rust
#[error("sbatch submission failed: {source}")]
SubmitFailed {
    #[source] source: anyhow::Error,
},
```

- [ ] **Step 2: `src/slurm/executor.rs` 新規作成**

```rust
//! Executor trait — abstraction over sbatch submission.

use async_trait::async_trait;
use slurm_async_runner::{SbatchCmd, SbatchManager};

use crate::error::JobManagerError;

#[async_trait]
pub trait Executor: Send + Sync {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError>;
}

/// Production: wraps A1 `SbatchManager.spawn().await`.
pub struct SbatchExecutor;

#[async_trait]
impl Executor for SbatchExecutor {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError> {
        let manager = SbatchManager::new(cmd);
        let handle = manager.spawn().await.map_err(|e| JobManagerError::SubmitFailed {
            source: anyhow::anyhow!(e),
        })?;
        handle.jobid().ok_or_else(|| JobManagerError::SubmitFailed {
            source: anyhow::anyhow!("sbatch returned no jobid"),
        })
    }
}
```

- [ ] **Step 3: `src/slurm/mod.rs` 更新**

```rust
pub mod querier;
pub mod executor;

pub use querier::{Querier, SlurmQuerier, InMemoryQuerier};
pub use executor::{Executor, SbatchExecutor};
```

- [ ] **Step 4: `src/lib.rs` 更新**

```rust
pub use slurm::executor::{Executor, SbatchExecutor};
```

- [ ] **Step 5: ビルド確認**

```bash
cargo build --all-features 2>&1 | tail -10
```

期待: 成功。

- [ ] **Step 6: コミット**

```bash
git add -A
git commit -m "feat(sp3): add Executor trait and SbatchExecutor

A1 SbatchManager を wrap して async fn submit(cmd) -> Result<u64>。
sbatch 失敗 / jobid 取れない場合は JobManagerError::SubmitFailed。"
```

---

### Task D.2: `DryRunExecutor`

**Files:**
- Modify: `src/slurm/executor.rs`, `src/slurm/mod.rs`, `src/lib.rs`

- [ ] **Step 1: failing test**

`src/slurm/executor.rs` に `#[cfg(test)] mod tests` を追加:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use slurm_async_runner::SbatchCmd;
    use std::path::PathBuf;

    #[tokio::test]
    async fn dry_run_returns_deterministic_jobid() {
        let exec = DryRunExecutor;
        let j1 = exec.submit(SbatchCmd::new(PathBuf::from("/tmp/a.sh"))).await.unwrap();
        let j2 = exec.submit(SbatchCmd::new(PathBuf::from("/tmp/a.sh"))).await.unwrap();
        let j3 = exec.submit(SbatchCmd::new(PathBuf::from("/tmp/b.sh"))).await.unwrap();

        assert_eq!(j1, j2, "same script => same fake jobid");
        assert_ne!(j1, j3, "different script => different jobid");
    }
}
```

- [ ] **Step 2: テスト fail 確認**

```bash
cargo test dry_run --all-features 2>&1 | tail -10
```

期待: `DryRunExecutor` 未定義。

- [ ] **Step 3: implement**

`src/slurm/executor.rs` に追加:

```rust
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// `jm submit --dry-run` 用。決定的な fake jobid を返す。
pub struct DryRunExecutor;

#[async_trait]
impl Executor for DryRunExecutor {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError> {
        let mut h = DefaultHasher::new();
        cmd.script.hash(&mut h);
        Ok(1 + (h.finish() % 9_999_999))
    }
}
```

- [ ] **Step 4: re-export + テスト pass**

`src/slurm/mod.rs`:

```rust
pub use executor::{Executor, SbatchExecutor, DryRunExecutor};
```

`src/lib.rs`:

```rust
pub use slurm::executor::DryRunExecutor;
```

```bash
cargo test dry_run --all-features 2>&1 | tail -10
```

- [ ] **Step 5: コミット**

```bash
git add -A
git commit -m "feat(sp3): add DryRunExecutor for --dry-run mode

SbatchCmd.script のハッシュから決定的な fake jobid を返す。
実 SLURM は呼ばない。"
```

---

### Task D.3: `MockExecutor`

**Files:**
- Modify: `src/slurm/executor.rs`, `src/slurm/mod.rs`, `src/lib.rs`

- [ ] **Step 1: failing test**

`src/slurm/executor.rs` の `mod tests` に追加:

```rust
#[tokio::test]
async fn mock_returns_recorded_jobids_in_order() {
    let exec = MockExecutor::new(vec![100, 200, 300]);
    assert_eq!(exec.submit(SbatchCmd::new(PathBuf::from("/tmp/a.sh"))).await.unwrap(), 100);
    assert_eq!(exec.submit(SbatchCmd::new(PathBuf::from("/tmp/b.sh"))).await.unwrap(), 200);
    assert_eq!(exec.submit(SbatchCmd::new(PathBuf::from("/tmp/c.sh"))).await.unwrap(), 300);
    assert_eq!(exec.calls().len(), 3);
}

#[tokio::test]
async fn mock_errors_when_exhausted() {
    let exec = MockExecutor::new(vec![100]);
    let _ = exec.submit(SbatchCmd::new(PathBuf::from("/x"))).await.unwrap();
    let result = exec.submit(SbatchCmd::new(PathBuf::from("/y"))).await;
    assert!(result.is_err());
}
```

- [ ] **Step 2: implement**

`src/slurm/executor.rs` に追加:

```rust
use std::sync::Mutex;

pub struct MockExecutor {
    recordings: Mutex<std::collections::VecDeque<u64>>,
    calls_log: Mutex<Vec<SbatchCmd>>,
}

impl MockExecutor {
    pub fn new(recordings: Vec<u64>) -> Self {
        Self {
            recordings: Mutex::new(recordings.into_iter().collect()),
            calls_log: Mutex::new(Vec::new()),
        }
    }

    pub fn calls(&self) -> Vec<SbatchCmd> {
        self.calls_log.lock().unwrap().clone()
    }
}

#[async_trait]
impl Executor for MockExecutor {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError> {
        self.calls_log.lock().unwrap().push(cmd.clone());
        self.recordings
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| JobManagerError::SubmitFailed {
                source: anyhow::anyhow!("MockExecutor recordings exhausted"),
            })
    }
}
```

`SbatchCmd: Clone` が要件。

- [ ] **Step 3: re-export + テスト pass**

```rust
// src/slurm/mod.rs
pub use executor::{Executor, SbatchExecutor, DryRunExecutor, MockExecutor};
// src/lib.rs
pub use slurm::executor::MockExecutor;
```

```bash
cargo test mock --all-features 2>&1 | tail -10
```

- [ ] **Step 4: コミット**

```bash
git add -A
git commit -m "feat(sp3): add MockExecutor for integration tests

事前録音した jobid を順に返す、calls log を保持。
SbatchCmd 検証が外部から可能。"
```

---

### Task D.4: `slurm::dependency::build` helper

**Files:**
- Create: `src/slurm/dependency.rs`
- Modify: `src/slurm/mod.rs`

- [ ] **Step 1: failing test**

`src/slurm/dependency.rs`:

```rust
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
        .filter_map(|e| submitted.get(&e.from).map(|j| (*j, e.kind.clone())))
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
        let parents = vec![JobEdge { from: p_jid.clone(), kind: DependencyType::AfterOk }];
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
            JobEdge { from: p1.clone(), kind: DependencyType::AfterOk },
            JobEdge { from: p2.clone(), kind: DependencyType::AfterAny },
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
```

- [ ] **Step 2: `src/slurm/mod.rs` 更新**

```rust
pub mod querier;
pub mod executor;
pub mod dependency;

pub use querier::{Querier, SlurmQuerier, InMemoryQuerier};
pub use executor::{Executor, SbatchExecutor, DryRunExecutor, MockExecutor};
```

- [ ] **Step 3: テスト pass 確認**

```bash
cargo test dependency --all-features 2>&1 | tail -20
```

期待: 3 テスト pass。`DependencyType` の Display 形式 (afterok / afterany) を前提。

- [ ] **Step 4: コミット**

```bash
git add -A
git commit -m "feat(sp3): add slurm::dependency::build for JobEdge -> SlurmDependency

submitted (BTreeMap<JobId, u64>) を経由して --dependency 文字列を組み立て
A1 SlurmDependency::from_str() に通す。"
```

---

## Phase E — Runner

### Task E.1: `flow::topology::topological_order` (Kahn + cycle detection)

**Files:**
- Create: `src/flow/mod.rs`, `src/flow/topology.rs`
- Modify: `src/error.rs`, `src/lib.rs`

- [ ] **Step 1: `JobManagerError::DependencyCycle` variant 追加** (なければ)

`src/error.rs`:

```rust
#[error("dependency cycle detected in flow {flow}")]
DependencyCycle { flow: uuid::Uuid },
```

- [ ] **Step 2: `src/flow/topology.rs` 新規作成**

```rust
//! Kahn's algorithm: topological sort with cycle detection.

use std::collections::{BTreeMap, VecDeque};

use gaussian_job_shared::entities::workflow::{Job, JobId};

use crate::error::JobManagerError;

pub fn topological_order(
    jobs: &BTreeMap<JobId, Job>,
    flow_uuid: uuid::Uuid,
) -> Result<Vec<JobId>, JobManagerError> {
    let mut indeg: BTreeMap<JobId, usize> = jobs
        .iter()
        .map(|(jid, job)| (jid.clone(), job.parents.len()))
        .collect();

    let mut queue: VecDeque<JobId> = indeg
        .iter()
        .filter_map(|(k, v)| if *v == 0 { Some(k.clone()) } else { None })
        .collect();

    let mut order = Vec::with_capacity(jobs.len());

    while let Some(jid) = queue.pop_front() {
        order.push(jid.clone());
        for (other_jid, other_job) in jobs {
            if other_job.parents.iter().any(|e| e.from == jid) {
                if let Some(c) = indeg.get_mut(other_jid) {
                    if *c > 0 {
                        *c -= 1;
                        if *c == 0 {
                            queue.push_back(other_jid.clone());
                        }
                    }
                }
            }
        }
    }

    if order.len() != jobs.len() {
        return Err(JobManagerError::DependencyCycle { flow: flow_uuid });
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobId, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

    fn empty_spec() -> JobSpec {
        JobSpec {
            program: Program("dummy".to_string()),
            body: String::new(),
            config: SlurmJobConfig {
                partition: "p".to_string(),
                time_limit: None, log_stdout: None, log_stderr: None,
                comment: None, job_name: None, array_spec: None,
                dependency: None, mail_user: None, mail_types: None,
                resource_spec: None,
            },
        }
    }

    fn job_with_parents(parents: Vec<JobEdge>) -> Job {
        Job { spec: empty_spec(), parents }
    }

    #[test]
    fn linear_chain_a_b_c() {
        let a = JobId("a".to_string());
        let b = JobId("b".to_string());
        let c = JobId("c".to_string());
        let mut jobs = BTreeMap::new();
        jobs.insert(a.clone(), job_with_parents(vec![]));
        jobs.insert(b.clone(), job_with_parents(vec![JobEdge { from: a.clone(), kind: DependencyType::AfterOk }]));
        jobs.insert(c.clone(), job_with_parents(vec![JobEdge { from: b.clone(), kind: DependencyType::AfterOk }]));

        let order = topological_order(&jobs, uuid::Uuid::nil()).unwrap();
        assert_eq!(order, vec![a, b, c]);
    }

    #[test]
    fn cycle_detected() {
        let a = JobId("a".to_string());
        let b = JobId("b".to_string());
        let mut jobs = BTreeMap::new();
        jobs.insert(a.clone(), job_with_parents(vec![JobEdge { from: b.clone(), kind: DependencyType::AfterOk }]));
        jobs.insert(b.clone(), job_with_parents(vec![JobEdge { from: a.clone(), kind: DependencyType::AfterOk }]));

        let result = topological_order(&jobs, uuid::Uuid::nil());
        assert!(matches!(result, Err(JobManagerError::DependencyCycle { .. })));
    }
}
```

- [ ] **Step 3: `src/flow/mod.rs` 新規作成**

```rust
pub mod topology;

pub use topology::topological_order;
```

- [ ] **Step 4: `src/lib.rs` 更新**

```rust
pub mod flow;
```

- [ ] **Step 5: テスト pass 確認**

```bash
cargo test flow --all-features 2>&1 | tail -20
```

期待: 2 テスト pass。

- [ ] **Step 6: コミット**

```bash
git add -A
git commit -m "feat(sp3): add flow::topology::topological_order

Kahn's algorithm + cycle detection。cycle 検出時は
JobManagerError::DependencyCycle { flow }。"
```

---

### Task E.2: `flow::FlowRun` struct + methods (`read` 除く)

**Files:**
- Create: `src/flow/run.rs`
- Modify: `src/flow/mod.rs`, `src/error.rs`, `src/lib.rs`

- [ ] **Step 1: `MissingPlanEntry` variant 追加** (なければ)

`src/error.rs`:

```rust
#[error("missing plan entry for job {job} in flow {flow}")]
MissingPlanEntry {
    flow: uuid::Uuid,
    job: gaussian_job_shared::entities::workflow::JobId,
},
```

- [ ] **Step 2: `src/flow/run.rs` 新規作成**

```rust
//! FlowRun — aggregate of flow.toml + plan.toml + optional common.toml.

use std::collections::BTreeMap;

use gaussian_job_shared::config::common::CommonConfig;
use gaussian_job_shared::entities::workflow::{JobEdge, JobFlow, JobId};
use slurm_async_runner::entities::slurm::SlurmJobConfig;

use crate::error::JobManagerError;
use crate::flow::topology;
use crate::persistence::common::merge_with_defaults;
use crate::plan::ExperimentPlan;

pub struct FlowRun {
    pub flow_uuid: uuid::Uuid,
    pub flow: JobFlow,
    pub plan: ExperimentPlan,
    pub common: Option<CommonConfig>,
}

impl FlowRun {
    pub fn topological_order(&self) -> Result<Vec<JobId>, JobManagerError> {
        topology::topological_order(&self.flow.jobs, self.flow_uuid)
    }

    pub fn parents_of(&self, jid: &JobId) -> &[JobEdge] {
        self.flow
            .jobs
            .get(jid)
            .map(|job| job.parents.as_slice())
            .unwrap_or(&[])
    }

    pub fn params_of(
        &self,
        jid: &JobId,
    ) -> Result<&BTreeMap<String, toml::Value>, JobManagerError> {
        self.plan
            .jobs
            .get(jid)
            .ok_or_else(|| JobManagerError::MissingPlanEntry {
                flow: self.flow_uuid,
                job: jid.clone(),
            })
    }

    pub fn effective_config(
        &self,
        jid: &JobId,
    ) -> Result<SlurmJobConfig, JobManagerError> {
        let job = self
            .flow
            .jobs
            .get(jid)
            .ok_or_else(|| JobManagerError::MissingPlanEntry {
                flow: self.flow_uuid,
                job: jid.clone(),
            })?;
        Ok(match &self.common {
            Some(c) => merge_with_defaults(c, &job.spec.config),
            None => job.spec.config.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobId, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

    fn empty_spec(partition: &str) -> JobSpec {
        JobSpec {
            program: Program("dummy".to_string()),
            body: String::new(),
            config: SlurmJobConfig {
                partition: partition.to_string(),
                time_limit: None, log_stdout: None, log_stderr: None,
                comment: None, job_name: None, array_spec: None,
                dependency: None, mail_user: None, mail_types: None,
                resource_spec: None,
            },
        }
    }

    pub(crate) fn fr_with_2_jobs() -> FlowRun {
        let a = JobId("a".to_string());
        let b = JobId("b".to_string());

        let mut jobs: BTreeMap<JobId, Job> = BTreeMap::new();
        jobs.insert(a.clone(), Job { spec: empty_spec(""), parents: vec![] });
        jobs.insert(b.clone(), Job {
            spec: empty_spec("short"),
            parents: vec![JobEdge { from: a.clone(), kind: DependencyType::AfterOk }],
        });

        let mut plan_jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>> = BTreeMap::new();
        plan_jobs.insert(a, BTreeMap::new());
        plan_jobs.insert(b, BTreeMap::new());

        FlowRun {
            flow_uuid: uuid::Uuid::nil(),
            flow: JobFlow {
                uuid: uuid::Uuid::nil(),
                created_at: chrono::Utc::now(),
                jobs,
            },
            plan: ExperimentPlan { jobs: plan_jobs },
            common: None,
        }
    }

    #[test]
    fn topological_order_returns_a_then_b() {
        let fr = fr_with_2_jobs();
        let order = fr.topological_order().unwrap();
        assert_eq!(order, vec![JobId("a".to_string()), JobId("b".to_string())]);
    }

    #[test]
    fn parents_of_b_is_a() {
        let fr = fr_with_2_jobs();
        let p = fr.parents_of(&JobId("b".to_string()));
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].from, JobId("a".to_string()));
    }

    #[test]
    fn params_of_missing_returns_error() {
        let fr = fr_with_2_jobs();
        let result = fr.params_of(&JobId("nope".to_string()));
        assert!(matches!(result, Err(JobManagerError::MissingPlanEntry { .. })));
    }

    #[test]
    fn effective_config_without_common_returns_spec_config() {
        let fr = fr_with_2_jobs();
        let cfg = fr.effective_config(&JobId("b".to_string())).unwrap();
        assert_eq!(cfg.partition, "short");
    }
}
```

- [ ] **Step 3: `src/flow/mod.rs` 更新**

```rust
pub mod topology;
pub mod run;

pub use topology::topological_order;
pub use run::FlowRun;
```

- [ ] **Step 4: `src/lib.rs`**

```rust
pub use flow::FlowRun;
```

- [ ] **Step 5: テスト pass 確認**

```bash
cargo test flow::run --all-features 2>&1 | tail -20
```

期待: 4 テスト pass。

- [ ] **Step 6: コミット**

```bash
git add -A
git commit -m "feat(sp3): add FlowRun aggregate (Airflow DAG Run analog)

flow_uuid + JobFlow + ExperimentPlan + Option<CommonConfig> を保持。
topological_order / parents_of / params_of / effective_config を提供。"
```

---

### Task E.3: `FlowRun::read`

**Files:**
- Modify: `src/flow/run.rs`

- [ ] **Step 1: failing test (既存 `mod tests` に追加)**

```rust
#[test]
fn read_constructs_from_disk_with_common() {
    use crate::persistence::{PathResolver, write_flow, write_plan, common::write_common};
    use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
    use std::path::PathBuf;
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let fr_src = fr_with_2_jobs();
    let uuid = uuid::Uuid::nil();

    let common = CommonConfig {
        slurm_default: SlurmJobConfig {
            partition: "long".to_string(),
            time_limit: None, log_stdout: None, log_stderr: None,
            comment: None, job_name: None, array_spec: None,
            dependency: None, mail_user: None, mail_types: None,
            resource_spec: None,
        },
        directories: DirectoryConfig { project_root: PathBuf::from(dir.path()) },
    };
    write_common(&resolver.common_toml(), &common).unwrap();
    write_flow(&resolver.flow_toml(&uuid), &fr_src.flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &fr_src.plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    assert_eq!(fr.flow_uuid, uuid);
    assert!(fr.common.is_some());
    assert_eq!(fr.flow.jobs.len(), 2);
}

#[test]
fn read_works_without_common_toml() {
    use crate::persistence::{PathResolver, write_flow, write_plan};
    use tempfile::tempdir;

    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let fr_src = fr_with_2_jobs();
    let uuid = uuid::Uuid::nil();

    write_flow(&resolver.flow_toml(&uuid), &fr_src.flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &fr_src.plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    assert!(fr.common.is_none());
}
```

- [ ] **Step 2: implement**

`src/flow/run.rs` の `impl FlowRun` に追加:

```rust
use crate::persistence::{PathResolver, read_flow, read_plan};
use crate::persistence::common::read_common;

impl FlowRun {
    pub fn read(
        resolver: &PathResolver,
        flow_uuid: uuid::Uuid,
    ) -> Result<Self, JobManagerError> {
        let flow = read_flow(&resolver.flow_toml(&flow_uuid))?;
        let plan = read_plan(&resolver.plan_toml(&flow_uuid))?;
        let common_path = resolver.common_toml();
        let common = if common_path.exists() {
            Some(read_common(&common_path)?)
        } else {
            None
        };
        Ok(Self { flow_uuid, flow, plan, common })
    }
    // ... existing methods
}
```

- [ ] **Step 3: テスト pass 確認**

```bash
cargo test flow::run --all-features 2>&1 | tail -10
```

- [ ] **Step 4: コミット**

```bash
git add -A
git commit -m "feat(sp3): FlowRun::read constructs from PathResolver + uuid

flow.toml / plan.toml は必須、common.toml はファイル不在なら None。"
```

---

### Task E.4: `decide_transition` に `parent_lifecycles` 引数を追加

**Files:**
- Modify: `src/runner/transition.rs`, `src/runner/mod.rs`, `src/lib.rs`

- [ ] **Step 1: failing test**

`src/runner/transition.rs` の test module に追加:

```rust
#[test]
fn skip_when_parent_failed_and_current_queued() {
    use crate::job::lifecycle::Lifecycle;
    let decision = decide_transition(
        Lifecycle::Queued,
        None,
        &[Lifecycle::Failed],
    );
    assert!(matches!(decision, Decision::SkipDueToParent { .. }));
}

#[test]
fn skip_when_parent_skipped_and_current_queued() {
    use crate::job::lifecycle::Lifecycle;
    let decision = decide_transition(
        Lifecycle::Queued,
        None,
        &[Lifecycle::Skipped],
    );
    assert!(matches!(decision, Decision::SkipDueToParent { .. }));
}

#[test]
fn no_change_when_all_parents_success_and_no_query() {
    use crate::job::lifecycle::Lifecycle;
    let decision = decide_transition(
        Lifecycle::Queued,
        None,
        &[Lifecycle::Success],
    );
    assert!(matches!(decision, Decision::NoChange));
}
```

- [ ] **Step 2: テスト fail 確認**

```bash
cargo test transition --all-features 2>&1 | tail -10
```

- [ ] **Step 3: implement — 全置換**

`src/runner/transition.rs` 全置換 (旧 `tick_many` 削除を含む):

```rust
//! State transition decisions for a single JobRun based on
//! current lifecycle, latest SLURM query, and parent lifecycles.

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::JobId;
use slurm_async_runner::JobStatus;

use crate::job::lifecycle::Lifecycle;

pub enum Decision {
    NoChange,
    Transition {
        from: Lifecycle,
        to: Lifecycle,
        slurm_status: Option<JobStatus>,
    },
    SkipDueToParent { parent: JobId },
}

pub struct TickResult {
    pub transitions: BTreeMap<JobId, Decision>,
}

pub fn decide_transition(
    current: Lifecycle,
    query: Option<&JobStatus>,
    parent_lifecycles: &[Lifecycle],
) -> Decision {
    if current.is_terminal() {
        return Decision::NoChange;
    }
    if matches!(current, Lifecycle::Queued)
        && parent_lifecycles
            .iter()
            .any(|p| matches!(p, Lifecycle::Failed | Lifecycle::Skipped))
    {
        return Decision::SkipDueToParent {
            parent: JobId("<unknown>".to_string()),
        };
    }
    match query {
        None => Decision::NoChange,
        Some(status) => {
            use slurm_async_runner::JobState;
            let next = match status.state {
                JobState::Pending => Lifecycle::Queued,
                JobState::Running => Lifecycle::Running,
                JobState::Completed => Lifecycle::Success,
                JobState::Failed
                | JobState::Timeout
                | JobState::OutOfMemory
                | JobState::NodeFail
                | JobState::Cancelled => Lifecycle::Failed,
                _ => current,
            };
            if next == current {
                Decision::NoChange
            } else {
                Decision::Transition {
                    from: current,
                    to: next,
                    slurm_status: Some(status.clone()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_when_parent_failed_and_current_queued() {
        let decision = decide_transition(Lifecycle::Queued, None, &[Lifecycle::Failed]);
        assert!(matches!(decision, Decision::SkipDueToParent { .. }));
    }

    #[test]
    fn skip_when_parent_skipped_and_current_queued() {
        let decision = decide_transition(Lifecycle::Queued, None, &[Lifecycle::Skipped]);
        assert!(matches!(decision, Decision::SkipDueToParent { .. }));
    }

    #[test]
    fn no_change_when_all_parents_success_and_no_query() {
        let decision = decide_transition(Lifecycle::Queued, None, &[Lifecycle::Success]);
        assert!(matches!(decision, Decision::NoChange));
    }

    #[test]
    fn terminal_returns_no_change() {
        let decision = decide_transition(Lifecycle::Success, None, &[]);
        assert!(matches!(decision, Decision::NoChange));
    }
}
```

旧 `tick_many` 関数は **完全削除**。

- [ ] **Step 4: `src/runner/mod.rs` 更新**

```rust
pub mod transition;

pub use transition::{Decision, TickResult, decide_transition};
```

`tick_many` re-export を削除。

- [ ] **Step 5: `src/lib.rs` から `tick_many` を削除**

```bash
sed -i 's|, tick_many||g' src/lib.rs
```

- [ ] **Step 6: テスト pass 確認**

```bash
cargo test transition --all-features 2>&1 | tail -20
```

期待: 4 テスト pass。旧 `tick_many` を使っていたコードがあれば Phase E.5 で `FlowRunner::tick` を呼ぶように移行。

- [ ] **Step 7: コミット**

```bash
git add -A
git commit -m "feat(sp3)!: extend decide_transition with parent_lifecycles, add SkipDueToParent

BREAKING: tick_many 関数を削除 (FlowRunner::tick に統合予定)。
decide_transition の signature 変更:
  旧: decide_transition(current, query)
  新: decide_transition(current, query, parent_lifecycles)

parent_lifecycles のいずれかが Failed/Skipped で current が Queued なら
Decision::SkipDueToParent を返す (Airflow upstream_failed 相当)。"
```

---

### Task E.5: `runner::flow::FlowRunner` + integration tests

**Files:**
- Create: `src/runner/flow.rs`
- Create: `tests/integration_sp3.rs`
- Modify: `src/runner/mod.rs`, `src/error.rs`, `src/lib.rs`

- [ ] **Step 1: `RenderError` variant 追加** (なければ)

`src/error.rs`:

```rust
#[error("bash render failed for job {job}: {reason}")]
RenderError {
    job: gaussian_job_shared::entities::workflow::JobId,
    reason: String,
},
```

- [ ] **Step 2: failing integration test を書く**

`tests/integration_sp3.rs`:

```rust
//! SP-3 re-arch integration tests using MockExecutor + InMemoryQuerier.

use job_manager::flow::FlowRun;
use job_manager::job::Lifecycle;
use job_manager::persistence::{PathResolver, write_flow, write_plan, read_job_run};
use job_manager::plan::ExperimentPlan;
use job_manager::runner::flow::FlowRunner;
use job_manager::slurm::executor::MockExecutor;
use job_manager::slurm::querier::InMemoryQuerier;
use std::collections::{BTreeMap, HashMap};
use tempfile::tempdir;

fn build_2_job_flow() -> (
    uuid::Uuid,
    gaussian_job_shared::entities::workflow::JobFlow,
    ExperimentPlan,
) {
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobFlow, JobId, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};

    let a = JobId("a".to_string());
    let b = JobId("b".to_string());
    let spec = JobSpec {
        program: Program("g16".to_string()),
        body: "echo hello".to_string(),
        config: SlurmJobConfig {
            partition: "p".to_string(),
            time_limit: None, log_stdout: None, log_stderr: None,
            comment: None, job_name: None, array_spec: None,
            dependency: None, mail_user: None, mail_types: None,
            resource_spec: None,
        },
    };
    let mut jobs = BTreeMap::new();
    jobs.insert(a.clone(), Job { spec: spec.clone(), parents: vec![] });
    jobs.insert(b.clone(), Job {
        spec: spec.clone(),
        parents: vec![JobEdge { from: a.clone(), kind: DependencyType::AfterOk }],
    });

    let uuid = uuid::Uuid::new_v4();
    let flow = JobFlow { uuid, created_at: chrono::Utc::now(), jobs };

    let mut plan_jobs = BTreeMap::new();
    plan_jobs.insert(a, BTreeMap::new());
    plan_jobs.insert(b, BTreeMap::new());
    let plan = ExperimentPlan { jobs: plan_jobs };

    (uuid, flow, plan)
}

#[tokio::test]
async fn submit_writes_batch_bash_and_status_in_topo_order() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let (uuid, flow, plan) = build_2_job_flow();
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    let exec = MockExecutor::new(vec![100, 200]);
    let querier = InMemoryQuerier::new(HashMap::new());
    let runner = FlowRunner::new(Box::new(exec), Box::new(querier), &resolver);

    let result = runner.submit(&fr, false).await.unwrap();
    assert_eq!(result.len(), 2);

    for jid in fr.flow.jobs.keys() {
        let p = resolver.batch_bash(&uuid, jid);
        assert!(p.exists(), "missing batch.bash for {jid:?}");
    }

    for jid in fr.flow.jobs.keys() {
        let s = resolver.status_file(&uuid, jid);
        let entry = read_job_run(&s).unwrap();
        assert_eq!(entry.lifecycle, Lifecycle::Queued);
        assert!(entry.slurm_jobid.is_some());
    }
}

#[tokio::test]
async fn submit_dry_run_writes_batch_bash_but_does_not_call_executor() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let (uuid, flow, plan) = build_2_job_flow();
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    let exec = MockExecutor::new(vec![]); // empty — error if called
    let querier = InMemoryQuerier::new(HashMap::new());
    let runner = FlowRunner::new(Box::new(exec), Box::new(querier), &resolver);

    let result = runner.submit(&fr, true).await.unwrap();
    assert!(result.is_empty(), "dry_run should not record jobids");

    for jid in fr.flow.jobs.keys() {
        assert!(resolver.batch_bash(&uuid, jid).exists());
        assert!(!resolver.status_file(&uuid, jid).exists());
    }
}

#[tokio::test]
async fn tick_marks_child_skipped_when_parent_failed() {
    use gaussian_job_shared::entities::workflow::JobId;
    use slurm_async_runner::{JobState, JobStatus};

    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let (uuid, flow, plan) = build_2_job_flow();
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let fr = FlowRun::read(&resolver, uuid).unwrap();
    let exec = MockExecutor::new(vec![100, 200]);
    let mut q = HashMap::new();
    q.insert(100, JobStatus { state: JobState::Failed, ..Default::default() });
    q.insert(200, JobStatus { state: JobState::Pending, ..Default::default() });
    let querier = InMemoryQuerier::new(q);
    let runner = FlowRunner::new(Box::new(exec), Box::new(querier), &resolver);

    runner.submit(&fr, false).await.unwrap();
    runner.tick(&fr).await.unwrap();

    let a_run = read_job_run(&resolver.status_file(&uuid, &JobId("a".to_string()))).unwrap();
    assert_eq!(a_run.lifecycle, Lifecycle::Failed);
    let b_run = read_job_run(&resolver.status_file(&uuid, &JobId("b".to_string()))).unwrap();
    assert_eq!(b_run.lifecycle, Lifecycle::Skipped);
}
```

- [ ] **Step 3: テスト fail 確認**

```bash
cargo test --test integration_sp3 --all-features 2>&1 | tail -20
```

期待: `FlowRunner` 未定義。

- [ ] **Step 4: `src/runner/flow.rs` を新規作成**

```rust
//! FlowRunner — orchestrates render + sbatch + status write per flow run.

use std::collections::BTreeMap;

use chrono::Utc;
use slurm_async_runner::SbatchCmd;

use crate::error::JobManagerError;
use crate::flow::run::FlowRun;
use crate::job::lifecycle::Lifecycle;
use crate::job::run::JobRun;
use crate::jobid::parse_job_id;
use crate::persistence::{PathResolver, write_job_run};
use crate::render::render_batch_bash;
use crate::runner::transition::{Decision, TickResult, decide_transition};
use crate::slurm::dependency;
use crate::slurm::executor::Executor;
use crate::slurm::querier::Querier;

use gaussian_job_shared::entities::workflow::JobId;

pub struct FlowRunner<'a> {
    pub executor: Box<dyn Executor>,
    pub querier: Box<dyn Querier>,
    pub resolver: &'a PathResolver,
}

impl<'a> FlowRunner<'a> {
    pub fn new(
        executor: Box<dyn Executor>,
        querier: Box<dyn Querier>,
        resolver: &'a PathResolver,
    ) -> Self {
        Self { executor, querier, resolver }
    }

    pub async fn submit(
        &self,
        flow_run: &FlowRun,
        dry_run: bool,
    ) -> Result<BTreeMap<JobId, u64>, JobManagerError> {
        let order = flow_run.topological_order()?;
        let mut submitted: BTreeMap<JobId, u64> = BTreeMap::new();

        for jid in order {
            let job = flow_run.flow.jobs.get(&jid).expect("topological yields existing jobs");
            let effective_config = flow_run.effective_config(&jid)?;
            let params = flow_run.params_of(&jid)?;
            let parts = parse_job_id(&jid.0).map_err(|e| JobManagerError::RenderError {
                job: jid.clone(),
                reason: format!("parse_job_id: {e}"),
            })?;
            let body = render_batch_bash(
                &flow_run.flow_uuid,
                &jid,
                &parts,
                params,
                &job.spec.body,
            );
            let batch_path = self.resolver.batch_bash(&flow_run.flow_uuid, &jid);
            self.write_batch_bash(&batch_path, &body)?;

            if dry_run {
                continue;
            }

            let mut cmd = self.build_sbatch_cmd(&effective_config, &batch_path);
            cmd.dependency = dependency::build(&job.parents, &submitted, &jid)?;
            let slurm_jobid = self.submit_one(&jid, cmd, &flow_run.flow_uuid).await?;
            submitted.insert(jid, slurm_jobid);
        }
        Ok(submitted)
    }

    pub async fn tick(&self, flow_run: &FlowRun) -> Result<TickResult, JobManagerError> {
        let mut all_runs: BTreeMap<JobId, JobRun> = BTreeMap::new();
        for jid in flow_run.flow.jobs.keys() {
            let path = self.resolver.status_file(&flow_run.flow_uuid, jid);
            if path.exists() {
                all_runs.insert(jid.clone(), crate::persistence::read_job_run(&path)?);
            }
        }

        let pending: Vec<(JobId, JobRun)> = all_runs
            .iter()
            .filter(|(_, r)| !r.lifecycle.is_terminal())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let jobids: Vec<u64> = pending.iter().filter_map(|(_, r)| r.slurm_jobid).collect();
        let statuses = self.querier.query(&jobids).await?;

        let mut transitions = BTreeMap::new();
        for (jid, run) in &pending {
            let parent_lifecycles: Vec<Lifecycle> = flow_run
                .parents_of(jid)
                .iter()
                .filter_map(|edge| all_runs.get(&edge.from).map(|r| r.lifecycle))
                .collect();

            let query_for = run.slurm_jobid.and_then(|j| statuses.get(&j));
            let mut decision = decide_transition(run.lifecycle, query_for, &parent_lifecycles);

            if let Decision::SkipDueToParent { parent: _ } = &decision {
                let actual = flow_run
                    .parents_of(jid)
                    .iter()
                    .find(|e| {
                        all_runs.get(&e.from)
                            .map(|r| matches!(r.lifecycle, Lifecycle::Failed | Lifecycle::Skipped))
                            .unwrap_or(false)
                    })
                    .map(|e| e.from.clone())
                    .unwrap_or_else(|| JobId("<unknown>".to_string()));
                decision = Decision::SkipDueToParent { parent: actual };
            }

            match &decision {
                Decision::NoChange => {}
                Decision::Transition { to, slurm_status, .. } => {
                    let new_run = JobRun {
                        lifecycle: *to,
                        updated_at: Utc::now(),
                        slurm_jobid: run.slurm_jobid,
                        slurm_status: slurm_status.clone(),
                        note: run.note.clone(),
                    };
                    let path = self.resolver.status_file(&flow_run.flow_uuid, jid);
                    write_job_run(&path, &new_run)?;
                }
                Decision::SkipDueToParent { parent } => {
                    let new_run = JobRun {
                        lifecycle: Lifecycle::Skipped,
                        updated_at: Utc::now(),
                        slurm_jobid: run.slurm_jobid,
                        slurm_status: run.slurm_status.clone(),
                        note: Some(format!("skipped due to parent {parent:?}")),
                    };
                    let path = self.resolver.status_file(&flow_run.flow_uuid, jid);
                    write_job_run(&path, &new_run)?;
                }
            }
            transitions.insert(jid.clone(), decision);
        }
        Ok(TickResult { transitions })
    }

    pub fn render_only(&self, flow_run: &FlowRun) -> Result<(), JobManagerError> {
        for jid in flow_run.topological_order()? {
            let job = flow_run.flow.jobs.get(&jid).expect("topological yields existing jobs");
            let params = flow_run.params_of(&jid)?;
            let parts = parse_job_id(&jid.0).map_err(|e| JobManagerError::RenderError {
                job: jid.clone(),
                reason: format!("parse_job_id: {e}"),
            })?;
            let body = render_batch_bash(
                &flow_run.flow_uuid,
                &jid,
                &parts,
                params,
                &job.spec.body,
            );
            let batch_path = self.resolver.batch_bash(&flow_run.flow_uuid, &jid);
            self.write_batch_bash(&batch_path, &body)?;
        }
        Ok(())
    }

    fn write_batch_bash(&self, path: &std::path::Path, body: &str) -> Result<(), JobManagerError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        std::fs::write(path, body).map_err(|source| JobManagerError::Io {
            path: path.to_path_buf(),
            source,
        })
    }

    fn build_sbatch_cmd(
        &self,
        cfg: &slurm_async_runner::entities::slurm::SlurmJobConfig,
        script: &std::path::Path,
    ) -> SbatchCmd {
        let mut cmd = SbatchCmd::new(script.to_path_buf());
        if !cfg.partition.is_empty() {
            cmd.partition = Some(cfg.partition.clone());
        }
        cmd.time_limit = cfg.time_limit.clone();
        cmd.rsc = cfg.resource_spec.clone();
        cmd.output = cfg.log_stdout.as_ref().map(|p| p.display().to_string());
        cmd.error = cfg.log_stderr.as_ref().map(|p| p.display().to_string());
        cmd.job_name = cfg.job_name.clone();
        cmd.array_spec = cfg.array_spec.clone();
        cmd.mail_user = cfg.mail_user.clone();
        cmd.mail_types = cfg.mail_types.clone();
        cmd.comment = cfg.comment.clone();
        cmd
    }

    async fn submit_one(
        &self,
        jid: &JobId,
        cmd: SbatchCmd,
        flow_uuid: &uuid::Uuid,
    ) -> Result<u64, JobManagerError> {
        let slurm_jobid = self.executor.submit(cmd).await?;
        let run = JobRun {
            lifecycle: Lifecycle::Queued,
            updated_at: Utc::now(),
            slurm_jobid: Some(slurm_jobid),
            slurm_status: None,
            note: None,
        };
        let path = self.resolver.status_file(flow_uuid, jid);
        write_job_run(&path, &run)?;
        Ok(slurm_jobid)
    }
}
```

- [ ] **Step 5: `src/runner/mod.rs` 更新**

```rust
pub mod flow;
pub mod transition;

pub use flow::FlowRunner;
pub use transition::{Decision, TickResult, decide_transition};
```

`src/lib.rs`:

```rust
pub use runner::FlowRunner;
```

- [ ] **Step 6: `InMemoryQuerier::new` 確認** (なければ追加)

`src/slurm/querier.rs`:

```rust
impl InMemoryQuerier {
    pub fn new(responses: std::collections::HashMap<u64, slurm_async_runner::JobStatus>) -> Self {
        Self { responses }
    }
}
```

- [ ] **Step 7: integration テスト pass 確認**

```bash
cargo test --test integration_sp3 --all-features 2>&1 | tail -30
```

期待: 3 テスト pass。

- [ ] **Step 8: コミット**

```bash
git add -A
git commit -m "feat(sp3): add FlowRunner with submit/tick/render_only

submit:
  topological_order → 各 job で effective_config → render → write batch.bash
  if !dry_run: build_sbatch_cmd + dependency → executor.submit → write JobRun(Queued)
tick:
  read all .status.toml → filter terminal → query SLURM → decide_transition
  parent_lifecycles を渡し、Failed/Skipped 親があれば Skipped に遷移
render_only:
  submit と同じだが executor は呼ばない

integration test 3 ケース:
- 順次 submit + .status.toml が Queued で書かれる
- dry_run で batch.bash のみ、executor 呼ばれず
- parent failed → child Skipped"
```

---

## Phase F — CLI

### Task F.1: `src/bin/jm.rs` 本実装 + smoke tests

**Files:**
- Modify: `src/bin/jm.rs` (A.1 stub を本実装に置換)
- Create: `tests/cli_smoke.rs`
- Modify: `Cargo.toml` (dev-dependencies に assert_cmd / predicates 追加)

- [ ] **Step 1: `Cargo.toml` `[dev-dependencies]` に追加**

```toml
[dev-dependencies]
tempfile = "3.10"
rstest = "0.23"
tokio = { version = "1.0", features = ["test-util"] }
assert_cmd = "2.0"
predicates = "3.0"
```

- [ ] **Step 2: `src/bin/jm.rs` 全置換**

`src/bin/jm.rs`:

```rust
//! `jm` — job-manager CLI.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "jm", about = "job-manager CLI")]
struct Cli {
    #[arg(long, global = true)]
    root: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Render batch.bash only.
    Run { target: String },
    /// Submit to SLURM (or DryRun).
    Submit {
        target: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Show flow + per-job status.
    Show { target: String },
    /// Query SLURM and update .status.toml.
    Tick { target: String },
    /// Cross-flow search.
    Search {
        root: PathBuf,
        #[arg(long)]
        program: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let root = resolve_root(&cli)?;
    match cli.cmd {
        Cmd::Run { target } => cmd_run(&root, &target).await,
        Cmd::Submit { target, dry_run } => cmd_submit(&root, &target, dry_run).await,
        Cmd::Show { target } => cmd_show(&root, &target).await,
        Cmd::Tick { target } => cmd_tick(&root, &target).await,
        Cmd::Search { root: search_root, program } => cmd_search(&search_root, program.as_deref()).await,
    }
}

fn resolve_root(cli: &Cli) -> anyhow::Result<PathBuf> {
    if let Some(p) = &cli.root {
        return Ok(p.clone());
    }
    if let Ok(p) = std::env::var("JM_ROOT") {
        return Ok(PathBuf::from(p));
    }
    anyhow::bail!("--root or JM_ROOT must be set")
}

fn parse_target(_root: &std::path::Path, target: &str) -> anyhow::Result<uuid::Uuid> {
    let p = std::path::Path::new(target);
    if p.is_absolute() {
        let last = p.file_name().and_then(|s| s.to_str()).ok_or_else(|| anyhow::anyhow!("invalid path"))?;
        return uuid::Uuid::parse_str(last).map_err(|e| anyhow::anyhow!("invalid uuid: {e}"));
    }
    uuid::Uuid::parse_str(target).map_err(|e| anyhow::anyhow!("invalid uuid: {e}"))
}

async fn cmd_run(root: &std::path::Path, target: &str) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::DryRunExecutor;
    use job_manager::slurm::querier::InMemoryQuerier;
    use std::collections::HashMap;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;
    let runner = FlowRunner::new(
        Box::new(DryRunExecutor),
        Box::new(InMemoryQuerier::new(HashMap::new())),
        &resolver,
    );
    runner.render_only(&fr)?;
    println!("rendered {} jobs in {}", fr.flow.jobs.len(), uuid);
    Ok(())
}

async fn cmd_submit(root: &std::path::Path, target: &str, dry_run: bool) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::{DryRunExecutor, Executor, SbatchExecutor};
    use job_manager::slurm::querier::InMemoryQuerier;
    use std::collections::HashMap;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;
    let exec: Box<dyn Executor> = if dry_run {
        Box::new(DryRunExecutor)
    } else {
        Box::new(SbatchExecutor)
    };
    let runner = FlowRunner::new(
        exec,
        Box::new(InMemoryQuerier::new(HashMap::new())),
        &resolver,
    );
    let jobids = runner.submit(&fr, dry_run).await?;
    println!("submitted {} jobs", jobids.len());
    for (jid, j) in jobids {
        println!("  {} -> {}", jid.0, j);
    }
    Ok(())
}

async fn cmd_show(root: &std::path::Path, target: &str) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::{PathResolver, read_job_run};

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;
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
    let fr = FlowRun::read(&resolver, uuid)?;
    let manager = Arc::new(SlurmManager::default());
    let querier = SlurmQuerier::new(manager);
    let runner = FlowRunner::new(
        Box::new(DryRunExecutor),
        Box::new(querier),
        &resolver,
    );
    let result = runner.tick(&fr).await?;
    println!("tick complete: {} transitions evaluated", result.transitions.len());
    Ok(())
}

async fn cmd_search(root: &std::path::Path, program: Option<&str>) -> anyhow::Result<()> {
    use futures::StreamExt;
    use job_manager::walk::walk_flows;

    let s = walk_flows(root);
    let mut s = std::pin::pin!(s);
    while let Some(item) = s.next().await {
        let flow = item?;
        if let Some(p) = program {
            if !flow.jobs.values().any(|j| j.spec.program.0 == p) {
                continue;
            }
        }
        println!("{}\t{}", flow.uuid, flow.created_at);
    }
    Ok(())
}
```

`SlurmManager::default()` の signature が違う場合 (例: `SlurmManager::new(dispatcher)` のみ) は dispatcher として `TokioDispatcher` 等を使う。

- [ ] **Step 3: smoke test を作成**

`tests/cli_smoke.rs`:

```rust
//! jm CLI smoke tests.

use assert_cmd::Command;
use predicates::prelude::*;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[test]
fn jm_help_runs() {
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--help");
    cmd.assert().success();
}

#[test]
fn jm_run_renders_batch_bash() {
    use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
    use job_manager::persistence::{PathResolver, write_flow, write_plan};
    use job_manager::plan::ExperimentPlan;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;

    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path());
    let uuid = uuid::Uuid::new_v4();
    let jid = JobId("a".to_string());
    let mut jobs = BTreeMap::new();
    jobs.insert(jid.clone(), Job {
        spec: JobSpec {
            program: Program("g16".to_string()),
            body: "echo hi".to_string(),
            config: SlurmJobConfig {
                partition: "p".to_string(),
                time_limit: None, log_stdout: None, log_stderr: None,
                comment: None, job_name: None, array_spec: None,
                dependency: None, mail_user: None, mail_types: None,
                resource_spec: None,
            },
        },
        parents: vec![],
    });
    let flow = JobFlow { uuid, created_at: chrono::Utc::now(), jobs };
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();

    let mut plan_jobs = BTreeMap::new();
    plan_jobs.insert(jid.clone(), BTreeMap::new());
    let plan = ExperimentPlan { jobs: plan_jobs };
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root").arg(dir.path()).arg("run").arg(uuid.to_string());
    cmd.assert().success().stdout(predicate::str::contains("rendered"));

    assert!(resolver.batch_bash(&uuid, &jid).exists());
}
```

- [ ] **Step 4: テスト pass 確認**

```bash
cargo build --bin jm --all-features 2>&1 | tail -10
cargo test --test cli_smoke --all-features 2>&1 | tail -20
```

期待: 2 テスト pass。

- [ ] **Step 5: コミット**

```bash
git add -A
git commit -m "feat(sp3): implement jm CLI with 5 subcommands

run: render_only (DryRunExecutor)
submit: SbatchExecutor または DryRunExecutor (--dry-run)
show: tabular 表示
tick: SlurmQuerier で SLURM query
search: walk_flows ストリーム

smoke test 2 件: --help と run で batch.bash 生成。"
```

---

## Phase G — Python API

### Task G.1: `py_export` rename + 新 modules

**Files:**
- Rename: `src/py_export/status.rs` → `src/py_export/job.rs`
- Rename: `src/py_export/filter.rs` → `src/py_export/search.rs`
- Rename: `src/py_export/tick.rs` → `src/py_export/transition.rs`
- Create: `src/py_export/flow.rs`, `src/py_export/render.rs`, `src/py_export/runner.rs`, `src/py_export/persistence.rs`
- Modify: `src/py_export/mod.rs`

- [ ] **Step 1: 既存 rename**

```bash
git mv src/py_export/status.rs src/py_export/job.rs
git mv src/py_export/filter.rs src/py_export/search.rs
git mv src/py_export/tick.rs src/py_export/transition.rs
```

- [ ] **Step 2: `src/py_export/job.rs` 内容更新**

```bash
sed -i \
  -e 's|StatusEntry|JobRun|g' \
  -e 's|PerJobStatus|Lifecycle|g' \
  -e 's|crate::status::|crate::job::|g' \
  -e 's|read_status|read_job_run|g' \
  -e 's|write_status|write_job_run|g' \
  src/py_export/job.rs
```

ファイル内の `#[pyclass(name = "...")]` 名や pyfunction 登録名も `JobRun` / `Lifecycle` / `read_job_run` / `write_job_run` に揃える (手動で確認)。

- [ ] **Step 3: `src/py_export/flow.rs` 新規作成**

```rust
//! Python wrapper for FlowRun.

use pyo3::prelude::*;
use pyo3_stub_gen::derive::*;

use crate::flow::run::FlowRun;
use crate::persistence::PathResolver;

#[gen_stub_pyclass]
#[pyclass(name = "FlowRun")]
pub struct PyFlowRun {
    pub inner: FlowRun,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFlowRun {
    #[staticmethod]
    pub fn read(root: std::path::PathBuf, flow_uuid: &str) -> PyResult<Self> {
        let resolver = PathResolver::new(root);
        let uuid = uuid::Uuid::parse_str(flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let inner = FlowRun::read(&resolver, uuid)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn flow_uuid(&self) -> String {
        self.inner.flow_uuid.to_string()
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyFlowRun>()?;
    Ok(())
}
```

- [ ] **Step 4: `src/py_export/render.rs` 新規作成**

```rust
use std::collections::BTreeMap;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::*;

use crate::jobid::parse_job_id;
use crate::render::render_batch_bash as inner;
use gaussian_job_shared::entities::workflow::JobId;

#[gen_stub_pyfunction]
#[pyfunction]
pub fn render_batch_bash(
    flow_uuid: &str,
    job_id: &str,
    body: &str,
    params: BTreeMap<String, String>,
) -> PyResult<String> {
    let flow_uuid = uuid::Uuid::parse_str(flow_uuid)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let jid = JobId(job_id.to_string());
    let parts = parse_job_id(&jid.0)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let params_toml: BTreeMap<String, toml::Value> = params
        .into_iter()
        .map(|(k, v)| (k, toml::Value::String(v)))
        .collect();
    Ok(inner(&flow_uuid, &jid, &parts, &params_toml, body))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(render_batch_bash, m)?)?;
    Ok(())
}
```

- [ ] **Step 5: `src/py_export/persistence.rs` 新規作成**

```rust
use pyo3::prelude::*;
use pyo3_stub_gen::derive::*;

use crate::persistence::common::{read_common as inner_read, write_common as inner_write};
use gaussian_job_shared::config::common::CommonConfig;

#[gen_stub_pyfunction]
#[pyfunction]
pub fn read_common(path: std::path::PathBuf) -> PyResult<String> {
    let cc = inner_read(&path)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    toml::to_string(&cc).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

#[gen_stub_pyfunction]
#[pyfunction]
pub fn write_common(path: std::path::PathBuf, toml_str: &str) -> PyResult<()> {
    let cc: CommonConfig = toml::from_str(toml_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    inner_write(&path, &cc).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read_common, m)?)?;
    m.add_function(wrap_pyfunction!(write_common, m)?)?;
    Ok(())
}
```

- [ ] **Step 6: `src/py_export/runner.rs` 新規作成**

```rust
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::*;

use crate::flow::run::FlowRun;
use crate::persistence::PathResolver;
use crate::runner::flow::FlowRunner;
use crate::slurm::executor::{DryRunExecutor, Executor, SbatchExecutor};
use crate::slurm::querier::InMemoryQuerier;

#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (root, flow_uuid, dry_run = false))]
pub fn submit_flow<'py>(
    py: Python<'py>,
    root: std::path::PathBuf,
    flow_uuid: String,
    dry_run: bool,
) -> PyResult<Bound<'py, PyAny>> {
    future_into_py(py, async move {
        let resolver = PathResolver::new(root);
        let uuid = uuid::Uuid::parse_str(&flow_uuid)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let fr = FlowRun::read(&resolver, uuid)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let exec: Box<dyn Executor> = if dry_run {
            Box::new(DryRunExecutor)
        } else {
            Box::new(SbatchExecutor)
        };
        let runner = FlowRunner::new(
            exec,
            Box::new(InMemoryQuerier::new(HashMap::new())),
            &resolver,
        );
        let result = runner
            .submit(&fr, dry_run)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let py_dict: HashMap<String, u64> = result.into_iter().map(|(k, v)| (k.0, v)).collect();
        Ok(py_dict)
    })
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(submit_flow, m)?)?;
    Ok(())
}
```

- [ ] **Step 7: `src/py_export/mod.rs` 更新**

旧 status / filter / tick の登録を新 job / search / transition に置換、新 flow / render / persistence / runner を追加:

```rust
mod error;
mod flow;
mod job;
mod jobid;
mod path;
mod persistence;
mod plan;
mod render;
mod runner;
mod search;
mod transition;
mod view;
mod walk;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    error::register(m)?;
    flow::register(m)?;
    job::register(m)?;
    jobid::register(m)?;
    path::register(m)?;
    persistence::register(m)?;
    plan::register(m)?;
    render::register(m)?;
    runner::register(m)?;
    search::register(m)?;
    transition::register(m)?;
    view::register(m)?;
    walk::register(m)?;
    Ok(())
}
```

各 submodule に `pub fn register(m: ...) -> PyResult<()>` が無ければ追加 (1〜2 行で `m.add_class` / `m.add_function` を並べる)。

- [ ] **Step 8: maturin build 確認**

```bash
uv run maturin develop --uv 2>&1 | tail -20
```

期待: 成功。

- [ ] **Step 9: コミット**

```bash
git add -A
git commit -m "refactor(sp3)!: py_export — rename status/filter/tick + add flow/render/persistence/runner

BREAKING:
- py_export::status → py_export::job (StatusEntry → JobRun, PerJobStatus → Lifecycle)
- py_export::filter → py_export::search
- py_export::tick → py_export::transition
- 新規 py_export::{flow, render, persistence, runner}

submit_flow は pyo3_async_runtimes で async。
common.toml は D2 owner のため TOML 文字列経由で受け渡す。"
```

---

### Task G.2: `python/job_manager/__init__.py` + pytest 全 sync

**Files:**
- Modify: `python/job_manager/__init__.py`
- Modify/Create: `python/tests/test_*.py`

- [ ] **Step 1: `__init__.py` 旧 export を全削除し新型に置換**

`python/job_manager/__init__.py`:

```python
from job_manager._core import (
    # SP-3 v2 新型
    FlowRun,
    JobRun,
    Lifecycle,
    # SP-3 v2 関数
    submit_flow,
    render_batch_bash,
    read_common,
    write_common,
    read_job_run,
    write_job_run,
    # 既存 (SP-1/SP-2)
    PathResolver,
    ExperimentPlan,
    read_flow,
    write_flow,
    read_plan,
    write_plan,
    build_job_id,
    parse_job_id,
    validate_step_id,
    validate_job_id,
    walk_flows,
    SearchFilter,
    CalcView,
)

__all__ = [
    "FlowRun", "JobRun", "Lifecycle",
    "submit_flow", "render_batch_bash",
    "read_common", "write_common",
    "read_job_run", "write_job_run",
    "read_flow", "write_flow", "read_plan", "write_plan",
    "PathResolver", "ExperimentPlan",
    "build_job_id", "parse_job_id", "validate_step_id", "validate_job_id",
    "walk_flows", "SearchFilter", "CalcView",
]
```

- [ ] **Step 2: 旧名残検出 + 書き換え**

```bash
grep -rln "PerJobStatus\|StatusEntry\|read_status\|write_status\|SlurmFacade\|A1SlurmFacade\|InMemorySlurmFacade" python/
```

各ヒットを書き換え:
- `StatusEntry` → `JobRun`
- `PerJobStatus.Done` → `Lifecycle.Success`
- `PerJobStatus.Queued` → `Lifecycle.Queued`
- 他 enum 値も同様
- `read_status` / `write_status` → `read_job_run` / `write_job_run`

- [ ] **Step 3: `python/tests/test_render.py` 新規作成**

```python
"""Render pytest — env-export bash content."""

from job_manager import render_batch_bash


def test_render_emits_jm_param_and_axis():
    out = render_batch_bash(
        flow_uuid="01997cdc-0000-7000-8000-000000000000",
        job_id="opt__a=0",
        body="echo hi",
        params={"route": "B3LYP"},
    )
    assert "export JM_FLOW_UUID='01997cdc-0000-7000-8000-000000000000'" in out
    assert "export JM_AXIS_A='0'" in out
    assert "export JM_PARAM_ROUTE='B3LYP'" in out
    assert "echo hi" in out


def test_render_escapes_single_quote_in_param():
    out = render_batch_bash(
        flow_uuid="01997cdc-0000-7000-8000-000000000000",
        job_id="x__a=0",
        body="",
        params={"note": "it's working"},
    )
    assert "export JM_PARAM_NOTE='it'\\''s working'" in out
```

- [ ] **Step 4: pytest 全 pass 確認**

```bash
uv run pytest python/tests 2>&1 | tail -20
```

期待: 全 PASS。失敗があれば旧 alias 名残を書き換え。

- [ ] **Step 5: コミット**

```bash
git add -A
git commit -m "refactor(sp3)!: update python __init__ and pytest for new types

旧 PerJobStatus/StatusEntry/SlurmFacade を python 側から完全削除、
Lifecycle/JobRun/submit_flow に置換。test_render.py を新規追加。"
```

---

### Task G.3: 最終 sweep — clippy / fmt / coverage / smoke

**Files:**
- 全プロジェクト

- [ ] **Step 1: clippy 全クリーン**

```bash
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -20
```

警告があれば fix。

- [ ] **Step 2: fmt クリーン**

```bash
cargo fmt --check || cargo fmt
```

- [ ] **Step 3: 全テスト pass**

```bash
cargo test --all-features 2>&1 | tail -10
uv run pytest python/tests 2>&1 | tail -10
```

- [ ] **Step 4: カバレッジ確認**

```bash
cargo llvm-cov --fail-under-lines 80 2>&1 | tail -20
```

80% 未満なら欠落テストを追加。

- [ ] **Step 5: jm smoke 手動確認**

```bash
mkdir -p /tmp/jm-smoke
# 適切な flow.toml / plan.toml を build_my_experiment.py 等で配置
cargo run --bin jm -- --root /tmp/jm-smoke run <uuid> 2>&1 | head
```

期待: `rendered N jobs in <uuid>` 出力。

- [ ] **Step 6: 最終コミット**

```bash
git add -A
git commit -m "chore(sp3): final sweep — clippy clean, fmt clean, coverage 80%+

SP-3 v2 全 phase 完了。"
```

---

## Self-Review

### 1. Spec coverage

- [x] Phase 0 (D2 PR) → Task 0.1
- [x] Phase A 全 rename → Task A.1–A.10
- [x] Phase B (common) → Task B.1–B.3
- [x] Phase C (render) → Task C.1–C.3
- [x] Phase D (slurm submit infra) → Task D.1–D.4
- [x] Phase E (runner) → Task E.1–E.5
- [x] Phase F (CLI) → Task F.1
- [x] Phase G (Python) → Task G.1–G.3
- [x] Lifecycle 5 値 (snake_case, `success` / `skipped`) → A.5
- [x] JobRun TOML schema 書き換え → A.5
- [x] Executor / Querier / FlowRunner → D.1–D.3, A.6, E.5
- [x] SlurmQuerier (not SbatchQuerier) → A.6
- [x] `parent_lifecycles` 引数 → E.4
- [x] Skipped 判定 → E.4 + E.5
- [x] Error variants (DependencyCycle / MissingPlanEntry / SubmitFailed / RenderError) → D.1, E.1, E.2, E.5
- [x] CLI 5 subcommands → F.1
- [x] Python pyfunctions → G.1, G.2

### 2. Placeholder scan

- ✅ "TBD" / "TODO" : 該当 0
- ✅ "Similar to Task N" : 該当 0 (各タスク独立、コード重複あっても明示)
- ✅ コードブロック付き : 全 step がコード又はコマンドを含む

### 3. Type consistency

- ✅ `Lifecycle` 5 値 (Queued / Running / Success / Failed / Skipped) — A.5 / E.4 / E.5 で一致
- ✅ `JobRun { lifecycle, updated_at, slurm_jobid, slurm_status, note }` — A.5 / E.5 で一致
- ✅ `Executor::submit(SbatchCmd) -> Result<u64>` — D.1 / D.2 / D.3 で一致
- ✅ `Querier::query(&[u64]) -> Result<HashMap<u64, JobStatus>>` — A.6 / E.5 で一致
- ✅ `Decision::{NoChange, Transition, SkipDueToParent}` — E.4 / E.5 で一致
- ✅ `FlowRunner::{submit, tick, render_only}` — E.5 / F.1 / G.1 で一致

---

## 完了基準

- [ ] Phase 0 D2 PR merged
- [ ] Phase A-G 全タスク完了 (各 commit が one-issue-per-commit)
- [ ] `cargo build --all-features` 成功 + `jm` binary 生成
- [ ] `cargo test --all-features` 成功 (新規 + 移植テスト 計 80+)
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` クリーン
- [ ] `cargo fmt --check` クリーン
- [ ] `uv run maturin develop --uv` 成功
- [ ] `uv run pytest python/tests` 全 PASS
- [ ] `cargo llvm-cov --fail-under-lines 80` クリーン
- [ ] **A1 (slurm-async-runner) 変更ゼロ**
- [ ] **D2 (gaussian-job-shared) 変更は Phase 0 のみ**
