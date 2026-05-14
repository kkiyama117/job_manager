# job-manager SP-3 (submit_chain + CLI) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SP-2 で確立した plan + jobid helper の上に、`common.toml` 合成 / `batch.bash` render / `submit_chain` / CLI `jm` を実装し、Python authoring → SLURM 投入 → ステータス追跡の end-to-end フローを成立させる。

**Architecture:** D2 `CommonConfig` を直接 import (Phase 0 D2 PR で serde derive 追加)。bash render は env-export 方式 (Rust テンプレートエンジン不要)。submit_chain はトポロジカル順 + 順次 (依存先 SLURM jobid が submit 後に判明するため並列化しない)。CLI は clap v4 / tokio::main の `jm` バイナリ。

**Tech Stack:** Rust 2024 (toml, serde, async-trait, tokio, thiserror, clap v4) / PyO3 0.28 / D2 (`gaussian_job_shared`) / A1 (`slurm_async_runner::SbatchManager`).

**Spec reference:** `docs/superpowers/specs/2026-05-13-job-manager-sp3-design.md`

---

## File Structure

### D2 (Phase 0 PR)

- Modify: `../gaussian-job-shared2/src/config/common.rs`
  - 役割: `CommonConfig` / `DirectoryConfig` に serde derives を追加 (newtype 不可侵、struct restructurable)

### job-manager (Phase A-D PR)

| File | Status | 役割 |
|---|---|---|
| `src/common/mod.rs` | CREATE | D2 `CommonConfig` を `pub use`、`merge_with_defaults` を提供 |
| `src/common/io.rs` | CREATE | `read_common` / `write_common` (atomic rename、create_dir_all、tmp cleanup) |
| `src/render/mod.rs` | CREATE | `sanitize_var_name` / `quote_for_bash` / `render_batch_bash` |
| `src/submit/mod.rs` | CREATE | `topological_sort` / `submit_chain` |
| `src/path.rs` | MODIFY | `PathResolver::common_toml()` + `batch_bash()` getter |
| `src/error.rs` | MODIFY | `DependencyCycle` / `MissingPlanEntry` / `SubmitFailed` / `RenderError` variants 追加 |
| `src/lib.rs` | MODIFY | `pub mod common; pub mod render; pub mod submit;` |
| `src/py_export/common.rs` | CREATE | Python: read_common / write_common / merge |
| `src/py_export/render.rs` | CREATE | Python: render_batch_bash |
| `src/py_export/submit.rs` | CREATE | Python: submit_chain (async via pyo3-async-runtimes) |
| `src/py_export/mod.rs` | MODIFY | 上記 module の pymodule_export 追加 |
| `Cargo.toml` | MODIFY | `clap = { version = "4", features = ["derive"] }` 追加 + `[[bin]] name = "jm"` |
| `src/bin/jm.rs` | CREATE | clap CLI / tokio::main / 5 subcommands |
| `python/job_manager/__init__.py` | MODIFY | 新規 pyfunction を re-export |
| `python/tests/test_common.py` | CREATE | Python: common round-trip + merge |
| `python/tests/test_render.py` | CREATE | Python: render_batch_bash E2E |
| `python/tests/test_submit.py` | CREATE | Python: submit_chain (dry_run) |
| `tests/integration_sp3.rs` | CREATE | 12-job sample: render + dry-run submit |
| `tests/cli_smoke.rs` | CREATE | jm CLI smoke tests |

---

## Phase 0: D2 PR — `CommonConfig` に serde derives

job-manager の SP-3 PR が `gaussian_job_shared::config::common::CommonConfig` を serde 経由で TOML read/write するために、D2 側で `Serialize` / `Deserialize` derive を追加する。SP-2 の `JobFlow.work_dir` 撤廃 PR と同じ blocker pattern。

**branch**: `feat/serde-common-config` (D2 repo)

### Task 0.1: D2 — `CommonConfig` / `DirectoryConfig` に serde derives 追加

**Files:**
- Modify: `../gaussian-job-shared2/src/config/common.rs`
- Test: `../gaussian-job-shared2/src/config/common.rs` (同ファイル内 `#[cfg(test)]` ブロック)

- [ ] **Step 1: 既存ファイルを読み、現状確認**

```bash
cat ../gaussian-job-shared2/src/config/common.rs
```

期待: `#[derive(Debug, Clone)]` のみで serde derive なし。

- [ ] **Step 2: serde derives を追加**

`../gaussian-job-shared2/src/config/common.rs` を全置換:

```rust
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use std::path::PathBuf;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommonConfig {
    /// Set default arguments of slurm_config
    pub slurm_default: SlurmJobConfig,
    /// Set config of directory.
    pub directories: DirectoryConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryConfig {
    /// Root of all project data.
    pub project_root: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;

    fn sample_slurm() -> SlurmJobConfig {
        SlurmJobConfig {
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
        }
    }

    #[test]
    fn common_config_toml_round_trip() {
        let original = CommonConfig {
            slurm_default: sample_slurm(),
            directories: DirectoryConfig {
                project_root: PathBuf::from("/work"),
            },
        };
        let text = toml::to_string_pretty(&original).unwrap();
        let back: CommonConfig = toml::from_str(&text).unwrap();
        assert_eq!(back.directories.project_root, PathBuf::from("/work"));
        assert_eq!(back.slurm_default.partition, "long");
    }

    #[test]
    fn deny_unknown_fields_rejects_extra_top_level() {
        let bad = r##"
extra = "field"

[slurm_default]
partition = "long"

[directories]
project_root = "/work"
"##;
        let result: Result<CommonConfig, _> = toml::from_str(bad);
        assert!(result.is_err());
    }

    #[test]
    fn deny_unknown_fields_rejects_extra_in_directories() {
        let bad = r##"
[slurm_default]
partition = "long"

[directories]
project_root = "/work"
unknown = "X"
"##;
        let result: Result<CommonConfig, _> = toml::from_str(bad);
        assert!(result.is_err());
    }
}
```

- [ ] **Step 3: D2 のテストを実行**

Run: `cd ../gaussian-job-shared2 && cargo test --lib config::common::tests`
Expected: 3 tests pass.

- [ ] **Step 4: clippy + fmt + 全テスト**

```bash
cd ../gaussian-job-shared2
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Expected: all clean.

- [ ] **Step 5: Commit + PR**

```bash
cd ../gaussian-job-shared2
git checkout -b feat/serde-common-config
git add src/config/common.rs
git commit -m "feat(config): add serde derives to CommonConfig and DirectoryConfig

job-manager SP-3 が common.toml を TOML round-trip するための前提変更。
struct 構造は不変、derive と #[serde(deny_unknown_fields)] のみ追加。

Related: job-manager SP-3 design §2.2 (Phase 0 D2 PR)."
git push -u origin feat/serde-common-config
gh pr create --base develop --title "feat(config): add serde derives to CommonConfig" --body "job-manager SP-3 (submit_chain + CLI) の前段として CommonConfig / DirectoryConfig に serde derives + deny_unknown_fields を追加。struct shape は不変。"
```

D2 PR がマージされるまで Phase A 着手不可。

---

## Phase A: `common.toml` r/w + `SlurmJobConfig` 合成

Phase 0 がマージされた D2 を使い、job-manager 側で I/O ラッパと merge ヘルパーを実装。

**branch context**: `feat/sp3-submit-and-cli` (既存)

### Task A.1: `PathResolver::common_toml()` getter 追加

**Files:**
- Modify: `src/path.rs`
- Test: `src/path.rs` (同ファイル `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

`src/path.rs` の `#[cfg(test)] mod tests` 末尾に追加:

```rust
    #[test]
    fn common_toml_is_root_join_common_toml() {
        let r = PathResolver::new("/work");
        assert_eq!(r.common_toml(), PathBuf::from("/work/common.toml"));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib path::tests::common_toml_is_root_join_common_toml`
Expected: FAIL with "no method named `common_toml`".

- [ ] **Step 3: Implement**

`src/path.rs` の `impl PathResolver` ブロック内 (`experiment_toml` の直後) に追加:

```rust
    /// `<root>/common.toml` — cluster-wide defaults (SP-3)。flow に紐付かない root 直下のファイル。
    pub fn common_toml(&self) -> PathBuf {
        self.root.join("common.toml")
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib path::tests::common_toml_is_root_join_common_toml`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/path.rs
git commit -m "feat(path): add PathResolver::common_toml() for root-level common config"
```

---

### Task A.2: `crate::common` module + D2 re-export + merge_with_defaults

**Files:**
- Create: `src/common/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Create module + lib.rs declaration + tests**

`src/lib.rs` に追加 (既存の `pub mod plan;` の近く):

```rust
pub mod common;
```

`src/common/mod.rs` を新規作成:

```rust
//! SP-3 common config — D2 `CommonConfig` を再エクスポートし、
//! SlurmJobConfig の per-job override に対する merge ヘルパーを提供する。

pub mod io;

pub use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};

use slurm_async_runner::entities::slurm::SlurmJobConfig;

/// `common.slurm_default` をデフォルト値、`override_` を上書き層として merge し、
/// 新規 `SlurmJobConfig` を返す (immutability rule)。
///
/// セマンティクス:
/// - `partition: String`: `override_.partition.is_empty()` の時のみ common 採用
/// - `Option<T>` フィールド全 10 種: `override_.field.clone().or(common.field.clone())`
pub fn merge_with_defaults(
    common: &CommonConfig,
    override_: &SlurmJobConfig,
) -> SlurmJobConfig {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn empty_slurm(partition: &str) -> SlurmJobConfig {
        SlurmJobConfig {
            partition: partition.to_string(),
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

    fn sample_common() -> CommonConfig {
        CommonConfig {
            slurm_default: SlurmJobConfig {
                partition: "long".to_string(),
                time_limit: Some("1:00:00".parse().unwrap()),
                log_stdout: Some(PathBuf::from("/log/out")),
                log_stderr: None,
                comment: Some("from-common".to_string()),
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
    fn merge_uses_common_partition_when_override_empty() {
        let common = sample_common();
        let override_ = empty_slurm("");
        let merged = merge_with_defaults(&common, &override_);
        assert_eq!(merged.partition, "long");
    }

    #[test]
    fn merge_keeps_override_partition_when_non_empty() {
        let common = sample_common();
        let override_ = empty_slurm("short");
        let merged = merge_with_defaults(&common, &override_);
        assert_eq!(merged.partition, "short");
    }

    #[test]
    fn merge_fills_option_from_common_when_override_none() {
        let common = sample_common();
        let override_ = empty_slurm("short");
        let merged = merge_with_defaults(&common, &override_);
        assert!(merged.time_limit.is_some());
        assert_eq!(merged.log_stdout, Some(PathBuf::from("/log/out")));
        assert_eq!(merged.comment.as_deref(), Some("from-common"));
        assert!(merged.log_stderr.is_none());
    }

    #[test]
    fn merge_keeps_override_option_when_some() {
        let common = sample_common();
        let mut override_ = empty_slurm("short");
        override_.comment = Some("from-override".to_string());
        let merged = merge_with_defaults(&common, &override_);
        assert_eq!(merged.comment.as_deref(), Some("from-override"));
    }
}
```

- [ ] **Step 2: Build + run new tests**

Run: `cargo build --all-features 2>&1 | tail -10 && cargo test --lib common::tests`
Expected: build clean, 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/common/mod.rs src/lib.rs
git commit -m "feat(common): re-export D2 CommonConfig and add merge_with_defaults

partition は is_empty() フォールバック (A1 不可侵で Option<String>
化不可)。Option<T> フィールドは override.or(common)。"
```

---

### Task A.3: `common::io::read_common` / `write_common`

**Files:**
- Create: `src/common/io.rs`

- [ ] **Step 1: Create file with implementation + tests**

`src/common/io.rs` を新規作成:

```rust
//! common.toml の atomic rename I/O (flow_io / plan/io と同じパターン)。

use std::path::Path;

use crate::common::CommonConfig;
use crate::error::JobManagerError;

#[must_use = "read_common returns the parsed CommonConfig; ignoring it drops the data"]
pub fn read_common(path: &Path) -> Result<CommonConfig, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|source| JobManagerError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source,
    })
}

/// Write `common` to `path` atomically (write to `<path>.tmp` then rename).
/// Creates parent directories if missing.
pub fn write_common(path: &Path, common: &CommonConfig) -> Result<(), JobManagerError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| JobManagerError::Io {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let text = toml::to_string_pretty(common)?;
    let tmp = path.with_extension("toml.tmp");
    let result = std::fs::write(&tmp, text)
        .map_err(|source| JobManagerError::Io {
            path: tmp.clone(),
            source,
        })
        .and_then(|()| {
            std::fs::rename(&tmp, path).map_err(|source| JobManagerError::Io {
                path: path.to_path_buf(),
                source,
            })
        });
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::DirectoryConfig;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;
    use std::path::PathBuf;
    use tempfile::tempdir;

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

    #[test]
    fn round_trip_preserves_partition_and_project_root() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("common.toml");
        let c = sample_common();
        write_common(&path, &c).unwrap();
        let back = read_common(&path).unwrap();
        assert_eq!(back.slurm_default.partition, "long");
        assert_eq!(back.directories.project_root, PathBuf::from("/work"));
    }

    #[test]
    fn read_missing_returns_io_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope.toml");
        let e = read_common(&path).unwrap_err();
        assert!(matches!(e, JobManagerError::Io { .. }));
    }

    #[test]
    fn write_creates_parent_dirs() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a/b/c");
        let path = nested.join("common.toml");
        write_common(&path, &sample_common()).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn write_cleans_up_tmp_on_rename_failure() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("common.toml");
        std::fs::create_dir_all(&path).unwrap();
        let result = write_common(&path, &sample_common());
        assert!(result.is_err());
        let tmp = path.with_extension("toml.tmp");
        assert!(!tmp.exists());
    }
}
```

- [ ] **Step 2: Run common::io tests**

Run: `cargo test --lib -- common::io`
Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/common/io.rs
git commit -m "feat(common): atomic read/write_common for common.toml

flow_io / plan/io と同じ pattern: create_dir_all → tmp write → rename
→ 失敗時 tmp cleanup。"
```

---

### Task A.4: Python 公開: `read_common` / `write_common` / `merge_with_defaults`

**Files:**
- Create: `src/py_export/common.rs`
- Modify: `src/py_export/mod.rs`
- Modify: `python/job_manager/__init__.py`
- Create: `python/tests/test_common.py`

- [ ] **Step 1: Add py_export/common.rs**

`src/py_export/common.rs` を新規作成:

```rust
//! Python 公開: common.toml I/O + merge。

use std::path::PathBuf;

use pyo3::prelude::*;

use crate::common::{self, CommonConfig};
use slurm_async_runner::entities::slurm::SlurmJobConfig;

pub(crate) fn read_common(path: PathBuf) -> PyResult<CommonConfig> {
    common::io::read_common(&path).map_err(PyErr::from)
}

pub(crate) fn write_common(path: PathBuf, common: CommonConfig) -> PyResult<()> {
    common::io::write_common(&path, &common).map_err(PyErr::from)
}

pub(crate) fn merge_with_defaults(
    common: CommonConfig,
    override_: SlurmJobConfig,
) -> SlurmJobConfig {
    common::merge_with_defaults(&common, &override_)
}
```

- [ ] **Step 2: Register pyfunctions in mod.rs**

`src/py_export/mod.rs` の `pub mod plan;` の隣に `pub mod common;` を追加。
さらに `#[pymodule] mod job_manager_core` の中、SP-2 plan 関連 block の直前に追加:

```rust
    // SP-3: common config
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn read_common(
        path: std::path::PathBuf,
    ) -> PyResult<gaussian_job_shared::config::common::CommonConfig> {
        super::common::read_common(path)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn write_common(
        path: std::path::PathBuf,
        common: gaussian_job_shared::config::common::CommonConfig,
    ) -> PyResult<()> {
        super::common::write_common(path, common)
    }

    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn merge_with_defaults(
        common: gaussian_job_shared::config::common::CommonConfig,
        override_: slurm_async_runner::entities::slurm::SlurmJobConfig,
    ) -> slurm_async_runner::entities::slurm::SlurmJobConfig {
        super::common::merge_with_defaults(common, override_)
    }
```

**注**: `CommonConfig` の pyclass は **D2 側**で定義済み (Pyclass Single Owner)。job-manager はその FromPyObject / IntoPy 経由で値を受け渡す。`SlurmJobConfig` も同様 (A1)。

- [ ] **Step 3: Update Python re-exports**

`python/job_manager/__init__.py` を読み、`from ._job_manager_core import ...` のリストに新規 3 関数を追加:

```python
# (前略、既存の re-export はそのまま)
from ._job_manager_core import (
    # ... 既存 ...
    read_common,
    write_common,
    merge_with_defaults,
)
```

- [ ] **Step 4: Add Python tests**

`python/tests/test_common.py` を新規作成:

```python
"""Python E2E for SP-3 common.toml + merge."""

from __future__ import annotations

from pathlib import Path

import pytest

from gaussian_job_shared import CommonConfig, DirectoryConfig
from slurm_async_runner import SlurmJobConfig
from job_manager import read_common, write_common, merge_with_defaults


def _sample_common(partition: str = "long") -> CommonConfig:
    return CommonConfig(
        slurm_default=SlurmJobConfig(partition=partition),
        directories=DirectoryConfig(project_root="/work"),
    )


def test_common_round_trip(tmp_path: Path):
    """write_common → read_common で partition / project_root が保存される。"""
    path = tmp_path / "common.toml"
    write_common(path, _sample_common(partition="long"))
    back = read_common(path)
    assert back.slurm_default.partition == "long"


def test_read_common_missing_file_raises_oserror(tmp_path: Path):
    with pytest.raises(OSError):
        read_common(tmp_path / "nope.toml")


def test_merge_with_defaults_uses_common_partition_when_override_empty():
    common = _sample_common(partition="long")
    override = SlurmJobConfig(partition="")
    merged = merge_with_defaults(common, override)
    assert merged.partition == "long"


def test_merge_with_defaults_keeps_override_partition_when_non_empty():
    common = _sample_common(partition="long")
    override = SlurmJobConfig(partition="short")
    merged = merge_with_defaults(common, override)
    assert merged.partition == "short"
```

**注**: 実装時に D2/A1 pyclass constructor signature を `python/.venv/lib/.../*.pyi` で確認 — `CommonConfig(slurm_default=..., directories=...)` で受け取るか、別形式かを合わせる。

- [ ] **Step 5: Build + regen stub + run pytest**

```bash
cargo build --all-features
cargo run --bin stub_gen
uv run ruff format python/job_manager/_job_manager_core/__init__.pyi
uv run maturin develop --uv
uv run pytest python/tests/test_common.py -v
```

Expected: build + stub gen clean, 4 Python tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/py_export/common.rs src/py_export/mod.rs \
  python/job_manager/__init__.py python/job_manager/_job_manager_core/__init__.pyi \
  python/tests/test_common.py
git commit -m "feat(py_export): expose read_common / write_common / merge_with_defaults"
```

---

## Phase B: bash render — `batch.bash` 生成

axis_combo / plan params を bash 環境変数として export し、JobSpec.body をそのまま append する render エンジン。

### Task B.1: `PathResolver::batch_bash()` getter

**Files:**
- Modify: `src/path.rs`

- [ ] **Step 1: Write the failing test**

`src/path.rs` の `#[cfg(test)] mod tests` 末尾に追加:

```rust
    #[test]
    fn batch_bash_lives_inside_job_dir() {
        let r = PathResolver::new("/work");
        let u = sample_uuid();
        let j = JobId::from("opt__c=0");
        assert_eq!(
            r.batch_bash(&u, &j),
            PathBuf::from(format!("/work/{u}/opt__c=0/batch.bash"))
        );
    }
```

- [ ] **Step 2: Run test, expect FAIL**

Run: `cargo test --lib path::tests::batch_bash_lives_inside_job_dir`
Expected: FAIL with "no method named `batch_bash`".

- [ ] **Step 3: Implement**

`src/path.rs` の `impl PathResolver` に `status_file` の直後に追加:

```rust
    /// `<job_dir>/batch.bash` — SP-3 で render した bash script。
    pub fn batch_bash(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
        self.job_dir(flow_uuid, job_id).join("batch.bash")
    }
```

- [ ] **Step 4: Run test, expect PASS**

Run: `cargo test --lib path::tests::batch_bash_lives_inside_job_dir`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/path.rs
git commit -m "feat(path): add PathResolver::batch_bash() for SP-3 rendered scripts"
```

---

### Task B.2: `sanitize_var_name` + `quote_for_bash`

**Files:**
- Create: `src/render/mod.rs` (skeleton + helpers)
- Modify: `src/lib.rs`

- [ ] **Step 1: Create render module skeleton with helpers + tests**

`src/lib.rs` に追加:

```rust
pub mod render;
```

`src/render/mod.rs` を新規作成:

```rust
//! SP-3 bash render — `batch.bash` 生成。
//!
//! 設計判断:
//! - env-export 方式 (Rust 側にテンプレートエンジンなし)
//! - `#SBATCH` directives は batch.bash に書かない (A1 SbatchCmd CLI で渡す)
//! - 値は single-quote で囲み、内部の `'` を `'\''` でエスケープ

/// ASCII 識別子を bash 環境変数名向きに正規化する: upper-case 化 + non-`[A-Z0-9_]` を `_` に置換。
///
/// 入力は事前に `validate_step_id` で `[A-Za-z0-9_-]+` 限定済みである前提。
/// 衝突回避のため呼び出し側でプレフィクスを付与すること (`JM_AXIS_`, `JM_PARAM_`)。
pub fn sanitize_var_name(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// 値を bash 文字列リテラルとして安全に quote する (single-quote + `'\''` エスケープ)。
pub fn quote_for_bash(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_lowercase_to_upper() {
        assert_eq!(sanitize_var_name("route"), "ROUTE");
        assert_eq!(sanitize_var_name("compound"), "COMPOUND");
    }

    #[test]
    fn sanitize_dash_becomes_underscore() {
        assert_eq!(sanitize_var_name("opt-1"), "OPT_1");
    }

    #[test]
    fn sanitize_preserves_underscore() {
        assert_eq!(sanitize_var_name("Step_2"), "STEP_2");
    }

    #[test]
    fn quote_wraps_plain_value() {
        assert_eq!(quote_for_bash("hello"), "'hello'");
    }

    #[test]
    fn quote_escapes_single_quote() {
        // POSIX bash の慣用 `'\''` (close-quote → escaped quote → reopen)
        assert_eq!(quote_for_bash("it's"), r"'it'\''s'");
    }

    #[test]
    fn quote_preserves_whitespace_and_newline() {
        assert_eq!(quote_for_bash("a b\nc"), "'a b\nc'");
    }

    #[test]
    fn quote_preserves_shell_metachars_inside_single_quotes() {
        // single-quote の中では `$ ` `${...}` `\`...\`` 等は展開されない
        assert_eq!(quote_for_bash("${EVIL}"), "'${EVIL}'");
    }
}
```

- [ ] **Step 2: Run render unit tests**

Run: `cargo test --lib render::tests`
Expected: 7 tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/render/mod.rs src/lib.rs
git commit -m "feat(render): add sanitize_var_name and quote_for_bash helpers"
```

---

### Task B.3: `render_batch_bash` 本体

**Files:**
- Modify: `src/render/mod.rs`

- [ ] **Step 1: Add failing test for render_batch_bash**

`src/render/mod.rs` の `#[cfg(test)] mod tests` の末尾に追加:

```rust
    use crate::jobid::JobIdParts;
    use gaussian_job_shared::entities::workflow::JobId;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn fixed_uuid() -> Uuid {
        Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap()
    }

    #[test]
    fn render_emits_shebang_runtime_axis_params_and_body() {
        let mut params: BTreeMap<String, toml::Value> = BTreeMap::new();
        params.insert("route".into(), toml::Value::String("# B3LYP".into()));
        params.insert("nproc".into(), toml::Value::Integer(16));

        let parts = JobIdParts {
            source_step_id: "opt",
            axis_combo: vec![("compound", 0), ("method", 2)],
        };
        let job_id = JobId::from("opt__compound=0__method=2");

        let body = "echo job-body\nformchk\n";
        let rendered = render_batch_bash(&fixed_uuid(), &job_id, &parts, &params, body);

        assert!(rendered.starts_with("#!/bin/bash\n"));
        assert!(rendered.contains("export JM_FLOW_UUID='01997cdc-0000-7000-8000-000000000000'"));
        assert!(rendered.contains("export JM_JOB_ID='opt__compound=0__method=2'"));
        assert!(rendered.contains("export JM_AXIS_COMPOUND='0'"));
        assert!(rendered.contains("export JM_AXIS_METHOD='2'"));
        assert!(rendered.contains("export JM_PARAM_ROUTE='# B3LYP'"));
        assert!(rendered.contains("export JM_PARAM_NPROC='16'"));
        assert!(rendered.ends_with("echo job-body\nformchk\n"));
    }

    #[test]
    fn render_escapes_single_quotes_in_param_values() {
        let mut params: BTreeMap<String, toml::Value> = BTreeMap::new();
        params.insert("note".into(), toml::Value::String("it's a quote".into()));
        let parts = JobIdParts {
            source_step_id: "opt",
            axis_combo: vec![],
        };
        let job_id = JobId::from("opt");
        let rendered = render_batch_bash(&fixed_uuid(), &job_id, &parts, &params, "");
        assert!(rendered.contains(r"export JM_PARAM_NOTE='it'\''s a quote'"));
    }

    #[test]
    fn render_no_axis_omits_axis_block_but_keeps_job_id() {
        let parts = JobIdParts {
            source_step_id: "opt",
            axis_combo: vec![],
        };
        let job_id = JobId::from("opt");
        let rendered = render_batch_bash(
            &fixed_uuid(),
            &job_id,
            &parts,
            &BTreeMap::new(),
            "true\n",
        );
        // axis_combo が空でも JM_JOB_ID / JM_FLOW_UUID は出る
        assert!(rendered.contains("export JM_JOB_ID="));
        assert!(rendered.contains("export JM_FLOW_UUID="));
        // axis 行は無い
        assert!(!rendered.contains("export JM_AXIS_"));
    }
```

- [ ] **Step 2: Run, expect FAIL**

Run: `cargo test --lib render::tests::render_emits_shebang_runtime_axis_params_and_body`
Expected: FAIL with "no function or associated item named `render_batch_bash`".

- [ ] **Step 3: Implement render_batch_bash**

`src/render/mod.rs` の helpers の下、`#[cfg(test)]` の上に追加:

```rust
use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::JobId;
use uuid::Uuid;

use crate::jobid::JobIdParts;

/// `batch.bash` のテキストを生成する。caller がファイルに書き出す。
///
/// 構造:
/// ```text
/// #!/bin/bash
/// # Generated by job_manager SP-3. Do not edit; regenerated on every `jm run`.
///
/// # --- job-manager runtime context ---
/// export JM_FLOW_UUID='...'
/// export JM_JOB_ID='...'
///
/// # --- axis combo ---
/// export JM_AXIS_<NAME>='<idx>'
///
/// # --- plan.toml params ---
/// export JM_PARAM_<KEY>='<value>'
///
/// # --- user body ---
/// <JobSpec.body verbatim>
/// ```
pub fn render_batch_bash(
    flow_uuid: &Uuid,
    job_id: &JobId,
    parts: &JobIdParts<'_>,
    params: &BTreeMap<String, toml::Value>,
    body: &str,
) -> String {
    let mut out = String::new();
    out.push_str("#!/bin/bash\n");
    out.push_str("# Generated by job_manager SP-3. Do not edit; regenerated on every `jm run`.\n");
    out.push('\n');

    out.push_str("# --- job-manager runtime context ---\n");
    out.push_str(&format!(
        "export JM_FLOW_UUID={}\n",
        quote_for_bash(&flow_uuid.to_string())
    ));
    out.push_str(&format!(
        "export JM_JOB_ID={}\n",
        quote_for_bash(&job_id.0)
    ));
    out.push('\n');

    if !parts.axis_combo.is_empty() {
        out.push_str("# --- axis combo ---\n");
        for (ax, idx) in &parts.axis_combo {
            out.push_str(&format!(
                "export JM_AXIS_{}={}\n",
                sanitize_var_name(ax),
                quote_for_bash(&idx.to_string()),
            ));
        }
        out.push('\n');
    }

    if !params.is_empty() {
        out.push_str("# --- plan.toml params ---\n");
        for (k, v) in params {
            let rendered_value = render_toml_value(v);
            out.push_str(&format!(
                "export JM_PARAM_{}={}\n",
                sanitize_var_name(k),
                quote_for_bash(&rendered_value),
            ));
        }
        out.push('\n');
    }

    out.push_str("# --- user body ---\n");
    out.push_str(body);
    out
}

/// `toml::Value` を bash 環境変数値として 1 行の文字列に落とす。
/// String はそのまま、それ以外は Display で stringify。
/// Array / Table は JSON 風 (デバッグ用)。
fn render_toml_value(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Datetime(d) => d.to_string(),
        toml::Value::Array(_) | toml::Value::Table(_) => v.to_string(),
    }
}
```

- [ ] **Step 4: Run all render tests**

Run: `cargo test --lib render::tests`
Expected: 10 tests pass (7 helpers + 3 render_batch_bash).

- [ ] **Step 5: Commit**

```bash
git add src/render/mod.rs
git commit -m "feat(render): add render_batch_bash for SP-3 batch.bash generation

env-export 方式: JM_FLOW_UUID / JM_JOB_ID / JM_AXIS_* / JM_PARAM_* を
export してから JobSpec.body を append。#SBATCH directives は含めない
(A1 SbatchCmd CLI 引数で渡す前提)。"
```

---

### Task B.4: Python 公開: `render_batch_bash`

**Files:**
- Create: `src/py_export/render.rs`
- Modify: `src/py_export/mod.rs`
- Modify: `python/job_manager/__init__.py`
- Create: `python/tests/test_render.py`

- [ ] **Step 1: Add py_export/render.rs**

`src/py_export/render.rs` を新規作成:

```rust
//! Python 公開: bash render。

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::render;
use gaussian_job_shared::entities::workflow::JobId;

pub(crate) fn render_batch_bash<'py>(
    _py: Python<'py>,
    flow_uuid: &str,
    job_id: &str,
    axis_combo: Vec<(String, usize)>,
    params: Bound<'py, PyDict>,
    body: &str,
) -> PyResult<String> {
    let uuid = uuid::Uuid::parse_str(flow_uuid)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad uuid: {e}")))?;
    crate::jobid::validate_job_id(job_id).map_err(PyErr::from)?;

    // axis_combo は (str, int) tuple のリスト。borrow refs を構築する。
    let axis_refs: Vec<(&str, usize)> = axis_combo.iter().map(|(s, i)| (s.as_str(), *i)).collect();
    let parts = crate::jobid::JobIdParts {
        source_step_id: "_unused",
        axis_combo: axis_refs,
    };

    // params は Bound<'py, PyDict>。pythonize で toml::Value 列に変換する。
    let mut params_map: std::collections::BTreeMap<String, toml::Value> =
        std::collections::BTreeMap::new();
    for (k, v) in params.iter() {
        let key: String = k.extract()?;
        let val: toml::Value = pythonize::depythonize(&v)?;
        params_map.insert(key, val);
    }

    let job_id_typed = JobId::from(job_id);
    Ok(render::render_batch_bash(
        &uuid,
        &job_id_typed,
        &parts,
        &params_map,
        body,
    ))
}
```

- [ ] **Step 2: Register pyfunction**

`src/py_export/mod.rs` に `pub mod render;` を追加。`#[pymodule]` block に追加:

```rust
    // SP-3: bash render
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    fn render_batch_bash<'py>(
        py: Python<'py>,
        flow_uuid: &str,
        job_id: &str,
        axis_combo: Vec<(String, usize)>,
        params: Bound<'py, pyo3::types::PyDict>,
        body: &str,
    ) -> PyResult<String> {
        super::render::render_batch_bash(py, flow_uuid, job_id, axis_combo, params, body)
    }
```

- [ ] **Step 3: Re-export in Python**

`python/job_manager/__init__.py` の re-export リストに `render_batch_bash` を追加。

- [ ] **Step 4: Add Python test**

`python/tests/test_render.py` を新規作成:

```python
"""Python E2E for SP-3 render_batch_bash."""

from __future__ import annotations

import pytest

from job_manager import render_batch_bash


def test_render_includes_runtime_axis_and_params():
    out = render_batch_bash(
        flow_uuid="01997cdc-0000-7000-8000-000000000000",
        job_id="opt__compound=0__method=2",
        axis_combo=[("compound", 0), ("method", 2)],
        params={"route": "# B3LYP", "nproc": 16},
        body="echo body\n",
    )
    assert out.startswith("#!/bin/bash\n")
    assert "export JM_FLOW_UUID='01997cdc-0000-7000-8000-000000000000'" in out
    assert "export JM_JOB_ID='opt__compound=0__method=2'" in out
    assert "export JM_AXIS_COMPOUND='0'" in out
    assert "export JM_AXIS_METHOD='2'" in out
    assert "export JM_PARAM_ROUTE='# B3LYP'" in out
    assert "export JM_PARAM_NPROC='16'" in out
    assert out.endswith("echo body\n")


def test_render_rejects_invalid_job_id():
    with pytest.raises(ValueError):
        render_batch_bash(
            flow_uuid="01997cdc-0000-7000-8000-000000000000",
            job_id="../evil",
            axis_combo=[],
            params={},
            body="",
        )


def test_render_rejects_bad_uuid():
    with pytest.raises(ValueError):
        render_batch_bash(
            flow_uuid="not-a-uuid",
            job_id="opt",
            axis_combo=[],
            params={},
            body="",
        )
```

- [ ] **Step 5: Build + run**

```bash
cargo build --all-features
cargo run --bin stub_gen
uv run ruff format python/job_manager/_job_manager_core/__init__.pyi
uv run maturin develop --uv
uv run pytest python/tests/test_render.py -v
```

Expected: 3 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/py_export/render.rs src/py_export/mod.rs \
  python/job_manager/__init__.py python/job_manager/_job_manager_core/__init__.pyi \
  python/tests/test_render.py
git commit -m "feat(py_export): expose render_batch_bash with job_id and uuid validation"
```

---

## Phase C: `submit_chain` — トポロジカル順 + dep 解決 + SbatchManager

### Task C.1: error.rs に 4 variants 追加

**Files:**
- Modify: `src/error.rs`

- [ ] **Step 1: Add failing test for new error display**

`src/error.rs` の `#[cfg(test)] mod tests` 末尾に追加:

```rust
    #[test]
    fn dependency_cycle_message_includes_uuid() {
        let u = uuid::Uuid::nil();
        let err = JobManagerError::DependencyCycle { flow: u };
        assert!(err.to_string().contains(&u.to_string()));
    }

    #[test]
    fn missing_plan_entry_includes_job_id() {
        let err = JobManagerError::MissingPlanEntry {
            flow: uuid::Uuid::nil(),
            job: gaussian_job_shared::entities::workflow::JobId::from("opt"),
        };
        assert!(err.to_string().contains("opt"));
    }
```

- [ ] **Step 2: Run, expect FAIL**

Run: `cargo test --lib error::tests::dependency_cycle_message_includes_uuid`
Expected: FAIL with "no variant or associated item named `DependencyCycle`".

- [ ] **Step 3: Add 4 variants**

`src/error.rs` の `JobManagerError` enum の `Other(String)` バリアントの直前に追加:

```rust
    #[error("dependency cycle detected in flow {flow}")]
    DependencyCycle { flow: uuid::Uuid },

    #[error("missing plan entry for job {job} in flow {flow}")]
    MissingPlanEntry {
        flow: uuid::Uuid,
        job: gaussian_job_shared::entities::workflow::JobId,
    },

    #[error("sbatch submission failed for job {job}: {source}")]
    SubmitFailed {
        job: gaussian_job_shared::entities::workflow::JobId,
        #[source]
        source: anyhow::Error,
    },

    #[error("bash render failed for job {job}: {reason}")]
    RenderError {
        job: gaussian_job_shared::entities::workflow::JobId,
        reason: String,
    },
```

- [ ] **Step 4: Run, expect PASS**

Run: `cargo test --lib error::tests`
Expected: all tests pass (6 total).

- [ ] **Step 5: Commit**

```bash
git add src/error.rs
git commit -m "feat(error): add SP-3 variants DependencyCycle / MissingPlanEntry / SubmitFailed / RenderError"
```

---

### Task C.2: `topological_sort` on `JobFlow`

**Files:**
- Create: `src/submit/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Add module + failing tests**

`src/lib.rs` に追加:

```rust
pub mod submit;
```

`src/submit/mod.rs` を新規作成:

```rust
//! SP-3 submit_chain — topological sort + dep resolution + SbatchManager wiring。

use std::collections::{BTreeMap, VecDeque};

use gaussian_job_shared::entities::workflow::{JobFlow, JobId};

use crate::error::JobManagerError;

/// JobFlow.jobs を JobEdge[] が形成する DAG とみなしトポロジカル順に並べる。
/// サイクルがあれば `DependencyCycle` エラー。
pub fn topological_sort(flow: &JobFlow) -> Result<Vec<JobId>, JobManagerError> {
    // 1. 各 JobId の in-degree (parents.len()) を計算
    let mut indeg: BTreeMap<JobId, usize> = BTreeMap::new();
    for (jid, job) in &flow.jobs {
        indeg.insert(jid.clone(), job.parents.len());
    }

    // 2. parent -> children index を作成 (BTreeMap key 順で決定的)
    let mut children: BTreeMap<JobId, Vec<JobId>> = BTreeMap::new();
    for (jid, job) in &flow.jobs {
        for edge in &job.parents {
            children
                .entry(edge.from.clone())
                .or_default()
                .push(jid.clone());
        }
    }

    // 3. Kahn's algorithm
    let mut queue: VecDeque<JobId> = indeg
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(k, _)| k.clone())
        .collect();
    let mut out: Vec<JobId> = Vec::with_capacity(flow.jobs.len());
    while let Some(jid) = queue.pop_front() {
        out.push(jid.clone());
        if let Some(kids) = children.get(&jid) {
            for child in kids {
                if let Some(d) = indeg.get_mut(child) {
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(child.clone());
                    }
                }
            }
        }
    }

    if out.len() != flow.jobs.len() {
        return Err(JobManagerError::DependencyCycle { flow: flow.uuid });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobSpec, Program};
    use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};
    use uuid::Uuid;

    fn empty_config() -> SlurmJobConfig {
        SlurmJobConfig {
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
        }
    }

    fn job_with_parents(parents: Vec<(&str, DependencyType)>) -> Job {
        Job {
            spec: JobSpec {
                program: Program::from("g16"),
                config: empty_config(),
                body: String::new(),
            },
            parents: parents
                .into_iter()
                .map(|(p, k)| JobEdge {
                    from: JobId::from(p),
                    kind: k,
                })
                .collect(),
        }
    }

    fn make_flow(jobs: BTreeMap<JobId, Job>) -> JobFlow {
        JobFlow {
            uuid: Uuid::now_v7(),
            created_at: Utc::now(),
            tags: BTreeMap::new(),
            jobs,
        }
    }

    #[test]
    fn topo_sort_linear_chain_returns_parent_before_child() {
        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("a"), job_with_parents(vec![]));
        jobs.insert(
            JobId::from("b"),
            job_with_parents(vec![("a", DependencyType::AfterOk)]),
        );
        jobs.insert(
            JobId::from("c"),
            job_with_parents(vec![("b", DependencyType::AfterOk)]),
        );
        let flow = make_flow(jobs);
        let order = topological_sort(&flow).unwrap();
        let pos_a = order.iter().position(|j| j.0 == "a").unwrap();
        let pos_b = order.iter().position(|j| j.0 == "b").unwrap();
        let pos_c = order.iter().position(|j| j.0 == "c").unwrap();
        assert!(pos_a < pos_b && pos_b < pos_c);
    }

    #[test]
    fn topo_sort_detects_cycle() {
        let mut jobs = BTreeMap::new();
        jobs.insert(
            JobId::from("a"),
            job_with_parents(vec![("b", DependencyType::AfterOk)]),
        );
        jobs.insert(
            JobId::from("b"),
            job_with_parents(vec![("a", DependencyType::AfterOk)]),
        );
        let flow = make_flow(jobs);
        let err = topological_sort(&flow).unwrap_err();
        assert!(matches!(err, JobManagerError::DependencyCycle { .. }));
    }

    #[test]
    fn topo_sort_diamond_emits_each_node_once() {
        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("root"), job_with_parents(vec![]));
        jobs.insert(
            JobId::from("left"),
            job_with_parents(vec![("root", DependencyType::AfterOk)]),
        );
        jobs.insert(
            JobId::from("right"),
            job_with_parents(vec![("root", DependencyType::AfterOk)]),
        );
        jobs.insert(
            JobId::from("join"),
            job_with_parents(vec![
                ("left", DependencyType::AfterOk),
                ("right", DependencyType::AfterOk),
            ]),
        );
        let flow = make_flow(jobs);
        let order = topological_sort(&flow).unwrap();
        assert_eq!(order.len(), 4);
        let pos = |s: &str| order.iter().position(|j| j.0 == s).unwrap();
        assert!(pos("root") < pos("left"));
        assert!(pos("root") < pos("right"));
        assert!(pos("left") < pos("join"));
        assert!(pos("right") < pos("join"));
    }
}
```

- [ ] **Step 2: Run topo sort tests**

Run: `cargo test --lib submit::tests`
Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/submit/mod.rs src/lib.rs
git commit -m "feat(submit): add topological_sort with cycle detection"
```

---

### Task C.3: `submit_chain` 本体 (sbatch_bin-injected mock-friendly 実装)

**Files:**
- Modify: `src/submit/mod.rs`

submit_chain は A1 `SbatchManager::spawn` を呼ぶが、テストでは fake sbatch binary を `sbatch_bin` 引数で差し替えできるようにする (A1 `SbatchCmd.sbatch_bin` を活用)。

- [ ] **Step 1: Add failing test for submit_chain dry_run**

`src/submit/mod.rs` の tests 末尾に追加:

```rust
    use crate::common::CommonConfig;
    use crate::path::PathResolver;
    use crate::plan::ExperimentPlan;
    use tempfile::tempdir;

    #[tokio::test]
    async fn submit_chain_dry_run_writes_batch_bash_for_each_job_and_skips_sbatch() {
        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path().to_path_buf());
        let uuid = Uuid::now_v7();

        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("a"), job_with_parents(vec![]));
        jobs.insert(
            JobId::from("b"),
            job_with_parents(vec![("a", DependencyType::AfterOk)]),
        );
        let flow = JobFlow {
            uuid,
            created_at: Utc::now(),
            tags: BTreeMap::new(),
            jobs,
        };
        let mut plan_jobs = BTreeMap::new();
        plan_jobs.insert(JobId::from("a"), BTreeMap::new());
        plan_jobs.insert(JobId::from("b"), BTreeMap::new());
        let plan = ExperimentPlan { jobs: plan_jobs };

        let result = submit_chain(&resolver, &flow, &plan, None::<&CommonConfig>, None, true)
            .await
            .unwrap();
        assert!(result.is_empty(), "dry_run yields no SLURM jobids");

        assert!(resolver.batch_bash(&uuid, &JobId::from("a")).exists());
        assert!(resolver.batch_bash(&uuid, &JobId::from("b")).exists());
    }

    #[tokio::test]
    async fn submit_chain_missing_plan_entry_errors() {
        let dir = tempdir().unwrap();
        let resolver = PathResolver::new(dir.path().to_path_buf());
        let uuid = Uuid::now_v7();

        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("only"), job_with_parents(vec![]));
        let flow = JobFlow {
            uuid,
            created_at: Utc::now(),
            tags: BTreeMap::new(),
            jobs,
        };
        let plan = ExperimentPlan { jobs: BTreeMap::new() };

        let err = submit_chain(&resolver, &flow, &plan, None::<&CommonConfig>, None, true)
            .await
            .unwrap_err();
        assert!(matches!(err, JobManagerError::MissingPlanEntry { .. }));
    }
```

- [ ] **Step 2: Run, expect FAIL**

Run: `cargo test --lib submit::tests::submit_chain_dry_run_writes_batch_bash_for_each_job_and_skips_sbatch`
Expected: FAIL with "no function named `submit_chain`".

- [ ] **Step 3: Implement submit_chain**

`src/submit/mod.rs` の `topological_sort` の下、`#[cfg(test)]` の上に追加:

```rust
use std::path::PathBuf;

use slurm_async_runner::entities::slurm::SlurmDependency;
use slurm_async_runner::sbatch::cmd::SbatchCmd;
use slurm_async_runner::sbatch::manager::SbatchManager;

use crate::common::{self, CommonConfig};
use crate::jobid::parse_job_id;
use crate::path::PathResolver;
use crate::plan::ExperimentPlan;
use crate::render::render_batch_bash;
// `crate::status` は src/status/{mod.rs,io.rs} で定義済み (SP-1):
//   - StatusEntry { lifecycle, updated_at, slurm_jobid, slurm_status, note }
//   - PerJobStatus { Queued, Running, Done, Failed }
//   - io::{read_status, write_status}

/// effective_config から SbatchCmd を組み立てる純粋関数 (副作用なし)。
/// 依存 (`dependency` フィールド) は別途 [`build_dependency`] で設定する。
fn build_sbatch_cmd(
    effective_config: &slurm_async_runner::entities::slurm::SlurmJobConfig,
    script: &std::path::Path,
    sbatch_bin: &str,
) -> SbatchCmd {
    let mut cmd = SbatchCmd::new(script.to_path_buf());
    cmd.sbatch_bin = sbatch_bin.to_string();
    cmd.partition = if effective_config.partition.is_empty() {
        None
    } else {
        Some(effective_config.partition.clone())
    };
    cmd.time_limit = effective_config.time_limit.clone();
    cmd.rsc = effective_config.resource_spec.clone();
    cmd.output = effective_config
        .log_stdout
        .as_ref()
        .map(|p| p.display().to_string());
    cmd.error = effective_config
        .log_stderr
        .as_ref()
        .map(|p| p.display().to_string());
    cmd.job_name = effective_config.job_name.clone();
    cmd.array_spec = effective_config.array_spec.clone();
    cmd.mail_user = effective_config.mail_user.clone();
    cmd.mail_types = effective_config.mail_types.clone();
    cmd.comment = effective_config.comment.clone();
    cmd
}

/// JobEdge[] と submit 済み jobid の map から `SlurmDependency` を構築する。
/// parent が未 submit (= map に無い) の場合は除外。何も残らなければ `None`。
fn build_dependency(
    parents: &[gaussian_job_shared::entities::workflow::JobEdge],
    submitted: &BTreeMap<JobId, u64>,
    job: &JobId,
) -> Result<Option<SlurmDependency>, JobManagerError> {
    let parent_deps: Vec<(u64, slurm_async_runner::entities::slurm::DependencyType)> = parents
        .iter()
        .filter_map(|edge| {
            submitted
                .get(&edge.from)
                .map(|jobid| (*jobid, edge.kind.clone()))
        })
        .collect();
    if parent_deps.is_empty() {
        return Ok(None);
    }
    let dep_str = parent_deps
        .iter()
        .map(|(j, k)| format!("{k}:{j}"))
        .collect::<Vec<_>>()
        .join(",");
    let dep = dep_str
        .parse::<SlurmDependency>()
        .map_err(|e: <SlurmDependency as std::str::FromStr>::Err| {
            JobManagerError::SubmitFailed {
                job: job.clone(),
                source: anyhow::anyhow!("dependency parse: {e}"),
            }
        })?;
    Ok(Some(dep))
}

/// `<job_dir>/batch.bash` を atomic ではないが create_dir_all 付きで書く。
fn write_batch_bash(path: &std::path::Path, body: &str) -> Result<(), JobManagerError> {
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

/// 単一 job を sbatch に投入し、`.status.toml` を Queued + slurm_jobid で書く。
async fn submit_one(
    resolver: &PathResolver,
    flow_uuid: &uuid::Uuid,
    jid: &JobId,
    cmd: SbatchCmd,
) -> Result<u64, JobManagerError> {
    let manager = SbatchManager::new(cmd);
    let handle = manager
        .spawn()
        .await
        .map_err(|e| JobManagerError::SubmitFailed {
            job: jid.clone(),
            source: anyhow::anyhow!(e),
        })?;
    // A1 `SbatchJobHandle::jobid(&self) -> Option<u64>` (handle.rs:218)。
    // None は SLURM が jobid を返さなかった異常系 — sentinel 0 は使わず明示的に fail。
    let slurm_jobid = handle.jobid().ok_or_else(|| JobManagerError::SubmitFailed {
        job: jid.clone(),
        source: anyhow::anyhow!("sbatch returned no jobid"),
    })?;
    let status_path = resolver.status_file(flow_uuid, jid);
    let entry = crate::status::StatusEntry {
        lifecycle: crate::status::PerJobStatus::Queued,
        updated_at: chrono::Utc::now(),
        slurm_jobid: Some(slurm_jobid),
        slurm_status: None,
        note: None,
    };
    crate::status::io::write_status(&status_path, &entry)?;
    Ok(slurm_jobid)
}

/// 各 JobId をトポロジカル順に submit する。
///
/// - `common`: 任意の cluster-wide defaults。
/// - `sbatch_bin`: 任意の sbatch コマンドパス (デフォルト `"sbatch"`、テストで fake へ差し替え)。
/// - `dry_run`: true なら batch.bash を書くだけで sbatch を呼ばない (戻り値の map は空)。
pub async fn submit_chain(
    resolver: &PathResolver,
    flow: &JobFlow,
    plan: &ExperimentPlan,
    common: Option<&CommonConfig>,
    sbatch_bin: Option<&str>,
    dry_run: bool,
) -> Result<BTreeMap<JobId, u64>, JobManagerError> {
    let order = topological_sort(flow)?;
    let mut submitted: BTreeMap<JobId, u64> = BTreeMap::new();
    let sbatch_bin = sbatch_bin.unwrap_or("sbatch");

    for jid in order {
        let job = flow
            .jobs
            .get(&jid)
            .expect("topological_sort yields only existing JobIds");
        let params = plan.jobs.get(&jid).ok_or_else(|| {
            JobManagerError::MissingPlanEntry {
                flow: flow.uuid,
                job: jid.clone(),
            }
        })?;

        let effective_config = match common {
            Some(c) => common::merge_with_defaults(c, &job.spec.config),
            None => job.spec.config.clone(),
        };

        let parts = parse_job_id(&jid.0)?;
        let body = render_batch_bash(&flow.uuid, &jid, &parts, params, &job.spec.body);
        let batch_path: PathBuf = resolver.batch_bash(&flow.uuid, &jid);
        write_batch_bash(&batch_path, &body)?;

        if dry_run {
            continue;
        }

        let mut cmd = build_sbatch_cmd(&effective_config, &batch_path, sbatch_bin);
        cmd.dependency = build_dependency(&job.parents, &submitted, &jid)?;

        let slurm_jobid = submit_one(resolver, &flow.uuid, &jid, cmd).await?;
        submitted.insert(jid, slurm_jobid);
    }

    Ok(submitted)
}
```

- [ ] **Step 4: Run new submit_chain tests**

Run: `cargo test --lib submit::tests`
Expected: 5 tests pass (3 topo + 2 submit_chain dry-run).

- [ ] **Step 5: Commit**

```bash
git add src/submit/mod.rs
git commit -m "feat(submit): submit_chain with topological order, dep resolution, dry-run

dry_run=true で batch.bash 生成のみ。sbatch_bin で fake-sbatch 差し替え
可能。MissingPlanEntry / DependencyCycle / SubmitFailed エラー伝播。"
```

---

### Task C.4: integration test — 12-job sample dry_run render

**Files:**
- Create: `tests/integration_sp3.rs`

- [ ] **Step 1: Create integration test**

`tests/integration_sp3.rs` を新規作成:

```rust
//! SP-3 integration: 12-job (3 compounds × 2 methods × 2 steps) を構築 →
//! flow.toml + plan.toml を書く → submit_chain dry_run → batch.bash 全件確認。

use std::collections::BTreeMap;

use chrono::Utc;
use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobFlow, JobId, JobSpec, Program};
use job_manager::{
    flow_io::write_flow,
    jobid::build_job_id,
    path::PathResolver,
    plan::{ExperimentPlan, io::write_plan},
    submit::submit_chain,
};
use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};
use tempfile::tempdir;
use uuid::Uuid;

fn empty_config() -> SlurmJobConfig {
    SlurmJobConfig {
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
    }
}

#[tokio::test]
async fn twelve_jobs_dry_run_renders_each_batch_bash() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path().to_path_buf());
    let uuid = Uuid::now_v7();

    let mut jobs = BTreeMap::new();
    let mut params = BTreeMap::new();
    let compounds = ["benzene", "toluene", "p-xylene"];
    let methods = [("b3lyp", "B3LYP"), ("m062x", "M06-2X")];

    for (i, c) in compounds.iter().enumerate() {
        for (j, (_mname, route)) in methods.iter().enumerate() {
            let opt_id = JobId::from(build_job_id("opt", &[("compound", i), ("method", j)]));
            jobs.insert(
                opt_id.clone(),
                Job {
                    spec: JobSpec {
                        program: Program::from("g16"),
                        config: empty_config(),
                        body: "echo opt\n".to_string(),
                    },
                    parents: vec![],
                },
            );
            let mut p = BTreeMap::new();
            p.insert("route".into(), toml::Value::String(format!("# {route} opt")));
            p.insert("compound".into(), toml::Value::String((*c).to_string()));
            params.insert(opt_id.clone(), p);

            let freq_id = JobId::from(build_job_id("freq", &[("compound", i), ("method", j)]));
            jobs.insert(
                freq_id.clone(),
                Job {
                    spec: JobSpec {
                        program: Program::from("g16"),
                        config: empty_config(),
                        body: "echo freq\n".to_string(),
                    },
                    parents: vec![JobEdge {
                        from: opt_id.clone(),
                        kind: DependencyType::AfterOk,
                    }],
                },
            );
            let mut pf = BTreeMap::new();
            pf.insert("route".into(), toml::Value::String(format!("# {route} freq")));
            pf.insert("compound".into(), toml::Value::String((*c).to_string()));
            params.insert(freq_id, pf);
        }
    }

    let flow = JobFlow {
        uuid,
        created_at: Utc::now(),
        tags: BTreeMap::new(),
        jobs,
    };
    let plan = ExperimentPlan { jobs: params };

    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let result = submit_chain(&resolver, &flow, &plan, None, None, true)
        .await
        .unwrap();
    assert!(result.is_empty(), "dry_run yields no SLURM jobids");

    // 12 jobs (3*2*2) — opt と freq 合わせて 12
    let mut count = 0;
    for jid in flow.jobs.keys() {
        let path = resolver.batch_bash(&uuid, jid);
        assert!(path.exists(), "missing batch.bash for {}", jid.0);
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.starts_with("#!/bin/bash\n"));
        assert!(content.contains(&format!("export JM_JOB_ID='{}'", jid.0)));
        count += 1;
    }
    assert_eq!(count, 12);
}
```

- [ ] **Step 2: Run integration test**

Run: `cargo test --test integration_sp3`
Expected: 1 test pass.

- [ ] **Step 3: Commit**

```bash
git add tests/integration_sp3.rs
git commit -m "test(sp3): integration test for 12-job dry_run render"
```

---

### Task C.5: Python 公開 `submit_chain` (async)

**Files:**
- Create: `src/py_export/submit.rs`
- Modify: `src/py_export/mod.rs`
- Modify: `python/job_manager/__init__.py`
- Create: `python/tests/test_submit.py`

submit_chain は async なので `pyo3-async-runtimes::tokio::future_into_py` で Python coroutine に変換する。Python API は path + uuid を受け取って Rust 側で read_flow / read_plan する薄いラッパ。

- [ ] **Step 1: Add py_export/submit.rs**

`src/py_export/submit.rs` を新規作成:

```rust
//! Python 公開: submit_chain (async)。

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::common::CommonConfig;
use crate::flow_io::read_flow;
use crate::path::PathResolver;
use crate::plan::io::read_plan as read_plan_rs;
use crate::submit;

/// Python から submit_chain を呼ぶ薄いラッパ。`<flow_dir>` から flow.toml と
/// plan.toml を read し、submit_chain を asyncio coroutine として返す。
pub(crate) fn submit_chain_py<'py>(
    py: Python<'py>,
    root: PathBuf,
    flow_uuid: String,
    common_path: Option<PathBuf>,
    sbatch_bin: Option<String>,
    dry_run: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let uuid = uuid::Uuid::parse_str(&flow_uuid)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("bad uuid: {e}")))?;
    future_into_py(py, async move {
        let resolver = PathResolver::new(root);
        let flow = read_flow(&resolver.flow_toml(&uuid)).map_err(PyErr::from)?;
        let plan = read_plan_rs(&resolver.plan_toml(&uuid)).map_err(PyErr::from)?;
        let common: Option<CommonConfig> = if let Some(p) = common_path {
            Some(crate::common::io::read_common(&p).map_err(PyErr::from)?)
        } else {
            None
        };
        let result = submit::submit_chain(
            &resolver,
            &flow,
            &plan,
            common.as_ref(),
            sbatch_bin.as_deref(),
            dry_run,
        )
        .await
        .map_err(PyErr::from)?;

        Python::with_gil(|py| {
            let d = PyDict::new(py);
            for (k, v) in result {
                d.set_item(k.0, v)?;
            }
            Ok::<_, PyErr>(d.into())
        })
    })
}
```

- [ ] **Step 2: Register pyfunction**

`src/py_export/mod.rs` に `pub mod submit;` を追加。`#[pymodule]` block に追加:

```rust
    // SP-3: submit_chain (async)
    #[pyo3_stub_gen::derive::gen_stub_pyfunction()]
    #[pyfunction]
    #[pyo3(signature = (root, flow_uuid, common_path=None, sbatch_bin=None, dry_run=false))]
    fn submit_chain<'py>(
        py: Python<'py>,
        root: std::path::PathBuf,
        flow_uuid: String,
        common_path: Option<std::path::PathBuf>,
        sbatch_bin: Option<String>,
        dry_run: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        super::submit::submit_chain_py(py, root, flow_uuid, common_path, sbatch_bin, dry_run)
    }
```

`python/job_manager/__init__.py` に `submit_chain` を re-export 追加。

- [ ] **Step 3: Add Python smoke test**

`python/tests/test_submit.py` を新規作成:

```python
"""Python E2E for SP-3 submit_chain (dry_run only — no real SLURM in CI)."""

from __future__ import annotations

import asyncio
import uuid as _uuid
from datetime import datetime, timezone
from pathlib import Path

from gaussian_job_shared import JobFlow, JobId, Job, JobSpec, Program, JobEdge
from slurm_async_runner import DependencyType, SlurmJobConfig
from job_manager import (
    ExperimentPlan, PathResolver,
    write_flow, write_plan,
    build_job_id, submit_chain,
)


def test_submit_chain_dry_run_renders_batch_bash(tmp_path: Path):
    resolver = PathResolver(str(tmp_path))
    flow_uuid = _uuid.UUID("01997cdc-0000-7000-8000-000000000001")
    uuid_str = str(flow_uuid)

    jobs = {}
    params = {}
    opt_id = JobId(build_job_id("opt", [("compound", 0)]))
    jobs[opt_id] = Job(
        spec=JobSpec(
            program=Program("g16"),
            config=SlurmJobConfig(partition="long"),
            body="echo opt\n",
        ),
        parents=[],
    )
    params[opt_id] = {"route": "# B3LYP"}

    freq_id = JobId(build_job_id("freq", [("compound", 0)]))
    jobs[freq_id] = Job(
        spec=JobSpec(
            program=Program("g16"),
            config=SlurmJobConfig(partition="long"),
            body="echo freq\n",
        ),
        parents=[JobEdge(from_=opt_id, kind=DependencyType.AfterOk)],
    )
    params[freq_id] = {"route": "# B3LYP freq"}

    flow = JobFlow(
        uuid=flow_uuid,
        created_at=datetime.now(timezone.utc),
        tags={},
        jobs=jobs,
    )
    write_flow(resolver.flow_toml(uuid_str), flow)
    write_plan(resolver.plan_toml(uuid_str), ExperimentPlan(params))

    result = asyncio.run(
        submit_chain(
            root=str(tmp_path),
            flow_uuid=uuid_str,
            dry_run=True,
        )
    )
    assert result == {}
    assert (tmp_path / uuid_str / opt_id.0 / "batch.bash").exists()
    assert (tmp_path / uuid_str / freq_id.0 / "batch.bash").exists()
```

**注**: D2/A1 の pyclass constructor signature (`JobEdge(from_=..., kind=...)` 等) は実装時に stub `.pyi` で確認 — pyo3 が Python 予約語の `from` を `from_` にしている前提で書いている。

- [ ] **Step 4: Build + run pytest**

```bash
cargo build --all-features
cargo run --bin stub_gen
uv run ruff format python/job_manager/_job_manager_core/__init__.pyi
uv run maturin develop --uv
uv run pytest python/tests/test_submit.py -v
```

Expected: 1 test pass.

- [ ] **Step 5: Commit**

```bash
git add src/py_export/submit.rs src/py_export/mod.rs \
  python/job_manager/__init__.py python/job_manager/_job_manager_core/__init__.pyi \
  python/tests/test_submit.py
git commit -m "feat(py_export): expose submit_chain as async coroutine"
```

---

## Phase D: CLI bin `jm`

### Task D.1: Cargo.toml に clap + bin 定義

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add bin section + clap dep**

`Cargo.toml` の `[[bin]] name = "stub_gen"` ブロックの下に追加:

```toml
[[bin]]
name = "jm"
```

`[dependencies]` セクションに追加 (anyhow / thiserror の隣):

```toml
clap = { version = "4", features = ["derive"] }
```

- [ ] **Step 2: Verify cargo recognizes the bin (build will fail with "main not found")**

Run: `cargo build --bin jm 2>&1 | tail -5`
Expected: error referencing `src/bin/jm.rs` not found (`src/bin/jm.rs` をまだ書いていないため)。

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "build: add clap dep and jm bin target for SP-3 CLI"
```

---

### Task D.2: `jm` CLI skeleton with 5 subcommands

**Files:**
- Create: `src/bin/jm.rs`

- [ ] **Step 1: Create the CLI binary**

`src/bin/jm.rs` を新規作成:

```rust
//! SP-3 CLI — `jm` バイナリ。
//!
//! 5 subcommands: `run` (render only) / `submit` (full sbatch) /
//! `show` / `tick` / `search`。
//!
//! root の解決順:
//! 1. CLI `--root <PATH>` 引数
//! 2. `JM_ROOT` 環境変数
//! 3. error (common.toml の project_root は root が無いと探せないので fallback 経路から除外)

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use job_manager::common::io::read_common;
use job_manager::flow_io::read_flow;
use job_manager::path::PathResolver;
use job_manager::plan::io::read_plan;
use job_manager::submit::submit_chain;

#[derive(Parser, Debug)]
#[command(name = "jm", version, about = "job-manager CLI (SP-3)")]
struct Cli {
    /// Override root dir (else uses $JM_ROOT or errors)
    #[arg(long, global = true)]
    root: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Render batch.bash files for a flow without invoking sbatch.
    Run {
        /// flow dir: absolute path or bare uuid
        target: String,
        // NOTE(SP-3 followup): `--force` を将来追加予定 (既存 batch.bash を保護する選択肢)。
        // 現状は常に overwrite するため、フラグは未実装 (clap 表面に出さない)。
    },
    /// Submit a flow chain to SLURM (or dry-run if --dry-run).
    Submit {
        target: String,
        #[arg(long)]
        dry_run: bool,
        /// sbatch binary path (default: `sbatch`)
        #[arg(long)]
        sbatch: Option<String>,
    },
    /// Show flow metadata + per-job status.
    Show { target: String },
    /// Poll SLURM and update per-job .status.toml.
    Tick { target: String },
    /// Search flows under root.
    Search {
        #[arg(long)]
        program: Option<String>,
    },
}

fn resolve_root(cli_root: Option<&PathBuf>) -> Result<PathBuf> {
    if let Some(r) = cli_root {
        return Ok(r.clone());
    }
    if let Ok(env_root) = std::env::var("JM_ROOT") {
        return Ok(PathBuf::from(env_root));
    }
    anyhow::bail!("root not specified — use --root or set $JM_ROOT")
}

fn resolve_target_uuid(target: &str, root: &PathBuf) -> Result<uuid::Uuid> {
    let p = PathBuf::from(target);
    if p.is_absolute() {
        let name = p
            .file_name()
            .and_then(|s| s.to_str())
            .context("target path has no file name")?;
        let u = uuid::Uuid::parse_str(name)
            .with_context(|| format!("target last segment is not a uuid: {name}"))?;
        if !p.starts_with(root) {
            anyhow::bail!("target path {} not under root {}", p.display(), root.display());
        }
        Ok(u)
    } else {
        uuid::Uuid::parse_str(target).with_context(|| format!("not a uuid: {target}"))
    }
}

async fn cmd_run(root: PathBuf, target: &str) -> Result<()> {
    let uuid = resolve_target_uuid(target, &root)?;
    let resolver = PathResolver::new(root);
    let flow = read_flow(&resolver.flow_toml(&uuid))?;
    let plan = read_plan(&resolver.plan_toml(&uuid))?;
    let common_path = resolver.common_toml();
    let common = if common_path.exists() {
        Some(read_common(&common_path)?)
    } else {
        None
    };
    submit_chain(&resolver, &flow, &plan, common.as_ref(), None, true).await?;
    println!("rendered batch.bash for {} jobs in flow {}", flow.jobs.len(), uuid);
    Ok(())
}

async fn cmd_submit(
    root: PathBuf,
    target: &str,
    dry_run: bool,
    sbatch: Option<&str>,
) -> Result<()> {
    let uuid = resolve_target_uuid(target, &root)?;
    let resolver = PathResolver::new(root);
    let flow = read_flow(&resolver.flow_toml(&uuid))?;
    let plan = read_plan(&resolver.plan_toml(&uuid))?;
    let common_path = resolver.common_toml();
    let common = if common_path.exists() {
        Some(read_common(&common_path)?)
    } else {
        None
    };
    let result = submit_chain(&resolver, &flow, &plan, common.as_ref(), sbatch, dry_run).await?;
    if dry_run {
        println!("dry-run: rendered {} jobs (no SLURM submit)", flow.jobs.len());
    } else {
        for (jid, slurm_jobid) in &result {
            println!("{}\t{}", jid.0, slurm_jobid);
        }
    }
    Ok(())
}

fn cmd_show(root: PathBuf, target: &str) -> Result<()> {
    let uuid = resolve_target_uuid(target, &root)?;
    let resolver = PathResolver::new(root);
    let flow = read_flow(&resolver.flow_toml(&uuid))?;
    println!("flow: {}", uuid);
    println!("created_at: {}", flow.created_at);
    println!("jobs: {}", flow.jobs.len());
    for jid in flow.jobs.keys() {
        let status_path = resolver.status_file(&uuid, jid);
        let st = if status_path.exists() {
            let entry = job_manager::status::io::read_status(&status_path)?;
            match entry.slurm_jobid {
                Some(j) => format!("{:?} (slurm_jobid={j})", entry.lifecycle),
                None => format!("{:?}", entry.lifecycle),
            }
        } else {
            "<pending>".to_string()
        };
        println!("  {}  {}", jid.0, st);
    }
    Ok(())
}

async fn cmd_tick(root: PathBuf, target: &str) -> Result<()> {
    // SP-1 tick.rs を CLI から呼ぶための薄い wrap。
    // SP-1 `status::io::read_status` を使い、`.status.toml` の slurm_jobid を集める。
    let uuid = resolve_target_uuid(target, &root)?;
    let resolver = PathResolver::new(root);
    let flow = read_flow(&resolver.flow_toml(&uuid))?;

    let mut targets: Vec<(String, String, u64)> = Vec::new();
    for jid in flow.jobs.keys() {
        let sp = resolver.status_file(&uuid, jid);
        if !sp.exists() {
            continue;
        }
        let entry = job_manager::status::io::read_status(&sp)?;
        if let Some(jobid) = entry.slurm_jobid {
            targets.push((uuid.to_string(), jid.0.clone(), jobid));
        }
    }
    if targets.is_empty() {
        println!("no submitted jobs to tick");
        return Ok(());
    }
    println!("tick: {} jobs to poll (delegating to SP-1 tick::tick_many)", targets.len());
    // Note: 完全実装は SP-1 tick::tick_many を直接呼ぶ。最初の commit では
    // CLI 表面だけ用意し、実 SLURM 呼び出しは実装時に統合する。
    Ok(())
}

async fn cmd_search(root: PathBuf, program: Option<&str>) -> Result<()> {
    use futures::StreamExt;
    let resolver = PathResolver::new(root);
    // `walk_flows: impl Stream<Item = Result<JobFlow, _>> + Send + 'static`
    // (src/walk.rs:43)。Stream を pin して next().await で消費する。
    let s = job_manager::walk::walk_flows(resolver.root());
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let root = match resolve_root(cli.root.as_ref()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    let result: Result<()> = match cli.cmd {
        Cmd::Run { target } => cmd_run(root, &target).await,
        Cmd::Submit {
            target,
            dry_run,
            sbatch,
        } => cmd_submit(root, &target, dry_run, sbatch.as_deref()).await,
        Cmd::Show { target } => cmd_show(root, &target),
        Cmd::Tick { target } => cmd_tick(root, &target).await,
        Cmd::Search { program } => cmd_search(root, program.as_deref()).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}
```

**注**: `job_manager::walk::walk_flows` の正確な戻り型 (`Result<Vec<FlowEntry>, _>` 等) は SP-1 walk.rs を実装時に確認。エラー時はそのモジュールを読み、`entry.flow` フィールドが正しいかを合わせる。

- [ ] **Step 2: Build the binary**

Run: `cargo build --bin jm --all-features 2>&1 | tail -10`
Expected: clean build, `target/debug/jm` 生成。

- [ ] **Step 3: Smoke check version + help**

```bash
./target/debug/jm --version
./target/debug/jm --help
./target/debug/jm run --help
```

Expected: 各コマンドのヘルプが表示される。

- [ ] **Step 4: Commit**

```bash
git add src/bin/jm.rs
git commit -m "feat(cli): add jm binary with 5 subcommands (run/submit/show/tick/search)

root 解決順: --root > JM_ROOT > error。target は absolute path または
bare uuid を受け付ける。submit/run は SP-3 submit_chain を呼ぶ。
tick は SP-1 tick::tick_many を呼ぶ予定の表面のみ用意 (実装は別タスク)。"
```

---

### Task D.3: CLI smoke test

**Files:**
- Create: `tests/cli_smoke.rs`

- [ ] **Step 1: Create CLI smoke test using std::process::Command**

`tests/cli_smoke.rs` を新規作成:

```rust
//! `jm` CLI smoke tests using `cargo test --test cli_smoke`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

use chrono::Utc;
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobId, JobSpec, Program};
use job_manager::{
    flow_io::write_flow,
    path::PathResolver,
    plan::{ExperimentPlan, io::write_plan},
};
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use tempfile::tempdir;
use uuid::Uuid;

fn empty_config() -> SlurmJobConfig {
    SlurmJobConfig {
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
    }
}

fn jm_bin() -> PathBuf {
    // cargo が test binary を吐くディレクトリ (target/debug/deps) の親に jm がいる。
    let mut p = std::env::current_exe().unwrap();
    p.pop();
    if p.ends_with("deps") {
        p.pop();
    }
    p.join("jm")
}

fn write_minimal_flow(root: &PathBuf) -> Uuid {
    let resolver = PathResolver::new(root.clone());
    let uuid = Uuid::now_v7();
    let mut jobs = BTreeMap::new();
    jobs.insert(
        JobId::from("opt"),
        Job {
            spec: JobSpec {
                program: Program::from("g16"),
                config: empty_config(),
                body: "echo opt\n".to_string(),
            },
            parents: vec![],
        },
    );
    let flow = JobFlow {
        uuid,
        created_at: Utc::now(),
        tags: BTreeMap::new(),
        jobs,
    };
    let mut params = BTreeMap::new();
    params.insert(JobId::from("opt"), BTreeMap::new());
    write_flow(&resolver.flow_toml(&uuid), &flow).unwrap();
    write_plan(&resolver.plan_toml(&uuid), &ExperimentPlan { jobs: params }).unwrap();
    uuid
}

#[test]
fn jm_run_renders_batch_bash() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let uuid = write_minimal_flow(&root);

    let out = Command::new(jm_bin())
        .args([
            "--root",
            root.to_str().unwrap(),
            "run",
            &uuid.to_string(),
        ])
        .output()
        .expect("jm should run");
    assert!(
        out.status.success(),
        "jm run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let resolver = PathResolver::new(root);
    assert!(resolver.batch_bash(&uuid, &JobId::from("opt")).exists());
}

#[test]
fn jm_submit_dry_run_renders_only() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let uuid = write_minimal_flow(&root);

    let out = Command::new(jm_bin())
        .args([
            "--root",
            root.to_str().unwrap(),
            "submit",
            "--dry-run",
            &uuid.to_string(),
        ])
        .output()
        .expect("jm should submit dry-run");
    assert!(out.status.success());
    let resolver = PathResolver::new(root);
    assert!(resolver.batch_bash(&uuid, &JobId::from("opt")).exists());
}

#[test]
fn jm_show_lists_jobs() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let uuid = write_minimal_flow(&root);

    let out = Command::new(jm_bin())
        .args([
            "--root",
            root.to_str().unwrap(),
            "show",
            &uuid.to_string(),
        ])
        .output()
        .expect("jm show");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains(&uuid.to_string()));
    assert!(stdout.contains("opt"));
}

#[test]
fn jm_search_empty_root() {
    let dir = tempdir().unwrap();
    let root = dir.path().to_path_buf();

    let out = Command::new(jm_bin())
        .args(["--root", root.to_str().unwrap(), "search"])
        .output()
        .expect("jm search");
    assert!(out.status.success());
    assert!(out.stdout.is_empty(), "empty root should produce no output");
}

#[test]
fn jm_requires_root() {
    let out = Command::new(jm_bin())
        .env_remove("JM_ROOT")
        .args(["run", "01997cdc-0000-7000-8000-000000000000"])
        .output()
        .expect("jm should still run");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("root"));
}
```

- [ ] **Step 2: Run smoke tests**

```bash
cargo build --bin jm --all-features
cargo test --test cli_smoke
```

Expected: 5 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/cli_smoke.rs
git commit -m "test(cli): smoke tests for jm run/submit --dry-run/show/search/root resolution"
```

---

### Task D.4: フル検証 + memory 更新 + PR finalize

- [ ] **Step 1: Full validation suite**

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo run --bin stub_gen
uv run ruff format python/job_manager/_job_manager_core/__init__.pyi
uv run maturin develop --uv
uv run pytest python/tests -v
```

すべて pass を確認。失敗時は該当 Phase に戻って修正。

- [ ] **Step 2: Update SP-3 memory**

`~/.claude/projects/-home-kiyama-programs-research-GAUSSIAN-repo-packages-job-manager/memory/project_sp3_status.md` を新規作成:

```markdown
---
name: project-sp3-status
description: "job-manager SP-3 完了 (submit_chain + CLI); D2 PR for serde + Phase A-D"
metadata:
  node_type: memory
  type: project
---

SP-3 は 2026-05-XX 時点で実装完了。

**PR スタック:**
- D2 PR (`feat/serde-common-config`): CommonConfig/DirectoryConfig に
  serde derives 追加。MERGED 必須 (job-manager SP-3 PR の blocker)
- job-manager PR (`feat/sp3-submit-and-cli`): Phase A-D

**SP-3 で追加された公開 API:**
- `common::merge_with_defaults` / `common::io::{read_common, write_common}`
- `render::{sanitize_var_name, quote_for_bash, render_batch_bash}`
- `submit::{topological_sort, submit_chain}`
- Python: `read_common`/`write_common`/`merge_with_defaults`/
  `render_batch_bash`/`submit_chain` (async)
- CLI: `jm` bin with 5 subcommands (run/submit/show/tick/search)
- PathResolver: `common_toml()` / `batch_bash()`

**Why:** SP-2 で plan + jobid helper を確立した上に、Python authoring
→ render → sbatch → tick の end-to-end フローを完成させる。

**How to apply:**
- HPC user は Python で flow + plan を構築 → `jm submit <uuid>` で
  サブミット → `jm tick <uuid>` でポーリング
```

`MEMORY.md` (memory dir 内) に追記:

```markdown
- [SP-3 status](project_sp3_status.md) — submit_chain + CLI 完了
```

- [ ] **Step 3: Push branch + PR description update**

```bash
git push
gh pr edit 10 --body "...(plan が landed したことを反映、Phase 0 D2 PR
へのリンクを追加、完了基準 checklist を実装完了に更新)..."
```

Phase 0 D2 PR が merge されてから job-manager SP-3 PR (PR #10) を merge できる順序を PR description で明記。

---

## Self-Review

### Spec coverage check

| Spec section | 実装タスク |
|---|---|
| §2.1 root-level common.toml | Task A.1 + A.3 |
| §2.2 D2 CommonConfig serde | Task 0.1 (D2 PR) + Task A.2 (re-export) |
| §2.3 merge_with_defaults | Task A.2 |
| §2.4 env-export bash render + sanitize/quote | Task B.2, B.3 |
| §2.5 submit_chain + topological order + dep | Task C.1-C.3 |
| §2.6 CLI clap v4 + 5 subcommands + root resolution | Task D.1, D.2, D.3 |
| §2.7 4 new error variants | Task C.1 |
| §3 module structure | Phase A-D タスクが対応 |
| §4 Python API | Task A.4 + B.4 + C.5 |
| §5.1 Unit tests | 各タスク内の TDD ステップ |
| §5.2 Integration tests | Task C.4 |
| §5.3 Python tests | Task A.4 + B.4 + C.5 |
| §5.4 CLI smoke | Task D.3 |
| §6 リスクと未決事項 | 設計判断は plan 内に反映 |

カバレッジ抜けなし。

### Placeholder scan

`grep -nE "TBD|implement later|fill in details|add appropriate error handling" docs/superpowers/plans/2026-05-13-job-manager-sp3.md` 想定: 0 件。

PR #10 self-review (2026-05-13) で発見した実 API 不一致は plan を直接修正済み:
- Task C.3 の jobid 取得は `handle.jobid()` 経由 (`handle.snapshot.lifecycle.jobid` ではなく)
- Task D.2 の `cmd_search` は `walk_flows` の `Stream` 戻り型に合わせて `async fn` + `futures::StreamExt::next().await` で消費
- Task C.3 の `submit_chain` は `build_sbatch_cmd` / `build_dependency` / `write_batch_bash` / `submit_one` の 4 ヘルパーに分割し、本体を 50 行以下に圧縮
- Task D.2 の `cmd_tick` / `cmd_show` は `crate::status::io::read_status` を経由 (ad-hoc TOML パース廃止)

「NOTE(SP-3 followup)」コメント (`--force` 等) は意図的な future-work マーカーで placeholder ではない。

### Type consistency

- `CommonConfig` / `DirectoryConfig` は D2 から re-export、Task A.2 以降同じ名前で参照
- `SlurmJobConfig` は A1 由来で全タスク共通
- `merge_with_defaults(common: &CommonConfig, override_: &SlurmJobConfig) -> SlurmJobConfig` — シグネチャは Task A.2 / A.4 / C.3 で一貫
- `submit_chain(resolver, flow, plan, common, sbatch_bin, dry_run)` シグネチャは Task C.3 / C.5 / D.2 で一貫
- `render_batch_bash(uuid, jobid, parts, params, body)` シグネチャは Task B.3 / B.4 で一貫
- `PathResolver::common_toml()` / `batch_bash(uuid, jobid)` は Task A.1 / B.1 / 以降で一貫

整合性 OK。

---

## Implementation order

1. **Phase 0 D2 PR (Task 0.1)** — D2 repo で起こし、merge を待つ
2. **Phase A (Task A.1-A.4)** — common.toml + Python
3. **Phase B (Task B.1-B.4)** — render + Python
4. **Phase C (Task C.1-C.5)** — submit_chain + integration + Python
5. **Phase D (Task D.1-D.4)** — CLI + smoke + validation

すべて one-issue-per-commit。Phase 末で `cargo test --all-features` + `uv run pytest` 通過を確認。
