# job-manager SP-3 (submit_chain + CLI) 設計 v1

- **Date**: 2026-05-13
- **Status**: Draft (SP-2 merged 後の brainstorm 出力、レビュー待ち)
- **Targets**: `crate::{common, render, submit}` + CLI bin `jm`
- **Subproject**: SP-3 of 3 — submit + bash render + CLI
- **References**:
  - SP-2 spec: `docs/superpowers/specs/2026-05-12-job-manager-sp2-design.md`
  - SP-2 status: memory `project-sp2-status` (PR #6 merged + followups PR #9 merged)
  - 上流 (D2): `../../../gaussian-job-shared2/` (newtype 不可侵)
  - 上流 (A1): `../../../slurm-async-runner2/` (`SbatchManager` / `SbatchCmd` 利用、struct 不可侵)

---

## 1. 背景

SP-1 (データ層) + SP-2 (plan + jobid helpers) が `develop` に landed。SP-3 は spec §12 で予告した 4 コンポーネントを実装する:

1. **`common.toml` 読み込み + `SlurmJobConfig` 合成** — 全 flow 共通の SLURM デフォルト
2. **`JobSpec.body` の bash render** — `plan.toml` params + `parse_job_id` axis_combo を環境変数として書き出した `batch.bash` 生成
3. **`submit_chain` 相当** — A1 `SbatchManager` 経由でトポロジカル順に sbatch、SLURM 依存を JobEdge から構築、status.toml 更新
4. **CLI** — `jm` バイナリ + 5 subcommands (`run` / `submit` / `show` / `tick` / `search`)

### 1.1 ユーザーフロー (想定)

```bash
# 1. flow 構築 (Python authoring、SP-2 §1.1 と同じ)
python build_my_experiment.py  # write_flow / write_plan を内部で呼ぶ

# 2. (dry-run) render batch.bash files
jm run /work/<flow_uuid>

# 3. submit to SLURM
jm submit /work/<flow_uuid>

# 4. poll status
jm tick /work/<flow_uuid>

# 5. inspect
jm show /work/<flow_uuid>

# 6. cross-flow search
jm search /work --program g16 --status Failed
```

### 1.2 SP-3 のスコープ

| 含める | 含めない |
|---|---|
| D2 `CommonConfig` の serde 対応 (Phase 0 D2 PR) + `crate::common` r/w + merge | grammar DSL (SP-2 で確定的に削除済み) |
| `crate::render::render_batch_bash` | post.bash 自動生成 (ユーザー責務) |
| `crate::submit::submit_chain` | 並列 sbatch (順次のみ、依存解決のため) |
| `PathResolver::common_toml()` / `batch_bash()` getter | `<flow_dir>/common.toml` (root 専有とする) |
| CLI `jm`: `run` / `submit` / `show` / `tick` / `search` | TUI / ncurses 等のインタラクティブ UI |
| Python pyfunctions (common r/w, render, submit) | Python 側からの CLI 起動 (`jm` 直接使う) |

---

## 2. 採用アプローチと設計判断

### 2.1 `common.toml` の場所 — **root-level** に置く

```
<root>/common.toml          ← cluster-wide defaults (本 SP-3)
<root>/<flow_uuid>/flow.toml
<root>/<flow_uuid>/plan.toml
<root>/<flow_uuid>/<JobId>/batch.bash   ← 本 SP-3
<root>/<flow_uuid>/<JobId>/.status.toml ← SP-1 既存
```

**理由:**
- D2 `CommonConfig.directories.project_root` は「全プロジェクトデータの root」と命名されており、root-level が一致
- per-flow にすると HPC ユーザーは同じ設定を flow 毎にコピーすることになる
- absent でも動く (optional)

### 2.2 `common.toml` schema — **D2 `CommonConfig` を直接 import**

D2 `gaussian_job_shared::config::common::CommonConfig` は多フィールド struct (newtype ではない) のため、SP-3 Phase 0 の D2 PR で serde derives を追加し job-manager から直接使う。job-manager 側でラッパや mirror struct を作らない (`feedback-use-shared-package-definitions` 参照: newtype は不可侵だが struct は D2 内で restructurable)。

**D2 側の変更 (SP-3 Phase 0 PR):**

```rust
// gaussian-job-shared2/src/config/common.rs
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
```

(`#[derive(Serialize, Deserialize)]` + `#[serde(deny_unknown_fields)]` を両 struct に追加するだけ。フィールド追加削除なし。)

**job-manager 側 (本 SP-3):**

```rust
// src/common/io.rs (新規) — struct 定義は持たず I/O だけ
use gaussian_job_shared::config::common::CommonConfig;

#[must_use = "read_common returns the parsed CommonConfig; ignoring it drops the data"]
pub fn read_common(path: &Path) -> Result<CommonConfig, JobManagerError> { ... }

pub fn write_common(path: &Path, common: &CommonConfig) -> Result<(), JobManagerError> { ... }

pub fn merge_with_defaults(
    common: &CommonConfig,
    override_: &SlurmJobConfig,
) -> SlurmJobConfig { ... }
```

**TOML 例:**

```toml
[slurm_default]
partition = "long"
# time_limit / job_name / array_spec / ... は per-job override 前提なので common では空

[directories]
project_root = "/work"

[slurm_default.resource_spec]
# 任意
```

`directories.project_root` は CLI の `--root` フラグ / `JM_ROOT` env var が未指定の場合のフォールバックとして利用 (§2.6 参照)。

### 2.3 SlurmJobConfig 合成のセマンティクス

`merge_with_defaults(common: &CommonConfig, override_: &SlurmJobConfig) -> SlurmJobConfig`:

- **`Option<T>` フィールド**: `override.field.clone().or(common.slurm_default.field.clone())`
- **必須フィールド (`partition: String` 等)**: `override` が空でない限り `override` を採用。`override.partition.is_empty()` の場合だけ `common` から補完
- **戻り値は新規 `SlurmJobConfig`** (immutability rule、引数は consume せず borrow)

A1 `SlurmJobConfig` の `Option<T>` 揃え (time_limit, log_stdout, log_stderr, comment, job_name, array_spec, dependency, mail_user, mail_types, resource_spec) と非 Option (`partition`) の 2 系統だけ抽出して扱う。**A1 は不可侵**なので `partition` の `Option<String>` 化はしない (empty-string fallback で対応)。

### 2.4 bash render — **env-export 方式**

`batch.bash` 構造:

```bash
#!/bin/bash
# Generated by job_manager SP-3. Do not edit; regenerated on every `jm run`.

# --- job-manager runtime context ---
export JM_FLOW_UUID='01997cdc-...'
export JM_JOB_ID='opt__compound=0__method=0'
export JM_AXIS_COMPOUND='0'
export JM_AXIS_METHOD='0'

# --- plan.toml params ---
export JM_PARAM_ROUTE='# B3LYP/6-31G* opt'
export JM_PARAM_COMPOUND='benzene'
export JM_PARAM_NPROC='16'

# --- user body (JobSpec.body) ---
<JobSpec.body verbatim>
```

**採用理由:**
- bash 自身が変数展開を担うので Rust 側にテンプレートエンジン不要 (YAGNI)
- bash injection 耐性: 値は single-quote で囲み、内部の `'` を `'\''` でエスケープ
- ユーザーは `${JM_PARAM_ROUTE}` を body 内で参照するだけ — 学習コスト最小
- `#SBATCH` directives は **batch.bash に書かない** (A1 `SbatchCmd` CLI 引数で渡す)

**Variable naming 規約:**

| 入力 | 出力 |
|---|---|
| flow_uuid | `JM_FLOW_UUID` |
| job_id | `JM_JOB_ID` |
| axis_combo `[("compound", 0)]` | `JM_AXIS_COMPOUND=0` (axis 名を ASCII upper-case + non-`[A-Z0-9_]` を `_` 置換) |
| plan params `{"route": "X"}` | `JM_PARAM_ROUTE='X'` (同上で sanitize) |

衝突回避: 既に `validate_job_id` / `validate_step_id` で axis 名と step_id を sanitize 済み (`[A-Za-z0-9_-]+`)。upper-case 化のみで衝突は起きない。

### 2.5 `submit_chain` — トポロジカル順 + 依存解決

```rust
pub async fn submit_chain(
    resolver: &PathResolver,
    flow: &JobFlow,
    plan: &ExperimentPlan,
    common: Option<&CommonConfig>,
    sbatch_bin: Option<&str>,  // None なら "sbatch"
    dry_run: bool,
) -> Result<BTreeMap<JobId, u64>, JobManagerError>;
```

**処理:**

1. JobFlow.jobs を `JobEdge[]` で形成される DAG とみなしトポロジカル sort
2. 各 JobId を順に処理:
   a. effective_config = `merge_with_defaults(common, &job.spec.config)`
   b. SLURM 依存 = `JobEdge` を「parent JobId → 既に sbatch 済みの SLURM jobid + DependencyType」に書き換え、`SlurmDependency` に集約
   c. `render_batch_bash` で `<job_dir>/batch.bash` を atomic 書き込み
   d. `SbatchCmd` を組み立て (script = batch.bash, partition/dep/etc. = effective_config), `SbatchManager.spawn().await` で submit
   e. SLURM jobid を取得し、`.status.toml` を `Queued` + `slurm_jobid` で書く (SP-1 status r/w 利用)
   f. map に `(JobId, SLURM_jobid)` を蓄積
3. dry_run = true なら d-f を skip し、c までで終了 (render only)
4. 任意の step でエラー → 早期 return (downstream 未処理のまま)。caller が再実行で resume 可能

**循環検出**: JobEdge は SP-1 で DAG 前提 (cycle なし)。SP-3 では submit 直前にトポロジカル sort 時 cycle 検出 → エラー variant 追加。

**並列性**: 順次に保つ (依存先 SLURM jobid が submit 前に知れないため)。同じレベルの並列 sbatch は将来検討。

### 2.6 CLI — `jm` バイナリ + clap v4

```
src/bin/jm.rs  ← 新規 (tokio::main)
```

Cargo.toml に:
```toml
[[bin]]
name = "jm"

[dependencies]
clap = { version = "4", features = ["derive"] }
```

**subcommand 詳細:**

| cmd | 引数 | 動作 |
|---|---|---|
| `run <flow_dir> [--force]` | flow_dir = `<root>/<uuid>` または `<uuid>` (root は env / config から) | flow.toml + plan.toml + common.toml を読み、各 job の batch.bash を render。既存ファイルは `--force` で上書き |
| `submit <flow_dir> [--dry-run] [--sbatch <bin>]` | 同上 | `submit_chain` を呼ぶ。`--dry-run` で render のみ |
| `show <flow_dir>` | 同上 | flow メタデータ + 各 job の状態 (`.status.toml` 読みつつ) を tabular で出力 |
| `tick <flow_dir>` | 同上 | SP-1 `tick.rs` 経由で SLURM 状態 query + `.status.toml` 更新 |
| `search <root> [--program X] [--status Y] ...` | root = absolute path | SP-1 `walk_flows` + `SearchFilter` 経由で検索結果を出力 |

`<flow_dir>` argument は 2 形式を accept:
- absolute path (`/work/01997cdc-...`) — root を path から逆算
- uuid-only — `--root` または `JM_ROOT` env var を使う

### 2.7 エラー型拡張

`error.rs` に variant 追加:

```rust
#[error("dependency cycle detected in flow {flow}")]
DependencyCycle { flow: uuid::Uuid },

#[error("missing plan entry for job {job} in flow {flow}")]
MissingPlanEntry { flow: uuid::Uuid, job: gaussian_job_shared::entities::workflow::JobId },

#[error("sbatch submission failed for job {job}: {source}")]
SubmitFailed { job: gaussian_job_shared::entities::workflow::JobId, #[source] source: anyhow::Error },

#[error("bash render failed for job {job}: {reason}")]
RenderError { job: gaussian_job_shared::entities::workflow::JobId, reason: String },
```

---

## 3. モジュール構成

```
src/
├── common/
│   ├── mod.rs            # CREATE: re-export D2 CommonConfig + merge_with_defaults
│   └── io.rs             # CREATE: read_common / write_common (atomic, optional)
├── render/
│   └── mod.rs            # CREATE: render_batch_bash, sanitize_var_name, quote_for_bash
├── submit/
│   └── mod.rs            # CREATE: submit_chain + topological_sort + dep_resolution
├── bin/
│   └── jm.rs             # CREATE: clap CLI + 5 subcommands
├── path.rs               # MODIFY: PathResolver::common_toml() + batch_bash() getter
├── error.rs              # MODIFY: 上記 4 variants 追加
└── py_export/
    ├── common.rs         # CREATE: read_common / write_common pyfunctions
    ├── render.rs         # CREATE: render_batch_bash pyfunction (offline test 用)
    ├── submit.rs         # CREATE: submit_chain pyfunction (tokio runtime 必要)
    └── mod.rs            # MODIFY: 上記 module を pub + pymodule_export
```

`PathResolver` 拡張:
```rust
pub fn common_toml(&self) -> PathBuf { self.root.join("common.toml") }
pub fn batch_bash(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf {
    self.job_dir(flow_uuid, job_id).join("batch.bash")
}
```

---

## 4. Python API

```python
from job_manager import (
    # SP-3
    read_common, write_common,
    render_batch_bash,
    submit_chain,
    # SP-1/SP-2 既存
    PathResolver, ExperimentPlan,
    write_flow, write_plan,
    build_job_id, parse_job_id, validate_step_id,
)
from gaussian_job_shared import CommonConfig, DirectoryConfig

# common 作成 (D2 の型を直接構築)
common = CommonConfig(
    slurm_default={"partition": "long", ...},
    directories=DirectoryConfig(project_root="/work"),
)
resolver = PathResolver("/work")
write_common(resolver.common_toml(), common)

# submit (async)
import asyncio
jobids = asyncio.run(submit_chain(resolver, flow, plan, common=common))
print(jobids)  # {JobId("opt__c=0__m=0"): 12345, ...}
```

Python authoring がメインで CLI はオプション (HPC ユーザーが手動で submit する時用)。

---

## 5. テスト計画

### 5.1 Unit (Rust)

- `common`: serde round-trip / merge_with_defaults (option / required / mixed)
- `render`: golden file テスト (固定 axis_combo + params → 期待 bash 文字列)
- `render`: bash quoting (single-quote 内の `'` エスケープ、改行)
- `submit`: トポロジカル sort + cycle 検出
- `submit`: mock SbatchManager (custom dispatcher) で sbatch を捏造、戻りの SLURM jobid 検証
- `submit`: 1 ジョブ失敗 → downstream 未処理を assert

### 5.2 Integration (Rust)

- `tests/integration_sp3.rs`:
  - 12-job sample (SP-2 と同じ pattern) → render 全 job → batch.bash 内容を spot-check
  - dry_run submit → render done, sbatch 呼び出し 0 回
  - mock submit → topological order どおりに SLURM jobid が払い出される

### 5.3 Python (`python/tests/test_submit.py` 等)

- common round-trip
- render の Python E2E
- submit_chain の dry_run

### 5.4 CLI (`tests/cli_smoke.rs`)

- `jm run` を tempdir + 既存 flow.toml/plan.toml 上で実行 → batch.bash が生成されること
- `jm show` の出力に flow uuid と各 job が含まれること
- `jm submit --dry-run` が batch.bash を生成するが sbatch を呼ばないこと
- `jm search` で空 root → empty
- 実 SLURM は CI 不可なので `--sbatch` で fake binary を渡してテスト

### 5.5 カバレッジ目標

`cargo llvm-cov --fail-under-lines 80`。

---

## 6. リスクと未決事項

| 項目 | リスク | 対応 |
|---|---|---|
| SlurmJobConfig 合成 | `partition: String` で `""` と「未指定」が区別不能 | A1 不可侵のため `is_empty()` fallback で対応。本格的に解消するには A1 側で `Option<String>` 化必要 (本 SP-3 スコープ外) |
| D2 PR merge 順 | Phase 0 (D2 serde derives) が先に merge されないと SP-3 がビルドできない | PR 順序: D2 PR → job-manager SP-3 PR。CI で path 依存を解決 (D2 path = `../gaussian-job-shared2`) |
| `directories` フィールド | D2 `DirectoryConfig.project_root` と `PathResolver.root` が二重管理になる | CLI の root 解決優先順: `--root` arg > `JM_ROOT` env > `common.directories.project_root`。`PathResolver::new` は確定値を受け取るだけ |
| bash injection | plan params に shell metachar が混入 | single-quote + `'\''` エスケープのみで防御 (POSIX) |
| 並列 sbatch | 依存解決に SLURM jobid が必要 → 順次のみ | SP-3 では順次。将来「ルートだけ並列」等を検討 |
| CLI の root 解決 | absolute path / uuid-only / env var / common.toml の優先順位 | CLI 引数 > `JM_ROOT` env > `common.directories.project_root` > error。一意化 |
| dependency 型変換 | A1 `SlurmDependency` は `to_string()` で `afterok:1,afterany:2` を期待 | `JobEdge` の `kind: DependencyType` から `SlurmDependency` を構築するヘルパーを `submit` 内 private に持つ |
| post.bash の自動生成 | spec で「Done は post.bash の専権」(SP-1) と書かれているが SP-3 で post.bash を render するか? | **しない**。`JobSpec.body` 自体が post.bash 相当を含む / 含めないかはユーザー責務 |

---

## 7. 実装フェーズ (plan へ展開する分割)

```
Phase 0 (D2 PR): CommonConfig + DirectoryConfig に serde derives 追加
   ↓
Phase A: common.toml (io + merge_with_defaults + PathResolver::common_toml)
   ↓
Phase B: bash render (sanitize + quote + render_batch_bash + PathResolver::batch_bash)
   ↓
Phase C: submit_chain (topo sort + dep resolution + SbatchManager wiring)
   ↓
Phase D: CLI bin (clap + 5 subcommands + smoke tests)
```

**Phase 0 (D2 PR) は SP-2 の `JobFlow.work_dir` 撤廃 PR と同じパターン:**
job-manager の SP-3 PR を blocker にする小さな D2 PR を先に出して merge する。D2 側変更は serde derives + `#[serde(deny_unknown_fields)]` の追加のみで、既存 D2 consumer に互換性破壊なし。

各 Phase は単独で testable / commit 可能。Phase A-C は Python API も同時に exposed。Phase D は CLI 専用。

---

## 8. 完了基準

- [ ] **Phase 0 D2 PR merged** (`CommonConfig` + `DirectoryConfig` に serde derives)
- [ ] `cargo build --all-features` 成功 + `jm` binary 生成
- [ ] `cargo test --all-features` 成功 (新規 50+ テスト)
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` クリーン
- [ ] `cargo fmt --check` クリーン
- [ ] `uv run maturin develop --uv` 成功
- [ ] `uv run pytest python/tests` 全 PASS
- [ ] `jm run` / `jm submit --dry-run` が tempdir で動作
- [ ] **SP-3 (job-manager) PR で A1 への変更ゼロ** (SLURM 不可侵)
- [ ] **D2 への変更は Phase 0 PR のみ** (serde derives 追加だけ、newtype 不可侵)
- [ ] 各 Phase で one-issue-per-commit (Conventional Commits)

---

## 9. 次工程

承認後:
1. `writing-plans` skill で plan v1 を生成 (`docs/superpowers/plans/2026-05-13-job-manager-sp3.md`)
2. branch `feat/sp3-submit-and-cli` を `develop` から切る
3. Phase A から one-issue-per-commit で実装、各 Phase 末で PR review (subagent or self)
4. 全 Phase 完了で PR を `develop` に
