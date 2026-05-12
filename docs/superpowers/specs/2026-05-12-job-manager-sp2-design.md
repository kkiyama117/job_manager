# job-manager SP-2 (plan + helpers) 設計 v5

- **Date**: 2026-05-12 (v1→v5 改訂)
- **Status**: Draft (brainstorming 完了、レビュー待ち)
- **Targets**: `crate::plan::*` + `crate::jobid::*` (Rust) / `job_manager._job_manager_core.{plan, jobid}` (Python)
- **Subproject**: SP-2 of 3 — per-job params sidecar + JobId helpers
- **References**:
  - SP-1 spec: `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`
  - 上流 (D2): `../../../gaussian-job-shared2/` — **`JobFlow.work_dir` フィールドのみ撤廃** (本 spec で要 D2 PR)
  - 上流 (A1): `../../../slurm-async-runner2/` — 不可侵

---

## v5 改訂サマリ — **experiment.toml DSL 撤廃**

ユーザー問いかけ「WHY NOT using experiment.toml? is it really needed?」を受け、grammar DSL の必要性を再評価:

**結論:** Rust 側に DSL を実装しない。`experiment.toml` 形式は SP-2 のスコープから外す。

### 理由

| DSL 機能 | Python 代替 | コメント |
|---|---|---|
| `[[axis]]` sweep 軸宣言 | Python list + `itertools.product` | Python の方が表現力豊か (任意の iterable / 条件分岐 / 計算済み値) |
| `${axis.field}` placeholder | Python f-string | `f"# {method['route']}/6-31G*"` — 直感的、型ロスなし |
| `parents = [{fanout=true}]` | Python で `JobEdge` を直接構築 | parent 解決ロジックを Python で書く方が透明 |
| `[[step]] sweep=[...]` | `for c, m in product(compounds, methods)` | Python ループの方が flexible |
| Legacy 形状検出 | (不要) | DSL が無いので legacy も無い |

### 削減効果

| 項目 | v4 (DSL あり) | v5 (DSL なし) |
|---|---|---|
| Rust モジュール | `grammar/{source, reader, placeholder, sweep, jobid, chain, build, mod}` | `jobid.rs` のみ |
| 行数 (推定) | 1500+ 行 (test 含まず) | 250 行 |
| エラーバリアント | 28 種 (Grammar*/Legacy/Placeholder/Dag) | 4 種 (JobId* のみ) |
| Python API | `expand_experiment` + helpers | `validate_*` / `build_job_id` / `parse_job_id` |
| fixture | 11 種 (`tests/fixtures/experiment/*.toml`) | 0 種 (struct round-trip は in-memory) |
| 学習コスト | DSL 仕様を覚える | Python を書く (既知) |

### v4 から残す決定

- D2 newtype は保持 (`JobId`/`Program`/`CalcType`/`Job`/`JobEdge`/`JobSpec`)
- `JobFlow.work_dir` 撤廃 (Phase 0 D2 PR)
- SP-1 follow-up で `flow.work_dir` 参照を `PathResolver::flow_dir(&uuid)` に置換 (Phase 1)
- JobId 命名規約 `<step_id>__<axis>=<idx>__...` (Python ユーザーが採用する規約。Rust の helper で構築・検証・パース)
- `ExperimentPlan` sidecar (`plan.toml`) で per-job params を保持 (SP-3 が bash 本体 render で使う)
- `PathResolver::plan_toml(&uuid)` / `experiment_toml(&uuid)` getter

### v4 から削る決定 (案 B 採用に伴う)

- `experiment.toml` schema 仕様 (§4) → 削除
- `${...}` placeholder 文法 (§4.5) → 削除
- Sweep 展開アルゴリズム (§6) → 削除
- Parent 解決セマンティクス (§7) → 削除
- Legacy 形状検出 (8 種) → 削除
- `expand_experiment` 公開 API → 削除
- `crate::grammar` モジュール全体 → 削除 (jobid 機能は `crate::jobid` に再配置)

---

## 1. 背景

SP-1 で確立した「データ層 + 並列走査 + tick」基盤の上に、SP-3 (submit + CLI) が必要とする最小限の追加機能だけを実装する:

1. **per-job params の永続化** (`plan.toml` sidecar) — SP-3 が bash 本体を render する時の入力
2. **JobId helpers** — search や Python authoring で必要な validate / build / parse 関数
3. **PathResolver の getter 追加** — `plan.toml` / `experiment.toml`-equivalent の path 解決

### 1.1 ユーザーの authoring 体験 (Python)

ユーザーは Python で直接 `JobFlow` と `ExperimentPlan` を構築する:

```python
from itertools import product
from uuid import uuid7
from datetime import datetime, timezone

from gaussian_job_shared import JobFlow, JobId, Job, JobSpec, Program, JobEdge
from slurm_async_runner import DependencyType, SlurmJobConfig
from job_manager import (
    ExperimentPlan, PathResolver,
    write_flow, write_plan,
    build_job_id, parse_job_id, validate_step_id,
)

# 軸定義 (Python のリストで十分)
compounds = ["benzene", "toluene", "p-xylene"]
methods = [{"name": "b3lyp", "route": "B3LYP"}, {"name": "m062x", "route": "M06-2X"}]

validate_step_id("opt")
validate_step_id("freq")

jobs: dict[JobId, Job] = {}
params: dict[JobId, dict] = {}

# Sweep 展開 (itertools.product) と placeholder 展開 (f-string)
for (i, c), (j, m) in product(enumerate(compounds), enumerate(methods)):
    opt_id = JobId(build_job_id("opt", [("compound", i), ("method", j)]))
    jobs[opt_id] = Job(
        spec=JobSpec(program=Program("g16"), config=SlurmJobConfig.default(), body=""),
        parents=[],
    )
    params[opt_id] = {
        "route": f"# {m['route']}/6-31G* opt",
        "compound": c,
        "nproc": 16,
    }

    # pair_by_axes (opt → freq): 同じ axis_combo を共有
    freq_id = JobId(build_job_id("freq", [("compound", i), ("method", j)]))
    jobs[freq_id] = Job(
        spec=JobSpec(program=Program("g16"), config=SlurmJobConfig.default(), body=""),
        parents=[JobEdge(from_=opt_id, kind=DependencyType.AfterOk)],
    )
    params[freq_id] = {
        "route": f"# {m['route']}/6-31G* freq",
        "compound": c,
        "nproc": 16,
    }

flow = JobFlow(
    uuid=uuid7(),
    created_at=datetime.now(timezone.utc),
    tags={"calc_type": "opt+freq", "project": "tddft"},
    jobs=jobs,
)
plan = ExperimentPlan(jobs=params)

resolver = PathResolver("/work_dir")
write_flow(resolver.flow_toml(flow.uuid), flow)
write_plan(resolver.plan_toml(flow.uuid), plan)
```

**Python authoring の利点:**
- itertools / list 内包表記 / f-string が使える
- 条件分岐 (例: 特定の compound だけ method を限定) が自然
- 既存 Python ライブラリ (pandas, numpy 等) と統合可能
- 型ヒントが効く (`mypy` / `pyright`)

**`fanout` / `reduce_over` 相当のパターン:**

```python
# fanout: 1 preflight job, then N opt jobs each depending on preflight
preflight_id = JobId("preflight")
jobs[preflight_id] = Job(spec=..., parents=[])

for (i, c), (j, m) in product(enumerate(compounds), enumerate(methods)):
    opt_id = JobId(build_job_id("opt", [("compound", i), ("method", j)]))
    jobs[opt_id] = Job(
        spec=...,
        parents=[JobEdge(from_=preflight_id, kind=DependencyType.AfterOk)],
    )

# reduce_over: M scan jobs reduce to 1 compare job
scan_ids = [...]  # all scan__theta=k jobs
for c_idx, c in enumerate(compounds):
    compare_id = JobId(build_job_id("compare", [("compound", c_idx)]))
    jobs[compare_id] = Job(
        spec=...,
        parents=[JobEdge(from_=sid, kind=DependencyType.AfterOk)
                 for sid in scan_ids
                 if parse_job_id(str(sid))["axis_combo"][0] == ("compound", c_idx)],
    )
```

これらは Python の通常のロジックで書ける。Rust grammar 層は不要。

### 1.2 SP-2 のスコープ

| 含める | 含めない |
|---|---|
| `crate::plan::ExperimentPlan` + atomic I/O | `experiment.toml` schema parser |
| `crate::jobid::{validate_step_id, validate_job_id, build_job_id, parse_job_id, JobIdParts}` | sweep / placeholder / parent 解決 DSL |
| `PathResolver::plan_toml() / experiment_toml()` getter | `expand_experiment` 公開関数 |
| Python pyfunctions / pyclass の re-export | `gaussian_batch.toml` legacy 互換 |
| **D2 `JobFlow.work_dir` 撤廃 (Phase 0)** | bash 本体 render (SP-3) |
| **SP-1 follow-up: work_dir 参照置換 (Phase 1)** | sbatch 投入 (SP-3) |
| | CLI コマンド (SP-3) |

### 1.3 サブプロジェクト位置付け

```
SP-1 (データ層, 完, follow-up PR で work_dir 撤廃)
       ↓
D2 PR (JobFlow.work_dir 撤廃のみ)
       ↓
       SP-2 (plan + jobid helpers, 本 spec)   ←── SP-3 (submit + CLI)
```

---

## 2. 採用アプローチ

### 2.1 比較した 3 案 (再掲、案 B を採用)

| | A: Rust DSL (v4) | **B: Python authoring (v5 採用)** | C: Python 完全委譲 |
|---|---|---|---|
| experiment.toml | Rust が parse | 不要 | 既存 Python tool が parse |
| sweep / placeholder | Rust 純粋関数 | Python (itertools, f-string) | Python tool |
| Rust SP-2 規模 | 大 (1500+ 行) | 小 (250 行) | 極小 (100 行) |
| ユーザーの authoring | TOML DSL | Python | TOML (Python tool 経由) |
| 学習コスト | DSL 仕様 | Python (既知) | TOML + Python tool |
| 拡張性 | DSL 拡張が必要 | Python の任意の機能 | Python tool 改変 |

**B を採用した理由:**
- DSL は declarative で簡潔だが、Rust 実装のメンテ負担に見合わない
- Python は表現力が高く、ユーザーは既に Python を書ける (SP-1 / SP-3 の Python API を使う)
- C は既存 `gaussian-experiment-manager` (legacy) に依存し続けることになり、新規プロジェクトの足かせ
- B は Rust SP-2 を小さく保ち、Python 側で UX を作りやすい (ヘルパー関数を Python で書ける)

### 2.2 案 B の設計判断

- **`JobFlow` / `ExperimentPlan` を最終的な authoring 形式とする** — ユーザーは Python でこれらを構築
- **Rust helper を最小化** — `JobId` 命名規約の helper だけ Rust で提供 (validation, build, parse)
- **TOML 永続化は struct の serde 直接** — `flow.toml` (D2 JobFlow) / `plan.toml` (ExperimentPlan)
- **D2 newtype を不可侵に扱う** — `JobId`/`Program`/`CalcType`/`Job`/`JobEdge`/`JobSpec` を import
- **`JobFlow.work_dir` のみ撤廃** — `<root>/<uuid>/` で導出 (Phase 0)

### 2.3 TOML ファイルと Rust 型の対応関係

SP-2 v5 では TOML は **2 種類のみ**:

| TOML | 役割 | 対応 Rust 型 | 結合方式 |
|---|---|---|---|
| `flow.toml` | JobFlow 永続化 | D2 `JobFlow` | serde 直 round-trip (`#[serde(deny_unknown_fields)]`) |
| `plan.toml` | per-job params 永続化 | `crate::plan::ExperimentPlan` | serde 直 round-trip |

両方とも struct の serde mirror なので、**fixture file は不要** (in-memory tempdir で round-trip テスト)。

`experiment.toml` という名前のファイルは SP-2 では **作らない・読まない**。`PathResolver::experiment_toml(&uuid)` getter は将来用にエクスポートするが、SP-2 では使わない。

---

## 3. 必須 D2 変更 (`JobFlow.work_dir` 撤廃のみ)

### 3.1 現状

```rust
pub struct JobFlow {
    pub uuid: Uuid,
    pub created_at: DateTime<Utc>,
    pub work_dir: PathBuf,                       // ← 撤廃
    pub tags: BTreeMap<String, String>,
    pub jobs: BTreeMap<JobId, Job>,
}
```

### 3.2 v4 (本 spec で要 D2 PR)

```rust
pub struct JobFlow {
    pub uuid: Uuid,
    pub created_at: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
    pub jobs: BTreeMap<JobId, Job>,
}
```

### 3.3 撤廃理由

- 永続化値が `<root>/<uuid>/` 規約から導出可能 (redundant)
- `mv` 等のディレクトリ移動で drift するリスク
- `JobFlow` を run/search に純粋なメタデータコンテナとして整理 (場所情報は PathResolver に集約)

### 3.4 影響範囲

- D2 (`gaussian-job-shared2`): struct 定義、tests、pyclass getter/setter
- job-manager (SP-1 既 merged): `crate::view::CalcView` 等で `flow.work_dir` を参照 → `PathResolver::flow_dir(&flow.uuid)` (follow-up PR)
- job-manager (SP-2 本 spec): 影響なし (元々 work_dir を直接参照しない)

### 3.5 newtype は撤廃しない

`JobId` / `Program` / `CalcType` は D2 の正典定義を import して使う。job-manager 側で再定義 / 別名作成は行わない。

---

## 4. JobId 命名規約 (Python authoring 用)

### 4.1 形式

```
<step_id>                                  # sweep 空のとき
<step_id>__<axis1>=<idx>__<axis2>=<idx>    # sweep のとき
```

例:
- `opt`
- `opt__compound=0__method=0`
- `freq__compound=2__method=1`

### 4.2 文字種・予約名

- 許可文字: `[A-Za-z0-9_\-=]+`
- step_id 自体の許可文字: `[A-Za-z0-9_\-]+` (`=` は予約)
- 予約 JobId: `flow`, `plan`, `experiment`, `derived`, `status`

これらの規約は **Python ユーザーが採用する慣用**であり、D2 の `JobId(pub String)` 自体は文字種制約を持たない。ユーザーは `build_job_id` / `validate_step_id` / `parse_job_id` ヘルパーを使って規約に従う。

### 4.3 ヘルパー API

```rust
/// step_id の検証 (`[A-Za-z0-9_-]+`、予約名禁止)。OK なら入力を返す。
pub fn validate_step_id(s: &str) -> Result<&str, JobManagerError>;

/// JobId 全体の検証 (文字種 + 予約名 + sweep encoding を含めた整合性)
pub fn validate_job_id(s: &str) -> Result<&str, JobManagerError>;

/// JobId 文字列を組み立てる (D2 newtype 包装は呼び側 `JobId::from(...)`)
pub fn build_job_id(source_step_id: &str, axis_combo: &[(&str, usize)]) -> String;

/// 借用ベース parse (alloc なし)。`&job_id.0` または string literal を渡す。
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>;

pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,
}
```

### 4.4 SP-3 / search での使い方

- **search** (SP-1 既存): `flow.jobs[*].spec.program` で program 絞り込み。step_id / axis_combo を絞りたい時は `parse_job_id(&jid.0)` で分解
- **SP-3 bash render**: `plan.jobs[&job_id]` で params を取得し、`parse_job_id` で `axis_combo` の値を参照 (compound 名等を bash header に含めたい場合)

---

## 5. `ExperimentPlan` 仕様

### 5.1 構造

```rust
use gaussian_job_shared::entities::workflow::JobId;
use std::collections::BTreeMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentPlan {
    pub jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>,
}
```

### 5.2 `plan.toml` 永続化形

```toml
[jobs."opt__compound=0__method=0"]
route = "# B3LYP/6-31G* opt"
compound = "benzene"
nproc = 16

[jobs."opt__compound=0__method=1"]
route = "# M06-2X/6-31G* opt"
compound = "benzene"
nproc = 16

[jobs."freq__compound=0__method=0"]
route = "# B3LYP/6-31G* freq"
compound = "benzene"
nproc = 16
```

### 5.3 I/O

```rust
pub fn read_plan(path: &Path) -> Result<ExperimentPlan, JobManagerError>;
pub fn write_plan(path: &Path, plan: &ExperimentPlan) -> Result<(), JobManagerError>;
```

`write_plan` は SP-1 の `flow_io` と同じ atomic rename pattern (`tmp_path` 経由)。

### 5.4 設計判断

- `params` の値は `toml::Value` (任意の TOML 型を保持、SP-3 が解釈)
- key は D2 の `JobId` (Pyclass Single Owner)
- `plan.toml` は **`flow.toml` と必ず 1:1 対応** (同じ `<root>/<uuid>/` 下に置く)
- `flow.toml` にある JobId は `plan.toml` に必ず存在する、逆も真 — invariant は呼び側責務 (SP-2 はチェックしない)

---

## 6. FS レイアウト

```
<root>/                                # PathResolver.root
└── <flow.uuid>/                       # PathResolver::flow_dir(&uuid) で導出
    ├── flow.toml                      # JobFlow (D2 v4)
    ├── plan.toml                      # ExperimentPlan (本 SP-2)
    └── <JobId>/                       # 各 Job のディレクトリ
        ├── .status.toml               # SP-1
        ├── input.gjf                  # SP-3
        ├── batch.bash                 # SP-3
        └── slurm-*.out                # SLURM 直書き
```

`PathResolver` に追加する getter:

```rust
impl PathResolver {
    pub fn plan_toml(&self, flow_uuid: &Uuid) -> PathBuf {
        self.flow_dir(flow_uuid).join("plan.toml")
    }

    /// 将来、ユーザーが experiment 定義 Python script を flow dir に保存したい場合の
    /// 慣用 path。SP-2 では使わない。
    pub fn experiment_toml(&self, flow_uuid: &Uuid) -> PathBuf {
        self.flow_dir(flow_uuid).join("experiment.toml")
    }
}
```

---

## 7. Rust モジュール構成

### 7.1 ディレクトリレイアウト

```
src/
├── jobid.rs                    # CREATE: validate / build / parse helpers
├── plan/
│   ├── mod.rs                  # CREATE: ExperimentPlan
│   └── io.rs                   # CREATE: read_plan / write_plan (atomic rename)
├── path.rs                     # MODIFY: plan_toml(), experiment_toml() getter 追加
├── error.rs                    # MODIFY: JobId* バリアント追加
└── py_export/
    ├── jobid.rs                # CREATE: validate/build/parse pyfunctions
    └── plan.rs                 # CREATE: PyExperimentPlan + read_plan/write_plan
```

`crate::grammar` モジュールは **作らない**。

### 7.2 主要型シグネチャ

#### jobid.rs

```rust
use crate::error::JobManagerError;

pub fn validate_step_id(s: &str) -> Result<&str, JobManagerError>;
pub fn validate_job_id(s: &str) -> Result<&str, JobManagerError>;
pub fn build_job_id(source_step_id: &str, axis_combo: &[(&str, usize)]) -> String;
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,
}
```

#### plan/mod.rs

```rust
use gaussian_job_shared::entities::workflow::JobId;
use std::collections::BTreeMap;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentPlan {
    pub jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>,
}
```

#### plan/io.rs

```rust
use std::path::Path;
use crate::error::JobManagerError;
use crate::plan::ExperimentPlan;

pub fn read_plan(path: &Path) -> Result<ExperimentPlan, JobManagerError>;
pub fn write_plan(path: &Path, plan: &ExperimentPlan) -> Result<(), JobManagerError>;
```

### 7.3 エラー型 (拡張)

`error.rs` に SP-1 既存 variants に加えて:

```rust
#[derive(Debug, thiserror::Error)]
pub enum JobManagerError {
    // ... SP-1 既存 ...

    #[error("invalid step id '{0}': must match [A-Za-z0-9_-]+")]
    InvalidStepId(String),

    #[error("invalid job id '{0}': must match [A-Za-z0-9_\\-=]+")]
    InvalidJobId(String),

    #[error("reserved id '{0}' (reserved: flow, plan, experiment, derived, status)")]
    ReservedJobId(String),

    #[error("job id parse error in '{id}' at piece '{piece}': {reason}")]
    JobIdParseError { id: String, piece: String, reason: String },
}
```

---

## 8. Python API (PyO3)

```python
from job_manager import (
    # plan
    ExperimentPlan,
    read_plan,
    write_plan,
    # jobid
    build_job_id,
    parse_job_id,
    validate_step_id,
    validate_job_id,
    # path (SP-1 既存 + SP-2 追加)
    PathResolver,
)
from gaussian_job_shared import JobFlow, JobId, Job, JobSpec, Program, JobEdge
from slurm_async_runner import DependencyType, SlurmJobConfig
```

§1.1 にユーザーの authoring 例を示した。

**設計判断:**
- `parse_job_id(s: str)` は Python dict を返す: `{"source_step_id": str, "axis_combo": list[tuple[str, int]]}`
- `build_job_id(step_id: str, axis_combo: list[tuple[str, int]]) -> str`
- `validate_step_id` / `validate_job_id` は OK で input をそのまま返し、NG で `ValueError` を raise
- `ExperimentPlan` は read-only Python view (`#[pyclass(frozen)]`)、`.jobs` getter で `dict[str, dict[str, Any]]` を返す
- D2 の `PyJobId` / `PyProgram` / `PyCalcType` は D2 側でエクスポート済み (Pyclass Single Owner、再エクスポートしない)

---

## 9. テスト計画

### 9.1 Unit tests (Rust, `#[cfg(test)]`)

- `jobid.rs`:
  - `validate_step_id`: 命名規約 OK / 予約名 reject / 文字種 reject / 空文字 reject
  - `validate_job_id`: 同上 + `=` 含む形式 OK
  - `build_job_id`: sweep 空 / 多軸 / 順序保証
  - `parse_job_id`: round-trip / malformed reject / 文字種 reject
  - `build_job_id` ↔ `parse_job_id` の整合性 (random axis_combo で)

- `plan/mod.rs` / `plan/io.rs`:
  - serde round-trip (BTreeMap key の順序、toml::Value の全型保持)
  - atomic rename ( `.tmp` → final )
  - missing file → `Io` エラー
  - malformed TOML → `TomlParse` エラー
  - deny_unknown_fields: 未知のトップレベルキーで reject

- `path.rs`:
  - `plan_toml(&uuid)` / `experiment_toml(&uuid)` の path 構築

### 9.2 Integration tests (`tests/`)

- end-to-end の **flow + plan 構築 → 永続化 → 読み戻し** (Python 側でやる例を Rust integration test でも)
- `tests/integration_plan.rs`:
  - 12-job sample を Rust で構築 (compound 3 × method 2 × step 2)
  - flow.toml + plan.toml を tempdir に永続化
  - 読み戻して同型確認
  - 各 JobId が `parse_job_id` で正しく分解できる

### 9.3 Python tests (`python/tests/test_jobid.py` / `test_plan.py`)

- `validate_step_id` / `validate_job_id` の OK/NG 例
- `build_job_id` の例
- `parse_job_id` の例 (戻り値の dict 構造)
- `ExperimentPlan` の `.jobs` getter
- `write_plan` / `read_plan` の round-trip
- §1.1 の authoring 例の動作確認 (Python で 12-job を構築して write)

### 9.4 fixture

**snapshot fixture は持たない** — flow.toml / plan.toml は struct の serde mirror なので、tempdir に write → read で round-trip 検証で十分。

### 9.5 カバレッジ目標

`cargo llvm-cov --fail-under-lines 80` で 80%+。

---

## 10. リスクと未決事項

| 項目 | リスク | 対応 |
|---|---|---|
| D2 PR の merge 順序 | D2 work_dir 撤廃 → SP-1 follow-up → SP-2 の順厳守 | 各 PR の base/head 明示 |
| SP-1 follow-up の規模 | `flow.work_dir` 参照箇所のみ | grep で網羅、PathResolver 経由に置換 |
| `toml::Value` round-trip | `params: BTreeMap<String, toml::Value>` で書き戻し | integration test で round-trip 検証 |
| `parse_job_id` 性能 | search で多数の JobId をパース | 借用ベース API (`JobIdParts<'a>`) で alloc 削減 |
| Python authoring の UX | DSL がないので「sweep の書き方」をドキュメント化必要 | README + Python docstring + §1.1 を doc 化 |
| Pyclass Single Owner rule | D2 の `PyJobId` を再定義しない | job-manager の `Cargo.toml` で D2 の `pyo3` feature をパス依存上で無効化 |
| 既存 `gaussian-experiment-manager` との互換 | experiment.toml 形式が使えない | β-adapter は本 spec の責務外。ユーザーは Python authoring に移行 |

---

## 11. 完了基準

- [ ] D2 PR (`JobFlow.work_dir` 撤廃) merged
- [ ] SP-1 follow-up PR (work_dir 参照置換) merged
- [ ] `cargo build --all-features` 成功
- [ ] `cargo test --lib` 成功 (カバレッジ 80%+)
- [ ] `cargo clippy -- -D warnings` 成功
- [ ] `cargo fmt --check` 成功
- [ ] `uv run maturin develop` 成功
- [ ] `uv run pytest python/tests` 成功
- [ ] `cargo run --bin stub_gen` で `.pyi` 再生成、`ruff format` クリーン
- [ ] §1.1 の Python authoring 例が動作 (12-job 構築 → write → read 戻し)
- [ ] **`crate::grammar` モジュールが存在しないことを `git ls-files src/` で確認** (案 B 採用の確証)
- [ ] **D2 への変更が `JobFlow.work_dir` 撤廃のみであることを diff で確認**

---

## 12. 次工程

SP-2 完了後 (SP-3 で行う):
- `common.toml` 読み込み + `SlurmJobConfig` 合成
- `JobSpec.body` の bash render (`plan.toml` の `params` + `parse_job_id` の `axis_combo` を活用)
- A1 `SbatchManager` 経由の `submit_chain` 相当
- CLI: `run` / `submit` / `show` / `tick` / `search`

SP-2 設計 v5 が承認されたら writing-plans skill で実装計画書 v5 (3 phases: D2 PR → SP-1 follow-up → SP-2 minimal) に変換する。
