# job-manager SP-2 (grammar) 設計 v4

- **Date**: 2026-05-12 (v1→v4 改訂)
- **Status**: Draft (brainstorming 完了、レビュー待ち)
- **Targets**: `crate::grammar::*` + `crate::plan::*` (Rust) / `job_manager._job_manager_core.grammar` (Python)
- **Subproject**: SP-2 of 3 — grammar (`experiment.toml` → `(JobFlow, ExperimentPlan)`)
- **References**:
  - SP-1 spec: `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md` (データ層、FS レイアウト確立)
  - Python リファレンス: `../../../gaussian-experiment-manager/src/gaussian_experiment_manager/grammar/`
  - 上流 (D2): `../../../gaussian-job-shared2/` — **work_dir フィールドのみ撤廃** (本 spec で要 D2 PR)
  - 上流 (A1): `../../../slurm-async-runner2/` (`DependencyType`, `SlurmJobConfig` — 不可侵)

---

## v4 改訂サマリ

ユーザー訂正の整理 (v1→v4):

1. **v2 で D2 newtype 撤廃を提案 → v3 で取消** — newtype は shared 定義として保持し、import して使う
2. **v4: フィールド単位での要否判断**
   - 「自前構造を使わない」= shared 型を import すること、ただし **shared 側の不要なフィールドは撤廃可能**
   - `JobFlow.work_dir` は `<root>/<uuid>/` で導出可能なので撤廃する
   - 他のフィールド (`uuid`/`created_at`/`tags`/`jobs`/`spec.*`/`parents` 等) は run + search で必須なので保持

D2 フィールド監査結果:

| Struct | Field | 判定 | 理由 |
|---|---|---|---|
| `JobFlow` | `uuid` | 保持 | identity / search key |
| `JobFlow` | `created_at` | 保持 | 時系列ソート |
| `JobFlow` | `work_dir` | **撤廃** | `<root>/<uuid>/` で導出可、`mv` で drift リスク |
| `JobFlow` | `tags` | 保持 | search (`tags["calc_type"]` 含む) |
| `JobFlow` | `jobs` | 保持 | DAG 本体 |
| `Job.spec` | flatten | 保持 | run の全情報 |
| `Job.parents` | — | 保持 | 依存ワイヤリング |
| `JobSpec.program` | — | 保持 | search by program |
| `JobSpec.config` | — | 保持 | sbatch directives (A1 不可侵) |
| `JobSpec.body` | — | 保持 | bash 本体 |
| `JobEdge.from` | — | 保持 | dep target |
| `JobEdge.kind` | — | 保持 | dep kind (A1 型) |

注: `JobFlow.calc_type` というフィールドは現行 D2 に **存在しない**。`CalcType` は newtype として定義されているが、どの struct のフィールドにも使われておらず、`tags["calc_type"]` (String) として運用されている。`CalcType` newtype 自体は保持 (downstream の typed 利用余地、SP-2 内では `FlowMeta.calc_type: Option<CalcType>` で利用)。

---

## 1. 背景

SP-1 で確立した「データ層 + 並列走査 + tick」基盤の上に、ユーザー入力 `experiment.toml` を D2 `JobFlow` + 最小限の `ExperimentPlan` sidecar に展開する **grammar 層**を構築する。SP-3 (submit + CLI) はこの 2 ファイル (`flow.toml`, `plan.toml`) を入力として bash 生成・sbatch 投入を行う。

### 1.1 Python 実装 (リファレンス) の課題

`gaussian-experiment-manager/grammar/` のレビューで顕在化した問題:

1. **`step.compounds` が first-class** — Gaussian 専用設計
2. **axis element の reserved key (`compounds`, `tags`) が暗黙** — cross-axis collision が silent merge (`sweep.py:78` TODO)
3. **`step.parent: str` 単数** — 真の DAG fan-in 不可
4. **parent 解決が set 比較の暗黙ディスパッチ** (`chain.py:62-72`)
5. **`step.id: str | None` optional** — `'<no-id>'` フォールバックが UX 汚染
6. **`step.calc_type` per-step** — JobFlow 単位で持つべき情報
7. **`${axis.field}` 展開が string 限定** で int/float が型ロス
8. **SLURM dependency kind が `afterok` 固定**

### 1.2 SP-2 のスコープ

| 含める | 含めない (SP-3) |
|---|---|
| `experiment.toml` のパース (strict、unknown key 拒否) | `common.toml` (cluster/account-level config) のマージ |
| Legacy `gaussian_batch.toml` 形状の検出 + エラー | bash body の rendering (`#SBATCH` block + 本文) |
| `[[axis]]` sweep 展開 (itertools.product 相当) | `SlurmJobConfig` の partition/account/time-limit 等の合成 |
| `${axis}` / `${axis.field}` プレースホルダ展開 | sbatch 投入 (A1 `SbatchManager` 経由) |
| 親 (`parents = [...]`) の解決 (pair / fanout / reduce) | CLI コマンド (`run`/`submit`/...) |
| `JobFlow` (D2 newtype 利用) 構築 | log_paths 解決 (SLURM `%j`/`%x` 展開) |
| 最小 `ExperimentPlan` sidecar 構築 | β-adapter / `gaussian_batch_cli` 互換 |
| JobId 文字種・予約名 validation | newtype 撤廃 (v3 で取消、本 spec でも変更しない) |
| `parse_job_id` helper (借用ベース) | |
| **D2 `JobFlow.work_dir` フィールド撤廃 (要 D2 PR)** | **D2 の他フィールド変更** |

### 1.3 サブプロジェクト位置付け

```
SP-1 (データ層, 完, follow-up PR で work_dir 撤廃)
       ↓
D2 PR (JobFlow.work_dir 撤廃のみ — newtype は保持)
       ↓
       SP-2 (grammar, 本 spec)   ←── SP-3 (submit + CLI)
```

---

## 2. 採用アプローチ: **Pure-Rust grammar + 最小 sidecar `plan.toml`**

### 2.1 比較した 3 案

| 比較項目 | A (採用) | B: 自前完結 JobFlow | C: Python 側で expand |
|---|---|---|---|
| TOML パース | Rust serde + 手書き validation | Rust serde | Python tomllib |
| Sweep / parent 解決 | Rust 純粋関数 | 同 | Python |
| `params` の所在 | `plan.toml` (job_id → params dict) | 自前 `Job` 構造に `params` フィールド追加 | Python メモリ内 |
| D2 への侵襲 | **work_dir 撤廃のみ** (newtype は import 利用) | params フィールド追加 (D2 が grammar 概念を持つ) | なし |
| Shared definition 活用 | ✅ D2 の `JobId`/`Program`/`CalcType` を import | ⚠️ D2 に grammar 知識を持ち込む | N/A |
| 再実装の動機適合 | ✅ | ⚠️ | ❌ |

**判断:**
- B は D2 が「grammar 由来の per-step params」を持つことになり、責務 (汎用 DAG コンテナ) を超える
- C は Pure-Rust pipeline 化の動機を満たさない
- **A**: D2 の newtype は温存、`work_dir` のみ撤廃 (redundant)、`params` だけ job-manager 側 `plan.toml`

### 2.2 案 A の設計判断

- **D2 newtype を不可侵に扱う** — `JobId`/`Program`/`CalcType`/`Job`/`JobEdge`/`JobSpec` を import して使う
- **`JobFlow.work_dir` のみ撤廃** — `<root>/<uuid>/` で導出。SoT が PathResolver に集約 (drift 防止)
- **`expand_experiment(toml_path) -> (JobFlow, ExperimentPlan)` は純粋関数** — work_dir が無くなることで `root` 引数も不要に
- **`JobSpec.body` と `JobSpec.config` は SP-2 時点では空** (`String::new()` / `SlurmJobConfig::default()`)。SP-3 が `plan.toml` の `params` + `common.toml` を merge して埋める
- **JobFlow uuid は v7** (SP-1 と一貫)

---

## 3. 必須 D2 変更 (`JobFlow.work_dir` 撤廃のみ)

### 3.1 現状 (v3 までの D2)

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
- `mv` 等のディレクトリ移動で drift するリスクがある
- `JobFlow` 自体は run/search に純粋なメタデータコンテナとして機能し、場所情報を持たない方がきれい
- `PathResolver::flow_dir(&Uuid) -> PathBuf` で参照側はワンライナーで導出可能

### 3.4 影響範囲

- **D2 (`gaussian-job-shared2`)**: `JobFlow` 定義、test 内 fixture、pyclass の getter/setter、TOML 例
- **job-manager (SP-1 既 merged)**: `crate::view::CalcView` 等で `flow.work_dir` を参照している箇所を `PathResolver::flow_dir(&flow.uuid)` に書き換える (follow-up PR)
- **job-manager (SP-2 本 spec)**: `to_jobflow_and_plan` / `expand_experiment` で work_dir を埋める処理を削除 — `root` 引数不要に
- **`gaussian-experiment-manager` (legacy)**: 影響あり (SP-2 移行時に対応、本 spec の責務外)

### 3.5 newtype は撤廃しない

`JobId` / `Program` / `CalcType` は v3 と同様 D2 の正典定義を import して使う。job-manager 側で再定義 / 別名作成は行わない。

---

## 4. `experiment.toml` Schema 仕様

### 4.1 全体構造

```toml
# 最上位許可キー: flow, axis, step のみ。strict (unknown key reject)。
[flow]                                    # 任意 block
calc_type = "opt+freq+td"                 # → JobFlow.tags["calc_type"]
tags      = { project = "tddft" }         # → JobFlow.tags にマージ

[[axis]]                                  # 軸定義 (0 以上)
name   = "compound"
values = ["benzene", "toluene"]           # list<str> = scalar axis

[[axis]]
name   = "method"
values = [                                # list<table> = struct axis
    { name = "b3lyp", route = "B3LYP" },
    { name = "m062x", route = "M06-2X" },
]

[[step]]                                  # ステップ定義 (1 以上)
id      = "opt"                           # 必須・unique・JobId 文字種制約あり
program = "g16"                           # 必須
sweep   = ["compound", "method"]          # 任意
parents = []                              # 任意

[step.params]                             # 任意 dict<str, toml::Value> (${...} 展開対象)
route = "# ${method.route}/6-31G* opt"
```

**v1 (Python 実装) からの差分:** `[step.tags]` block を削除 (per-job tags 不採用)。`[flow.tags]` は引き続き JobFlow.tags にマージされる (search 用)。

### 4.2 `[flow]` block

```toml
[flow]
calc_type = "opt+freq+td"        # 任意。文字列。JobFlow.tags["calc_type"] になる。
tags      = { ... }              # 任意。BTreeMap<String, String>.
```

- `tags` 内に `"calc_type"` キーがあれば error (重複表現)
- 値はすべて string

### 4.3 `[[axis]]` block

```toml
[[axis]]
name   = "method"                # 必須。識別子 `[A-Za-z_][A-Za-z0-9_]*`
values = [ ... ]                 # 必須。空リスト禁止。
```

- `values` の型:
  - **scalar axis**: `list<string>` — `${<name>}` で要素文字列を展開
  - **struct axis**: `list<table>` — 全要素が同じキー集合、値は string/int/float/bool
- 混在はエラー
- duplicate `name` はエラー

### 4.4 `[[step]]` block

```toml
[[step]]
id      = "opt"                        # 必須・unique・JobId 文字種
program = "g16"                        # 必須
sweep   = ["compound", "method"]       # 任意
parents = [ ... ]                      # 任意
[step.params]                          # 任意 dict<str, toml::Value>. ${...} は string 値内のみ展開。
```

- `id` 文字種: `[A-Za-z0-9_\-]+`
- `id` 予約名禁止: `flow`, `plan`, `experiment`, `derived`, `status`
- `id` の duplicate (別 step 間) はエラー
- `sweep` の要素は `[[axis]]` で定義済みの name、重複なし
- `params` の値: TOML 標準型をそのまま保持、`${...}` は string 値内のみ再帰展開

#### 4.4.1 `step.parents`

```toml
parents = [
    { id = "opt" },                                   # pair_by_axes (default)
    { id = "preflight", fanout = true },              # 1:N
    { id = "scan", reduce_over = ["theta"] },         # N:1
    { id = "opt", kind = "afterany" },                # SLURM dependency kind 上書き
]
```

| Field | Type | Default | 意味 |
|---|---|---|---|
| `id` | `string` | (必須) | 参照先 step.id |
| `fanout` | `bool` | `false` | true = 親軸が子軸の真部分集合と validate |
| `reduce_over` | `list<string>` | `[]` | 非空 = 親軸 = 子軸 ∪ reduce_over と validate |
| `kind` | `string` | `"afterok"` | SLURM `DependencyType` |

**Mode 決定ルール:**

| `fanout` | `reduce_over` | Mode | Validation |
|---|---|---|---|
| `false` | `[]` | pair_by_axes | parent.sweep == child.sweep |
| `true` | `[]` | fanout | parent.sweep ⊊ child.sweep |
| `false` | 非空 | reduce_over | parent.sweep == child.sweep ∪ reduce_over |
| `true` | 非空 | **error** | `BothFanoutAndReduce` |

#### 4.4.2 Legacy 形状検出

- 最上位に `[gaussian_input]` block → `LegacyToml`
- `[env].compound_id` / `[env].project_base` → 同上
- `[[sweep]]` block → `LegacyToml { hint: "[[sweep]] was renamed to [[axis]]" }`
- `step.compounds` → `LegacyToml { hint: "step.compounds was removed; use [[axis]] name=\"compound\"" }`
- `step.calc_type` → `LegacyToml { hint: "step.calc_type was moved to [flow].calc_type" }`
- `step.parent` (単数) → `LegacyToml { hint: "step.parent was renamed to step.parents (list)" }`
- `step.sweep_over` → `LegacyToml { hint: "step.sweep_over was renamed to step.sweep" }`
- `step.tags` → `LegacyToml { hint: "step.tags was removed in v2 (per-job tags 不採用)" }`

### 4.5 Placeholder syntax `${...}`

```
${ident}                  # scalar axis 参照
${ident.ident}            # struct axis field 参照
```

- 展開対象: **string 型 TOML 値の中のみ**。`step.params` を再帰的に走査
- 値が int/float/bool の場合は `Display` で文字列化
- 各種エラー: `PlaceholderUnknownAxis` / `PlaceholderUnknownField` / `PlaceholderInvalidScalarField` / `PlaceholderAmbiguousStructAxis` / `PlaceholderAxisNotInSweep` / `PlaceholderSyntaxError`
- エスケープ: `$${...}` で literal `${...}`

---

## 5. JobId 命名規約 (決定論的)

### 5.1 形式

```
<step.id>                                  # sweep 空のとき
<step.id>__<axis1>=<idx>__<axis2>=<idx>    # sweep のとき (axis 順 = step.sweep 宣言順)
```

例 (step.id="opt", sweep=["compound", "method"]):
- `opt__compound=0__method=0`
- `opt__compound=0__method=1`

### 5.2 文字種・予約名

- 許可文字: `[A-Za-z0-9_\-=]+`
- step.id 自体の許可文字は `[A-Za-z0-9_\-]+` (`=` は予約)
- 予約 JobId: `flow`, `plan`, `experiment`, `derived`, `status`
- duplicate JobId は build 段階で error

### 5.3 D2 newtype の使い方

D2 の `JobId(pub String)` は **string-newtype**。job-manager 内の表現規約は:

- **構築**: `JobId::from(build_job_id(step_id, axis_combo))`
- **検証**: `validate_job_id(s: &str) -> Result<&str, JobManagerError>` を構築前に呼ぶ
- **借用パース**: `parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>` に `&job_id.0` を渡す

### 5.4 JobId パースによる導出可能性

JobId が `<step_id>__<axis>=<idx>__...` の決定論的形式を保つので、SP-3 やその他の consumer は以下を導出できる:

```rust
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>;

pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,
}
```

→ `ExperimentPlan` 側で `source_step_id` / `axis_combo` を冗長に保持する必要がない。

---

## 6. Sweep 展開セマンティクス

### 6.1 アルゴリズム

```
for each step:
    if step.sweep is empty:
        emit ExpandedStep with axis_combo = {}
    else:
        let axes = [resolve(name) for name in step.sweep]
        for indices in itertools.product(*[range(len(ax.values)) for ax in axes]):
            let combo = { name_i: indices_i for (name_i, indices_i) in zip(step.sweep, indices) }
            let expansion_ctx = { name_i: axes[i].values[indices_i] for i in ... }
            emit ExpandedStep {
                source_step_id: step.id,
                axis_combo: combo,
                program: step.program,
                params: expand_placeholders(step.params, expansion_ctx),
                parents_raw: step.parents,
            }
```

`ExpandedStep` は **中間表現** で、最終出力 (JobFlow + ExperimentPlan) には持ち込まない。axis_combo / source_step_id は JobId 文字列に埋め込まれて消える。

### 6.2 順序保証

`step` 出現順、各 step 内では axis 宣言順の product (最後の axis が最速回転)。

---

## 7. Parent 解決セマンティクス

### 7.1 全体フロー

```
expanded: list<ExpandedStep>
step_index_by_id: BTreeMap<step.id, list<expanded_idx>>

for child in expanded:
    for parent_ref in child.parents_raw:
        let parent_step = lookup(parent_ref.id)
        let parents_expanded = step_index_by_id[parent_step.id]
        let edges = resolve_edges(parent_ref, parent_step, child, parents_expanded)
        child.parents.extend(edges)        # JobEdge { from: JobId, kind: DependencyType }
```

### 7.2 3 modes の解決ロジック

#### pair_by_axes (default)

```
validate parent.sweep_set == child.sweep_set
for parent_e in parents_expanded:
    if all(parent_e.axis_combo[ax] == child_e.axis_combo[ax] for ax in parent.sweep):
        emit JobEdge { from: parent_e.job_id, kind: parent_ref.kind }
```

#### fanout

```
validate parent.sweep_set ⊊ child.sweep_set
for parent_e in parents_expanded:
    if all(parent_e.axis_combo[ax] == child_e.axis_combo[ax] for ax in parent.sweep):
        emit JobEdge { from: parent_e.job_id, kind: parent_ref.kind }
```

#### reduce_over

```
validate parent.sweep_set == child.sweep_set ∪ set(reduce_over)
validate set(reduce_over) ⊆ parent.sweep_set
validate set(reduce_over) ∩ child.sweep_set == ∅
for parent_e in parents_expanded:
    if all(parent_e.axis_combo[ax] == child_e.axis_combo[ax] for ax in child.sweep):
        emit JobEdge { from: parent_e.job_id, kind: parent_ref.kind }
```

### 7.3 Validation 全列挙

| Code | 条件 |
|---|---|
| `UnknownStepId` | `parents[].id` が `[[step]]` に未定義 |
| `SelfParent` | `parents[].id == self.id` |
| `BothFanoutAndReduce` | `fanout=true` かつ `reduce_over` 非空 |
| `ReduceOverNotSubsetOfParent` | `reduce_over ⊄ parent.sweep` |
| `ReduceOverIntersectsChild` | `reduce_over ∩ child.sweep ≠ ∅` |
| `PairByAxesMismatch` | pair モードで parent.sweep != child.sweep |
| `FanoutNotProperSubset` | fanout モードで parent.sweep ⊄ child.sweep または等しい |
| `ReduceCoverageMismatch` | reduce モードで parent.sweep != child.sweep ∪ reduce_over |
| `UnknownDependencyKind` | `kind` 文字列が `DependencyType::from_str` で parse 失敗 |
| `DagHasCycle` | 構築後の DAG にサイクル (Kahn's algorithm で検出) |

---

## 8. 出力アーティファクト

### 8.1 `JobFlow` (D2 v4 形 — work_dir 撤廃後)

```rust
use gaussian_job_shared::entities::workflow::{JobFlow, Job, JobEdge, JobSpec, JobId, Program};
use slurm_async_runner::entities::slurm::SlurmJobConfig;

JobFlow {
    uuid:        Uuid::now_v7(),
    created_at:  Utc::now(),
    tags:        { /* [flow.tags] + ("calc_type", value) if [flow].calc_type present */ },
    jobs: BTreeMap {
        JobId::from("opt__compound=0__method=0") => Job {
            spec: JobSpec {
                program: Program::from("g16"),
                config:  SlurmJobConfig::default(),
                body:    String::new(),
            },
            parents: vec![],
        },
        ...
    },
}
```

run + search に必要な情報のみ:
- run: `jobs[*].spec.body` + `jobs[*].spec.config` で sbatch、`jobs[*].parents` で依存ワイヤリング。各 Job のディレクトリは `PathResolver::flow_dir(&flow.uuid).join(job_id.0.as_str())` で導出
- search: `tags` (flow-level) + `jobs[*].spec.program` (per-job) + `created_at` + `uuid`

### 8.2 `ExperimentPlan` (最小 sidecar)

```rust
use gaussian_job_shared::entities::workflow::JobId;

pub struct ExperimentPlan {
    pub jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>,
}
```

`plan.toml` 永続化形:

```toml
[jobs."opt__compound=0__method=0"]
route = "# B3LYP/6-31G* opt"
nproc = 16
```

**v1 (Python 実装) からの差分:**
- `plan_version` / `flow_uuid` / `source_hash` 撤廃
- `PlanEntry` 構造体撤廃、`params` のみフラットに
- `source_step_id` / `axis_combo` 撤廃 (JobId パースで導出)
- `tags` 撤廃

### 8.3 FS レイアウト

```
<root>/                                # PathResolver.root
└── <flow.uuid>/                       # PathResolver で導出 (D2 に永続化フィールド無し)
    ├── flow.toml                      # JobFlow (D2 v4 形)
    ├── plan.toml                      # ExperimentPlan
    ├── experiment.toml                # 入力 TOML のコピー
    └── <JobId>/                       # 各 Job のディレクトリ
        ├── .status.toml               # SP-1
        ├── input.gjf                  # SP-3
        ├── batch.bash                 # SP-3
        └── slurm-*.out                # SLURM 直書き
```

`PathResolver` に追加する getter:
- `plan_toml(&Uuid) -> PathBuf` → `<root>/<uuid>/plan.toml`
- `experiment_toml(&Uuid) -> PathBuf` → `<root>/<uuid>/experiment.toml`

---

## 9. Rust モジュール構成

### 9.1 ディレクトリレイアウト

```
src/
├── grammar/
│   ├── mod.rs                  # re-exports + expand_experiment pipeline
│   ├── source.rs               # data types
│   ├── reader.rs               # parse_experiment (strict + legacy detect)
│   ├── placeholder.rs          # ${...} lex + expand
│   ├── sweep.rs                # expand_sweeps
│   ├── jobid.rs                # validate / parse / build helpers
│   ├── chain.rs                # resolve_parents + cycle check
│   └── build.rs                # to_jobflow_and_plan
├── plan/
│   ├── mod.rs                  # ExperimentPlan
│   └── io.rs                   # read_plan / write_plan (atomic rename)
├── path.rs                     # MODIFY: plan_toml(), experiment_toml() getter 追加
├── error.rs                    # MODIFY: GrammarError variant 群追加
└── py_export/
    ├── grammar.rs              # expand_experiment pyfunction
    └── plan.rs                 # ExperimentPlan pyclass
```

### 9.2 主要型シグネチャ

#### grammar/source.rs

```rust
use gaussian_job_shared::entities::workflow::{Program, CalcType};
use slurm_async_runner::entities::slurm::DependencyType;

#[derive(Debug, Clone)]
pub struct ExperimentSource {
    pub flow: FlowMeta,
    pub axes: Vec<AxisDef>,
    pub steps: Vec<RawStep>,
}

#[derive(Debug, Clone, Default)]
pub struct FlowMeta {
    pub calc_type: Option<CalcType>,            // D2 newtype を import 利用
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct AxisDef {
    pub name: String,
    pub values: AxisValues,
}

#[derive(Debug, Clone)]
pub enum AxisValues {
    Scalar(Vec<String>),
    Struct { fields: Vec<String>, rows: Vec<BTreeMap<String, toml::Value>> },
}

#[derive(Debug, Clone)]
pub struct RawStep {
    pub id: String,                             // step.id (grammar-only). JobId と別概念。
    pub program: Program,                       // D2 newtype 直
    pub sweep: Vec<String>,
    pub parents: Vec<ParentRef>,
    pub params: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone)]
pub struct ParentRef {
    pub id: String,                             // step.id 参照 (展開前)
    pub fanout: bool,
    pub reduce_over: Vec<String>,
    pub kind: DependencyType,
}
```

#### grammar/jobid.rs

```rust
pub fn validate_step_id(s: &str) -> Result<&str, JobManagerError>;
pub fn validate_job_id(s: &str) -> Result<&str, JobManagerError>;
pub fn build_job_id(source_step_id: &str, axis_combo: &[(&str, usize)]) -> String;
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>;

pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,
}
```

#### grammar/build.rs

```rust
use gaussian_job_shared::entities::workflow::JobFlow;

pub(crate) fn to_jobflow_and_plan(
    flow_meta: &FlowMeta,
    resolved: Vec<ResolvedStep>,
) -> Result<(JobFlow, ExperimentPlan), JobManagerError>;
```

注: v4 では `root` 引数不要 (`JobFlow.work_dir` が撤廃されるため)。

#### grammar/mod.rs (公開 API)

```rust
use gaussian_job_shared::entities::workflow::JobFlow;

/// `experiment.toml` → `(JobFlow, ExperimentPlan)`。pure (toml_path 読込のみ)。
pub fn expand_experiment(
    toml_path: &Path,
) -> Result<(JobFlow, ExperimentPlan), JobManagerError>;
```

### 9.3 エラー型 (拡張)

```rust
#[derive(Debug, thiserror::Error)]
pub enum JobManagerError {
    // ... SP-1 既存 ...

    #[error("grammar parse error at {path}: {source}")]
    GrammarTomlParse { path: PathBuf, #[source] source: toml::de::Error },

    #[error("legacy TOML shape detected at {path}: {hint}")]
    LegacyToml { path: PathBuf, hint: String },

    #[error("unknown key '{key}' in {location}")]
    UnknownKey { key: String, location: String },

    #[error("missing required key '{key}' in {location}")]
    MissingKey { key: String, location: String },

    #[error("wrong type for '{key}' in {location}: expected {expected}, got {got}")]
    WrongType { key: String, location: String, expected: &'static str, got: &'static str },

    #[error("duplicate axis name '{0}'")]
    DuplicateAxis(String),

    #[error("axis '{0}' has empty values")]
    EmptyAxis(String),

    #[error("axis '{name}' has mixed scalar/struct values")]
    MixedAxisValues { name: String },

    #[error("axis '{name}' struct values have inconsistent fields at row {row}")]
    StructAxisFieldMismatch { name: String, row: usize },

    #[error("duplicate step id '{0}'")]
    DuplicateStepId(String),

    #[error("invalid step id '{0}': must match [A-Za-z0-9_-]+ and not be reserved")]
    InvalidStepId(String),

    #[error("step '{step}' references unknown axis '{axis}'")]
    UnknownAxisRef { step: String, axis: String },

    #[error("step '{step}' has duplicate axis '{axis}' in sweep")]
    DuplicateSweepAxis { step: String, axis: String },

    #[error("step '{0}' parent references unknown step id '{1}'")]
    UnknownStepId(String, String),

    #[error("step '{0}' parent references itself")]
    SelfParent(String),

    #[error("parent ref for '{0}': cannot set both fanout=true and reduce_over=[...]")]
    BothFanoutAndReduce(String),

    #[error("parent ref for '{id}': pair_by_axes requires parent.sweep == child.sweep, got parent={parent:?}, child={child:?}")]
    PairByAxesMismatch { id: String, parent: Vec<String>, child: Vec<String> },

    #[error("parent ref for '{id}': fanout requires parent.sweep ⊊ child.sweep, got parent={parent:?}, child={child:?}")]
    FanoutNotProperSubset { id: String, parent: Vec<String>, child: Vec<String> },

    #[error("parent ref for '{0}': reduce_over coverage mismatch")]
    ReduceCoverageMismatch(String),

    #[error("unknown dependency kind '{0}'")]
    UnknownDependencyKind(String),

    #[error("placeholder ${{{0}}}: unknown axis (not in step.sweep)")]
    PlaceholderAxisNotInSweep(String),

    #[error("placeholder ${{{0}.{1}}}: unknown field on axis")]
    PlaceholderUnknownField(String, String),

    #[error("placeholder ${{{0}}}: scalar axis does not have fields")]
    PlaceholderInvalidScalarField(String),

    #[error("placeholder ${{{0}}}: struct axis requires .field selector")]
    PlaceholderAmbiguousStructAxis(String),

    #[error("placeholder syntax error at offset {offset}: {message}")]
    PlaceholderSyntaxError { offset: usize, message: String },

    #[error("DAG contains cycle involving {0:?}")]
    DagHasCycle(Vec<String>),

    #[error("reserved job id '{0}'")]
    ReservedJobId(String),

    #[error("[flow].tags has reserved key 'calc_type' (use [flow].calc_type instead)")]
    FlowTagsHasCalcType,
}
```

---

## 10. Python API (PyO3)

```python
from job_manager import (
    expand_experiment,       # (toml_path: str) -> tuple[JobFlow, ExperimentPlan]
    ExperimentPlan,
    read_plan,
    write_plan,
    parse_job_id,            # (job_id_str: str) -> dict
)
from job_manager import PathResolver
from gaussian_job_shared import JobFlow, JobId    # D2 newtype を import (Pyclass Single Owner)

flow, plan = expand_experiment("./experiment.toml")

for job_id, job in flow.jobs.items():
    print(job_id, job.spec.program)
    params = plan.jobs[job_id]
    parts = parse_job_id(str(job_id))
    print(parts["source_step_id"], parts["axis_combo"])

# 永続化
resolver = PathResolver("/work_dir")
from job_manager import write_flow
write_flow(resolver.flow_toml(flow.uuid), flow)
write_plan(resolver.plan_toml(flow.uuid), plan)
```

**設計判断:**
- `expand_experiment` は sync (TOML パース・展開は CPU 軽い・I/O は toml_path 読込のみ)
- `ExperimentPlan` は `jobs: dict[str, dict[str, Any]]` のみ公開 (pythonize、key は `JobId.0` の文字列表現)
- `parse_job_id` は Python から JobId 文字列を構成要素に分解する helper

---

## 11. テスト計画

### 11.1 Unit tests (Rust, `#[cfg(test)]`)

- `placeholder.rs`: `${a}` / `${a.b}` / `$${literal}` 正常 + malformed
- `reader.rs`: 最小 valid / unknown key reject / legacy 8 パターン / axis scalar/struct/mixed
- `sweep.rs`: sweep 空 / 多軸 product / placeholder 展開 / 全 axis 型
- `chain.rs`: pair / fanout / reduce_over の正常 + error / kind バリエーション / DAG cycle
- `jobid.rs`: 命名規約 / 予約名 reject / 文字種 reject / `parse_job_id` round-trip / `build_job_id` ↔ `parse_job_id` 整合
- `build.rs`: JobFlow.uuid v7 / tags merge (`[flow].calc_type` + `[flow.tags]`) / calc_type 重複 error

### 11.2 Integration tests (`tests/`)

- end-to-end `expand_experiment("fixtures/minimal.toml")` で JobFlow + plan 完全生成
- 大きめ (axes 3x2x2 = 12, steps 3) で graph 構造の確認
- `flow.toml` + `plan.toml` を tempdir に永続化し、read 戻して同型確認
- JobFlow + plan + parse_job_id の三者整合性

### 11.3 Python tests (`python/tests/`)

- `expand_experiment` の戻り値型 (D2 の `PyJobFlow` / `PyJobId` 等)
- `parse_job_id` の Python 呼び出し
- 各 fixture (minimal / sweep / parent / multi-parent / error)
- 例外ラップ (Rust の `JobManagerError::Grammar*` が Python 例外に変換)

### 11.4 fixture (`tests/fixtures/`)

- `minimal_step.toml`: 1 step / no sweep / no parents
- `single_axis.toml`: 1 axis × 1 step (sweep)
- `pair_chain.toml`: 2 step, pair_by_axes
- `fanout.toml`: parent axes ⊂ child axes
- `reduce.toml`: parent axes ⊃ child axes
- `multi_parent.toml`: 1 child, 2 parents
- `legacy_*.toml`: 各 legacy 形状 (rejection)
- `error_*.toml`: 各 validation error (rejection)

### 11.5 カバレッジ目標

`cargo llvm-cov --fail-under-lines 80` で 80%+。

---

## 12. リスクと未決事項

| 項目 | リスク | 対応 |
|---|---|---|
| D2 PR の merge 順序 | D2 work_dir 撤廃 → job-manager SP-1 follow-up → SP-2 の順を厳守 | 各 PR の base/head 関係を明示。マイルストーン化 |
| SP-1 follow-up の規模 | `JobFlow.work_dir` 参照箇所のみ。範囲は限定的 | 影響範囲を grep で網羅、PathResolver 経由に置換 |
| `toml::Value` の serde round-trip | `params` を `BTreeMap<String, toml::Value>` で持ち plan.toml に書き戻す際の互換性 | integration test で round-trip 検証 |
| `parse_job_id` の重複コスト | search 時に多数の JobId をパース | 借用ベース API `JobIdParts<'a>` で alloc 削減 |
| Placeholder lex 性能 | 大規模 `params` で遅い可能性 | 1-pass scanner 手書き、`regex` 依存追加せず |
| DAG cycle 検出のメモリ | O(V+E) で十分 | `petgraph` 依存追加せず手書き |
| `gaussian-experiment-manager` (Python ref) との互換 | 入力 TOML schema は変わる | legacy detection で migration hint を返す |
| common.toml 統合の遅延 | SP-3 まで `SlurmJobConfig::default()` で空 body | 既知の段取り、SP-3 spec で扱う |
| axis values の type | TOML の Date/DateTime 等を `${...}` 展開時にどう扱うか | string/int/float/bool に制限。それ以外は `WrongType` |
| Pyclass Single Owner rule | D2 の `PyJobId`/`PyProgram`/`PyCalcType` を job-manager が誤って再定義 | job-manager の `Cargo.toml` で D2 の `pyo3` feature をパス依存上で無効化 (SP-1 と同じ規約) |

---

## 13. 完了基準

- [ ] D2 PR (`JobFlow.work_dir` 撤廃) が D2 main に merge 済み
- [ ] SP-1 follow-up PR (job-manager 側 work_dir 参照の置換) が main に merge 済み
- [ ] `cargo build --all-features` 成功
- [ ] `cargo test --lib` 成功 (カバレッジ 80%+)
- [ ] `cargo clippy -- -D warnings` 成功
- [ ] `cargo fmt --check` 成功
- [ ] `uv run maturin develop` 成功
- [ ] `uv run pytest python/tests` 成功
- [ ] `cargo run --bin stub_gen` で `.pyi` 再生成、`ruff format` クリーン
- [ ] `expand_experiment` を 12-job fixture で実行、JobFlow + plan の構造確認
- [ ] 全 validation error path の Python テストが green
- [ ] legacy detection が 8 種類すべてで適切な hint 文字列を返す
- [ ] **D2 への変更が `JobFlow.work_dir` 撤廃のみであることを diff で確認** (newtype 等は変更しない)

## 14. 次工程

SP-2 完了後 (SP-3 で行う):
- `common.toml` 読み込み + `SlurmJobConfig` 合成
- `JobSpec.body` の bash render
- A1 `SbatchManager` 経由の `submit_chain` 相当
- CLI: `run` / `submit` / `show` / `tick` / `search`

SP-2 設計 v4 が承認されたら writing-plans skill で実装計画書 v4 (3 phases: D2 PR → SP-1 follow-up → SP-2) に変換する。
