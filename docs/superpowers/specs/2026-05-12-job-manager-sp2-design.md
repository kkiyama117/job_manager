# job-manager SP-2 (grammar) 設計 v3

- **Date**: 2026-05-12 (v1: 朝、v2: 午後、v3: 夕 — D2 変更撤回後の最終改訂)
- **Status**: Draft (brainstorming 完了、レビュー待ち)
- **Targets**: `crate::grammar::*` + `crate::plan::*` (Rust) / `job_manager._job_manager_core.grammar` (Python)
- **Subproject**: SP-2 of 3 — grammar (`experiment.toml` → `(JobFlow, ExperimentPlan)`)
- **References**:
  - SP-1 spec: `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md` (データ層、FS レイアウト確立)
  - Python リファレンス: `../../../gaussian-experiment-manager/src/gaussian_experiment_manager/grammar/`
  - 上流 (D2): `../../../gaussian-job-shared2/` — **不可侵** (本 spec で変更しない)
  - 上流 (A1): `../../../slurm-async-runner2/` (`DependencyType`, `SlurmJobConfig` — 不可侵)

---

## v3 改訂サマリ

ユーザーから v2 における誤解の訂正を受領した:

> "I said, 'Don't use your own structure in non-platform-dependent
> domains,' but that doesn't mean 'remove newtype'; it means 'use the
> definitions in the shared package to avoid duplicate definitions.'"

正しい解釈は:

1. **D2 (shared package) の既存 newtype をそのまま使う** — `JobId` / `Program` / `CalcType` を D2 から import して使い、job-manager 側で `String` に置き換えたり別名を作ったりしない
2. **D2 への破壊的変更は行わない** — newtype 撤廃 PR も `JobFlow.work_dir` 撤廃 PR も行わない
3. A1 (SLURM 構造) は引き続き不可侵
4. v2 で行った設計変更のうち、**D2 を温存しても成立するもの**は v3 にそのまま引き継ぐ (per-job tags 不採用、`PlanEntry` 削除、`source_hash` 削除など)

v2 からの主な差分 (v3 で取り消した変更):

- ~~D2 から `JobId` / `Program` / `CalcType` newtype を撤廃 (要 D2 PR)~~ → **取消**: D2 から import して使う
- ~~D2 から `JobFlow.work_dir` フィールドを撤廃 (要 D2 PR)~~ → **取消**: D2 を変更せず、`expand_experiment` に `root: &Path` を渡して `work_dir = root.join(uuid.to_string())` で埋める
- ~~SP-1 follow-up PR で型を `String` に移行~~ → **取消**: SP-1 は既存のまま (D2 newtype 利用)

v3 でそのまま残す v2 の決定 (D2 非依存):

- `ExperimentPlan` を `BTreeMap<JobId, BTreeMap<String, toml::Value>>` まで簡素化 (`PlanEntry` 削除)
- `step.tags` block を `experiment.toml` から削除 (per-job tags は SP-1 SearchFilter で使われていない)
- `plan_version` / `flow_uuid` / `source_hash` を撤廃 (再展開検出は YAGNI)
- 8 種類の legacy detection patterns

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
| `JobFlow` (D2 既存形) 構築 (D2 newtype を使用) | log_paths 解決 (SLURM `%j`/`%x` 展開) |
| 最小 `ExperimentPlan` sidecar 構築 | β-adapter / `gaussian_batch_cli` 互換 |
| JobId 文字種・予約名 validation | **D2 への変更 (本 spec では一切行わない)** |
| `parse_job_id` helper (借用ベース) | |

### 1.3 サブプロジェクト位置付け

```
SP-1 (データ層, 完, D2 v1 newtype を利用)
       ↓
       SP-2 (grammar, 本 spec, 同じく D2 v1 newtype を利用)   ←── SP-3 (submit + CLI)
```

D2 を変更しないので、PR スタックは job-manager 単独で 1 PR (`feat/sp2-impl` → `main`)。

---

## 2. 採用アプローチ: **Pure-Rust grammar + 最小 sidecar `plan.toml`**

### 2.1 比較した 3 案

| 比較項目 | A (採用) | B: 自前完結 JobFlow | C: Python 側で expand |
|---|---|---|---|
| TOML パース | Rust serde + 手書き validation | Rust serde | Python tomllib |
| Sweep / parent 解決 | Rust 純粋関数 | 同 | Python |
| `params` の所在 | `plan.toml` (job_id → params dict) | 自前 `Job` 構造に `params` フィールド追加 | Python メモリ内 |
| D2 への侵襲 | **なし** (newtype import のみ) | params フィールド追加 (D2 が grammar 概念を持つ) | なし |
| Shared definition 活用 | ✅ D2 の `JobId`/`Program`/`CalcType` を import | ⚠️ D2 に grammar 知識を持ち込む | N/A |
| 再実装の動機適合 | ✅ | ⚠️ | ❌ |

**判断:**
- B は D2 (`gaussian-job-shared2`) が「grammar 由来の per-step params」を持つことになり、D2 の責務 (汎用 DAG コンテナ) を超える。
- C は Pure-Rust pipeline 化の動機を満たさない。
- **A**: D2 は完全に温存し、`params` だけ job-manager 側の `plan.toml` に持つ。D2 への変更ゼロ。

### 2.2 案 A の設計判断

- **D2 を不可侵に扱う** — `JobFlow { uuid, created_at, work_dir, tags, jobs: BTreeMap<JobId, Job> }`、`Job { spec: JobSpec, parents: Vec<JobEdge> }`、`JobSpec { program: Program, config: SlurmJobConfig, body }`、`JobEdge { from: JobId, kind: DependencyType }` をそのまま使う。
- **`expand_experiment(toml_path, root) -> (JobFlow, ExperimentPlan)`** — `root` から `JobFlow.work_dir = root.join(uuid.to_string())` を導出。ディスクには書かない (純粋関数)。
- **`JobSpec.body` と `JobSpec.config` は SP-2 時点では空** (`String::new()` / `SlurmJobConfig::default()`)。SP-3 が `plan.toml` の `params` + `common.toml` を merge して埋める。
- **JobFlow uuid は v7** (SP-1 と一貫)。
- **shared package の型 (D2 の `JobId`/`Program`/`CalcType` 等) を job-manager 側で再定義しない** — `crate::grammar::*` は必要なら `use gaussian_job_shared::entities::workflow::{JobId, Program, CalcType, Job, JobEdge, JobFlow, JobSpec};` で import する。

---

## 3. `experiment.toml` Schema 仕様

### 3.1 全体構造

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

**v1 からの差分:** `[step.tags]` block を削除 (per-job tags 不採用)。`[flow.tags]` は引き続き JobFlow.tags にマージされる (search 用)。

### 3.2 `[flow]` block

```toml
[flow]
calc_type = "opt+freq+td"        # 任意。文字列。JobFlow.tags["calc_type"] になる。
tags      = { ... }              # 任意。BTreeMap<String, String>.
```

- `tags` 内に `"calc_type"` キーがあれば error (重複表現)
- 値はすべて string

### 3.3 `[[axis]]` block

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

### 3.4 `[[step]]` block

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

#### 3.4.1 `step.parents`

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

#### 3.4.2 Legacy 形状検出

- 最上位に `[gaussian_input]` block → `LegacyToml { hint: "see gaussian-experiment-manager → SP-2 migration notes" }`
- `[env].compound_id` / `[env].project_base` → 同上
- `[[sweep]]` block → `LegacyToml { hint: "[[sweep]] was renamed to [[axis]]" }`
- `step.compounds` → `LegacyToml { hint: "step.compounds was removed; use [[axis]] name=\"compound\"" }`
- `step.calc_type` → `LegacyToml { hint: "step.calc_type was moved to [flow].calc_type" }`
- `step.parent` (単数) → `LegacyToml { hint: "step.parent was renamed to step.parents (list)" }`
- `step.sweep_over` → `LegacyToml { hint: "step.sweep_over was renamed to step.sweep" }`
- `step.tags` → `LegacyToml { hint: "step.tags was removed in v2 (per-job tags 不採用)" }`

### 3.5 Placeholder syntax `${...}`

```
${ident}                  # scalar axis 参照
${ident.ident}            # struct axis field 参照
```

- 展開対象: **string 型 TOML 値の中のみ**。`step.params` を再帰的に走査
- 値が int/float/bool の場合は `Display` で文字列化
- 各種エラー: `PlaceholderUnknownAxis` / `PlaceholderUnknownField` / `PlaceholderInvalidScalarField` / `PlaceholderAmbiguousStructAxis` / `PlaceholderAxisNotInSweep` / `PlaceholderSyntaxError`
- エスケープ: `$${...}` で literal `${...}`

---

## 4. JobId 命名規約 (決定論的)

### 4.1 形式

```
<step.id>                                  # sweep 空のとき
<step.id>__<axis1>=<idx>__<axis2>=<idx>    # sweep のとき (axis 順 = step.sweep 宣言順)
```

例 (step.id="opt", sweep=["compound", "method"]):
- `opt__compound=0__method=0`
- `opt__compound=0__method=1`
- ...

### 4.2 文字種・予約名

- 許可文字: `[A-Za-z0-9_\-=]+`
- step.id 自体の許可文字は `[A-Za-z0-9_\-]+` (`=` は予約)
- 予約 JobId: `flow`, `plan`, `experiment`, `derived`, `status`
- duplicate JobId は build 段階で error

### 4.3 D2 newtype の使い方

D2 の `JobId(pub String)` は **string-newtype**。job-manager 内の表現規約は:

- **構築**: `JobId::from(build_job_id(step_id, axis_combo))` または `JobId(s)`
- **検証**: `validate_job_id(s: &str) -> Result<&str, JobManagerError>` を構築前に呼ぶ (D2 の newtype 自身は validate を行わない)
- **借用パース**: `parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>` に `&job_id.0` を渡してパース

`parse_job_id` を `&JobId` ではなく `&str` で受け取る理由: 文字列リテラルからも呼べる柔軟性、`D2 newtype` への過度な依存回避、`AsRef<str>` 実装の D2 側依存を不要にする。

### 4.4 JobId パースによる導出可能性

JobId が `<step_id>__<axis>=<idx>__...` の決定論的形式を保つので、SP-3 やその他の consumer は以下を導出できる:

```rust
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>;

pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,    // 順序保証 (= step.sweep 宣言順)
}
```

呼び側は `parse_job_id(&job_id.0)` のように `JobId.0` の内部 `String` を `&str` として渡す。

→ これにより `ExperimentPlan` 側で `source_step_id` / `axis_combo` を冗長に保持する必要がなくなる。

---

## 5. Sweep 展開セマンティクス

### 5.1 アルゴリズム

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

### 5.2 順序保証

`step` 出現順、各 step 内では axis 宣言順の product (最後の axis が最速回転)。

---

## 6. Parent 解決セマンティクス

### 6.1 全体フロー

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

### 6.2 3 modes の解決ロジック

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

### 6.3 Validation 全列挙

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

## 7. 出力アーティファクト

### 7.1 `JobFlow` (D2 v1 既存形を不変利用)

```rust
use gaussian_job_shared::entities::workflow::{JobFlow, Job, JobEdge, JobSpec, JobId, Program};
use slurm_async_runner::entities::slurm::SlurmJobConfig;

JobFlow {
    uuid:        Uuid::now_v7(),
    created_at:  Utc::now(),
    work_dir:    root.join(uuid.to_string()),       // ← v3: root 引数から導出
    tags:        { /* [flow.tags] + ("calc_type", value) if [flow].calc_type present */ },
    jobs: BTreeMap {
        JobId::from("opt__compound=0__method=0") => Job {
            spec: JobSpec {
                program: Program::from("g16"),            // D2 newtype
                config:  SlurmJobConfig::default(),       // SP-3 が埋める
                body:    String::new(),                   // SP-3 が render
            },
            parents: vec![ /* JobEdge { from: JobId, kind: DependencyType } */ ],
        },
        ...
    },
}
```

run + search に必要な情報のみ:
- run: `jobs[*].spec.body` + `jobs[*].spec.config` で sbatch、`jobs[*].parents` で依存ワイヤリング、`work_dir` でディレクトリ
- search: `tags` (flow-level) + `jobs[*].spec.program` (per-job) + `created_at` + `uuid`

### 7.2 `ExperimentPlan` (最小 sidecar)

```rust
use gaussian_job_shared::entities::workflow::JobId;

pub struct ExperimentPlan {
    pub jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>,  // params のみ
}
```

`plan.toml` 永続化形:

```toml
[jobs."opt__compound=0__method=0"]
route = "# B3LYP/6-31G* opt"
nproc = 16

[jobs."opt__compound=0__method=1"]
route = "# M06-2X/6-31G* opt"
nproc = 16
```

**v1 (本セクションでは Python 実装に対する v1) からの差分:**
- `plan_version` / `flow_uuid` / `source_hash` 撤廃
- `PlanEntry` 構造体撤廃、`params` のみフラットに
- `source_step_id` / `axis_combo` 撤廃 (JobId パースで導出)
- `tags` 撤廃

### 7.3 FS レイアウト

```
<root>/                                # PathResolver.root = expand_experiment の root 引数
└── <flow.uuid>/                       # = JobFlow.work_dir
    ├── flow.toml                      # JobFlow (D2 v1)
    ├── plan.toml                      # ExperimentPlan
    ├── experiment.toml                # 入力 TOML のコピー (再展開時の元データ)
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

## 8. Rust モジュール構成

### 8.1 ディレクトリレイアウト

```
src/
├── grammar/
│   ├── mod.rs                  # re-exports (expand_experiment, ExperimentSource, ...)
│   ├── source.rs               # data: ExperimentSource, FlowMeta, AxisDef, AxisValues, RawStep, ParentRef
│   ├── reader.rs               # parse_experiment: TOML bytes/path → ExperimentSource (strict + legacy detect)
│   ├── placeholder.rs          # ${...} の lex + expand (string 値内のみ、$$ escape)
│   ├── sweep.rs                # expand_sweeps: ExperimentSource → list<ExpandedStep>
│   ├── jobid.rs                # JobId 生成 + 文字種/予約名 validate + parse_job_id
│   ├── chain.rs                # resolve_parents: list<ExpandedStep> → JobEdge 配線 + DAG cycle check
│   └── build.rs                # to_jobflow_and_plan: list<ResolvedStep> + FlowMeta + root → (JobFlow, ExperimentPlan)
├── plan/
│   ├── mod.rs                  # ExperimentPlan
│   └── io.rs                   # read_plan / write_plan (atomic rename)
├── path.rs                     # MODIFY: plan_toml(), experiment_toml() getter 追加
├── error.rs                    # MODIFY: GrammarError variant 群追加
└── py_export/
    ├── grammar.rs              # expand_experiment pyfunction
    └── plan.rs                 # ExperimentPlan pyclass (read-only view)
```

### 8.2 主要型シグネチャ

#### grammar/source.rs

```rust
use gaussian_job_shared::entities::workflow::{Program, CalcType};
use slurm_async_runner::entities::slurm::DependencyType;

#[derive(Debug, Clone)]
pub struct ExperimentSource {
    pub flow: FlowMeta,
    pub axes: Vec<AxisDef>,                   // 宣言順
    pub steps: Vec<RawStep>,                  // 宣言順
}

#[derive(Debug, Clone, Default)]
pub struct FlowMeta {
    pub calc_type: Option<CalcType>,          // D2 newtype 直 (v3)
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
    Struct {
        fields: Vec<String>,
        rows: Vec<BTreeMap<String, toml::Value>>,
    },
}

#[derive(Debug, Clone)]
pub struct RawStep {
    pub id: String,                           // step.id は grammar-only。JobId と別概念。
    pub program: Program,                     // D2 newtype 直 (v3)
    pub sweep: Vec<String>,
    pub parents: Vec<ParentRef>,
    pub params: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone)]
pub struct ParentRef {
    pub id: String,                           // step.id 参照 (展開前)
    pub fanout: bool,
    pub reduce_over: Vec<String>,
    pub kind: DependencyType,                 // A1 不変
}
```

注: `step.id` (grammar の入力概念) は `JobId` (展開後の job 識別子) と別概念のため、`RawStep.id`/`ParentRef.id` は `String`。展開後の job 識別子は `JobId` (D2)。

#### grammar/jobid.rs

```rust
/// step.id (`[A-Za-z0-9_-]+`、予約名禁止) の検証。OK なら入力をそのまま返す。
pub fn validate_step_id(s: &str) -> Result<&str, JobManagerError>;

/// JobId 全体の検証 (文字種 `[A-Za-z0-9_\-=]+`、予約名禁止)
pub fn validate_job_id(s: &str) -> Result<&str, JobManagerError>;

/// JobId 文字列を組み立てる (axis_combo 順序保証)。D2 newtype 包装は呼び側 (`JobId::from(...)`)。
pub fn build_job_id(source_step_id: &str, axis_combo: &[(&str, usize)]) -> String;

/// 借用ベースの parse (alloc なし)。`&JobId.0` または string literal を渡す。
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError>;

pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,    // 順序保証 (= step.sweep 宣言順)
}
```

#### grammar/sweep.rs

```rust
use gaussian_job_shared::entities::workflow::{JobId, Program};

#[derive(Debug, Clone)]
pub(crate) struct ExpandedStep {
    pub job_id: JobId,                        // D2 newtype 直 (v3)
    pub program: Program,                     // D2 newtype 直 (v3)
    pub sweep: Vec<String>,
    pub axis_combo: BTreeMap<String, usize>,
    pub params: BTreeMap<String, toml::Value>,
    pub parents_raw: Vec<ParentRef>,
}

pub(crate) fn expand_sweeps(src: &ExperimentSource) -> Result<Vec<ExpandedStep>, JobManagerError>;
```

#### grammar/chain.rs

```rust
use gaussian_job_shared::entities::workflow::{JobId, JobEdge, Program};

pub(crate) fn resolve_parents(
    src: &ExperimentSource,
    expanded: Vec<ExpandedStep>,
) -> Result<Vec<ResolvedStep>, JobManagerError>;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStep {
    pub job_id: JobId,                        // D2 newtype 直
    pub program: Program,                     // D2 newtype 直
    pub params: BTreeMap<String, toml::Value>,
    pub parents: Vec<JobEdge>,                // D2 既存
}
```

#### grammar/build.rs

```rust
use std::path::Path;
use gaussian_job_shared::entities::workflow::JobFlow;

pub(crate) fn to_jobflow_and_plan(
    flow_meta: &FlowMeta,
    resolved: &[ResolvedStep],
    root: &Path,                              // ← v3: work_dir 導出のため
) -> (JobFlow, ExperimentPlan);
```

#### grammar/mod.rs (公開 API)

```rust
use std::path::Path;
use gaussian_job_shared::entities::workflow::JobFlow;

/// `experiment.toml` → `(JobFlow, ExperimentPlan)`。pure (I/O は toml_path 読込のみ)。
///
/// `root` は `JobFlow.work_dir = root.join(uuid.to_string())` のため。
/// ディスクへの書き込みは呼び側責務。
pub fn expand_experiment(
    toml_path: &Path,
    root: &Path,
) -> Result<(JobFlow, ExperimentPlan), JobManagerError>;
```

### 8.3 エラー型 (拡張)

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

## 9. Python API (PyO3)

```python
from job_manager import (
    expand_experiment,       # (toml_path: str, root: str) -> tuple[JobFlow, ExperimentPlan]
    ExperimentPlan,          # read-only view
    read_plan,               # (path: str) -> ExperimentPlan
    write_plan,              # (path: str, plan: ExperimentPlan) -> None
    parse_job_id,            # (job_id_str: str) -> dict
)
from job_manager import PathResolver           # SP-1
from gaussian_job_shared import JobFlow, JobId # D2 newtype (v3: import そのまま使う)

flow, plan = expand_experiment("./experiment.toml", "/work_dir")

# JobFlow / Plan の関係:
for job_id, job in flow.jobs.items():
    print(job_id, job.spec.program)        # JobId / Program (D2 newtype)
    params = plan.jobs[job_id]              # params for SP-3 bash render
    parts = parse_job_id(str(job_id))    # JobId は文字列化して渡す (pythonize 経由)
    print(parts["source_step_id"], parts["axis_combo"])

# 永続化は呼び側
resolver = PathResolver("/work_dir")
from job_manager import write_flow         # SP-1
write_flow(resolver.flow_toml(flow.uuid), flow)
write_plan(resolver.plan_toml(flow.uuid), plan)
```

**設計判断:**
- `expand_experiment` は sync (TOML パース・展開は CPU 軽い・I/O 無し以外は引数 path 読込のみ)
- `ExperimentPlan` は `jobs: dict[JobId | str, dict[str, Any]]` のみ公開 (pythonize で TOML Value を Python dict に変換、key は D2 の `PyJobId` または `str`)
- `parse_job_id` は Python から JobId 文字列を構成要素に分解する helper
- D2 の `PyJobId` / `PyProgram` / `PyCalcType` pyclass は D2 側で既にエクスポート済み (Pyclass Single Owner rule に従い、job-manager は再エクスポートしない)

---

## 10. テスト計画

### 10.1 Unit tests (Rust, `#[cfg(test)]`)

- `placeholder.rs`: `${a}` / `${a.b}` / `$${literal}` 正常 + malformed
- `reader.rs`: 最小 valid / unknown key reject / legacy 8 パターン / axis scalar/struct/mixed
- `sweep.rs`: sweep 空 / 多軸 product / placeholder 展開 / 全 axis 型
- `chain.rs`: pair / fanout / reduce_over の正常 + error / kind バリエーション / DAG cycle
- `jobid.rs`: 命名規約 / 予約名 reject / 文字種 reject / `parse_job_id` round-trip / `build_job_id` ↔ `parse_job_id` 整合
- `build.rs`: JobFlow.uuid v7 / JobFlow.work_dir = root + uuid / tags merge (`[flow].calc_type` + `[flow.tags]`) / calc_type 重複 error

### 10.2 Integration tests (`tests/`)

- end-to-end `expand_experiment("fixtures/minimal.toml", tempdir)` で JobFlow + plan 完全生成
- 大きめ (axes 3x2x2 = 12, steps 3) で graph 構造の確認
- `flow.toml` + `plan.toml` を tempdir に永続化し、read 戻して同型確認
- JobFlow + plan + parse_job_id の三者整合性 (各 JobId のソース step が plan に存在)

### 10.3 Python tests (`python/tests/`)

- `expand_experiment` の戻り値型 (D2 の `PyJobFlow`, `PyJobId` etc.)
- `parse_job_id` の Python 呼び出し
- 各 fixture (minimal / sweep / parent / multi-parent / error)
- 例外ラップ (Rust の `JobManagerError::Grammar*` が Python 例外に変換)

### 10.4 fixture (`tests/fixtures/`)

- `minimal_step.toml`: 1 step / no sweep / no parents
- `single_axis.toml`: 1 axis × 1 step (sweep)
- `pair_chain.toml`: 2 step, pair_by_axes
- `fanout.toml`: parent axes ⊂ child axes
- `reduce.toml`: parent axes ⊃ child axes
- `multi_parent.toml`: 1 child, 2 parents
- `legacy_*.toml`: 各 legacy 形状 (rejection)
- `error_*.toml`: 各 validation error (rejection)

### 10.5 カバレッジ目標

`cargo llvm-cov --fail-under-lines 80` で 80%+。

---

## 11. リスクと未決事項

| 項目 | リスク | 対応 |
|---|---|---|
| D2 newtype 利用箇所での変換コスト | `String` ↔ `JobId` の `From` 変換が散在 | newtype は `pub struct JobId(pub String)` で `.0` 取り出し可、変換は無視できるコスト |
| `toml::Value` の serde round-trip | `params` を `BTreeMap<String, toml::Value>` で持ち plan.toml に書き戻す際の互換性 | integration test で round-trip 検証 |
| `parse_job_id` の重複コスト | search 時に多数の JobId をパース | 借用ベース API `JobIdParts<'a>` で alloc 削減 |
| Placeholder lex 性能 | 大規模 `params` で遅い可能性 | 1-pass scanner 手書き、`regex` 依存追加せず |
| DAG cycle 検出のメモリ | O(V+E) で十分 | `petgraph` 依存追加せず手書き |
| `gaussian-experiment-manager` (Python ref) との互換 | 入力 TOML schema は変わる (compounds/sweep_over/parent 廃止) | legacy detection で migration hint を返す |
| common.toml 統合の遅延 | SP-3 まで `SlurmJobConfig::default()` で空 body | 既知の段取り、SP-3 spec で扱う |
| axis values の type | TOML の Date/DateTime 等を `${...}` 展開時にどう扱うか | string/int/float/bool に制限。それ以外は `WrongType` |
| Pyclass Single Owner rule の遵守 | D2 の `PyJobId`/`PyProgram`/`PyCalcType` を job-manager が誤って再定義 | job-manager の `Cargo.toml` で D2 の `pyo3` feature をパス依存上で無効化 (SP-1 と同じ規約) |

---

## 12. 完了基準

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
- [ ] **D2 (`gaussian-job-shared2`) に対する変更が一切無いことを diff で確認**

## 13. 次工程

SP-2 完了後 (SP-3 で行う):
- `common.toml` 読み込み + `SlurmJobConfig` 合成
- `JobSpec.body` の bash render
- A1 `SbatchManager` 経由の `submit_chain` 相当
- CLI: `run` / `submit` / `show` / `tick` / `search`

SP-2 設計 v3 が承認されたら writing-plans skill で実装計画書 v3 (単一 phase) に変換する。
