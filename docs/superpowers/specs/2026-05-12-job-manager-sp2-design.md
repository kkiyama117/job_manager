# job-manager SP-2 (grammar) 設計

- **Date**: 2026-05-12
- **Status**: Draft (brainstorming 完了、レビュー待ち)
- **Targets**: `crate::grammar::*` + `crate::plan::*` (Rust) / `job_manager._job_manager_core.grammar` (Python)
- **Subproject**: SP-2 of 3 — grammar (`experiment.toml` → `(JobFlow, ExperimentPlan)`)
- **References**:
  - SP-1 spec: `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md` (データ層、FS レイアウト確立)
  - Python リファレンス: `../../../gaussian-experiment-manager/src/gaussian_experiment_manager/grammar/` (reader/sweep/chain/source)
  - 上流 (D2): `../../../gaussian-job-shared2/` (`JobFlow` / `Job` / `JobSpec` / `JobEdge` / `JobId` / `Program` / `CalcType`)
  - 上流 (A1): `../../../slurm-async-runner2/` (`DependencyType`, `SlurmJobConfig`)

---

## 1. 背景

SP-1 で確立した「データ層 + 並列走査 + tick」基盤の上に、ユーザー入力 `experiment.toml` を `JobFlow` (D2) + `ExperimentPlan` (job-manager 自前 sidecar) に展開する **grammar 層**を構築する。SP-3 (submit + CLI) はこの 2 ファイル (`flow.toml`, `plan.toml`) を入力として bash 生成・sbatch 投入を行う。

### 1.1 Python 実装 (リファレンス) の課題

`gaussian-experiment-manager/grammar/` のコード読みで顕在化した問題:

1. **`step.compounds` が first-class** — `compounds: list[str]` がスキーマに組み込まれ、Gaussian 専用設計になっている。SP-1 spec §1.1 #7 で program-agnostic 化を要求済み。
2. **axis element の reserved key (`compounds`, `tags`) が暗黙** — `[[sweep]] axis = [{...}, ...]` の inline table dict の中で `compounds` と `tags` だけが特別扱いされる。ユーザーは特殊キーリストを暗記する必要がある。さらに cross-axis の `compounds` collision は黙って "last wins" になる (`sweep.py:78` の TODO コメント)。
3. **`step.parent: str | None` が単一文字列** — 真の DAG (fan-in 多親) を表現できない。reduction-sweep でしか多親に到達できない。
4. **parent 解決が set 比較の暗黙ディスパッチ** — `parent.sweep_over` と `child.sweep_over` の集合関係 (==, ⊂, ⊃) で pair / fanout / reduce が自動選択される。コードを動かさないと意図が読めない (`chain.py:62-72`)。
5. **`step.id: str | None` が optional** — `'<no-id>'` フォールバックがエラーメッセージに混入し、ユーザー診断を難しくする (`sweep.py:42` 等)。
6. **`step.calc_type` が per-step** — JobFlow には CalcType フィールドが無く、Python ref 自体でも使い道不明瞭。step ごとに重複情報を書く負債。
7. **`${axis.field}` 展開が string-typed のみ** — int/float の axis field を string params に注入すると `str(v)` で型を失う。診断は `_PLACEHOLDER_RE` の存在チェックのみ。
8. **SLURM dependency kind が `afterok` 固定** — `DependencyType` enum (`Afterok`, `Afterany`, `Aftercorr`, `After`, `AfterNotOk`, `Singleton`) のうち 1 つだけしか使えない。

### 1.2 SP-2 のスコープ

| 含める | 含めない (SP-3) |
|---|---|
| `experiment.toml` のパース (strict、unknown key 拒否) | `common.toml` (cluster/account-level config) のマージ |
| Legacy `gaussian_batch.toml` 形状の検出 + エラー | bash body の rendering (`#SBATCH` block + 本文) |
| `[[axis]]` sweep 展開 (itertools.product 相当) | `SlurmJobConfig` の partition/account/time-limit 等の合成 |
| `${axis}` / `${axis.field}` プレースホルダ展開 | sbatch 投入 (A1 `SbatchManager` 経由) |
| 親 (`parents = [...]`) の解決 (pair / fanout / reduce) | CLI コマンド (`run`/`submit`/...) |
| `JobFlow` (D2 形) 構築 (uuid v7 生成、body/config は空) | `flow.toml` / `plan.toml` のディスク書き込み (SP-1 の `write_flow` + 新規 `write_plan` は提供するが、`expand_experiment` 自体は pure) |
| `ExperimentPlan` (job-manager 自前 sidecar) 構築 | log_paths 解決 (SLURM `%j`/`%x` 展開) |
| validation (予約 JobId、JobId 文字種、duplicate、parent 整合性) | β-adapter / `gaussian_batch_cli` 互換 |

### 1.3 サブプロジェクト位置付け

```
SP-1 (データ層, 完)   ←── SP-2 (grammar, 本spec)   ←── SP-3 (submit + CLI)
                              │
                              └── 完了後の判断ポイント: common.toml integration を SP-3 に押し込むか、
                                  独立 SP-2.5 として切るか
```

---

## 2. 採用アプローチ: **Approach A — Pure-Rust grammar + sidecar `plan.toml`**

### 2.1 比較した 3 案

| 比較項目 | A (採用) | B: tags 詰め込み | C: Python 側で expand |
|---|---|---|---|
| TOML パース | Rust serde + 手書き validation | Rust serde | Python tomllib |
| Sweep / parent 解決 | Rust (純粋関数) | Rust (純粋関数) | Python |
| compounds/params/calc_type の所在 | `plan.toml` (型安全な sidecar) | `JobFlow.tags` に namespaced key (`job.<id>.calc_type = "..."` 等) | Python メモリ内 |
| D2 スキーマ変更 | 不要 | 不要 | 不要 |
| 予約名衝突リスク | 小 (`plan.toml` は新規ファイル) | 大 (`JobFlow.tags` の `BTreeMap<String,String>` flat 構造、衝突防止コスト) | 中 |
| SP-3 (bash render) が読みやすい | ✅ 専用構造体 + serde | ⚠️ tags の string パース | ❌ Python ↔ Rust 境界跨ぎ多発 |
| 型情報の保持 | ✅ `params: BTreeMap<String, toml::Value>` で保持 | ❌ 文字列化 | ✅ Python dict |
| 再実装の動機 (Pure-Rust pipeline) 適合 | ✅ | ✅ | ❌ |

**判断:**
- C は再実装の動機 (Pure-Rust pipeline 化、SP-1 と一貫した型安全 I/O) を満たさない。
- B は `JobFlow.tags` の flat `BTreeMap<String, String>` に nested 構造を詰めるため、エスケープ規約・衝突回避ルールが新たに必要。さらに axis combo の int を string 化することで型情報が失われる。
- **A**: D2 を一切触らず、SP-2 が自前所有する `ExperimentPlan` 構造体を新規導入。`plan.toml` は `<root>/<flow.uuid>/` に `flow.toml` と並べる (SP-1 で確立した FS レイアウトの兄弟ファイル)。SP-3 は `flow.toml` (グラフ) + `plan.toml` (per-job semantic 情報) を読んで bash を render する。

### 2.2 案 A の設計判断

- **D2 は不変** — `JobFlow` / `Job` / `JobSpec` / `JobEdge` への PR は出さない。per-job semantic 情報は全て `plan.toml` に分離。
- **`expand_experiment(path) -> (JobFlow, ExperimentPlan)` は純粋関数。** ディスク書き込みは呼び側 (SP-3 / CLI / テスト) の責務。
- **`JobFlow.body` と `JobSpec.config` は SP-2 時点では空** (`String::new()` / `SlurmJobConfig::default()`)。SP-3 が `plan.toml` の `params` + `common.toml` を merge して埋める。
- **JobFlow uuid は v7** (SP-1 と一貫)。`created_at` は展開時刻。
- **`JobFlow.work_dir` は SP-1 規約 = `<root>/<flow.uuid>/`** で `expand_experiment` の呼び側が `PathResolver` から得て渡す形にする (grammar は work_dir を知らない: `expand_experiment(toml_path, work_dir)` で受ける)。

---

## 3. `experiment.toml` Schema 仕様

### 3.1 全体構造

```toml
# 最上位許可キー: flow, axis, step のみ。strict (unknown key reject)。
[flow]                                    # 任意 block (省略時 uuid と created_at のみ生成)
calc_type = "opt+freq+td"                 # → JobFlow.tags["calc_type"]
tags      = { project = "tddft" }         # → JobFlow.tags にマージ (calc_type と衝突 → error)

[[axis]]                                  # 軸定義 (0 以上)。step 間で共有。
name   = "compound"
values = ["benzene", "toluene"]           # list<str> = scalar axis

[[axis]]
name   = "method"
values = [                                # list<table> = struct axis
    { name = "b3lyp", route = "B3LYP" },
    { name = "m062x", route = "M06-2X" },
]

[[step]]                                  # ステップ定義 (1 以上)。
id      = "opt"                           # 必須・unique・JobId 文字種制約あり
program = "g16"                           # 必須
sweep   = ["compound", "method"]          # 任意 (空: スカラジョブ)
parents = []                              # 任意

[step.params]                             # 任意 dict<str, toml::Value> (型保持)
route = "# ${method.route}/6-31G* opt"

[step.tags]                               # 任意 dict<str, str> (${...} 展開後 plan.toml に保存)
method = "${method.name}"
```

### 3.2 `[flow]` block

```toml
[flow]
calc_type = "opt+freq+td"        # 任意。CalcType (D2). 文字列。
tags      = { ... }              # 任意。BTreeMap<String, String>.
```

- 省略時は `[flow]` block 自体無しでよい。
- `tags` 内のキーが `"calc_type"` と衝突したら error (`calc_type` は最上位フィールドに昇格されているため、tags 内で同名禁止)。
- 値はすべて string。non-string 値はエラー。

### 3.3 `[[axis]]` block

```toml
[[axis]]
name   = "method"                # 必須。axis 名 (step.sweep から参照)。
values = [ ... ]                 # 必須。空リスト禁止。
```

- `name` の文字種: `[A-Za-z_][A-Za-z0-9_]*` (identifier-like、`${name}` 展開で曖昧性回避)
- `values` の型:
  - **scalar axis**: `values: list<string>`
    - `${<name>}` で要素文字列を展開
    - `${<name>.<field>}` はエラー (scalar に field 概念無し)
  - **struct axis**: `values: list<table>`
    - 全要素の table が同じキー集合を持つ必要 (validate; 集合違いはエラー)
    - 全要素のキー値は string/int/float/bool のいずれか (table/array 不可)
    - `${<name>.<field>}` で `<field>` の値を展開 (非 string なら `Display` で文字列化)
    - `${<name>}` (no field) はエラー (struct axis では曖昧)
  - **mixed (string と table の混在)**: エラー
- duplicate `name` はエラー。
- 同じ `axis.name` を `[flow.tags]` の placeholder 名と衝突させても問題ない (展開コンテキストが異なる)。

### 3.4 `[[step]]` block

```toml
[[step]]
id      = "opt"                        # 必須・unique・JobId 文字種
program = "g16"                        # 必須。Program (D2).
sweep   = ["compound", "method"]       # 任意。axis name のリスト。未定義 axis 参照でエラー。
parents = [ ... ]                      # 任意。後述。

[step.params]                          # 任意。dict<str, toml::Value>. ${...} は string 値内のみ展開。
[step.tags]                            # 任意。dict<str, str>. ${...} 展開後 plan.toml.tags に保存。
```

- `id` 文字種: `[A-Za-z0-9_\-]+` (path-safe)
- `id` 予約名禁止: `flow`, `plan`, `experiment`, `derived`, `status` (= `<flow.uuid>/<JobId>/` の sibling として置かれるファイル/ディレクトリ stem との衝突回避)
- `id` の duplicate (別 step 間) はエラー
- `sweep` の要素は `[[axis]]` で定義済みの name に限る (未定義参照は error)
- `sweep` の要素 duplicate はエラー
- `params` の値: TOML 標準の `string`/`int`/`float`/`bool`/`array<...>`/`table` をそのまま保持。`${...}` 展開は **string 値の中のみ**。配列要素やネスト table 内の string も再帰的に展開する。

#### 3.4.1 `step.parents`

```toml
parents = [
    { id = "opt" },                                   # pair_by_axes (default)
    { id = "preflight", fanout = true },              # 1:N
    { id = "scan", reduce_over = ["theta"] },         # N:1
    { id = "opt", kind = "afterany" },                # SLURM dependency kind 上書き
]
```

各要素の schema:

| Field | Type | Default | 意味 |
|---|---|---|---|
| `id` | `string` | (必須) | 参照先 step.id |
| `fanout` | `bool` | `false` | true = 親軸が子軸の真部分集合と validate |
| `reduce_over` | `list<string>` | `[]` | 非空 = 親軸 = 子軸 ∪ reduce_over と validate |
| `kind` | `string` | `"afterok"` | SLURM `DependencyType` (`afterok` / `afterany` / `aftercorr` / `after` / `afternotok` / `singleton`) |

**Mode 決定ルール (field 有無で意図表明):**

| `fanout` | `reduce_over` | Mode | Validation |
|---|---|---|---|
| `false` | `[]` | **pair_by_axes** | parent.sweep == child.sweep (順序無視・集合一致) |
| `true` | `[]` | **fanout** | parent.sweep ⊊ child.sweep |
| `false` | 非空 | **reduce_over** | parent.sweep == child.sweep ∪ reduce_over 、`reduce_over ⊆ parent.sweep`、`reduce_over ∩ child.sweep == ∅` |
| `true` | 非空 | **error** | `BothFanoutAndReduce` |

#### 3.4.2 Legacy 形状の検出

Python 版と同等の検出を行い、移行メッセージを返す:

- 最上位に `[gaussian_input]` block が存在 → `LegacyToml { hint: "see gaussian-experiment-manager → SP-2 migration notes" }`
- `[env].compound_id` または `[env].project_base` が存在 → 同上
- `[[sweep]]` block が存在 → `LegacyToml { hint: "[[sweep]] was renamed to [[axis]] in SP-2" }`
- `step.compounds` フィールドが存在 → `LegacyToml { hint: "step.compounds was removed; declare an [[axis]] name=\"compound\" instead" }`
- `step.calc_type` フィールドが存在 → `LegacyToml { hint: "step.calc_type was moved to [flow].calc_type" }`
- `step.parent` (単数) フィールドが存在 → `LegacyToml { hint: "step.parent was renamed to step.parents (list)" }`
- `step.sweep_over` フィールドが存在 → `LegacyToml { hint: "step.sweep_over was renamed to step.sweep" }`

これらは `UnknownKey` よりも先に検出して具体的メッセージを返す (UX 改善)。

### 3.5 Placeholder syntax `${...}`

```
${ident}                  # scalar axis 参照
${ident.ident}            # struct axis field 参照
```

- 文字種: `${[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?}`
- 展開対象: **string 型 TOML 値の中のみ**。`step.params`, `step.tags`, ネスト string 値 (e.g. `params.list = ["${a}", "${b}"]` の各要素) を再帰的に走査。
- 値が int/float/bool の場合は `Display` で文字列化。
- 未定義の axis 名参照 → `PlaceholderUnknownAxis`
- 未定義の field 参照 → `PlaceholderUnknownField`
- scalar axis に `.field` 参照 → `PlaceholderInvalidScalarField`
- struct axis に no-field 参照 → `PlaceholderAmbiguousStructAxis`
- 同 step が `sweep` していない axis 名の参照 → `PlaceholderAxisNotInSweep`
- placeholder syntax 自体の malformed (例: `${.foo}`, `${foo.bar.baz}`) → `PlaceholderSyntaxError`

エスケープ: `$${...}` で literal `${...}` (二重 $ 一個に縮約)。lex 段で先に処理。

---

## 4. JobId 命名規約 (決定論的)

### 4.1 形式

```
<step.id>                                  # sweep 空のとき
<step.id>__<axis1>=<idx>__<axis2>=<idx>    # sweep のとき (axis 順 = step.sweep 宣言順)
```

例 (step.id="opt", sweep=["compound", "method"], compound 3 件 × method 2 件):
- `opt__compound=0__method=0`
- `opt__compound=0__method=1`
- `opt__compound=1__method=0`
- ...

### 4.2 文字種・予約名

- 許可文字: `[A-Za-z0-9_\-=]+` (axis index の `=` も許可)
- step.id 自体の許可文字は `[A-Za-z0-9_\-]+` (`=` は予約)
- 予約 JobId: `flow`, `plan`, `experiment`, `derived`, `status` (= file/dir basename 衝突回避)
- duplicate JobId は不可能なはず (step.id unique × axis combo の deterministic 生成) だが、念のため build 段階で `BTreeMap::insert` の重複検出を error にする

### 4.3 axis combo の正準化

axis index は `usize` (0-origin、`[[axis]] values` の出現順)。これにより:
- 同じ `experiment.toml` から再生成すれば同一 JobId が生まれる (再現性)
- axis values の **順序を入れ替えると JobId が変わる**。これは仕様。ユーザーが axis values を unstable に並べることはない前提。

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
                tags: expand_placeholders(step.tags, expansion_ctx),
                parents_raw: step.parents,  # 解決は次フェーズ
            }
```

### 5.2 placeholder 展開コンテキスト

step 内で参照可能な axis = `step.sweep` に含まれるもののみ。他の step の sweep にしか登場しない axis を参照すると `PlaceholderAxisNotInSweep`。

### 5.3 順序保証

`step` 出現順、各 step 内では axis 宣言順の product (最後の axis が最速回転)。SP-3 が依存しても破れないように仕様として固定。

---

## 6. Parent 解決セマンティクス

### 6.1 全体フロー

```
expanded: list<ExpandedStep>                              # 5.1 で確定
step_index_by_id: BTreeMap<step.id, list<expanded idx>>   # source_step_id でグルーピング

for child in expanded:
    for parent_ref in child.parents_raw:
        let parent_step = lookup(parent_ref.id)            # 未定義 → UnknownStepId
        let parents_expanded = step_index_by_id[parent_step.id]
        let edges = resolve_edges(parent_ref, parent_step, child, parents_expanded)
        child.parents.extend(edges)                        # JobEdge { from: JobId, kind: DependencyType }
```

### 6.2 3 modes の解決ロジック

#### pair_by_axes (default: `fanout=false, reduce_over=[]`)

```
validate parent.sweep_set == child.sweep_set    # 集合一致 (順序無視)
for parent_e in parents_expanded:
    # axis_combo の共通 axis すべてで同値ならエッジ
    if all(parent_e.axis_combo[ax] == child_e.axis_combo[ax] for ax in parent.sweep):
        emit JobEdge { from: parent_e.job_id, kind: parent_ref.kind }
```

#### fanout (`fanout=true, reduce_over=[]`)

```
validate parent.sweep_set ⊊ child.sweep_set
for parent_e in parents_expanded:
    if all(parent_e.axis_combo[ax] == child_e.axis_combo[ax] for ax in parent.sweep):
        emit JobEdge { from: parent_e.job_id, kind: parent_ref.kind }
# child の 1 つに対して parent は 1 つ (parent.sweep を完全一致させる) — 1:N
```

#### reduce_over (`reduce_over=非空`)

```
validate parent.sweep_set == child.sweep_set ∪ set(reduce_over)
validate set(reduce_over) ⊆ parent.sweep_set
validate set(reduce_over) ∩ child.sweep_set == ∅
for parent_e in parents_expanded:
    # parent の axis_combo のうち child と共有する軸が一致するなら集約対象
    if all(parent_e.axis_combo[ax] == child_e.axis_combo[ax] for ax in child.sweep):
        emit JobEdge { from: parent_e.job_id, kind: parent_ref.kind }
# child の 1 つに対して parent は len(reduce_axis_1) * len(reduce_axis_2) * ... 個 — N:1
```

### 6.3 Field-presence vs Mode-string の選択理由

採用: **field-presence ベース** (`fanout=true` / `reduce_over=[...]`)。

| 軸 | field-presence | mode 文字列 |
|---|---|---|
| 共通ケースの記述量 | `{id="x"}` (簡潔) | `{id="x", mode="pair_by_axes"}` 必須 or default で結局 implicit |
| 意図表明 | フィールド名 (`fanout`, `reduce_over`) が直接意図を語る | enum 文字列 (`"fanout"`) で abstract |
| grep | `grep 'fanout'` / `grep 'reduce_over'` | `grep 'mode = "fanout"'` |
| typo 検出 | strict schema で `fan_out` を rejected unknown key | strict schema で `mode = "fanot"` を rejected enum |
| 拡張性 | 新モード (`cartesian = true` 等) を flag 追加で表現 | enum variant を追加 |
| 同時指定エラー | `fanout=true` + `reduce_over=[...]` → 明示 error | enum なので構造的に不可能 |

採用判断: field-presence が共通ケースで最短かつ、フィールド名が直接意味を語る点を重視。同時指定の不正は明示的 error として扱う。

### 6.4 Validation 全列挙

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
| `DagHasCycle` | 構築後の DAG にサイクル (= 自分自身を遠回しに祖先に持つ) |

DAG cycle 検出は `parents` 解決完了後に Kahn's algorithm で実行する。

---

## 7. 出力アーティファクト

### 7.1 `JobFlow` (D2 形)

```rust
JobFlow {
    uuid:        Uuid::now_v7(),
    created_at:  Utc::now(),
    work_dir:    /* 呼び側が PathResolver から渡す */,
    tags:        { /* [flow.tags] + ("calc_type", value) if [flow].calc_type present */ },
    jobs: BTreeMap {
        JobId("opt__compound=0__method=0") => Job {
            spec: JobSpec {
                program: Program("g16"),
                config:  SlurmJobConfig::default(),   // SP-3 が埋める
                body:    String::new(),               // SP-3 が render
            },
            parents: vec![ /* JobEdge {from, kind} */ ],
        },
        ...
    },
}
```

### 7.2 `ExperimentPlan` (job-manager 自前)

```rust
pub struct ExperimentPlan {
    pub plan_version: u32,                       // = 1 (schema バージョニング)
    pub flow_uuid: Uuid,                         // クロスチェック用
    pub source_hash: String,                     // experiment.toml の sha256 (再展開検出)
    pub jobs: BTreeMap<JobId, PlanEntry>,
}

pub struct PlanEntry {
    pub source_step_id: String,
    pub axis_combo: BTreeMap<String, usize>,     // axis_name -> index
    pub params: BTreeMap<String, toml::Value>,   // ${...} 展開済み
    pub tags: BTreeMap<String, String>,          // ${...} 展開済み (step.tags のみ; flow.tags は JobFlow に)
}
```

`plan.toml` 永続化形:

```toml
plan_version = 1
flow_uuid    = "0193a8c0-7a4f-7c1e-9a3b-1234567890ab"
source_hash  = "sha256:abc123..."

[jobs."opt__compound=0__method=0"]
source_step_id = "opt"
axis_combo     = { compound = 0, method = 0 }
[jobs."opt__compound=0__method=0".params]
route = "# B3LYP/6-31G* opt"
[jobs."opt__compound=0__method=0".tags]
method = "b3lyp"
```

### 7.3 FS レイアウト (SP-1 の拡張)

```
<root>/                                # PathResolver.root
└── <flow.uuid>/                       # = JobFlow.work_dir (SP-1 規約)
    ├── flow.toml                      # JobFlow (SP-1 で確立)
    ├── plan.toml                      # NEW (SP-2): ExperimentPlan
    ├── experiment.toml                # NEW (SP-2): 入力 TOML のコピー (再展開時の照合用)
    └── <JobId>/                       # 各 Job のディレクトリ (SP-1)
        ├── .status.toml               # SP-1
        ├── input.gjf                  # SP-3 担当
        ├── batch.bash                 # SP-3 担当
        └── slurm-*.out                # SLURM 直書き
```

`plan.toml` と `experiment.toml` の sibling 配置は **規約**。`PathResolver` に `plan_toml(uuid)` / `experiment_toml(uuid)` getter を追加する。

---

## 8. Rust モジュール構成

### 8.1 ディレクトリレイアウト

```
src/
├── grammar/
│   ├── mod.rs                  # re-exports (expand_experiment, ExperimentSource, ExpandedStep, ...)
│   ├── source.rs               # data: ExperimentSource, FlowMeta, AxisDef, AxisValues, RawStep, ParentRef
│   ├── reader.rs               # parse_experiment: TOML bytes/path → ExperimentSource (strict + legacy detect)
│   ├── placeholder.rs          # ${...} の lex + expand (string 値内のみ、$$ escape 対応)
│   ├── sweep.rs                # expand_sweeps: ExperimentSource → list<ExpandedStep>
│   ├── jobid.rs                # JobId 生成 + 文字種/予約名 validate
│   ├── chain.rs                # resolve_parents: list<ExpandedStep> → JobEdge 配線 + DAG cycle check
│   └── build.rs                # to_jobflow_and_plan: list<ResolvedStep> + FlowMeta + work_dir
│                               #     → (JobFlow, ExperimentPlan)
├── plan/
│   ├── mod.rs                  # ExperimentPlan, PlanEntry, plan_version 定数
│   └── io.rs                   # read_plan / write_plan (atomic rename, SP-1 と同じ pattern)
├── path.rs                     # MODIFY: plan_toml(), experiment_toml() getter 追加
├── error.rs                    # MODIFY: GrammarError variant 群追加
└── py_export/
    ├── grammar.rs              # parse_experiment / expand_experiment pyfunctions
    └── plan.rs                 # ExperimentPlan / PlanEntry pyclass (read_only view)
```

### 8.2 主要型シグネチャ

#### grammar/source.rs

```rust
#[derive(Debug, Clone)]
pub struct ExperimentSource {
    pub flow: FlowMeta,
    pub axes: Vec<AxisDef>,                   // 宣言順
    pub steps: Vec<RawStep>,                  // 宣言順
}

#[derive(Debug, Clone, Default)]
pub struct FlowMeta {
    pub calc_type: Option<CalcType>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct AxisDef {
    pub name: String,
    pub values: AxisValues,
}

#[derive(Debug, Clone)]
pub enum AxisValues {
    Scalar(Vec<String>),                      // ${name} で展開
    Struct {
        fields: Vec<String>,                  // schema 一致を保つために抽出
        rows: Vec<BTreeMap<String, toml::Value>>,
    },
}

#[derive(Debug, Clone)]
pub struct RawStep {
    pub id: String,
    pub program: Program,
    pub sweep: Vec<String>,                   // axis name list (宣言順)
    pub parents: Vec<ParentRef>,
    pub params: BTreeMap<String, toml::Value>,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ParentRef {
    pub id: String,
    pub fanout: bool,
    pub reduce_over: Vec<String>,             // 空ベクトル = 未使用
    pub kind: DependencyType,                 // default: Afterok (reader が埋める)
}
```

#### grammar/sweep.rs

```rust
/// 中間表現: 展開後・親未解決
#[derive(Debug, Clone)]
pub struct ExpandedStep {
    pub job_id: JobId,
    pub source_step_id: String,
    pub program: Program,
    pub sweep: Vec<String>,                   // 親解決時に必要
    pub axis_combo: BTreeMap<String, usize>,
    pub params: BTreeMap<String, toml::Value>,
    pub tags: BTreeMap<String, String>,
    pub parents_raw: Vec<ParentRef>,          // 解決前 (chain.rs で消費)
}

pub fn expand_sweeps(src: &ExperimentSource) -> Result<Vec<ExpandedStep>, JobManagerError>;
```

#### grammar/chain.rs

```rust
/// 中間表現に JobEdge を生やして返す (DAG cycle check 込み)
pub fn resolve_parents(
    src: &ExperimentSource,
    expanded: Vec<ExpandedStep>,
) -> Result<Vec<ResolvedStep>, JobManagerError>;

#[derive(Debug, Clone)]
pub struct ResolvedStep {
    pub job_id: JobId,
    pub source_step_id: String,
    pub program: Program,
    pub axis_combo: BTreeMap<String, usize>,
    pub params: BTreeMap<String, toml::Value>,
    pub tags: BTreeMap<String, String>,
    pub parents: Vec<JobEdge>,                // 確定
}
```

#### grammar/build.rs

```rust
pub fn to_jobflow_and_plan(
    src: &ExperimentSource,
    resolved: &[ResolvedStep],
    work_dir: PathBuf,
    source_hash: String,
) -> (JobFlow, ExperimentPlan);
```

#### grammar/mod.rs (公開 API)

```rust
/// パイプライン全体: TOML path → (JobFlow, ExperimentPlan)
pub fn expand_experiment(
    toml_path: &Path,
    work_dir: PathBuf,
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

    #[error("axis '{name}' struct values have inconsistent fields: row 0 has {first:?}, row {row} has {other:?}")]
    StructAxisFieldMismatch { name: String, first: Vec<String>, row: usize, other: Vec<String> },

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

    #[error("parent ref for '{0}': reduce_over coverage mismatch, expected parent.sweep == child.sweep ∪ reduce_over")]
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
    DagHasCycle(Vec<JobId>),

    #[error("reserved job id '{0}'")]
    ReservedJobId(String),
}
```

---

## 9. Python API (PyO3)

```python
from job_manager import (
    expand_experiment,       # (toml_path: str, work_dir: str) -> tuple[JobFlow, ExperimentPlan]
    ExperimentPlan,          # read-only view
    PlanEntry,
    read_plan,               # (path: str) -> ExperimentPlan
    write_plan,              # (path: str, plan: ExperimentPlan) -> None
)
from job_manager import PathResolver       # SP-1
from gaussian_job_shared import JobFlow    # D2 (pyclass single owner)

resolver = PathResolver("/work_dir")
flow, plan = expand_experiment("./experiment.toml", str(resolver.root()))

# flow は D2 の JobFlow pyclass (re-export). plan は job-manager 自前.
for job_id, entry in plan.jobs.items():
    print(job_id, entry.source_step_id, entry.params)

# 永続化は呼び側
from job_manager import write_flow         # SP-1
write_flow(resolver.flow_toml(flow.uuid), flow)
write_plan(resolver.plan_toml(flow.uuid), plan)
```

**設計判断:**
- `JobFlow` は D2 の pyclass を返すだけ (Pyclass Single Owner)。SP-2 は wrapper 作らない。
- `ExperimentPlan` / `PlanEntry` は read-only な view pyclass。`params` フィールドは `pythonize` で Python dict に変換 (TOML Value を素直に変換)。
- `expand_experiment` は sync 関数 (TOML パース・展開は CPU 軽い・I/O 無し)。

---

## 10. テスト計画

### 10.1 Unit tests (Rust, `#[cfg(test)]`)

- `placeholder.rs`:
  - `${a}` / `${a.b}` / `$$ {literal}` のレキシング正常系
  - malformed (`${`, `${.}`, `${a.b.c}`) を error
- `reader.rs`:
  - 最小 valid (`[[step]]` 1 つ) パース
  - unknown top-level key reject
  - legacy 形状検出 (各種パターン 7 種)
  - axis values の scalar / struct / mixed
- `sweep.rs`:
  - sweep 空のときに ExpandedStep 1 つ
  - 2 軸 3×2 で 6 個生成、順序保証
  - placeholder 展開 (各 axis 型)
- `chain.rs`:
  - pair_by_axes (axes 一致時 / 不一致時 / 順序入れ替え)
  - fanout (proper subset / 同集合 → error / 非包含 → error)
  - reduce_over (一致 / coverage 不足 → error / child と intersect → error)
  - `kind` のバリエーション (`afterok`, `afterany`, default 補完)
  - DAG cycle 検出 (A→B→C→A)
- `jobid.rs`:
  - 命名規約 (`opt__compound=0__method=1`)
  - 予約名 reject
  - 文字種 reject
- `build.rs`:
  - JobFlow.uuid が v7
  - JobFlow.tags に [flow].calc_type + [flow.tags] が merge される
  - calc_type と tags.calc_type の衝突 → error
  - work_dir が正しく入る

### 10.2 Integration tests (`tests/`)

- end-to-end `expand_experiment("fixtures/minimal.toml", tmp)` で JobFlow + plan 完全生成
- 大きめ (axes 3x2x2 = 12, steps 3) で graph 構造の確認
- `flow.toml` + `plan.toml` を tempdir に永続化し、read 戻して同型確認
- experiment.toml → plan.toml → experiment.toml の意味的不変条件 (source_hash 検証)

### 10.3 Python tests (`python/tests/`)

- `expand_experiment` の戻り値型 (JobFlow / ExperimentPlan)
- 各 fixture (minimal / sweep / parent / multi-parent / error cases)
- 例外ラップ (Rust の `JobManagerError::Grammar*` が Python の `ExperimentGrammarError` に変換)

### 10.4 fixture

`tests/fixtures/` 配下:
- `minimal_step.toml`: 1 step / no sweep / no parents
- `single_axis.toml`: 1 axis × 1 step (sweep)
- `pair_chain.toml`: 2 step, pair_by_axes
- `fanout.toml`: parent axes ⊂ child axes
- `reduce.toml`: parent axes ⊃ child axes with explicit `reduce_over`
- `multi_parent.toml`: 1 child, 2 parents
- `legacy_*.toml`: 各 legacy 形状 (rejection 確認)
- `error_*.toml`: 各 validation error (rejection 確認)

### 10.5 カバレッジ目標

`cargo llvm-cov --fail-under-lines 80` で 80%+。

---

## 11. リスクと未決事項

| 項目 | リスク | 対応 |
|---|---|---|
| `toml::Value` の serde 経由 round-trip | `params` を一旦 `BTreeMap<String, toml::Value>` で持ち plan.toml に書き戻すとき、TOML が `inline table` 等を保つか | `toml = "1.1"` の `Value::serialize` を信頼 + integration test で round-trip 検証 |
| placeholder の lex 性能 | 単純な正規表現マッチで実装する場合、大規模 `params` で遅い可能性 | 1-pass scanner を手書きで実装。`regex` を依存に追加しない |
| DAG cycle 検出のメモリ | O(V+E) Kahn で十分 (V = JobId 数、E = JobEdge 数)。実用範囲 ~1000 jobs | `petgraph` 依存を追加しない (cycle 検出だけなら手書きで足りる) |
| plan.toml と flow.toml の不整合検出 | ユーザーが片方だけ編集する可能性 | `plan_version` + `flow_uuid` + `source_hash` で再展開時に detect → warning |
| `source_hash` の安定性 | TOML の文字エンコーディング / 改行で hash が変わる | 正規化せず raw bytes で sha256 — 再展開検出の目的に十分 |
| `ExperimentPlan` の Python 公開 | `pythonize` で Python dict 変換するが、双方向 (Python dict → Plan) は非対応 | SP-2 では read-only。`write_plan` も Rust 側から渡す前提 |
| common.toml 統合の遅延 | SP-3 まで `SlurmJobConfig::default()` のままなので bash 生成テストが空 body になる | SP-2 完了時点で SP-3 spec を起こす段取りを project memory に残す |
| axis values の type | TOML の Date/DateTime 等を `${...}` 展開時にどう扱うか | SP-2 では string/int/float/bool に制限。それ以外は `WrongType` |
| `compounds` axis のユーザー慣習 | Python 版経験者は `step.compounds` を書きがち | legacy detection で明示エラー + `[[axis]] name="compound"` migration hint |

---

## 12. 完了基準

- [ ] `cargo build --all-features` 成功
- [ ] `cargo test --lib` 成功 (カバレッジ 80%+)
- [ ] `cargo clippy -- -D warnings` 成功
- [ ] `cargo fmt --check` 成功
- [ ] `uv run maturin develop` 成功
- [ ] `uv run pytest python/tests` 成功
- [ ] `cargo run --bin stub_gen` で `.pyi` 再生成、`ruff format` クリーン
- [ ] `expand_experiment` を 12-job fixture (3 step × 2×2 axis) で実行、JobFlow + plan の構造確認
- [ ] 全 validation error path の Python テストが green
- [ ] legacy detection が 7 種類すべてで適切な hint 文字列を返す

## 13. 次工程

SP-2 完了後:
- **SP-3 (submit + CLI)**:
  - `common.toml` 読み込み + `SlurmJobConfig` 合成
  - `JobSpec.body` の bash render (template engine 選定: minijinja / 手書き / askama)
  - `flow.toml` + `plan.toml` の永続化レイヤ (一部は SP-1 既存 + SP-2 の `write_plan` で完成)
  - A1 `SbatchManager` 経由の `submit_chain` 相当
  - CLI: `run` / `submit` / `show` / `tick` / `search`
- **判断ポイント**: SP-2 で `common.toml` を扱う代替案 (SP-2.5 として切る) が浮上したらここで再検討。

SP-2 設計が承認されたら writing-plans skill で実装計画書に変換します。
