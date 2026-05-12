# job-manager SP-2 (grammar) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `experiment.toml` → `(JobFlow, ExperimentPlan)` の Pure-Rust grammar 層を `crate::grammar::*` + `crate::plan::*` として実装する。D2 (`gaussian-job-shared2`) は **newtype 不可侵** — `JobId` / `Program` / `CalcType` / `Job` / `JobEdge` / `JobSpec` を D2 から import して使う。**`JobFlow.work_dir` フィールドのみ撤廃** (redundant — `<root>/<uuid>/` で導出可)。

**Architecture:** 3 段 PR スタック。**Phase 0** で D2 から `JobFlow.work_dir` フィールドのみを撤廃 (newtype は保持)。**Phase 1** で job-manager SP-1 コードの `flow.work_dir` 参照を `PathResolver::flow_dir(uuid)` に置換。**Phase 2** で SP-2 grammar を実装し、`expand_experiment(toml_path)` 純粋関数を公開する。

**Tech Stack:** Rust 2024, PyO3 0.28 (abi3-py312), tokio 1.0, serde + toml 1.1, chrono, uuid v7, pythonize, rstest, pyo3-stub-gen。Python 3.12+, pytest, maturin, ruff。

**Spec:** `docs/superpowers/specs/2026-05-12-job-manager-sp2-design.md` (v4)

---

## PR Stack & 実行順序

```
gaussian-job-shared2 repo:
  Phase 0 PR:  refactor!: drop JobFlow.work_dir field
               base=main, head=refactor/drop-jobflow-work-dir
  ↓ (merged)
job-manager repo:
  Phase 1 PR:  refactor(sp1): adopt D2 v4 (drop work_dir references)
               base=main, head=refactor/sp1-drop-work-dir
  ↓ (merged)
job-manager repo:
  Phase 2 PR:  feat(sp2): grammar layer (experiment.toml → JobFlow + plan)
               base=main, head=feat/sp2-impl
```

各 Phase は前 Phase の merge を待つ。Phase 0 / Phase 1 のスコープは v2 提案より大幅に小さい (newtype 撤廃を取りやめ、work_dir のみ)。

---

## D2 利用ポリシー (newtype 保持、不要フィールドのみ撤廃)

| D2 型 / フィールド | 判定 | 用途・撤廃理由 |
|---|---|---|
| `JobId` newtype | **保持** | 展開後の job 識別子 (job-manager 側で再定義禁止) |
| `Program` newtype | **保持** | step.program / JobSpec.program |
| `CalcType` newtype | **保持** | [flow].calc_type のドメイン型 (FlowMeta で利用) |
| `Job` / `JobEdge` / `JobSpec` | **保持** | run + search で全フィールド load-bearing |
| `JobFlow.uuid` | **保持** | identity / search key |
| `JobFlow.created_at` | **保持** | 時系列ソート |
| `JobFlow.work_dir` | **Phase 0 で撤廃** | `<root>/<uuid>/` で導出可、`mv` で drift リスク |
| `JobFlow.tags` | **保持** | search (`tags["calc_type"]` 含む) |
| `JobFlow.jobs` | **保持** | DAG 本体 |

D2 の `pyo3` feature をパス依存上で無効化する規約 (Pyclass Single Owner rule) は SP-1 と同じ。

---

## File Structure Overview

### Phase 0 (D2 repo `../gaussian-job-shared2/`)

```
src/
├── entities/workflow.rs         # MODIFY: drop work_dir field + test fixture updates
└── py_export/entities/workflow/
    └── mod.rs                   # MODIFY: drop work_dir pyclass getter/setter
```

### Phase 1 (job-manager `src/`)

```
src/
├── flow_io.rs                   # MODIFY: drop work_dir in test fixtures
├── view.rs                      # MODIFY: flow.work_dir → resolver.flow_dir(uuid)
├── walk.rs                      # MODIFY: drop work_dir in test fixtures
└── (他、work_dir 参照箇所を grep で網羅)
```

### Phase 2 (job-manager `src/grammar/` + `src/plan/`)

```
src/
├── error.rs                     # MODIFY: add GrammarError variants
├── path.rs                      # MODIFY: add plan_toml() + experiment_toml() getter
├── grammar/
│   ├── mod.rs                   # CREATE: pub use + expand_experiment pipeline
│   ├── source.rs                # CREATE: ExperimentSource/FlowMeta/AxisDef/AxisValues/RawStep/ParentRef
│   ├── placeholder.rs           # CREATE: ${...} lex + recursive expand
│   ├── jobid.rs                 # CREATE: validate / parse / build helpers
│   ├── reader.rs                # CREATE: TOML strict parse + legacy detect
│   ├── sweep.rs                 # CREATE: expand_sweeps (itertools.product)
│   ├── chain.rs                 # CREATE: resolve_parents + Kahn cycle check
│   └── build.rs                 # CREATE: to_jobflow_and_plan
├── plan/
│   ├── mod.rs                   # CREATE: ExperimentPlan
│   └── io.rs                    # CREATE: read_plan / write_plan (atomic rename)
├── lib.rs                       # MODIFY: re-export grammar + plan
└── py_export/
    ├── mod.rs                   # MODIFY: register sub-modules
    ├── grammar.rs               # CREATE: expand_experiment pyfunction + parse_job_id
    └── plan.rs                  # CREATE: ExperimentPlan / PlanEntry pyclass

tests/
├── fixtures/                    # CREATE: TOML fixtures (minimal/sweep/parent/legacy/error)
└── integration_grammar.rs       # CREATE: end-to-end tests

python/
├── job_manager/__init__.py      # MODIFY: re-exports
└── tests/test_grammar.py        # CREATE: Python E2E
```

---

# Phase 0: D2 PR — drop `JobFlow.work_dir` (in `../gaussian-job-shared2/`)

**Branch (in D2 repo):** `refactor/drop-jobflow-work-dir`
**Target PR base:** `main` of `gaussian-job-shared2`
**Scope:** `JobFlow` から `work_dir: PathBuf` フィールドを撤廃するのみ。newtype 等は一切変更しない。

---

### Task P0.1: D2 — ブランチ作成 + 影響範囲スキャン

**Files:** none

- [ ] **Step 1: D2 repo へ移動して main の状態を確認**

Run: `cd ../gaussian-job-shared2 && git status && git log --oneline -3`

Expected: `On branch main, working tree clean` + 直近 3 件のコミット。

- [ ] **Step 2: 作業ブランチを切る**

Run: `git checkout -b refactor/drop-jobflow-work-dir`

- [ ] **Step 3: work_dir 参照箇所を網羅 grep**

Run: `grep -rn "work_dir" src/ tests/ Cargo.toml README.md 2>/dev/null`

Expected: `src/entities/workflow.rs` 内の field 定義・test、`src/py_export/entities/workflow/mod.rs` の pyclass getter/setter、README 等。すべて控える。

---

### Task P0.2: D2 — `JobFlow.work_dir` フィールド撤廃

**Files:**
- Modify: `src/entities/workflow.rs`

- [ ] **Step 1: 失敗テスト追加** (work_dir が無くても roundtrip できる)

`src/entities/workflow.rs` の `mod tests` に:

```rust
#[test]
fn job_flow_v4_has_no_work_dir() {
    // v4: work_dir フィールド撤廃確認。コンパイル時点で field が無いことを assert。
    let flow = JobFlow {
        uuid: Uuid::nil(),
        created_at: Utc.with_ymd_and_hms(2026, 5, 12, 0, 0, 0).unwrap(),
        tags: BTreeMap::new(),
        jobs: BTreeMap::new(),
    };
    let s = toml::to_string(&flow).unwrap();
    assert!(!s.contains("work_dir"), "TOML must not contain work_dir: {s}");
}
```

Run: `cargo test --lib job_flow_v4_has_no_work_dir 2>&1 | tail -5`

Expected: 失敗 (`work_dir: PathBuf` 必須フィールドが struct literal に無い)。

- [ ] **Step 2: フィールド削除**

`src/entities/workflow.rs` の `JobFlow` 定義から:

```rust
    /// Working directory: `<work_dir>/<JobId>/` is each Job's folder.
    /// TaskManager creates these and writes the rendered `.bash` etc.
    pub work_dir: PathBuf,
```

の 3 行を削除。`use std::path::PathBuf;` も他で使われていなければ削除。

- [ ] **Step 3: 既存テストの fixture を更新**

`empty_flow()` ヘルパーから `work_dir: PathBuf::from("/tmp/flow"),` 行を削除。
`job_flow_empty_jobs_roundtrip` 内の `assert_eq!(back.work_dir, flow.work_dir);` 行を削除。
`job_flow_duplicate_jobid_rejected_at_deserialize` の TOML literal から `work_dir = "/tmp/flow"` 行を削除。

- [ ] **Step 4: テスト通過確認**

Run: `cargo test --lib 2>&1 | tail -10`

Expected: 全 pass。

- [ ] **Step 5: コミット**

```bash
git add src/entities/workflow.rs
git commit -m "refactor!: drop JobFlow.work_dir field

work_dir is derivable from <root>/<uuid>/ via downstream PathResolver,
and persisting it created a drift risk on directory moves. JobFlow
becomes a pure run/search metadata container with no location info.

BREAKING: downstream code that read flow.work_dir must switch to
PathResolver::flow_dir(&flow.uuid) (see job-manager SP-1 follow-up PR)."
```

---

### Task P0.3: D2 — pyclass getter/setter 撤廃

**Files:**
- Modify: `src/py_export/entities/workflow/mod.rs`

- [ ] **Step 1: pyclass の work_dir 関連を削除**

`PyJobFlow` の `work_dir` getter/setter (もし `#[getter]` / `#[setter]` で work_dir を扱う method があれば) を削除。`__new__` から `work_dir` 引数も削除。

- [ ] **Step 2: D2 でビルド + テスト**

Run: `cargo build --all-features 2>&1 | tail -5 && cargo test --all-features 2>&1 | tail -10`

Expected: 全 pass。

- [ ] **Step 3: コミット**

```bash
git add src/py_export/
git commit -m "refactor!: drop PyJobFlow work_dir getter/setter

Mirrors the upstream JobFlow.work_dir removal."
```

---

### Task P0.4: D2 — clippy + fmt + 全テスト

- [ ] **Step 1: cargo fmt**

Run: `cargo fmt && cargo fmt --check`

Expected: 整形差分なし。

- [ ] **Step 2: cargo clippy**

Run: `cargo clippy --all-features -- -D warnings 2>&1 | tail -10`

Expected: 警告 0。

- [ ] **Step 3: 全テスト**

Run: `cargo test --all-features 2>&1 | tail -10`

Expected: 全 pass。

---

### Task P0.5: D2 — PR 作成

- [ ] **Step 1: push + PR**

```bash
git push -u origin refactor/drop-jobflow-work-dir
gh pr create --base main --title "refactor!: drop JobFlow.work_dir field" --body "$(cat <<'EOF'
## Summary

\`JobFlow.work_dir: PathBuf\` フィールドを撤廃する破壊的変更。

## Why

- 永続化値が \`<root>/<uuid>/\` 規約から導出可能 (redundant)
- \`mv\` 等のディレクトリ移動で drift するリスクがある
- \`JobFlow\` を run/search に純粋なメタデータコンテナとして整理 (場所情報は PathResolver に集約)

## Scope

- \`JobFlow\` struct から \`work_dir\` フィールドを削除
- pyclass \`PyJobFlow\` の対応する getter/setter / \`__new__\` 引数を削除
- 既存テスト fixture を更新

newtype (\`JobId\` / \`Program\` / \`CalcType\` / etc.) は変更しない。

## Migration

Downstream (job-manager) 側の \`flow.work_dir\` 参照は
\`PathResolver::flow_dir(&flow.uuid)\` 経由に置換する (別 PR で対応)。

## Test plan

- [ ] cargo test --all-features 通過
- [ ] cargo clippy --all-features -- -D warnings 通過
- [ ] cargo fmt --check 通過
EOF
)"
```

- [ ] **Step 2: merge 完了まで待つ** (review + CI green)

- [ ] **Step 3: job-manager repo に戻る**

Run: `cd ../job-manager && git status`

Expected: SP-2 plan ブランチに戻り、本 plan ファイルがあること。

---

# Phase 1: SP-1 follow-up — `work_dir` 参照置換 (job-manager `refactor/sp1-drop-work-dir` branch, base=main)

**前提:** Phase 0 PR が D2 main に merged 済み。
**Scope:** job-manager SP-1 既存コード内の `flow.work_dir` 参照を `PathResolver::flow_dir(&flow.uuid)` 経由に置換するのみ。型 (`JobId`/`Program` 等) は変更しない。

---

### Task P1.1: Phase 1 — ブランチ作成 + 失敗箇所の確認

**Files:** none

- [ ] **Step 1: main を最新化 + Cargo.toml の D2 依存を最新版にする**

```bash
git checkout main
git pull origin main
# Cargo.toml の gaussian-job-shared 依存が path = "../gaussian-job-shared2"
# になっていれば、Phase 0 merge 済みの main を反映する。tag/branch 固定なら適宜 bump。
cargo build 2>&1 | tail -10
```

Expected: ビルドが work_dir 関連で失敗 — 失敗メッセージが Phase 1 の作業対象。

- [ ] **Step 2: 作業ブランチ**

Run: `git checkout -b refactor/sp1-drop-work-dir`

- [ ] **Step 3: work_dir 参照箇所を grep**

Run: `grep -rn "work_dir\|\.work_dir" src/ tests/ python/ 2>/dev/null`

Expected: SP-1 既存コード (`view.rs` / `flow_io.rs` test fixture / `walk.rs` test fixture / 他) で参照箇所がリストされる。

---

### Task P1.2: Phase 1 — work_dir 参照を `PathResolver::flow_dir(&uuid)` に置換

**Files:**
- Modify: `src/view.rs`
- Modify: `src/flow_io.rs` (test fixture)
- Modify: `src/walk.rs` (test fixture)
- Modify: 他、grep で見つかった箇所すべて

- [ ] **Step 1: `src/view.rs` の `flow.work_dir` 参照を置換**

`flow.work_dir.join(...)` 形式の使用箇所を `resolver.flow_dir(&flow.uuid).join(...)` または同等表現に置換。`PathResolver` インスタンスが API 経由で渡されていない場合は、関数シグネチャに `resolver: &PathResolver` を追加するのではなく `root: &Path` を取り `root.join(uuid.to_string())` で十分なら最小修正に留める (call site が増えるなら resolver 受け取る方が一貫)。

- [ ] **Step 2: `src/flow_io.rs` の test fixture**

`JobFlow { ... work_dir: ..., ... }` の struct literal から `work_dir: ...` 行を削除。期待 TOML から `work_dir = "..."` 行を削除。

- [ ] **Step 3: `src/walk.rs` の test fixture**

同上。

- [ ] **Step 4: 残り参照箇所**

Task P1.1 Step 3 の grep 結果すべてを上記方針で置換。`PathBuf` import が不要になったファイルは整理。

- [ ] **Step 5: ビルド確認**

Run: `cargo build 2>&1 | tail -10`

Expected: 成功 (work_dir 参照ゼロ)。

- [ ] **Step 6: 全テスト**

Run: `cargo test --all-features 2>&1 | tail -10`

Expected: 全 pass。

- [ ] **Step 7: コミット**

```bash
git add src/
git commit -m "refactor(sp1): drop work_dir references after D2 v4 PR

D2 から JobFlow.work_dir が撤廃されたため、SP-1 内で flow.work_dir を
参照していた箇所を PathResolver::flow_dir(&flow.uuid) ベースに置換。
機能変更なし、型変更なし。"
```

---

### Task P1.3: Phase 1 — stub 再生成 + fmt/clippy

- [ ] **Step 1: stub_gen**

Run: `cargo run --bin stub_gen --features stub_gen 2>&1 | tail -5 && uv run ruff format python/job_manager/_job_manager_core/__init__.pyi`

Expected: `.pyi` から PyJobFlow の work_dir エントリが消えていることを diff で確認。

- [ ] **Step 2: cargo fmt + clippy**

Run: `cargo fmt && cargo clippy --all-features -- -D warnings 2>&1 | tail -10`

Expected: 警告 0、整形差分なし。

- [ ] **Step 3: Python テスト**

Run: `uv run maturin develop 2>&1 | tail -3 && uv run pytest python/tests 2>&1 | tail -10`

Expected: 全 pass。SP-1 テストが work_dir 不在で壊れていれば修正。

- [ ] **Step 4: コミット (stub のみ)**

```bash
git add python/job_manager/_job_manager_core/__init__.pyi
git commit -m "chore(stubs): regenerate .pyi after work_dir removal"
```

---

### Task P1.4: Phase 1 — PR 作成

- [ ] **Step 1: push + PR**

```bash
git push -u origin refactor/sp1-drop-work-dir
gh pr create --base main --title "refactor(sp1): adopt D2 v4 (drop JobFlow.work_dir references)" --body "$(cat <<'EOF'
## Summary

D2 v4 (\`refactor!: drop JobFlow.work_dir field\`) merge を追跡し、
job-manager SP-1 既存コード内の \`flow.work_dir\` 参照を
\`PathResolver::flow_dir(&flow.uuid)\` 経由に置換する。

## Scope

- 機能変更なし
- 型変更なし (JobId / Program / CalcType / 他は不変)
- work_dir 参照のみ置換
- test fixture の \`work_dir: ...\` 行を削除
- 再生成済み \`.pyi\` を含む

## Test plan

- [ ] cargo test --all-features 通過
- [ ] cargo clippy --all-features -- -D warnings 通過
- [ ] uv run pytest python/tests 通過
EOF
)"
```

- [ ] **Step 2: merge 完了まで待つ**

---

# Phase 2: SP-2 grammar (job-manager `feat/sp2-impl` branch, base=main)

**前提:** Phase 0 (D2 v4 PR) と Phase 1 (SP-1 follow-up) が main にマージ済み。
**Scope:** SP-2 grammar 実装本体。

---

### Task 1: ブランチ作成 + Cargo.toml 確認

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: main を最新化してブランチ作成**

```bash
git checkout main
git pull origin main
git checkout -b feat/sp2-impl
```

- [ ] **Step 2: Cargo.toml の依存を確認**

Run: `grep -E "^(toml|serde|chrono|uuid|tempfile)" Cargo.toml`

Expected: `toml = "1.1"`, `serde = ...`, `chrono = ...`, `uuid = ...` が既にあり、新規依存は不要。

- [ ] **Step 3: ビルド確認**

Run: `cargo build 2>&1 | tail -3`

Expected: 成功。

---

### Task 2: src/error.rs — Grammar* variants 追加

**Files:**
- Modify: `src/error.rs`
- Modify: `src/py_export/error.rs`

- [ ] **Step 1: 失敗テストを追加**

`src/error.rs` 末尾の `mod tests` に:

```rust
#[test]
fn grammar_unknown_key_variant_carries_location() {
    let err = JobManagerError::UnknownKey {
        key: "compounds".to_string(),
        location: "/path/to/experiment.toml: [[step]]".to_string(),
    };
    let msg = err.to_string();
    assert!(msg.contains("compounds"));
    assert!(msg.contains("[[step]]"));
}

#[test]
fn grammar_legacy_toml_variant_carries_hint() {
    let err = JobManagerError::LegacyToml {
        path: PathBuf::from("/tmp/x.toml"),
        hint: "step.compounds was removed".to_string(),
    };
    assert!(err.to_string().contains("step.compounds was removed"));
}

#[test]
fn grammar_dag_cycle_lists_jobs() {
    let err = JobManagerError::DagHasCycle(vec!["a".to_string(), "b".to_string()]);
    let msg = err.to_string();
    assert!(msg.contains("a"));
    assert!(msg.contains("b"));
}
```

- [ ] **Step 2: テスト失敗確認**

Run: `cargo test --lib error::tests::grammar 2>&1 | tail -10`

Expected: コンパイルエラー (variant 未定義)。

- [ ] **Step 3: variant を `JobManagerError` に追加**

`src/error.rs` の `pub enum JobManagerError { ... }` に追加 (既存 variant の後):

```rust
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
```

- [ ] **Step 4: テスト通過確認**

Run: `cargo test --lib error::tests::grammar 2>&1 | tail -5`

Expected: `test result: ok`.

- [ ] **Step 5: `src/py_export/error.rs` で Python 例外マッピングを更新**

新 variant を `PyValueError` にまとめてマッピング。既存の `match` arm に追加:

```rust
JobManagerError::GrammarTomlParse { .. }
| JobManagerError::LegacyToml { .. }
| JobManagerError::UnknownKey { .. }
| JobManagerError::MissingKey { .. }
| JobManagerError::WrongType { .. }
| JobManagerError::DuplicateAxis(_)
| JobManagerError::EmptyAxis(_)
| JobManagerError::MixedAxisValues { .. }
| JobManagerError::StructAxisFieldMismatch { .. }
| JobManagerError::DuplicateStepId(_)
| JobManagerError::InvalidStepId(_)
| JobManagerError::UnknownAxisRef { .. }
| JobManagerError::DuplicateSweepAxis { .. }
| JobManagerError::UnknownStepId(_, _)
| JobManagerError::SelfParent(_)
| JobManagerError::BothFanoutAndReduce(_)
| JobManagerError::PairByAxesMismatch { .. }
| JobManagerError::FanoutNotProperSubset { .. }
| JobManagerError::ReduceCoverageMismatch(_)
| JobManagerError::UnknownDependencyKind(_)
| JobManagerError::PlaceholderAxisNotInSweep(_)
| JobManagerError::PlaceholderUnknownField(_, _)
| JobManagerError::PlaceholderInvalidScalarField(_)
| JobManagerError::PlaceholderAmbiguousStructAxis(_)
| JobManagerError::PlaceholderSyntaxError { .. }
| JobManagerError::DagHasCycle(_)
| JobManagerError::ReservedJobId(_)
| JobManagerError::FlowTagsHasCalcType => PyValueError::new_err(msg),
```

- [ ] **Step 6: コミット**

```bash
git add src/error.rs src/py_export/error.rs
git commit -m "feat(error): add SP-2 GrammarError variants

src/error.rs に grammar / legacy / validation / placeholder / DAG
cycle 用 variant を追加。py_export/error.rs で PyValueError マッピング。
SP-2 spec §9.3 に対応。"
```

---

### Task 3: src/grammar/source.rs — データ型を定義

**Files:**
- Create: `src/grammar/mod.rs`
- Create: `src/grammar/source.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: モジュールスケルトンを `src/grammar/mod.rs` に作る**

```rust
//! `experiment.toml` → `(JobFlow, ExperimentPlan)` の grammar 層。
//!
//! See `docs/superpowers/specs/2026-05-12-job-manager-sp2-design.md`.

pub mod source;
```

- [ ] **Step 2: `src/grammar/source.rs` を作成**

```rust
//! data: ExperimentSource / FlowMeta / AxisDef / AxisValues / RawStep / ParentRef.

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::{CalcType, Program};
use slurm_async_runner::entities::slurm::DependencyType;

#[derive(Debug, Clone)]
pub struct ExperimentSource {
    pub flow: FlowMeta,
    pub axes: Vec<AxisDef>,
    pub steps: Vec<RawStep>,
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
    Scalar(Vec<String>),
    Struct {
        fields: Vec<String>,
        rows: Vec<BTreeMap<String, toml::Value>>,
    },
}

#[derive(Debug, Clone)]
pub struct RawStep {
    pub id: String,                                 // step.id (grammar-only). JobId と別概念。
    pub program: Program,                           // D2 newtype を import 利用
    pub sweep: Vec<String>,
    pub parents: Vec<ParentRef>,
    pub params: BTreeMap<String, toml::Value>,
}

#[derive(Debug, Clone)]
pub struct ParentRef {
    pub id: String,                                 // step.id 参照 (展開前)
    pub fanout: bool,
    pub reduce_over: Vec<String>,
    pub kind: DependencyType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn experiment_source_holds_empty_collections() {
        let src = ExperimentSource {
            flow: FlowMeta::default(),
            axes: vec![],
            steps: vec![],
        };
        assert!(src.axes.is_empty());
        assert!(src.steps.is_empty());
    }

    #[test]
    fn axis_values_scalar_variant_carries_strings() {
        let v = AxisValues::Scalar(vec!["a".into(), "b".into()]);
        match v {
            AxisValues::Scalar(xs) => assert_eq!(xs.len(), 2),
            _ => panic!("expected scalar"),
        }
    }
}
```

- [ ] **Step 3: `src/lib.rs` で grammar モジュールを公開**

`src/lib.rs` の既存モジュール宣言群の隣に追加:

```rust
pub mod grammar;
```

- [ ] **Step 4: テスト実行**

Run: `cargo test --lib grammar::source:: 2>&1 | tail -5`

Expected: `test result: ok`.

- [ ] **Step 5: コミット**

```bash
git add src/grammar/ src/lib.rs
git commit -m "feat(grammar): add data types (ExperimentSource/AxisDef/RawStep/ParentRef)

SP-2 spec §9.2 source.rs に対応。データ構造のみ、I/O やロジックは
後続タスクで追加。"
```

---

### Task 4: src/grammar/placeholder.rs — `${...}` の lex + expand

**Files:**
- Create: `src/grammar/placeholder.rs`
- Modify: `src/grammar/mod.rs`

- [ ] **Step 1: 失敗するテスト + 実装を書く**

`src/grammar/placeholder.rs`:

```rust
//! ${ident}, ${ident.field} placeholder の 1-pass scanner + expander.
//! `$${...}` で literal `${...}` をエスケープする。

use std::collections::BTreeMap;

use crate::error::JobManagerError;

#[derive(Debug, Clone)]
pub enum AxisCtxValue {
    Scalar(String),
    Struct(BTreeMap<String, String>),
}

pub type AxisCtx = BTreeMap<String, AxisCtxValue>;

pub fn expand_placeholders(input: &str, ctx: &AxisCtx) -> Result<String, JobManagerError> {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'$' {
            out.push('$');
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let end = bytes[i + 2..].iter().position(|&c| c == b'}');
            let Some(end_off) = end else {
                return Err(JobManagerError::PlaceholderSyntaxError {
                    offset: i,
                    message: "unterminated ${...}".to_string(),
                });
            };
            let inner = &input[i + 2..i + 2 + end_off];
            let resolved = resolve_ref(inner, ctx, i)?;
            out.push_str(&resolved);
            i += 2 + end_off + 1;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Ok(out)
}

fn resolve_ref(inner: &str, ctx: &AxisCtx, offset: usize) -> Result<String, JobManagerError> {
    if inner.is_empty() {
        return Err(JobManagerError::PlaceholderSyntaxError {
            offset,
            message: "empty placeholder ${}".to_string(),
        });
    }
    let parts: Vec<&str> = inner.splitn(2, '.').collect();
    let axis = parts[0];
    if axis.is_empty() || !valid_ident(axis) {
        return Err(JobManagerError::PlaceholderSyntaxError {
            offset,
            message: format!("invalid axis identifier '{axis}'"),
        });
    }
    let Some(value) = ctx.get(axis) else {
        return Err(JobManagerError::PlaceholderAxisNotInSweep(axis.to_string()));
    };

    match (parts.len(), value) {
        (1, AxisCtxValue::Scalar(s)) => Ok(s.clone()),
        (1, AxisCtxValue::Struct(_)) => {
            Err(JobManagerError::PlaceholderAmbiguousStructAxis(axis.to_string()))
        }
        (2, AxisCtxValue::Scalar(_)) => {
            Err(JobManagerError::PlaceholderInvalidScalarField(axis.to_string()))
        }
        (2, AxisCtxValue::Struct(fields)) => {
            let field = parts[1];
            if field.is_empty() || !valid_ident(field) {
                return Err(JobManagerError::PlaceholderSyntaxError {
                    offset,
                    message: format!("invalid field identifier '{field}'"),
                });
            }
            fields
                .get(field)
                .cloned()
                .ok_or_else(|| JobManagerError::PlaceholderUnknownField(axis.into(), field.into()))
        }
        _ => unreachable!(),
    }
}

fn valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn expand_params(
    params: BTreeMap<String, toml::Value>,
    ctx: &AxisCtx,
) -> Result<BTreeMap<String, toml::Value>, JobManagerError> {
    let mut out = BTreeMap::new();
    for (k, v) in params {
        out.insert(k, expand_value(v, ctx)?);
    }
    Ok(out)
}

fn expand_value(v: toml::Value, ctx: &AxisCtx) -> Result<toml::Value, JobManagerError> {
    match v {
        toml::Value::String(s) => Ok(toml::Value::String(expand_placeholders(&s, ctx)?)),
        toml::Value::Array(arr) => {
            let new = arr
                .into_iter()
                .map(|x| expand_value(x, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(toml::Value::Array(new))
        }
        toml::Value::Table(t) => {
            let mut new = toml::value::Table::new();
            for (k, vv) in t {
                new.insert(k, expand_value(vv, ctx)?);
            }
            Ok(toml::Value::Table(new))
        }
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_scalar(name: &str, val: &str) -> AxisCtx {
        let mut c = AxisCtx::new();
        c.insert(name.to_string(), AxisCtxValue::Scalar(val.to_string()));
        c
    }

    fn ctx_struct(name: &str, fields: &[(&str, &str)]) -> AxisCtx {
        let mut c = AxisCtx::new();
        let mut m = BTreeMap::new();
        for (k, v) in fields {
            m.insert(k.to_string(), v.to_string());
        }
        c.insert(name.to_string(), AxisCtxValue::Struct(m));
        c
    }

    #[test]
    fn passthrough_no_placeholder() {
        let ctx = AxisCtx::new();
        assert_eq!(expand_placeholders("plain text", &ctx).unwrap(), "plain text");
    }

    #[test]
    fn scalar_axis_expands() {
        let ctx = ctx_scalar("name", "benzene");
        assert_eq!(expand_placeholders("c=${name}", &ctx).unwrap(), "c=benzene");
    }

    #[test]
    fn struct_axis_field_expands() {
        let ctx = ctx_struct("method", &[("name", "b3lyp"), ("route", "B3LYP")]);
        assert_eq!(
            expand_placeholders("# ${method.route}/6-31G* opt", &ctx).unwrap(),
            "# B3LYP/6-31G* opt"
        );
    }

    #[test]
    fn dollar_escape() {
        let ctx = AxisCtx::new();
        assert_eq!(expand_placeholders("$${name}", &ctx).unwrap(), "${name}");
    }

    #[test]
    fn unknown_axis_errors() {
        let ctx = ctx_scalar("name", "x");
        let e = expand_placeholders("${other}", &ctx).unwrap_err();
        assert!(matches!(e, JobManagerError::PlaceholderAxisNotInSweep(_)));
    }

    #[test]
    fn scalar_with_field_errors() {
        let ctx = ctx_scalar("name", "x");
        let e = expand_placeholders("${name.field}", &ctx).unwrap_err();
        assert!(matches!(e, JobManagerError::PlaceholderInvalidScalarField(_)));
    }

    #[test]
    fn struct_without_field_errors() {
        let ctx = ctx_struct("m", &[("name", "x")]);
        let e = expand_placeholders("${m}", &ctx).unwrap_err();
        assert!(matches!(e, JobManagerError::PlaceholderAmbiguousStructAxis(_)));
    }

    #[test]
    fn struct_unknown_field_errors() {
        let ctx = ctx_struct("m", &[("name", "x")]);
        let e = expand_placeholders("${m.missing}", &ctx).unwrap_err();
        assert!(matches!(e, JobManagerError::PlaceholderUnknownField(_, _)));
    }

    #[test]
    fn unterminated_errors() {
        let ctx = AxisCtx::new();
        let e = expand_placeholders("hello ${name", &ctx).unwrap_err();
        assert!(matches!(e, JobManagerError::PlaceholderSyntaxError { .. }));
    }

    #[test]
    fn empty_braces_errors() {
        let ctx = AxisCtx::new();
        let e = expand_placeholders("${}", &ctx).unwrap_err();
        assert!(matches!(e, JobManagerError::PlaceholderSyntaxError { .. }));
    }

    #[test]
    fn expand_value_handles_array_and_table_recursively() {
        let ctx = ctx_scalar("x", "X");
        let mut t = toml::value::Table::new();
        t.insert("nested".into(), toml::Value::String("${x}-y".into()));
        let v = toml::Value::Array(vec![
            toml::Value::String("${x}".into()),
            toml::Value::Table(t),
            toml::Value::Integer(42),
        ]);
        let out = expand_value(v, &ctx).unwrap();
        let arr = out.as_array().unwrap();
        assert_eq!(arr[0].as_str().unwrap(), "X");
        assert_eq!(arr[1].as_table().unwrap()["nested"].as_str().unwrap(), "X-y");
        assert_eq!(arr[2].as_integer().unwrap(), 42);
    }
}
```

- [ ] **Step 2: `src/grammar/mod.rs` で placeholder を公開**

```rust
pub mod source;
pub mod placeholder;
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib grammar::placeholder:: 2>&1 | tail -5`

Expected: `test result: ok. 11 passed`.

- [ ] **Step 4: コミット**

```bash
git add src/grammar/
git commit -m "feat(grammar): placeholder lexer + recursive expander

\${name} と \${name.field} の 1-pass scanner、\$\$ escape、配列・テーブル
ネストを再帰展開。SP-2 spec §4.5 に対応。"
```

---

### Task 5: src/grammar/jobid.rs — validate / parse / build helpers

**Files:**
- Create: `src/grammar/jobid.rs`
- Modify: `src/grammar/mod.rs`

- [ ] **Step 1: テスト + 実装を書く**

`src/grammar/jobid.rs`:

```rust
//! JobId (= String) の構築 / 検証 / パース helper。
//!
//! JobId 形式: `<step.id>` (sweep 空) or `<step.id>__<axis>=<idx>__...`

use crate::error::JobManagerError;

const RESERVED_JOB_IDS: &[&str] = &["flow", "plan", "experiment", "derived", "status"];

fn valid_step_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn valid_job_id_char(c: char) -> bool {
    valid_step_id_char(c) || c == '='
}

pub fn validate_step_id(s: &str) -> Result<&str, JobManagerError> {
    if s.is_empty() || !s.chars().all(valid_step_id_char) {
        return Err(JobManagerError::InvalidStepId(s.to_string()));
    }
    if RESERVED_JOB_IDS.contains(&s) {
        return Err(JobManagerError::ReservedJobId(s.to_string()));
    }
    Ok(s)
}

pub fn validate_job_id(s: &str) -> Result<&str, JobManagerError> {
    if s.is_empty() || !s.chars().all(valid_job_id_char) {
        return Err(JobManagerError::InvalidStepId(s.to_string()));
    }
    Ok(s)
}

pub fn build_job_id(source_step_id: &str, axis_combo: &[(&str, usize)]) -> String {
    if axis_combo.is_empty() {
        return source_step_id.to_string();
    }
    let mut s = String::with_capacity(source_step_id.len() + axis_combo.len() * 16);
    s.push_str(source_step_id);
    for (ax, idx) in axis_combo {
        s.push_str("__");
        s.push_str(ax);
        s.push('=');
        s.push_str(&idx.to_string());
    }
    s
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,
}

pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError> {
    if s.is_empty() {
        return Err(JobManagerError::InvalidStepId(String::new()));
    }
    let mut iter = s.split("__");
    let source_step_id = iter.next().expect("split yields >=1");
    validate_step_id(source_step_id)?;
    let mut axis_combo: Vec<(&str, usize)> = Vec::new();
    for piece in iter {
        let Some(eq_pos) = piece.find('=') else {
            return Err(JobManagerError::InvalidStepId(s.to_string()));
        };
        let (ax, idx_str) = piece.split_at(eq_pos);
        let idx_str = &idx_str[1..];
        let idx: usize = idx_str
            .parse()
            .map_err(|_| JobManagerError::InvalidStepId(s.to_string()))?;
        axis_combo.push((ax, idx));
    }
    Ok(JobIdParts {
        source_step_id,
        axis_combo,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_step_id_accepts_path_safe() {
        assert!(validate_step_id("opt").is_ok());
        assert!(validate_step_id("opt-1").is_ok());
        assert!(validate_step_id("opt_2").is_ok());
        assert!(validate_step_id("Step123").is_ok());
    }

    #[test]
    fn validate_step_id_rejects_reserved() {
        assert!(matches!(
            validate_step_id("flow"),
            Err(JobManagerError::ReservedJobId(_))
        ));
        assert!(matches!(
            validate_step_id("plan"),
            Err(JobManagerError::ReservedJobId(_))
        ));
    }

    #[test]
    fn validate_step_id_rejects_invalid_chars() {
        assert!(matches!(
            validate_step_id("opt=1"),
            Err(JobManagerError::InvalidStepId(_))
        ));
        assert!(matches!(
            validate_step_id("opt/sub"),
            Err(JobManagerError::InvalidStepId(_))
        ));
        assert!(matches!(
            validate_step_id(""),
            Err(JobManagerError::InvalidStepId(_))
        ));
    }

    #[test]
    fn build_and_parse_round_trip_no_sweep() {
        let s = build_job_id("opt", &[]);
        assert_eq!(s, "opt");
        let parts = parse_job_id(&s).unwrap();
        assert_eq!(parts.source_step_id, "opt");
        assert!(parts.axis_combo.is_empty());
    }

    #[test]
    fn build_and_parse_round_trip_with_sweep() {
        let s = build_job_id("opt", &[("compound", 0), ("method", 2)]);
        assert_eq!(s, "opt__compound=0__method=2");
        let parts = parse_job_id(&s).unwrap();
        assert_eq!(parts.source_step_id, "opt");
        assert_eq!(parts.axis_combo, vec![("compound", 0), ("method", 2)]);
    }

    #[test]
    fn parse_rejects_malformed() {
        assert!(parse_job_id("opt__nothing").is_err());
        assert!(parse_job_id("opt__compound=abc").is_err());
        assert!(parse_job_id("__compound=0").is_err());
    }
}
```

- [ ] **Step 2: `src/grammar/mod.rs` で公開**

```rust
pub mod source;
pub mod placeholder;
pub mod jobid;
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib grammar::jobid:: 2>&1 | tail -5`

Expected: `test result: ok. 6 passed`.

- [ ] **Step 4: コミット**

```bash
git add src/grammar/
git commit -m "feat(grammar): jobid validate/build/parse helpers

SP-2 spec §5 に対応。"
```

---

### Task 6: src/grammar/reader.rs — TOML strict parse + legacy detection

**Files:**
- Create: `src/grammar/reader.rs`
- Modify: `src/grammar/mod.rs`

- [ ] **Step 1: 実装を書く** (テストはコード末尾)

`src/grammar/reader.rs` (新規ファイル):

```rust
//! `experiment.toml` のパース。strict (unknown key 拒否) + legacy 形状検出。

use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

use gaussian_job_shared::entities::workflow::{CalcType, Program};
use slurm_async_runner::entities::slurm::DependencyType;

use crate::error::JobManagerError;
use crate::grammar::jobid::validate_step_id;
use crate::grammar::source::{
    AxisDef, AxisValues, ExperimentSource, FlowMeta, ParentRef, RawStep,
};

const TOP_LEVEL_ALLOWED: &[&str] = &["flow", "axis", "step"];
const FLOW_ALLOWED: &[&str] = &["calc_type", "tags"];
const AXIS_ALLOWED: &[&str] = &["name", "values"];
const STEP_ALLOWED: &[&str] = &["id", "program", "sweep", "parents", "params"];
const PARENT_ALLOWED: &[&str] = &["id", "fanout", "reduce_over", "kind"];

pub fn parse_experiment(path: &Path) -> Result<ExperimentSource, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|e| JobManagerError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    parse_experiment_str(&text, path)
}

pub fn parse_experiment_str(text: &str, path: &Path) -> Result<ExperimentSource, JobManagerError> {
    let value: toml::Value = toml::from_str(text).map_err(|e| JobManagerError::GrammarTomlParse {
        path: path.to_path_buf(),
        source: e,
    })?;
    let table = value.as_table().ok_or_else(|| JobManagerError::WrongType {
        key: "<root>".to_string(),
        location: format!("{}: top level", path.display()),
        expected: "table",
        got: type_name(&value),
    })?;

    detect_legacy(table, path)?;
    reject_unknown_keys(table, TOP_LEVEL_ALLOWED, &format!("{}: top level", path.display()))?;

    let flow = parse_flow_block(table.get("flow"), path)?;
    let axes = parse_axes(table.get("axis"), path)?;
    let steps = parse_steps(table.get("step"), path)?;

    if steps.is_empty() {
        return Err(JobManagerError::MissingKey {
            key: "[[step]]".to_string(),
            location: format!("{}: at least one [[step]] required", path.display()),
        });
    }

    Ok(ExperimentSource { flow, axes, steps })
}

fn detect_legacy(table: &toml::value::Table, path: &Path) -> Result<(), JobManagerError> {
    if table.contains_key("gaussian_input") {
        return Err(JobManagerError::LegacyToml {
            path: path.to_path_buf(),
            hint: "see gaussian-experiment-manager → SP-2 migration notes (legacy [gaussian_input] block)".to_string(),
        });
    }
    if let Some(env) = table.get("env").and_then(|v| v.as_table()) {
        if env.contains_key("compound_id") || env.contains_key("project_base") {
            return Err(JobManagerError::LegacyToml {
                path: path.to_path_buf(),
                hint: "[env].compound_id/[env].project_base were removed; experiment.toml has no [env] block".to_string(),
            });
        }
    }
    if table.contains_key("sweep") {
        return Err(JobManagerError::LegacyToml {
            path: path.to_path_buf(),
            hint: "[[sweep]] was renamed to [[axis]] in SP-2".to_string(),
        });
    }
    if let Some(steps) = table.get("step").and_then(|v| v.as_array()) {
        for s in steps {
            let Some(st) = s.as_table() else { continue };
            if st.contains_key("compounds") {
                return Err(JobManagerError::LegacyToml {
                    path: path.to_path_buf(),
                    hint: "step.compounds was removed; declare an [[axis]] name=\"compound\" instead".to_string(),
                });
            }
            if st.contains_key("calc_type") {
                return Err(JobManagerError::LegacyToml {
                    path: path.to_path_buf(),
                    hint: "step.calc_type was moved to [flow].calc_type".to_string(),
                });
            }
            if st.contains_key("parent") && !st.contains_key("parents") {
                return Err(JobManagerError::LegacyToml {
                    path: path.to_path_buf(),
                    hint: "step.parent was renamed to step.parents (list)".to_string(),
                });
            }
            if st.contains_key("sweep_over") {
                return Err(JobManagerError::LegacyToml {
                    path: path.to_path_buf(),
                    hint: "step.sweep_over was renamed to step.sweep".to_string(),
                });
            }
            if st.contains_key("tags") {
                return Err(JobManagerError::LegacyToml {
                    path: path.to_path_buf(),
                    hint: "step.tags was removed in v2 (per-job tags 不採用)".to_string(),
                });
            }
        }
    }
    Ok(())
}

fn parse_flow_block(value: Option<&toml::Value>, path: &Path) -> Result<FlowMeta, JobManagerError> {
    let Some(v) = value else { return Ok(FlowMeta::default()) };
    let table = v.as_table().ok_or_else(|| JobManagerError::WrongType {
        key: "flow".into(),
        location: format!("{}: [flow]", path.display()),
        expected: "table",
        got: type_name(v),
    })?;
    reject_unknown_keys(table, FLOW_ALLOWED, &format!("{}: [flow]", path.display()))?;

    let calc_type = match table.get("calc_type") {
        Some(toml::Value::String(s)) => Some(CalcType::from(s.clone())),    // D2 newtype 包装
        Some(other) => {
            return Err(JobManagerError::WrongType {
                key: "calc_type".into(),
                location: format!("{}: [flow]", path.display()),
                expected: "string",
                got: type_name(other),
            });
        }
        None => None,
    };

    let mut tags = BTreeMap::new();
    if let Some(t) = table.get("tags") {
        let tbl = t.as_table().ok_or_else(|| JobManagerError::WrongType {
            key: "tags".into(),
            location: format!("{}: [flow]", path.display()),
            expected: "table",
            got: type_name(t),
        })?;
        for (k, vv) in tbl {
            let s = vv.as_str().ok_or_else(|| JobManagerError::WrongType {
                key: k.clone(),
                location: format!("{}: [flow.tags]", path.display()),
                expected: "string",
                got: type_name(vv),
            })?;
            if k == "calc_type" {
                return Err(JobManagerError::FlowTagsHasCalcType);
            }
            tags.insert(k.clone(), s.to_string());
        }
    }
    Ok(FlowMeta { calc_type, tags })
}

fn parse_axes(value: Option<&toml::Value>, path: &Path) -> Result<Vec<AxisDef>, JobManagerError> {
    let Some(v) = value else { return Ok(Vec::new()) };
    let arr = v.as_array().ok_or_else(|| JobManagerError::WrongType {
        key: "axis".into(),
        location: format!("{}: [[axis]]", path.display()),
        expected: "array of tables",
        got: type_name(v),
    })?;
    let mut out = Vec::with_capacity(arr.len());
    let mut seen = BTreeMap::<String, ()>::new();
    for (i, ax) in arr.iter().enumerate() {
        let table = ax.as_table().ok_or_else(|| JobManagerError::WrongType {
            key: format!("axis[{i}]"),
            location: format!("{}: [[axis]] element {i}", path.display()),
            expected: "table",
            got: type_name(ax),
        })?;
        reject_unknown_keys(
            table,
            AXIS_ALLOWED,
            &format!("{}: [[axis]] element {i}", path.display()),
        )?;
        let name = require_string(table, "name", &format!("{}: [[axis]] element {i}", path.display()))?;
        if seen.insert(name.clone(), ()).is_some() {
            return Err(JobManagerError::DuplicateAxis(name));
        }
        let values_val = table
            .get("values")
            .ok_or_else(|| JobManagerError::MissingKey {
                key: "values".into(),
                location: format!("{}: [[axis]] name=\"{}\"", path.display(), name),
            })?;
        let values = parse_axis_values(values_val, &name, path)?;
        out.push(AxisDef { name, values });
    }
    Ok(out)
}

fn parse_axis_values(
    value: &toml::Value,
    axis_name: &str,
    path: &Path,
) -> Result<AxisValues, JobManagerError> {
    let arr = value.as_array().ok_or_else(|| JobManagerError::WrongType {
        key: "values".into(),
        location: format!("{}: [[axis]] name=\"{}\"", path.display(), axis_name),
        expected: "array",
        got: type_name(value),
    })?;
    if arr.is_empty() {
        return Err(JobManagerError::EmptyAxis(axis_name.to_string()));
    }
    let first = &arr[0];
    if first.is_str() {
        let mut out = Vec::with_capacity(arr.len());
        for (i, v) in arr.iter().enumerate() {
            let s = v.as_str().ok_or_else(|| {
                if v.is_table() {
                    JobManagerError::MixedAxisValues {
                        name: axis_name.to_string(),
                    }
                } else {
                    JobManagerError::WrongType {
                        key: format!("values[{i}]"),
                        location: format!("{}: [[axis]] name=\"{}\"", path.display(), axis_name),
                        expected: "string",
                        got: type_name(v),
                    }
                }
            })?;
            out.push(s.to_string());
        }
        Ok(AxisValues::Scalar(out))
    } else if first.is_table() {
        let first_table = first.as_table().unwrap();
        let mut fields: Vec<String> = first_table.keys().cloned().collect();
        fields.sort();
        let mut rows = Vec::with_capacity(arr.len());
        for (i, v) in arr.iter().enumerate() {
            let t = v.as_table().ok_or_else(|| {
                if v.is_str() {
                    JobManagerError::MixedAxisValues {
                        name: axis_name.to_string(),
                    }
                } else {
                    JobManagerError::WrongType {
                        key: format!("values[{i}]"),
                        location: format!("{}: [[axis]] name=\"{}\"", path.display(), axis_name),
                        expected: "table",
                        got: type_name(v),
                    }
                }
            })?;
            let mut these_fields: Vec<String> = t.keys().cloned().collect();
            these_fields.sort();
            if these_fields != fields {
                return Err(JobManagerError::StructAxisFieldMismatch {
                    name: axis_name.to_string(),
                    row: i,
                });
            }
            let mut row_map = BTreeMap::new();
            for (k, vv) in t {
                match vv {
                    toml::Value::String(_)
                    | toml::Value::Integer(_)
                    | toml::Value::Float(_)
                    | toml::Value::Boolean(_) => {
                        row_map.insert(k.clone(), vv.clone());
                    }
                    _ => {
                        return Err(JobManagerError::WrongType {
                            key: format!("values[{i}].{k}"),
                            location: format!("{}: [[axis]] name=\"{}\"", path.display(), axis_name),
                            expected: "string/int/float/bool",
                            got: type_name(vv),
                        });
                    }
                }
            }
            rows.push(row_map);
        }
        Ok(AxisValues::Struct { fields, rows })
    } else {
        Err(JobManagerError::WrongType {
            key: "values[0]".into(),
            location: format!("{}: [[axis]] name=\"{}\"", path.display(), axis_name),
            expected: "string or table",
            got: type_name(first),
        })
    }
}

fn parse_steps(value: Option<&toml::Value>, path: &Path) -> Result<Vec<RawStep>, JobManagerError> {
    let Some(v) = value else { return Ok(Vec::new()) };
    let arr = v.as_array().ok_or_else(|| JobManagerError::WrongType {
        key: "step".into(),
        location: format!("{}: [[step]]", path.display()),
        expected: "array of tables",
        got: type_name(v),
    })?;
    let mut out = Vec::with_capacity(arr.len());
    let mut seen_ids = BTreeMap::<String, ()>::new();
    for (i, s) in arr.iter().enumerate() {
        let table = s.as_table().ok_or_else(|| JobManagerError::WrongType {
            key: format!("step[{i}]"),
            location: format!("{}: [[step]] element {i}", path.display()),
            expected: "table",
            got: type_name(s),
        })?;
        reject_unknown_keys(
            table,
            STEP_ALLOWED,
            &format!("{}: [[step]] element {i}", path.display()),
        )?;
        let id = require_string(table, "id", &format!("{}: [[step]] element {i}", path.display()))?;
        validate_step_id(&id)?;
        if seen_ids.insert(id.clone(), ()).is_some() {
            return Err(JobManagerError::DuplicateStepId(id));
        }
        let program = require_string(table, "program", &format!("{}: [[step]] id=\"{}\"", path.display(), id))?;

        let sweep = match table.get("sweep") {
            None => Vec::new(),
            Some(toml::Value::Array(a)) => {
                let mut v = Vec::with_capacity(a.len());
                let mut seen = BTreeMap::<String, ()>::new();
                for (j, item) in a.iter().enumerate() {
                    let s = item.as_str().ok_or_else(|| JobManagerError::WrongType {
                        key: format!("sweep[{j}]"),
                        location: format!("{}: [[step]] id=\"{}\"", path.display(), id),
                        expected: "string",
                        got: type_name(item),
                    })?;
                    if seen.insert(s.to_string(), ()).is_some() {
                        return Err(JobManagerError::DuplicateSweepAxis {
                            step: id.clone(),
                            axis: s.to_string(),
                        });
                    }
                    v.push(s.to_string());
                }
                v
            }
            Some(other) => {
                return Err(JobManagerError::WrongType {
                    key: "sweep".into(),
                    location: format!("{}: [[step]] id=\"{}\"", path.display(), id),
                    expected: "array of strings",
                    got: type_name(other),
                });
            }
        };

        let parents = parse_parents(table.get("parents"), &id, path)?;

        let params = match table.get("params") {
            None => BTreeMap::new(),
            Some(toml::Value::Table(t)) => t.clone().into_iter().collect(),
            Some(other) => {
                return Err(JobManagerError::WrongType {
                    key: "params".into(),
                    location: format!("{}: [[step]] id=\"{}\"", path.display(), id),
                    expected: "table",
                    got: type_name(other),
                });
            }
        };

        out.push(RawStep {
            id,
            program: Program::from(program),                    // D2 newtype 包装
            sweep,
            parents,
            params,
        });
    }
    Ok(out)
}

fn parse_parents(
    value: Option<&toml::Value>,
    step_id: &str,
    path: &Path,
) -> Result<Vec<ParentRef>, JobManagerError> {
    let Some(v) = value else { return Ok(Vec::new()) };
    let arr = v.as_array().ok_or_else(|| JobManagerError::WrongType {
        key: "parents".into(),
        location: format!("{}: [[step]] id=\"{}\"", path.display(), step_id),
        expected: "array of tables",
        got: type_name(v),
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for (i, p) in arr.iter().enumerate() {
        let table = p.as_table().ok_or_else(|| JobManagerError::WrongType {
            key: format!("parents[{i}]"),
            location: format!("{}: [[step]] id=\"{}\"", path.display(), step_id),
            expected: "table",
            got: type_name(p),
        })?;
        reject_unknown_keys(
            table,
            PARENT_ALLOWED,
            &format!("{}: [[step]] id=\"{}\" parents[{i}]", path.display(), step_id),
        )?;
        let id = require_string(table, "id", &format!("{}: parents[{i}]", path.display()))?;
        let fanout = match table.get("fanout") {
            None => false,
            Some(toml::Value::Boolean(b)) => *b,
            Some(other) => {
                return Err(JobManagerError::WrongType {
                    key: "fanout".into(),
                    location: format!("{}: parents[{i}]", path.display()),
                    expected: "boolean",
                    got: type_name(other),
                });
            }
        };
        let reduce_over = match table.get("reduce_over") {
            None => Vec::new(),
            Some(toml::Value::Array(a)) => {
                let mut v = Vec::with_capacity(a.len());
                for (j, item) in a.iter().enumerate() {
                    let s = item.as_str().ok_or_else(|| JobManagerError::WrongType {
                        key: format!("reduce_over[{j}]"),
                        location: format!("{}: parents[{i}]", path.display()),
                        expected: "string",
                        got: type_name(item),
                    })?;
                    v.push(s.to_string());
                }
                v
            }
            Some(other) => {
                return Err(JobManagerError::WrongType {
                    key: "reduce_over".into(),
                    location: format!("{}: parents[{i}]", path.display()),
                    expected: "array of strings",
                    got: type_name(other),
                });
            }
        };
        let kind = match table.get("kind") {
            None => DependencyType::Afterok,
            Some(toml::Value::String(s)) => DependencyType::from_str(s)
                .map_err(|_| JobManagerError::UnknownDependencyKind(s.clone()))?,
            Some(other) => {
                return Err(JobManagerError::WrongType {
                    key: "kind".into(),
                    location: format!("{}: parents[{i}]", path.display()),
                    expected: "string",
                    got: type_name(other),
                });
            }
        };
        out.push(ParentRef { id, fanout, reduce_over, kind });
    }
    Ok(out)
}

fn reject_unknown_keys(
    table: &toml::value::Table,
    allowed: &[&str],
    location: &str,
) -> Result<(), JobManagerError> {
    for k in table.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(JobManagerError::UnknownKey {
                key: k.clone(),
                location: location.to_string(),
            });
        }
    }
    Ok(())
}

fn require_string(
    table: &toml::value::Table,
    key: &str,
    location: &str,
) -> Result<String, JobManagerError> {
    let v = table.get(key).ok_or_else(|| JobManagerError::MissingKey {
        key: key.into(),
        location: location.into(),
    })?;
    let s = v.as_str().ok_or_else(|| JobManagerError::WrongType {
        key: key.into(),
        location: location.into(),
        expected: "string",
        got: type_name(v),
    })?;
    Ok(s.to_string())
}

fn type_name(v: &toml::Value) -> &'static str {
    match v {
        toml::Value::String(_) => "string",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p() -> PathBuf { PathBuf::from("/tmp/test.toml") }

    #[test]
    fn minimal_valid_parses() {
        let s = "[[step]]\nid = \"opt\"\nprogram = \"g16\"\n";
        let r = parse_experiment_str(s, &p()).unwrap();
        assert_eq!(r.steps.len(), 1);
        assert_eq!(r.steps[0].id, "opt");
        assert_eq!(r.steps[0].program, "g16");
    }

    #[test]
    fn unknown_top_level_key_rejected() {
        let s = "[[step]]\nid = \"opt\"\nprogram = \"g16\"\n\n[garbage]\nk = \"v\"\n";
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::UnknownKey { .. }));
    }

    #[test]
    fn missing_steps_rejected() {
        let s = "[flow]\ncalc_type = \"x\"\n";
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::MissingKey { .. }));
    }

    #[test]
    fn legacy_sweep_block_detected() {
        let s = "[[sweep]]\nname = \"a\"\n\n[[step]]\nid = \"opt\"\nprogram = \"g16\"\n";
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::LegacyToml { .. }));
    }

    #[test]
    fn legacy_step_compounds_detected() {
        let s = "[[step]]\nid = \"opt\"\nprogram = \"g16\"\ncompounds = [\"benzene\"]\n";
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::LegacyToml { .. }));
    }

    #[test]
    fn legacy_step_tags_detected() {
        let s = "[[step]]\nid = \"opt\"\nprogram = \"g16\"\ntags = { x = \"y\" }\n";
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::LegacyToml { .. }));
    }

    #[test]
    fn scalar_axis_parsed() {
        let s = r#"
[[axis]]
name = "compound"
values = ["benzene", "toluene"]

[[step]]
id = "opt"
program = "g16"
sweep = ["compound"]
"#;
        let r = parse_experiment_str(s, &p()).unwrap();
        match &r.axes[0].values {
            AxisValues::Scalar(xs) => assert_eq!(xs, &vec!["benzene".to_string(), "toluene".to_string()]),
            _ => panic!("expected scalar"),
        }
    }

    #[test]
    fn struct_axis_parsed() {
        let s = r#"
[[axis]]
name = "method"
values = [
    { name = "b3lyp", route = "B3LYP" },
    { name = "m062x", route = "M06-2X" },
]

[[step]]
id = "opt"
program = "g16"
sweep = ["method"]
"#;
        let r = parse_experiment_str(s, &p()).unwrap();
        match &r.axes[0].values {
            AxisValues::Struct { fields, rows } => {
                assert_eq!(fields, &vec!["name".to_string(), "route".to_string()]);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected struct"),
        }
    }

    #[test]
    fn mixed_axis_rejected() {
        let s = r#"
[[axis]]
name = "x"
values = ["a", { b = "c" }]

[[step]]
id = "opt"
program = "g16"
"#;
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::MixedAxisValues { .. }));
    }

    #[test]
    fn struct_axis_field_mismatch_rejected() {
        let s = r#"
[[axis]]
name = "x"
values = [
    { a = "1", b = "2" },
    { a = "1" },
]

[[step]]
id = "opt"
program = "g16"
"#;
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::StructAxisFieldMismatch { .. }));
    }

    #[test]
    fn flow_tags_calc_type_collision_rejected() {
        let s = r#"
[flow]
calc_type = "opt"
tags = { calc_type = "x" }

[[step]]
id = "opt"
program = "g16"
"#;
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::FlowTagsHasCalcType));
    }

    #[test]
    fn parents_with_fanout_and_reduce_parses() {
        let s = r#"
[[step]]
id = "child"
program = "g16"
parents = [
    { id = "p1", fanout = true },
    { id = "p2", reduce_over = ["x"], kind = "afterany" },
]
"#;
        let r = parse_experiment_str(s, &p()).unwrap();
        assert_eq!(r.steps[0].parents.len(), 2);
        assert!(r.steps[0].parents[0].fanout);
        assert_eq!(r.steps[0].parents[1].reduce_over, vec!["x".to_string()]);
        assert_eq!(r.steps[0].parents[1].kind, DependencyType::Afterany);
    }

    #[test]
    fn duplicate_step_id_rejected() {
        let s = r#"
[[step]]
id = "opt"
program = "g16"

[[step]]
id = "opt"
program = "g16"
"#;
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::DuplicateStepId(_)));
    }

    #[test]
    fn reserved_step_id_rejected() {
        let s = "[[step]]\nid = \"flow\"\nprogram = \"g16\"\n";
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::ReservedJobId(_)));
    }

    #[test]
    fn invalid_step_id_chars_rejected() {
        let s = "[[step]]\nid = \"opt/sub\"\nprogram = \"g16\"\n";
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::InvalidStepId(_)));
    }

    #[test]
    fn duplicate_sweep_axis_rejected() {
        let s = r#"
[[axis]]
name = "a"
values = ["1"]

[[step]]
id = "opt"
program = "g16"
sweep = ["a", "a"]
"#;
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::DuplicateSweepAxis { .. }));
    }

    #[test]
    fn unknown_dependency_kind_rejected() {
        let s = r#"
[[step]]
id = "opt"
program = "g16"

[[step]]
id = "child"
program = "g16"
parents = [{ id = "opt", kind = "not_a_kind" }]
"#;
        let e = parse_experiment_str(s, &p()).unwrap_err();
        assert!(matches!(e, JobManagerError::UnknownDependencyKind(_)));
    }
}
```

- [ ] **Step 2: `src/grammar/mod.rs` で reader を公開**

```rust
pub mod source;
pub mod placeholder;
pub mod jobid;
pub mod reader;
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib grammar::reader:: 2>&1 | tail -5`

Expected: `test result: ok. 16 passed`.

- [ ] **Step 4: コミット**

```bash
git add src/grammar/
git commit -m "feat(grammar): TOML strict parser + legacy detection

SP-2 spec §4 に対応。"
```

---

### Task 7: src/grammar/sweep.rs — expand_sweeps

**Files:**
- Create: `src/grammar/sweep.rs`
- Modify: `src/grammar/mod.rs`

- [ ] **Step 1: 実装を書く**

`src/grammar/sweep.rs`:

```rust
//! Sweep axes の itertools.product 相当の展開。

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::{JobId, Program};

use crate::error::JobManagerError;
use crate::grammar::jobid::build_job_id;
use crate::grammar::placeholder::{expand_params, AxisCtx, AxisCtxValue};
use crate::grammar::source::{AxisValues, ExperimentSource, ParentRef, RawStep};

#[derive(Debug, Clone)]
pub(crate) struct ExpandedStep {
    pub job_id: JobId,                              // D2 newtype を import 利用
    pub program: Program,                           // D2 newtype を import 利用
    pub sweep: Vec<String>,
    pub axis_combo: BTreeMap<String, usize>,
    pub params: BTreeMap<String, toml::Value>,
    pub parents_raw: Vec<ParentRef>,
}

pub(crate) fn expand_sweeps(
    src: &ExperimentSource,
) -> Result<Vec<ExpandedStep>, JobManagerError> {
    let axes_by_name: BTreeMap<&str, &super::source::AxisDef> =
        src.axes.iter().map(|a| (a.name.as_str(), a)).collect();

    let mut out: Vec<ExpandedStep> = Vec::new();
    for step in &src.steps {
        for ax in &step.sweep {
            if !axes_by_name.contains_key(ax.as_str()) {
                return Err(JobManagerError::UnknownAxisRef {
                    step: step.id.clone(),
                    axis: ax.clone(),
                });
            }
        }
        let lens: Vec<usize> = step
            .sweep
            .iter()
            .map(|n| axis_len(axes_by_name[n.as_str()]))
            .collect();
        if step.sweep.is_empty() {
            out.push(materialize(step, &[], &axes_by_name)?);
        } else {
            for indices in cartesian(&lens) {
                let combo: Vec<(&str, usize)> = step
                    .sweep
                    .iter()
                    .map(|s| s.as_str())
                    .zip(indices)
                    .collect();
                out.push(materialize(step, &combo, &axes_by_name)?);
            }
        }
    }
    Ok(out)
}

fn axis_len(ax: &super::source::AxisDef) -> usize {
    match &ax.values {
        AxisValues::Scalar(v) => v.len(),
        AxisValues::Struct { rows, .. } => rows.len(),
    }
}

fn cartesian(lens: &[usize]) -> Vec<Vec<usize>> {
    let total: usize = lens.iter().product();
    (0..total)
        .map(|mut k| {
            let mut out = vec![0; lens.len()];
            for i in (0..lens.len()).rev() {
                out[i] = k % lens[i];
                k /= lens[i];
            }
            out
        })
        .collect()
}

fn materialize(
    step: &RawStep,
    axis_combo: &[(&str, usize)],
    axes_by_name: &BTreeMap<&str, &super::source::AxisDef>,
) -> Result<ExpandedStep, JobManagerError> {
    let mut ctx = AxisCtx::new();
    for (ax_name, idx) in axis_combo {
        let ax = axes_by_name[ax_name];
        let v = match &ax.values {
            AxisValues::Scalar(vals) => AxisCtxValue::Scalar(vals[*idx].clone()),
            AxisValues::Struct { rows, .. } => {
                let row = &rows[*idx];
                let mut m = BTreeMap::new();
                for (k, val) in row {
                    m.insert(k.clone(), display_toml(val));
                }
                AxisCtxValue::Struct(m)
            }
        };
        ctx.insert(ax_name.to_string(), v);
    }
    let params = expand_params(step.params.clone(), &ctx)?;
    let job_id_str = build_job_id(&step.id, axis_combo);
    let combo_map: BTreeMap<String, usize> =
        axis_combo.iter().map(|(n, i)| (n.to_string(), *i)).collect();

    Ok(ExpandedStep {
        job_id: JobId::from(job_id_str),            // D2 newtype 包装
        program: step.program.clone(),              // D2 Program newtype
        sweep: step.sweep.clone(),
        axis_combo: combo_map,
        params,
        parents_raw: step.parents.clone(),
    })
}

fn display_toml(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => s.clone(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        _ => format!("{v}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grammar::reader::parse_experiment_str;
    use std::path::PathBuf;

    fn p() -> PathBuf { PathBuf::from("/tmp/x.toml") }

    #[test]
    fn no_sweep_yields_single_expanded() {
        let s = "[[step]]\nid = \"opt\"\nprogram = \"g16\"\n";
        let src = parse_experiment_str(s, &p()).unwrap();
        let out = expand_sweeps(&src).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].job_id.0, "opt");          // JobId.0 で内部 String にアクセス
        assert!(out[0].axis_combo.is_empty());
    }

    #[test]
    fn two_axes_3x2_yields_6_in_order() {
        let s = r#"
[[axis]]
name = "c"
values = ["x", "y", "z"]

[[axis]]
name = "m"
values = ["1", "2"]

[[step]]
id = "opt"
program = "g16"
sweep = ["c", "m"]
"#;
        let src = parse_experiment_str(s, &p()).unwrap();
        let out = expand_sweeps(&src).unwrap();
        assert_eq!(out.len(), 6);
        let ids: Vec<&str> = out.iter().map(|e| e.job_id.0.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "opt__c=0__m=0", "opt__c=0__m=1",
                "opt__c=1__m=0", "opt__c=1__m=1",
                "opt__c=2__m=0", "opt__c=2__m=1",
            ]
        );
    }

    #[test]
    fn placeholders_expand_in_params() {
        let s = r#"
[[axis]]
name = "m"
values = [{ name = "b3lyp", route = "B3LYP" }]

[[step]]
id = "opt"
program = "g16"
sweep = ["m"]
[step.params]
route = "# ${m.route}/6-31G*"
"#;
        let src = parse_experiment_str(s, &p()).unwrap();
        let out = expand_sweeps(&src).unwrap();
        let route = out[0].params.get("route").unwrap().as_str().unwrap();
        assert_eq!(route, "# B3LYP/6-31G*");
    }

    #[test]
    fn unknown_axis_ref_errors() {
        let s = r#"
[[step]]
id = "opt"
program = "g16"
sweep = ["missing"]
"#;
        let src = parse_experiment_str(s, &p()).unwrap();
        let e = expand_sweeps(&src).unwrap_err();
        assert!(matches!(e, JobManagerError::UnknownAxisRef { .. }));
    }
}
```

- [ ] **Step 2: `src/grammar/mod.rs` で sweep を公開** (内部のみ)

```rust
pub mod source;
pub mod placeholder;
pub mod jobid;
pub mod reader;
pub(crate) mod sweep;
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib grammar::sweep:: 2>&1 | tail -5`

Expected: `test result: ok. 4 passed`.

- [ ] **Step 4: コミット**

```bash
git add src/grammar/
git commit -m "feat(grammar): sweep expansion with placeholder substitution

SP-2 spec §6 に対応。"
```

---

### Task 8: src/grammar/chain.rs — resolve_parents + DAG cycle check

**Files:**
- Create: `src/grammar/chain.rs`
- Modify: `src/grammar/mod.rs`

- [ ] **Step 1: 実装を書く**

`src/grammar/chain.rs`:

```rust
//! Parent ref → JobEdge 解決 + DAG cycle 検出。

use std::collections::{BTreeMap, BTreeSet};

use gaussian_job_shared::entities::workflow::{JobEdge, JobId, Program};

use crate::error::JobManagerError;
use crate::grammar::jobid::parse_job_id;
use crate::grammar::source::{ExperimentSource, ParentRef};
use crate::grammar::sweep::ExpandedStep;

#[derive(Debug, Clone)]
pub(crate) struct ResolvedStep {
    pub job_id: JobId,                              // D2 newtype を import 利用
    pub program: Program,                           // D2 newtype を import 利用
    pub params: BTreeMap<String, toml::Value>,
    pub parents: Vec<JobEdge>,
}

pub(crate) fn resolve_parents(
    src: &ExperimentSource,
    expanded: Vec<ExpandedStep>,
) -> Result<Vec<ResolvedStep>, JobManagerError> {
    let step_sweep_by_id: BTreeMap<&str, &Vec<String>> =
        src.steps.iter().map(|s| (s.id.as_str(), &s.sweep)).collect();
    let mut index_by_step: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, e) in expanded.iter().enumerate() {
        let parts = parse_job_id(&e.job_id.0)?;       // JobId → &str via .0
        index_by_step.entry(parts.source_step_id).or_default().push(i);
    }

    let mut resolved: Vec<ResolvedStep> = expanded
        .iter()
        .map(|e| ResolvedStep {
            job_id: e.job_id.clone(),
            program: e.program.clone(),
            params: e.params.clone(),
            parents: Vec::new(),
        })
        .collect();

    for (child_idx, child) in expanded.iter().enumerate() {
        let child_parts = parse_job_id(&child.job_id.0)?;
        let child_step_id = child_parts.source_step_id;

        for parent_ref in &child.parents_raw {
            if parent_ref.id == child_step_id {
                return Err(JobManagerError::SelfParent(child_step_id.to_string()));
            }
            if parent_ref.fanout && !parent_ref.reduce_over.is_empty() {
                return Err(JobManagerError::BothFanoutAndReduce(child_step_id.to_string()));
            }
            let Some(parent_sweep) = step_sweep_by_id.get(parent_ref.id.as_str()) else {
                return Err(JobManagerError::UnknownStepId(
                    child_step_id.to_string(),
                    parent_ref.id.clone(),
                ));
            };
            let parent_indices = index_by_step
                .get(parent_ref.id.as_str())
                .expect("step_index built from same src");
            let edges = resolve_edges(
                parent_ref,
                parent_sweep,
                &child.sweep,
                child,
                parent_indices,
                &expanded,
            )?;
            resolved[child_idx].parents.extend(edges);
        }
    }

    detect_cycle(&resolved)?;
    Ok(resolved)
}

fn resolve_edges(
    parent_ref: &ParentRef,
    parent_sweep: &[String],
    child_sweep: &[String],
    child: &ExpandedStep,
    parent_indices: &[usize],
    expanded: &[ExpandedStep],
) -> Result<Vec<JobEdge>, JobManagerError> {
    let parent_set: BTreeSet<&str> = parent_sweep.iter().map(String::as_str).collect();
    let child_set: BTreeSet<&str> = child_sweep.iter().map(String::as_str).collect();
    let reduce_set: BTreeSet<&str> = parent_ref.reduce_over.iter().map(String::as_str).collect();

    let mode = if parent_ref.fanout {
        if !parent_set.is_subset(&child_set) || parent_set == child_set {
            return Err(JobManagerError::FanoutNotProperSubset {
                id: parent_ref.id.clone(),
                parent: parent_sweep.to_vec(),
                child: child_sweep.to_vec(),
            });
        }
        Mode::Fanout
    } else if !reduce_set.is_empty() {
        if !reduce_set.is_subset(&parent_set) {
            return Err(JobManagerError::ReduceCoverageMismatch(parent_ref.id.clone()));
        }
        let expected: BTreeSet<&str> = child_set.union(&reduce_set).copied().collect();
        if parent_set != expected {
            return Err(JobManagerError::ReduceCoverageMismatch(parent_ref.id.clone()));
        }
        if !reduce_set.is_disjoint(&child_set) {
            return Err(JobManagerError::ReduceCoverageMismatch(parent_ref.id.clone()));
        }
        Mode::Reduce
    } else {
        if parent_set != child_set {
            return Err(JobManagerError::PairByAxesMismatch {
                id: parent_ref.id.clone(),
                parent: parent_sweep.to_vec(),
                child: child_sweep.to_vec(),
            });
        }
        Mode::Pair
    };

    let shared_axes: &[String] = match mode {
        Mode::Pair | Mode::Fanout => parent_sweep,
        Mode::Reduce => child_sweep,
    };

    let mut out = Vec::new();
    for &pi in parent_indices {
        let parent = &expanded[pi];
        let all_match = shared_axes.iter().all(|ax| {
            parent.axis_combo.get(ax.as_str()) == child.axis_combo.get(ax.as_str())
        });
        if all_match {
            out.push(JobEdge {
                from: parent.job_id.clone(),
                kind: parent_ref.kind,
            });
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
enum Mode { Pair, Fanout, Reduce }

fn detect_cycle(resolved: &[ResolvedStep]) -> Result<(), JobManagerError> {
    let mut in_degree: BTreeMap<&str, usize> = BTreeMap::new();
    let mut succs: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for r in resolved {
        in_degree.entry(r.job_id.0.as_str()).or_insert(0);
    }
    for r in resolved {
        for edge in &r.parents {
            *in_degree.entry(r.job_id.0.as_str()).or_insert(0) += 1;
            succs.entry(edge.from.0.as_str()).or_default().push(r.job_id.0.as_str());
        }
    }
    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter_map(|(k, d)| if *d == 0 { Some(*k) } else { None })
        .collect();
    let mut visited = 0usize;
    while let Some(n) = queue.pop() {
        visited += 1;
        if let Some(children) = succs.get(n) {
            for c in children.clone() {
                let d = in_degree.get_mut(c).unwrap();
                *d -= 1;
                if *d == 0 {
                    queue.push(c);
                }
            }
        }
    }
    if visited != in_degree.len() {
        let unresolved: Vec<String> = in_degree
            .iter()
            .filter_map(|(k, d)| if *d > 0 { Some(k.to_string()) } else { None })
            .collect();
        return Err(JobManagerError::DagHasCycle(unresolved));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grammar::reader::parse_experiment_str;
    use crate::grammar::sweep::expand_sweeps;
    use std::path::PathBuf;

    fn p() -> PathBuf { PathBuf::from("/tmp/x.toml") }

    fn pipeline(src: &str) -> Result<Vec<ResolvedStep>, JobManagerError> {
        let parsed = parse_experiment_str(src, &p())?;
        let expanded = expand_sweeps(&parsed)?;
        resolve_parents(&parsed, expanded)
    }

    #[test]
    fn pair_by_axes_wires_1_to_1() {
        let s = r#"
[[axis]]
name = "c"
values = ["x", "y"]

[[step]]
id = "a"
program = "p"
sweep = ["c"]

[[step]]
id = "b"
program = "p"
sweep = ["c"]
parents = [{ id = "a" }]
"#;
        let r = pipeline(s).unwrap();
        let bs: Vec<_> = r.iter().filter(|x| x.job_id.0.starts_with("b__")).collect();
        assert_eq!(bs.len(), 2);
        for b in bs { assert_eq!(b.parents.len(), 1); }
    }

    #[test]
    fn pair_mismatch_rejected() {
        let s = r#"
[[axis]]
name = "c"
values = ["x"]
[[axis]]
name = "m"
values = ["1"]

[[step]]
id = "a"
program = "p"
sweep = ["c"]

[[step]]
id = "b"
program = "p"
sweep = ["c", "m"]
parents = [{ id = "a" }]
"#;
        let e = pipeline(s).unwrap_err();
        assert!(matches!(e, JobManagerError::PairByAxesMismatch { .. }));
    }

    #[test]
    fn fanout_wires_1_to_n() {
        let s = r#"
[[axis]]
name = "c"
values = ["x", "y"]
[[axis]]
name = "m"
values = ["1", "2"]

[[step]]
id = "a"
program = "p"
sweep = ["c"]

[[step]]
id = "b"
program = "p"
sweep = ["c", "m"]
parents = [{ id = "a", fanout = true }]
"#;
        let r = pipeline(s).unwrap();
        let bs: Vec<_> = r.iter().filter(|x| x.job_id.0.starts_with("b__")).collect();
        for b in &bs { assert_eq!(b.parents.len(), 1); }
        assert_eq!(bs.len(), 4);
    }

    #[test]
    fn fanout_equal_set_rejected() {
        let s = r#"
[[axis]]
name = "c"
values = ["x"]

[[step]]
id = "a"
program = "p"
sweep = ["c"]

[[step]]
id = "b"
program = "p"
sweep = ["c"]
parents = [{ id = "a", fanout = true }]
"#;
        let e = pipeline(s).unwrap_err();
        assert!(matches!(e, JobManagerError::FanoutNotProperSubset { .. }));
    }

    #[test]
    fn reduce_over_wires_n_to_1() {
        let s = r#"
[[axis]]
name = "c"
values = ["x"]
[[axis]]
name = "m"
values = ["1", "2", "3"]

[[step]]
id = "a"
program = "p"
sweep = ["c", "m"]

[[step]]
id = "b"
program = "p"
sweep = ["c"]
parents = [{ id = "a", reduce_over = ["m"] }]
"#;
        let r = pipeline(s).unwrap();
        let bs: Vec<_> = r.iter().filter(|x| x.job_id.0.starts_with("b__")).collect();
        assert_eq!(bs.len(), 1);
        assert_eq!(bs[0].parents.len(), 3);
    }

    #[test]
    fn reduce_over_intersects_child_rejected() {
        let s = r#"
[[axis]]
name = "c"
values = ["x"]
[[axis]]
name = "m"
values = ["1"]

[[step]]
id = "a"
program = "p"
sweep = ["c", "m"]

[[step]]
id = "b"
program = "p"
sweep = ["c", "m"]
parents = [{ id = "a", reduce_over = ["m"] }]
"#;
        let e = pipeline(s).unwrap_err();
        assert!(matches!(e, JobManagerError::ReduceCoverageMismatch(_)));
    }

    #[test]
    fn both_fanout_and_reduce_rejected() {
        let s = r#"
[[axis]]
name = "c"
values = ["x"]

[[step]]
id = "a"
program = "p"
sweep = ["c"]

[[step]]
id = "b"
program = "p"
parents = [{ id = "a", fanout = true, reduce_over = ["c"] }]
"#;
        let e = pipeline(s).unwrap_err();
        assert!(matches!(e, JobManagerError::BothFanoutAndReduce(_)));
    }

    #[test]
    fn self_parent_rejected() {
        let s = r#"
[[step]]
id = "a"
program = "p"
parents = [{ id = "a" }]
"#;
        let e = pipeline(s).unwrap_err();
        assert!(matches!(e, JobManagerError::SelfParent(_)));
    }

    #[test]
    fn unknown_step_rejected() {
        let s = r#"
[[step]]
id = "a"
program = "p"
parents = [{ id = "missing" }]
"#;
        let e = pipeline(s).unwrap_err();
        assert!(matches!(e, JobManagerError::UnknownStepId(_, _)));
    }

    #[test]
    fn cycle_detected() {
        let s = r#"
[[step]]
id = "a"
program = "p"
parents = [{ id = "b" }]

[[step]]
id = "b"
program = "p"
parents = [{ id = "a" }]
"#;
        let e = pipeline(s).unwrap_err();
        assert!(matches!(e, JobManagerError::DagHasCycle(_)));
    }
}
```

- [ ] **Step 2: `src/grammar/mod.rs` で chain を公開**

```rust
pub mod source;
pub mod placeholder;
pub mod jobid;
pub mod reader;
pub(crate) mod sweep;
pub(crate) mod chain;
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib grammar::chain:: 2>&1 | tail -5`

Expected: `test result: ok. 10 passed`.

- [ ] **Step 4: コミット**

```bash
git add src/grammar/
git commit -m "feat(grammar): parent resolution (pair/fanout/reduce) + DAG cycle check

SP-2 spec §7 に対応。"
```

---

### Task 9: src/plan/ — ExperimentPlan + I/O

**Files:**
- Create: `src/plan/mod.rs`
- Create: `src/plan/io.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: `src/plan/mod.rs` を作成**

```rust
//! Experiment plan sidecar (SP-2)。

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::JobId;

pub mod io;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExperimentPlan {
    /// Map key は D2 `JobId` newtype (v3: shared package 定義を import)。
    pub jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>,
}
```

- [ ] **Step 2: `src/plan/io.rs` を作成 (atomic rename pattern)**

```rust
//! plan.toml の atomic rename I/O (SP-1 の flow_io と同じパターン)。

use std::path::Path;

use crate::error::JobManagerError;
use crate::plan::ExperimentPlan;

pub fn read_plan(path: &Path) -> Result<ExperimentPlan, JobManagerError> {
    let text = std::fs::read_to_string(path).map_err(|e| JobManagerError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    toml::from_str(&text).map_err(|e| JobManagerError::TomlParse {
        path: path.to_path_buf(),
        source: e,
    })
}

pub fn write_plan(path: &Path, plan: &ExperimentPlan) -> Result<(), JobManagerError> {
    let text = toml::to_string_pretty(plan)?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, text).map_err(|e| JobManagerError::Io {
        path: tmp.clone(),
        source: e,
    })?;
    std::fs::rename(&tmp, path).map_err(|e| JobManagerError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn sample_plan() -> ExperimentPlan {
        use gaussian_job_shared::entities::workflow::JobId;
        let mut params = BTreeMap::new();
        params.insert("route".into(), toml::Value::String("# B3LYP".into()));
        params.insert("nproc".into(), toml::Value::Integer(16));
        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("opt__c=0"), params);
        ExperimentPlan { jobs }
    }

    #[test]
    fn round_trip_preserves_params() {
        use gaussian_job_shared::entities::workflow::JobId;
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.toml");
        let p = sample_plan();
        write_plan(&path, &p).unwrap();
        let back = read_plan(&path).unwrap();
        assert_eq!(back.jobs.len(), 1);
        let params = &back.jobs[&JobId::from("opt__c=0")];
        assert_eq!(params.get("route").unwrap().as_str().unwrap(), "# B3LYP");
        assert_eq!(params.get("nproc").unwrap().as_integer().unwrap(), 16);
    }

    #[test]
    fn read_missing_returns_io_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope.toml");
        let e = read_plan(&path).unwrap_err();
        assert!(matches!(e, JobManagerError::Io { .. }));
    }
}
```

- [ ] **Step 3: `src/lib.rs` で plan モジュールを公開**

```rust
pub mod plan;
```

- [ ] **Step 4: テスト通過確認**

Run: `cargo test --lib plan:: 2>&1 | tail -5`

Expected: `test result: ok. 2 passed`.

- [ ] **Step 5: コミット**

```bash
git add src/plan/ src/lib.rs
git commit -m "feat(plan): ExperimentPlan + atomic rename I/O

最小構造 BTreeMap<JobId, BTreeMap<String, toml::Value>>。
SP-2 spec §8.2 に対応。"
```

---

### Task 10: src/grammar/build.rs — to_jobflow_and_plan

**Files:**
- Create: `src/grammar/build.rs`
- Modify: `src/grammar/mod.rs`

- [ ] **Step 1: 実装を書く**

`src/grammar/build.rs`:

```rust
//! ResolvedStep → (JobFlow, ExperimentPlan).
//! v4: Phase 0 で D2 から JobFlow.work_dir が撤廃されるため root 引数不要。

use std::collections::BTreeMap;

use chrono::Utc;
use gaussian_job_shared::entities::workflow::{Job, JobFlow, JobSpec};
use slurm_async_runner::entities::slurm::SlurmJobConfig;
use uuid::Uuid;

use crate::error::JobManagerError;
use crate::grammar::chain::ResolvedStep;
use crate::grammar::source::FlowMeta;
use crate::plan::ExperimentPlan;

pub(crate) fn to_jobflow_and_plan(
    flow_meta: &FlowMeta,
    resolved: Vec<ResolvedStep>,
) -> Result<(JobFlow, ExperimentPlan), JobManagerError> {
    let mut tags = flow_meta.tags.clone();
    if let Some(ct) = &flow_meta.calc_type {
        tags.insert("calc_type".to_string(), ct.0.clone());   // CalcType.0 で内部 String
    }

    let mut jobs = BTreeMap::new();
    let mut plan_jobs = BTreeMap::new();
    for r in resolved {
        let spec = JobSpec {
            program: r.program,                     // Program newtype をそのまま
            config: SlurmJobConfig::default(),
            body: String::new(),
        };
        let job = Job { spec, parents: r.parents };
        if jobs.insert(r.job_id.clone(), job).is_some() {
            return Err(JobManagerError::DuplicateStepId(r.job_id.0.clone()));
        }
        plan_jobs.insert(r.job_id, r.params);
    }

    let flow = JobFlow {
        uuid: Uuid::now_v7(),
        created_at: Utc::now(),
        tags,
        jobs,
    };
    let plan = ExperimentPlan { jobs: plan_jobs };
    Ok((flow, plan))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grammar::chain::resolve_parents;
    use crate::grammar::reader::parse_experiment_str;
    use crate::grammar::sweep::expand_sweeps;
    use std::path::PathBuf;

    fn pipeline(src: &str) -> Result<(JobFlow, ExperimentPlan), JobManagerError> {
        let parsed = parse_experiment_str(src, &PathBuf::from("/tmp/x.toml"))?;
        let expanded = expand_sweeps(&parsed)?;
        let resolved = resolve_parents(&parsed, expanded)?;
        to_jobflow_and_plan(&parsed.flow, resolved)
    }

    #[test]
    fn flow_uuid_is_v7() {
        let s = "[[step]]\nid = \"opt\"\nprogram = \"g16\"\n";
        let (flow, _) = pipeline(s).unwrap();
        assert_eq!(flow.uuid.get_version_num(), 7);
    }

    #[test]
    fn flow_calc_type_lands_in_tags() {
        let s = r#"
[flow]
calc_type = "opt+freq"
tags = { project = "x" }

[[step]]
id = "opt"
program = "g16"
"#;
        let (flow, _) = pipeline(s).unwrap();
        assert_eq!(flow.tags.get("calc_type"), Some(&"opt+freq".to_string()));
        assert_eq!(flow.tags.get("project"), Some(&"x".to_string()));
    }

    #[test]
    fn jobs_and_plan_keys_match() {
        let s = r#"
[[axis]]
name = "c"
values = ["x", "y"]

[[step]]
id = "opt"
program = "g16"
sweep = ["c"]
[step.params]
route = "# ${c}"
"#;
        use gaussian_job_shared::entities::workflow::JobId;
        let (flow, plan) = pipeline(s).unwrap();
        assert_eq!(flow.jobs.len(), 2);
        assert_eq!(plan.jobs.len(), 2);
        let flow_keys: Vec<_> = flow.jobs.keys().collect();
        let plan_keys: Vec<_> = plan.jobs.keys().collect();
        assert_eq!(flow_keys, plan_keys);
        let route = plan.jobs[&JobId::from("opt__c=0")].get("route").unwrap().as_str().unwrap();
        assert_eq!(route, "# x");
    }
}
```

- [ ] **Step 2: `src/grammar/mod.rs` で build を公開** (内部のみ)

```rust
pub mod source;
pub mod placeholder;
pub mod jobid;
pub mod reader;
pub(crate) mod sweep;
pub(crate) mod chain;
pub(crate) mod build;
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib grammar::build:: 2>&1 | tail -5`

Expected: `test result: ok. 3 passed`.

- [ ] **Step 4: コミット**

```bash
git add src/grammar/
git commit -m "feat(grammar): build JobFlow + ExperimentPlan from resolved steps

SP-2 spec §8 に対応。"
```

---

### Task 11: src/grammar/mod.rs — expand_experiment pipeline 公開

**Files:**
- Modify: `src/grammar/mod.rs`

- [ ] **Step 1: パイプライン公開関数を `src/grammar/mod.rs` に追加**

```rust
use std::path::Path;

use gaussian_job_shared::entities::workflow::JobFlow;

use crate::error::JobManagerError;
use crate::plan::ExperimentPlan;

/// `experiment.toml` → `(JobFlow, ExperimentPlan)`。
/// pure (toml_path 読込のみ、ディスク書き込みは呼び側責務)。
pub fn expand_experiment(toml_path: &Path) -> Result<(JobFlow, ExperimentPlan), JobManagerError> {
    let src = reader::parse_experiment(toml_path)?;
    let expanded = sweep::expand_sweeps(&src)?;
    let resolved = chain::resolve_parents(&src, expanded)?;
    build::to_jobflow_and_plan(&src.flow, resolved)
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::JobId;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn pipeline_end_to_end() {
        let mut f = NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
[flow]
calc_type = "opt"
tags = {{ project = "x" }}

[[axis]]
name = "c"
values = ["benzene"]

[[step]]
id = "opt"
program = "g16"
sweep = ["c"]
[step.params]
route = "# ${{c}}"
"#
        )
        .unwrap();
        let (flow, plan) = expand_experiment(f.path()).unwrap();
        assert_eq!(flow.tags["calc_type"], "opt");
        assert_eq!(flow.tags["project"], "x");
        assert!(flow.jobs.contains_key(&JobId::from("opt__c=0")));
        assert_eq!(
            plan.jobs[&JobId::from("opt__c=0")]["route"].as_str().unwrap(),
            "# benzene"
        );
    }
}
```

- [ ] **Step 2: テスト通過確認**

Run: `cargo test --lib grammar::pipeline_tests:: 2>&1 | tail -5`

Expected: `test result: ok`.

- [ ] **Step 3: コミット**

```bash
git add src/grammar/mod.rs
git commit -m "feat(grammar): expose expand_experiment pipeline

SP-2 spec §9.2 public API に対応。"
```

---

### Task 12: src/path.rs — plan_toml / experiment_toml getter

**Files:**
- Modify: `src/path.rs`

- [ ] **Step 1: 失敗テストを末尾に追加**

```rust
#[test]
fn plan_toml_path_under_flow_dir() {
    let r = PathResolver::new(PathBuf::from("/root"));
    let uuid = Uuid::parse_str("0193a8c0-0000-7000-8000-000000000000").unwrap();
    let p = r.plan_toml(&uuid);
    assert!(p.ends_with("plan.toml"));
    assert!(p.starts_with("/root"));
}

#[test]
fn experiment_toml_path_under_flow_dir() {
    let r = PathResolver::new(PathBuf::from("/root"));
    let uuid = Uuid::nil();
    let p = r.experiment_toml(&uuid);
    assert!(p.ends_with("experiment.toml"));
}
```

- [ ] **Step 2: getter を実装**

`src/path.rs` の `impl PathResolver { ... }` 内に追加:

```rust
pub fn plan_toml(&self, flow_uuid: &Uuid) -> PathBuf {
    self.flow_dir(flow_uuid).join("plan.toml")
}

pub fn experiment_toml(&self, flow_uuid: &Uuid) -> PathBuf {
    self.flow_dir(flow_uuid).join("experiment.toml")
}
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib path:: 2>&1 | tail -5`

Expected: `test result: ok`.

- [ ] **Step 4: コミット**

```bash
git add src/path.rs
git commit -m "feat(path): plan_toml() and experiment_toml() resolvers"
```

---

### Task 13: src/py_export/plan.rs — PyExperimentPlan

**Files:**
- Create: `src/py_export/plan.rs`

- [ ] **Step 1: PyExperimentPlan + I/O wrapper を書く**

`src/py_export/plan.rs`:

```rust
//! Python 公開: ExperimentPlan (read-only view).

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

use crate::plan::{io as plan_io, ExperimentPlan};

#[gen_stub_pyclass]
#[pyclass(name = "ExperimentPlan", module = "job_manager._job_manager_core", frozen)]
#[derive(Clone)]
pub struct PyExperimentPlan {
    pub(crate) inner: ExperimentPlan,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyExperimentPlan {
    #[getter]
    fn jobs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let dict = pyo3::types::PyDict::new(py);
        for (k, params) in &self.inner.jobs {
            let pdict = pyo3::types::PyDict::new(py);
            for (pk, pv) in params {
                let py_value = pythonize::pythonize(py, pv)?;
                pdict.set_item(pk, py_value)?;
            }
            // D2 JobId(pub String) の内部文字列を Python dict key として使う
            dict.set_item(&k.0, pdict)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!("ExperimentPlan(jobs={} entries)", self.inner.jobs.len())
    }
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn read_plan(path: PathBuf) -> PyResult<PyExperimentPlan> {
    let plan = plan_io::read_plan(&path).map_err(crate::py_export::error::to_py_err)?;
    Ok(PyExperimentPlan { inner: plan })
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn write_plan(path: PathBuf, plan: PyExperimentPlan) -> PyResult<()> {
    plan_io::write_plan(&path, &plan.inner).map_err(crate::py_export::error::to_py_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn py_experiment_plan_holds_inner() {
        use gaussian_job_shared::entities::workflow::JobId;
        let mut params = BTreeMap::new();
        params.insert("k".into(), toml::Value::String("v".into()));
        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("a"), params);
        let pp = PyExperimentPlan { inner: ExperimentPlan { jobs } };
        assert_eq!(pp.inner.jobs.len(), 1);
    }
}
```

- [ ] **Step 2: ビルド**

Run: `cargo build --features pyo3,pythonize,stub_gen 2>&1 | tail -5`

Expected: 成功。

- [ ] **Step 3: コミット**

```bash
git add src/py_export/plan.rs
git commit -m "feat(py): PyExperimentPlan + read_plan/write_plan pyfunctions

SP-2 spec §10 に対応。"
```

---

### Task 14: src/py_export/grammar.rs — expand_experiment + parse_job_id

**Files:**
- Create: `src/py_export/grammar.rs`
- Modify: `src/py_export/mod.rs`

- [ ] **Step 1: pyfunction を実装**

`src/py_export/grammar.rs`:

```rust
//! Python 公開: expand_experiment / parse_job_id.

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_stub_gen::derive::gen_stub_pyfunction;

use crate::grammar;
use crate::grammar::jobid;
use crate::py_export::plan::PyExperimentPlan;

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn expand_experiment(
    py: Python<'_>,
    toml_path: PathBuf,
) -> PyResult<(PyObject, PyExperimentPlan)> {
    let (flow, plan) =
        grammar::expand_experiment(&toml_path).map_err(crate::py_export::error::to_py_err)?;
    // JobFlow は D2 の構造体 (v4: work_dir 撤廃後)。pythonize で Python dict 化して返す。
    // consumer は dict として受け取り、必要なら gaussian_job_shared が用意する from_dict
    // 相当 API を使う想定。
    let flow_py: Bound<'_, PyAny> = pythonize::pythonize(py, &flow)?;
    Ok((flow_py.unbind(), PyExperimentPlan { inner: plan }))
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn parse_job_id<'py>(py: Python<'py>, job_id: &str) -> PyResult<Bound<'py, PyDict>> {
    let parts = jobid::parse_job_id(job_id).map_err(crate::py_export::error::to_py_err)?;
    let dict = PyDict::new(py);
    dict.set_item("source_step_id", parts.source_step_id)?;
    let pylist = PyList::new(
        py,
        parts.axis_combo.iter().map(|(k, v)| (k.to_string(), *v)),
    )?;
    dict.set_item("axis_combo", pylist)?;
    Ok(dict)
}
```

- [ ] **Step 2: `src/py_export/mod.rs` で register**

既存の register 関数 (`_job_manager_core` module に items を追加する箇所) に以下を加える:

```rust
pub mod grammar;
pub mod plan;

// register 関数内:
m.add_function(wrap_pyfunction!(grammar::expand_experiment, m)?)?;
m.add_function(wrap_pyfunction!(grammar::parse_job_id, m)?)?;
m.add_class::<plan::PyExperimentPlan>()?;
m.add_function(wrap_pyfunction!(plan::read_plan, m)?)?;
m.add_function(wrap_pyfunction!(plan::write_plan, m)?)?;
```

- [ ] **Step 3: ビルド + テスト**

Run: `cargo build --features pyo3,pythonize,stub_gen 2>&1 | tail -5`

Expected: 成功。

Run: `cargo test --lib 2>&1 | tail -5`

Expected: `test result: ok`.

- [ ] **Step 4: コミット**

```bash
git add src/py_export/
git commit -m "feat(py): expand_experiment + parse_job_id pyfunctions

JobFlow は pythonize 経由で Python dict として返す。
SP-2 spec §10 に対応。"
```

---

### Task 15: src/lib.rs + python/job_manager/__init__.py — re-exports

**Files:**
- Modify: `src/lib.rs`
- Modify: `python/job_manager/__init__.py`

- [ ] **Step 1: `src/lib.rs` で grammar / plan を public re-export**

`src/lib.rs` に追加:

```rust
pub use grammar::expand_experiment;
pub use grammar::jobid::{build_job_id, parse_job_id, validate_step_id, JobIdParts};
pub use plan::{io::{read_plan, write_plan}, ExperimentPlan};
```

`JobManagerError` も SP-1 で既に re-export されているか確認、無ければ `pub use error::JobManagerError;` を追加。

- [ ] **Step 2: Python 側 re-export**

`python/job_manager/__init__.py` を確認し SP-1 既存 import の隣に追加:

```python
from job_manager._job_manager_core import (
    # ... SP-1 既存 import ...
    expand_experiment,
    parse_job_id,
    ExperimentPlan,
    read_plan,
    write_plan,
)

__all__ = [
    # ... SP-1 既存 ...
    "expand_experiment",
    "parse_job_id",
    "ExperimentPlan",
    "read_plan",
    "write_plan",
]
```

- [ ] **Step 3: maturin で Python 側ビルド + smoke**

Run: `uv run maturin develop 2>&1 | tail -3 && uv run python -c 'from job_manager import expand_experiment, parse_job_id, ExperimentPlan; print("ok")'`

Expected: `ok`.

- [ ] **Step 4: コミット**

```bash
git add src/lib.rs python/job_manager/__init__.py
git commit -m "feat(api): re-export SP-2 grammar + plan public surface"
```

---

### Task 16: tests/fixtures/ — TOML fixtures

**Files:** 多数 (下記 11 ファイル)

- [ ] **Step 1: `tests/fixtures/minimal_step.toml`**

```toml
[[step]]
id = "opt"
program = "g16"
```

- [ ] **Step 2: `tests/fixtures/single_axis.toml`**

```toml
[[axis]]
name = "compound"
values = ["benzene", "toluene", "p-xylene"]

[[step]]
id = "opt"
program = "g16"
sweep = ["compound"]
[step.params]
route = "# ${compound}"
```

- [ ] **Step 3: `tests/fixtures/pair_chain.toml`**

```toml
[[axis]]
name = "c"
values = ["x", "y"]

[[step]]
id = "opt"
program = "g16"
sweep = ["c"]

[[step]]
id = "freq"
program = "g16"
sweep = ["c"]
parents = [{ id = "opt" }]
```

- [ ] **Step 4: `tests/fixtures/fanout.toml`**

```toml
[[axis]]
name = "c"
values = ["x", "y"]
[[axis]]
name = "m"
values = ["1", "2"]

[[step]]
id = "prep"
program = "g16"
sweep = ["c"]

[[step]]
id = "opt"
program = "g16"
sweep = ["c", "m"]
parents = [{ id = "prep", fanout = true }]
```

- [ ] **Step 5: `tests/fixtures/reduce.toml`**

```toml
[[axis]]
name = "c"
values = ["x"]
[[axis]]
name = "m"
values = ["1", "2", "3"]

[[step]]
id = "scan"
program = "g16"
sweep = ["c", "m"]

[[step]]
id = "compare"
program = "g16"
sweep = ["c"]
parents = [{ id = "scan", reduce_over = ["m"] }]
```

- [ ] **Step 6: `tests/fixtures/multi_parent.toml`**

```toml
[[step]]
id = "a"
program = "g16"

[[step]]
id = "b"
program = "g16"

[[step]]
id = "merge"
program = "post"
parents = [
    { id = "a" },
    { id = "b", kind = "afterany" },
]
```

- [ ] **Step 7: `tests/fixtures/legacy_step_compounds.toml`**

```toml
[[step]]
id = "opt"
program = "g16"
compounds = ["benzene"]
```

- [ ] **Step 8: `tests/fixtures/legacy_step_calc_type.toml`**

```toml
[[step]]
id = "opt"
program = "g16"
calc_type = "opt"
```

- [ ] **Step 9: `tests/fixtures/legacy_step_tags.toml`**

```toml
[[step]]
id = "opt"
program = "g16"
tags = { k = "v" }
```

- [ ] **Step 10: `tests/fixtures/error_both_fanout_and_reduce.toml`**

```toml
[[axis]]
name = "c"
values = ["x"]

[[step]]
id = "a"
program = "g16"
sweep = ["c"]

[[step]]
id = "b"
program = "g16"
parents = [{ id = "a", fanout = true, reduce_over = ["c"] }]
```

- [ ] **Step 11: `tests/fixtures/error_dag_cycle.toml`**

```toml
[[step]]
id = "a"
program = "g16"
parents = [{ id = "b" }]

[[step]]
id = "b"
program = "g16"
parents = [{ id = "a" }]
```

- [ ] **Step 12: コミット**

```bash
git add tests/fixtures/
git commit -m "test(grammar): TOML fixtures for integration tests"
```

---

### Task 17: tests/integration_grammar.rs — Rust integration

**Files:**
- Create: `tests/integration_grammar.rs`

- [ ] **Step 1: integration test を書く**

`tests/integration_grammar.rs`:

```rust
//! Integration tests for the SP-2 grammar pipeline.
//! v4: D2 から work_dir 撤廃後、`expand_experiment` は toml_path のみ。

use std::path::{Path, PathBuf};

use gaussian_job_shared::entities::workflow::JobId;
use job_manager::{expand_experiment, parse_job_id, read_plan, write_plan};
use tempfile::tempdir;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn minimal_step_yields_single_job() {
    let (flow, plan) = expand_experiment(&fixture("minimal_step.toml")).unwrap();
    assert_eq!(flow.jobs.len(), 1);
    assert!(flow.jobs.contains_key(&JobId::from("opt")));
    assert!(plan.jobs.contains_key(&JobId::from("opt")));
}

#[test]
fn single_axis_expands_to_3_jobs() {
    let (flow, plan) = expand_experiment(&fixture("single_axis.toml")).unwrap();
    assert_eq!(flow.jobs.len(), 3);
    assert_eq!(plan.jobs.len(), 3);
    let route0 = plan.jobs[&JobId::from("opt__compound=0")]["route"]
        .as_str()
        .unwrap();
    assert_eq!(route0, "# benzene");
}

#[test]
fn pair_chain_creates_1_to_1_edges() {
    let (flow, _) = expand_experiment(&fixture("pair_chain.toml")).unwrap();
    let freq_jobs: Vec<_> = flow
        .jobs
        .iter()
        .filter(|(k, _)| k.0.starts_with("freq__"))
        .collect();
    assert_eq!(freq_jobs.len(), 2);
    for (_, j) in freq_jobs {
        assert_eq!(j.parents.len(), 1);
    }
}

#[test]
fn fanout_creates_1_to_n_edges() {
    let (flow, _) = expand_experiment(&fixture("fanout.toml")).unwrap();
    let opt_jobs: Vec<_> = flow
        .jobs
        .iter()
        .filter(|(k, _)| k.0.starts_with("opt__"))
        .collect();
    assert_eq!(opt_jobs.len(), 4);
    for (_, j) in opt_jobs {
        assert_eq!(j.parents.len(), 1);
    }
}

#[test]
fn reduce_creates_n_to_1_edges() {
    let (flow, _) = expand_experiment(&fixture("reduce.toml")).unwrap();
    let compare: Vec<_> = flow
        .jobs
        .iter()
        .filter(|(k, _)| k.0.starts_with("compare__"))
        .collect();
    assert_eq!(compare.len(), 1);
    assert_eq!(compare[0].1.parents.len(), 3);
}

#[test]
fn multi_parent_with_mixed_kinds() {
    use slurm_async_runner::entities::slurm::DependencyType;
    let (flow, _) = expand_experiment(&fixture("multi_parent.toml")).unwrap();
    let merge = flow.jobs.get(&JobId::from("merge")).unwrap();
    assert_eq!(merge.parents.len(), 2);
    let kinds: Vec<DependencyType> = merge.parents.iter().map(|e| e.kind).collect();
    assert!(kinds.contains(&DependencyType::Afterok));
    assert!(kinds.contains(&DependencyType::Afterany));
}

#[test]
fn legacy_step_compounds_errors() {
    let e = expand_experiment(&fixture("legacy_step_compounds.toml")).unwrap_err();
    assert!(matches!(e, job_manager::JobManagerError::LegacyToml { .. }));
}

#[test]
fn dag_cycle_detected() {
    let e = expand_experiment(&fixture("error_dag_cycle.toml")).unwrap_err();
    assert!(matches!(e, job_manager::JobManagerError::DagHasCycle(_)));
}

#[test]
fn plan_toml_round_trip() {
    let (_, plan) = expand_experiment(&fixture("single_axis.toml")).unwrap();
    let dir = tempdir().unwrap();
    let path = dir.path().join("plan.toml");
    write_plan(&path, &plan).unwrap();
    let back = read_plan(&path).unwrap();
    assert_eq!(back.jobs.len(), plan.jobs.len());
}

#[test]
fn parse_job_id_round_trip_via_grammar() {
    let (flow, _) = expand_experiment(&fixture("fanout.toml")).unwrap();
    for job_id in flow.jobs.keys() {
        let parts = parse_job_id(&job_id.0).unwrap();
        assert!(!parts.source_step_id.is_empty());
    }
}
```

- [ ] **Step 2: テスト実行**

Run: `cargo test --test integration_grammar 2>&1 | tail -10`

Expected: `test result: ok. 10 passed`.

- [ ] **Step 3: コミット**

```bash
git add tests/integration_grammar.rs
git commit -m "test(grammar): integration tests (minimal/sweep/parent/error/round-trip)"
```

---

### Task 18: python/tests/test_grammar.py — Python E2E

**Files:**
- Create: `python/tests/test_grammar.py`

- [ ] **Step 1: Python E2E テストを書く**

`python/tests/test_grammar.py`:

```python
"""Python E2E for SP-2 grammar."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from job_manager import (
    ExperimentPlan,
    expand_experiment,
    parse_job_id,
    read_plan,
    write_plan,
)

FIXTURES = Path(__file__).parent.parent.parent / "tests" / "fixtures"


def test_minimal_step():
    flow, plan = expand_experiment(str(FIXTURES / "minimal_step.toml"))
    # flow は pythonize 経由の dict (key は JobId の内部 String 表現)
    assert "opt" in flow["jobs"]
    assert "opt" in plan.jobs


def test_single_axis_expands_3_jobs():
    _, plan = expand_experiment(str(FIXTURES / "single_axis.toml"))
    assert len(plan.jobs) == 3
    assert plan.jobs["opt__compound=0"]["route"] == "# benzene"


def test_parse_job_id_decomposes():
    parts = parse_job_id("opt__compound=2__method=0")
    assert parts["source_step_id"] == "opt"
    assert parts["axis_combo"] == [("compound", 2), ("method", 0)]


def test_parse_job_id_no_sweep():
    parts = parse_job_id("compare")
    assert parts["source_step_id"] == "compare"
    assert parts["axis_combo"] == []


def test_invalid_job_id_raises():
    with pytest.raises(ValueError):
        parse_job_id("opt__compound=abc")


def test_legacy_compounds_raises():
    with pytest.raises(ValueError):
        expand_experiment(str(FIXTURES / "legacy_step_compounds.toml"))


def test_dag_cycle_raises():
    with pytest.raises(ValueError):
        expand_experiment(str(FIXTURES / "error_dag_cycle.toml"))


def test_plan_round_trip():
    _, plan = expand_experiment(str(FIXTURES / "single_axis.toml"))
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "plan.toml"
        write_plan(str(p), plan)
        back = read_plan(str(p))
        assert len(back.jobs) == len(plan.jobs)


def test_jobflow_has_no_work_dir():
    """v4: D2 から work_dir 撤廃後、pythonize 結果にも work_dir キーは無い。"""
    flow, _ = expand_experiment(str(FIXTURES / "minimal_step.toml"))
    assert "work_dir" not in flow
```

- [ ] **Step 2: maturin で Python パッケージを再ビルド**

Run: `uv run maturin develop 2>&1 | tail -3`

Expected: 成功。

- [ ] **Step 3: テスト実行**

Run: `uv run pytest python/tests/test_grammar.py -v 2>&1 | tail -15`

Expected: 全 pass。

- [ ] **Step 4: コミット**

```bash
git add python/tests/test_grammar.py
git commit -m "test(py): grammar E2E (expand_experiment + parse_job_id + plan I/O)"
```

---

### Task 19: 仕上げ — fmt / clippy / stub_gen / coverage / README

**Files:**
- Modify: `README.md`
- Generated: `python/job_manager/_job_manager_core/__init__.pyi`

- [ ] **Step 1: cargo fmt**

Run: `cargo fmt && cargo fmt --check`

Expected: 整形差分なし。

- [ ] **Step 2: cargo clippy**

Run: `cargo clippy --all-features -- -D warnings 2>&1 | tail -10`

Expected: 警告 0 件。

- [ ] **Step 3: stub_gen**

Run: `cargo run --bin stub_gen --features stub_gen 2>&1 | tail -3 && uv run ruff format python/`

Expected: 成功。

- [ ] **Step 4: cargo llvm-cov でカバレッジ**

Run: `cargo llvm-cov --fail-under-lines 80 --all-features 2>&1 | tail -5`

Expected: ≥ 80%。不足するモジュールがあれば該当タスクに戻ってテスト追加。

- [ ] **Step 5: README.md に SP-2 capability セクション追加**

`README.md` の SP-1 capability セクションの後ろに追加:

```markdown
## SP-2 (grammar) capabilities

- `expand_experiment(toml_path)` — `experiment.toml` を解析・展開して `(JobFlow, ExperimentPlan)` を返す純粋関数
- `[[axis]]` で sweep 軸を宣言 (scalar/struct)、`[[step]]` で sweep + parents 指定
- parent 解決の 3 mode (pair_by_axes / fanout / reduce_over) と SLURM `DependencyType` per-edge 指定
- `${axis}` / `${axis.field}` プレースホルダ展開 (`$$` エスケープ)
- legacy `gaussian_batch.toml` 形状の検出と migration hint (8 パターン)
- JobId 形式: `<step.id>__<axis>=<idx>__...` (`parse_job_id` で分解可)
- `ExperimentPlan` sidecar (`plan.toml`) で per-job params を D2 と分離保持
```

- [ ] **Step 6: 最終全テスト**

Run: `cargo test --all-features 2>&1 | tail -5 && uv run pytest python/tests 2>&1 | tail -5`

Expected: 両方 pass。

- [ ] **Step 7: コミット**

```bash
git add README.md python/
git diff --cached --quiet || git commit -m "chore(sp2): polish — fmt + clippy + stubs + README"
```

---

### Task 20: PR 作成

**Files:** none

- [ ] **Step 1: push して PR を開く**

```bash
git push -u origin feat/sp2-impl
gh pr create --base main --title "feat(sp2): grammar layer (experiment.toml → JobFlow + plan)" --body "$(cat <<'EOF'
## Summary

SP-2 grammar 実装。\`expand_experiment(path)\` を純粋関数として公開し、experiment.toml から (JobFlow, ExperimentPlan) を構築する。

## 前提

- D2 (\`gaussian-job-shared2\`) の \`JobFlow.work_dir\` 撤廃 PR が merged
- SP-1 follow-up PR (work_dir 参照を PathResolver 経由に置換) が merged
- A1 (\`slurm-async-runner2\`) は不可侵
- D2 newtype (\`JobId\` / \`Program\` / \`CalcType\`) は保持・import 利用

## 主な追加

- \`crate::grammar::*\` — reader / placeholder / jobid / sweep / chain / build / source
- \`crate::plan::*\` — ExperimentPlan + atomic I/O
- \`crate::JobManagerError\` に Grammar* / Legacy / Placeholder / Dag variant
- \`PathResolver::plan_toml\` / \`experiment_toml\` getter
- Python: \`expand_experiment\` / \`parse_job_id\` / \`ExperimentPlan\` / \`read_plan\` / \`write_plan\`

## 設計

詳細は spec v4 (\`docs/superpowers/specs/2026-05-12-job-manager-sp2-design.md\`) を参照。

## Test plan

- [ ] cargo test --all-features 通過
- [ ] cargo clippy --all-features -- -D warnings 通過
- [ ] cargo fmt --check 通過
- [ ] uv run maturin develop 成功
- [ ] uv run pytest python/tests 通過
- [ ] cargo llvm-cov ≥ 80%
- [ ] cargo run --bin stub_gen で .pyi 再生成、ruff format クリーン
- [ ] integration_grammar.rs 全 pass
- [ ] python/tests/test_grammar.py 全 pass
EOF
)"
```

---

## Capabilities (SP-2 完了時)

```
job_manager::expand_experiment(path) -> Result<(JobFlow, ExperimentPlan), JobManagerError>
job_manager::parse_job_id(s) -> Result<JobIdParts<'_>, JobManagerError>
job_manager::build_job_id(step, combo) -> String
job_manager::validate_step_id(s) -> Result<&str, JobManagerError>
job_manager::validate_job_id(s) -> Result<&str, JobManagerError>
job_manager::read_plan(path) -> Result<ExperimentPlan, JobManagerError>
job_manager::write_plan(path, &plan) -> Result<(), JobManagerError>
job_manager::PathResolver::plan_toml(&uuid) -> PathBuf
job_manager::PathResolver::experiment_toml(&uuid) -> PathBuf
```

D2 から import (v4: work_dir 撤廃後の JobFlow):

```
gaussian_job_shared::entities::workflow::{JobId, Program, CalcType, JobFlow, Job, JobEdge, JobSpec};
```

Python:

```python
from job_manager import expand_experiment, parse_job_id, ExperimentPlan, read_plan, write_plan
```

---

## Out of scope (deferred to SP-3)

- `common.toml` 読み込み + `SlurmJobConfig` 合成
- `JobSpec.body` の bash render
- A1 `SbatchManager` 経由の sbatch 投入
- CLI (`run` / `submit` / `show` / `tick` / `search`)
- log_paths 解決 (SLURM `%j`/`%x` 展開)

---

## Self-Review

### Spec coverage check

spec (v4) の各セクションに対応するタスク:

| Spec section | Task |
|---|---|
| §1 背景 | 説明のみ |
| §2 採用アプローチ | 全タスク |
| §3 必須 D2 変更 (work_dir 撤廃) | Phase 0 (Task P0.1-P0.5) + Phase 1 (Task P1.1-P1.4) |
| §4.1 全体構造 | Task 6 (reader) |
| §4.2 [flow] block | Task 6 (`parse_flow_block`) |
| §4.3 [[axis]] block | Task 6 (`parse_axes` / `parse_axis_values`) |
| §4.4 [[step]] block | Task 6 (`parse_steps`) |
| §4.4.1 step.parents | Task 6 (`parse_parents`) |
| §4.4.2 Legacy 検出 | Task 6 + Task 16 fixtures |
| §4.5 Placeholder | Task 4 |
| §5 JobId 命名 | Task 5 |
| §6 Sweep 展開 | Task 7 |
| §7 Parent 解決 | Task 8 |
| §8.1 JobFlow 出力 | Task 10 |
| §8.2 ExperimentPlan | Task 9 + Task 10 |
| §8.3 FS レイアウト | Task 12 |
| §9 Rust モジュール | Task 3-12 全体 |
| §10 Python API | Task 13 + Task 14 + Task 15 |
| §11 テスト計画 | Task 16-18 |
| §12 リスク | 各タスクで mitigate |
| §13 完了基準 | Task 19 で確認 |

### Type consistency

- `JobId` / `Program` / `CalcType` は **D2 から import** して使用 (job-manager 側で再定義しない)
- `JobFlow.work_dir` フィールドは v4 で **撤廃** (D2 PR で削除、SP-1 は PathResolver::flow_dir() 経由)
- `DependencyType` (A1) はそのまま `ParentRef.kind` / `JobEdge.kind` に流れる
- `ExperimentPlan.jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>` は build / io / py_export 全てで同一型 (key は D2 `JobId` newtype)
- `JobIdParts<'a>` は `parse_job_id` の戻り値、`source_step_id: &'a str` + `axis_combo: Vec<(&'a str, usize)>` で一貫
- `expand_experiment(toml_path: &Path) -> Result<(JobFlow, ExperimentPlan), JobManagerError>` のシグネチャは Task 11 / Task 14 / Task 17 で同一
- `parse_job_id(s: &str)` の戻り値型 (Rust `JobIdParts<'_>` / Python dict) は Task 5 / Task 14 / Task 18 で一貫

### Placeholder scan

- 全タスクで実コードが提示されている (TBD/TODO なし)
- 全エラーメッセージ・テスト名は具体的
- 型 / 関数名はタスク間で一致 (`expand_experiment`, `parse_job_id`, `build_job_id`, `ExperimentPlan`, `JobIdParts`)
- 全ステップに具体的な command / expected output / commit message が含まれる

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-12-job-manager-sp2.md`.

**実行方法 2 択:**

1. **Subagent-Driven (推奨)** — 各タスクごとに fresh subagent をディスパッチし、レビュー → 次タスクの反復。各 Task の責務が `grammar/<file>.rs` 単位で明確なので、Task 境界での文脈リセットと相性がよい。

2. **Inline Execution** — このセッション内で executing-plans でバッチ実行。チェックポイントでレビュー。

**どちらで進めますか?**

- Subagent-Driven の場合: `superpowers:subagent-driven-development` skill を起動
- Inline の場合: `superpowers:executing-plans` skill を起動
