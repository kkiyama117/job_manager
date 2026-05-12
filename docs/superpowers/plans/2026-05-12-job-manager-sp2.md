# job-manager SP-2 (plan + jobid helpers) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SP-3 (submit + CLI) が必要とする最小限の Rust 機能を実装する: (1) `ExperimentPlan` (per-job params の永続化), (2) `JobId` helpers (validate / build / parse), (3) `PathResolver` getter 追加。`experiment.toml` DSL は **実装しない** — ユーザーは Python で `JobFlow` を直接構築する (spec §1.1 参照)。

**Architecture:** 3 段 PR スタック。**Phase 0** で D2 から `JobFlow.work_dir` フィールドのみを撤廃。**Phase 1** で job-manager SP-1 コードの `flow.work_dir` 参照を置換。**Phase 2** で SP-2 (plan + jobid) を実装。grammar DSL を扱わないので Phase 2 は v4 の 20 task → 11 task に縮小。

**Tech Stack:** Rust 2024, PyO3 0.28 (abi3-py312), tokio 1.0, serde + toml 1.1, chrono, uuid v7, pythonize, rstest, pyo3-stub-gen。Python 3.12+, pytest, maturin, ruff。

**Spec:** `docs/superpowers/specs/2026-05-12-job-manager-sp2-design.md` (v5)

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
  Phase 2 PR:  feat(sp2): plan sidecar + jobid helpers (no experiment.toml DSL)
               base=main, head=feat/sp2-impl
```

---

## D2 利用ポリシー (newtype 保持、不要フィールドのみ撤廃)

| D2 型 / フィールド | 判定 | 用途・撤廃理由 |
|---|---|---|
| `JobId` newtype | **保持** | 展開後の job 識別子。SP-2 で命名規約 helpers を提供 |
| `Program` newtype | **保持** | step.program / JobSpec.program (Python で構築) |
| `CalcType` newtype | **保持** | `tags["calc_type"]` の typed 表現 |
| `Job` / `JobEdge` / `JobSpec` | **保持** | Python authoring の構築対象 |
| `JobFlow.uuid` | **保持** | identity / search key |
| `JobFlow.created_at` | **保持** | 時系列ソート |
| `JobFlow.work_dir` | **Phase 0 で撤廃** | `<root>/<uuid>/` で導出可、`mv` で drift |
| `JobFlow.tags` | **保持** | search (`tags["calc_type"]` 含む) |
| `JobFlow.jobs` | **保持** | DAG 本体 |

D2 の `pyo3` feature をパス依存上で無効化する規約 (Pyclass Single Owner) は SP-1 と同じ。

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

### Phase 2 (job-manager `src/`)

```
src/
├── error.rs                     # MODIFY: add JobId* / Plan* error variants
├── path.rs                      # MODIFY: add plan_toml() + experiment_toml() getter
├── jobid.rs                     # CREATE: validate / build / parse helpers
├── plan/
│   ├── mod.rs                   # CREATE: ExperimentPlan
│   └── io.rs                    # CREATE: read_plan / write_plan (atomic rename)
├── lib.rs                       # MODIFY: re-export jobid + plan
└── py_export/
    ├── mod.rs                   # MODIFY: register sub-modules
    ├── jobid.rs                 # CREATE: validate/build/parse pyfunctions
    └── plan.rs                  # CREATE: PyExperimentPlan + read_plan/write_plan pyfunctions

tests/
└── integration_plan.rs          # CREATE: 12-job Python-style construction + round-trip

python/
├── job_manager/__init__.py      # MODIFY: re-exports
└── tests/
    ├── test_jobid.py            # CREATE: validate/build/parse
    └── test_plan.py             # CREATE: Python authoring example (§1.1)
```

**`src/grammar/` モジュールは作らない** (spec §1.2 / §2 案 B 採用の表明)。

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

`PyJobFlow` の `work_dir` getter/setter / `__new__` 引数を削除。

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
- \`JobFlow\` を run/search に純粋なメタデータコンテナとして整理

## Scope

- \`JobFlow\` struct から \`work_dir\` フィールドを削除
- pyclass \`PyJobFlow\` の対応する getter/setter / \`__new__\` 引数を削除
- 既存テスト fixture を更新

newtype は変更しない。

## Migration

Downstream (job-manager) 側の \`flow.work_dir\` 参照は
\`PathResolver::flow_dir(&flow.uuid)\` 経由に置換する (別 PR)。

## Test plan

- [ ] cargo test --all-features 通過
- [ ] cargo clippy --all-features -- -D warnings 通過
- [ ] cargo fmt --check 通過
EOF
)"
```

- [ ] **Step 2: merge 完了まで待つ**

- [ ] **Step 3: job-manager repo に戻る**

Run: `cd ../job-manager && git status`

---

# Phase 1: SP-1 follow-up — `work_dir` 参照置換 (job-manager `refactor/sp1-drop-work-dir`, base=main)

**前提:** Phase 0 PR が D2 main に merged 済み。
**Scope:** job-manager SP-1 既存コード内の `flow.work_dir` 参照を `PathResolver::flow_dir(&flow.uuid)` 経由に置換するのみ。型変更なし。

---

### Task P1.1: Phase 1 — ブランチ作成 + 失敗箇所の確認

- [ ] **Step 1: main を最新化**

```bash
git checkout main
git pull origin main
cargo build 2>&1 | tail -10
```

Expected: ビルドが work_dir 関連で失敗 — 失敗メッセージが Phase 1 の作業対象。

- [ ] **Step 2: 作業ブランチ**

Run: `git checkout -b refactor/sp1-drop-work-dir`

- [ ] **Step 3: work_dir 参照箇所を grep**

Run: `grep -rn "work_dir\|\.work_dir" src/ tests/ python/ 2>/dev/null`

Expected: SP-1 既存コードで参照箇所がリストされる。

---

### Task P1.2: Phase 1 — work_dir 参照置換

**Files:**
- Modify: `src/view.rs`
- Modify: `src/flow_io.rs` (test fixture)
- Modify: `src/walk.rs` (test fixture)
- Modify: 他、grep で見つかった箇所すべて

- [ ] **Step 1: `src/view.rs` の `flow.work_dir` 参照を置換**

`flow.work_dir.join(...)` を `resolver.flow_dir(&flow.uuid).join(...)` に置換。`PathResolver` が無いなら関数シグネチャに `resolver: &PathResolver` を追加するか、call site で計算。

- [ ] **Step 2: `src/flow_io.rs` の test fixture**

`JobFlow { ..., work_dir: ..., ... }` の struct literal から行を削除。期待 TOML から `work_dir = "..."` 行を削除。

- [ ] **Step 3: `src/walk.rs` の test fixture**

同上。

- [ ] **Step 4: 残り参照箇所**

Task P1.1 Step 3 の grep 結果すべてを置換。

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

Expected: 全 pass。

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
- 型変更なし
- work_dir 参照のみ置換 + 再生成済み \`.pyi\`

## Test plan

- [ ] cargo test --all-features 通過
- [ ] cargo clippy --all-features -- -D warnings 通過
- [ ] uv run pytest python/tests 通過
EOF
)"
```

- [ ] **Step 2: merge 完了まで待つ**

---

# Phase 2: SP-2 minimal — plan + jobid helpers (job-manager `feat/sp2-impl`, base=main)

**前提:** Phase 0 と Phase 1 が main にマージ済み。
**Scope:** SP-3 が必要とする最小機能 — `ExperimentPlan` sidecar + `JobId` helpers + `PathResolver` getter。**`experiment.toml` DSL は実装しない**。

---

### Task 1: ブランチ作成 + Cargo.toml 確認

**Files:**
- Modify: `Cargo.toml` (必要なら)

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

### Task 2: src/error.rs — JobId* + Plan* バリアント追加

**Files:**
- Modify: `src/error.rs`

- [ ] **Step 1: 失敗テストを追加**

`src/error.rs` 末尾の `mod tests` に:

```rust
#[test]
fn invalid_step_id_carries_input() {
    let err = JobManagerError::InvalidStepId("opt=1".to_string());
    assert!(err.to_string().contains("opt=1"));
}

#[test]
fn reserved_job_id_carries_name() {
    let err = JobManagerError::ReservedJobId("flow".to_string());
    assert!(err.to_string().contains("flow"));
    assert!(err.to_string().contains("reserved"));
}
```

Run: `cargo test --lib invalid_step_id 2>&1 | tail -5`

Expected: 失敗 (バリアント未定義)。

- [ ] **Step 2: バリアント追加**

```rust
#[error("invalid step id '{0}': must match [A-Za-z0-9_-]+")]
InvalidStepId(String),

#[error("invalid job id '{0}': must match [A-Za-z0-9_\\-=]+")]
InvalidJobId(String),

#[error("reserved id '{0}' (reserved: flow, plan, experiment, derived, status)")]
ReservedJobId(String),

#[error("job id parse error in '{id}' at piece '{piece}': {reason}")]
JobIdParseError {
    id: String,
    piece: String,
    reason: String,
},
```

- [ ] **Step 3: Python 側のエラーマッピング更新**

`src/py_export/error.rs` の `to_py_err` で `InvalidStepId` / `InvalidJobId` / `ReservedJobId` / `JobIdParseError` を `PyValueError` にマップ。

- [ ] **Step 4: テスト通過確認**

Run: `cargo test --lib error:: 2>&1 | tail -5`

Expected: 全 pass。

- [ ] **Step 5: コミット**

```bash
git add src/error.rs src/py_export/error.rs
git commit -m "feat(error): add JobId* error variants for SP-2

SP-2 spec §7.3 に対応。grammar DSL を実装しないため、Grammar* /
Legacy / Placeholder / Dag 等のバリアントは追加しない。"
```

---

### Task 3: src/jobid.rs — validate / build / parse helpers

**Files:**
- Create: `src/jobid.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: 実装 + テストを書く**

`src/jobid.rs` (新規):

```rust
//! JobId 命名規約 helper。
//!
//! 規約:
//! - step_id: `[A-Za-z0-9_-]+`、予約名禁止
//! - JobId: `<step_id>` または `<step_id>__<axis>=<idx>__...`
//! - 予約: `flow`, `plan`, `experiment`, `derived`, `status`
//!
//! D2 の `JobId(pub String)` 自身は文字種制約を持たない。本モジュールは
//! Python authoring で「規約に従った JobId 文字列」を作る helper を提供する。

use crate::error::JobManagerError;

const RESERVED_IDS: &[&str] = &["flow", "plan", "experiment", "derived", "status"];

fn valid_step_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn valid_job_id_char(c: char) -> bool {
    valid_step_id_char(c) || c == '='
}

/// step_id の検証 (`[A-Za-z0-9_-]+`、予約名禁止)。OK なら入力を返す。
pub fn validate_step_id(s: &str) -> Result<&str, JobManagerError> {
    if s.is_empty() || !s.chars().all(valid_step_id_char) {
        return Err(JobManagerError::InvalidStepId(s.to_string()));
    }
    if RESERVED_IDS.contains(&s) {
        return Err(JobManagerError::ReservedJobId(s.to_string()));
    }
    Ok(s)
}

/// JobId 全体の検証 (文字種 + 予約名 + sweep encoding 整合性)。
pub fn validate_job_id(s: &str) -> Result<&str, JobManagerError> {
    if s.is_empty() || !s.chars().all(valid_job_id_char) {
        return Err(JobManagerError::InvalidJobId(s.to_string()));
    }
    // parse できれば形式 OK
    parse_job_id(s)?;
    Ok(s)
}

/// JobId 文字列を組み立てる。D2 newtype 包装は呼び側 `JobId::from(...)`。
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

/// 借用ベース parse (alloc なし)。`&job_id.0` または string literal を渡す。
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError> {
    if s.is_empty() {
        return Err(JobManagerError::InvalidJobId(String::new()));
    }
    let mut iter = s.split("__");
    let source_step_id = iter.next().expect("split yields >=1");
    validate_step_id(source_step_id)?;

    let mut axis_combo: Vec<(&str, usize)> = Vec::new();
    for piece in iter {
        let Some(eq_pos) = piece.find('=') else {
            return Err(JobManagerError::JobIdParseError {
                id: s.to_string(),
                piece: piece.to_string(),
                reason: "expected '<axis>=<idx>'".to_string(),
            });
        };
        let (ax, idx_str) = piece.split_at(eq_pos);
        let idx_str = &idx_str[1..];
        if ax.is_empty() || !ax.chars().all(valid_step_id_char) {
            return Err(JobManagerError::JobIdParseError {
                id: s.to_string(),
                piece: piece.to_string(),
                reason: format!("invalid axis name '{ax}'"),
            });
        }
        let idx: usize = idx_str.parse().map_err(|_| JobManagerError::JobIdParseError {
            id: s.to_string(),
            piece: piece.to_string(),
            reason: format!("invalid index '{idx_str}'"),
        })?;
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
        for name in &["flow", "plan", "experiment", "derived", "status"] {
            assert!(matches!(
                validate_step_id(name),
                Err(JobManagerError::ReservedJobId(_))
            ));
        }
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
    fn validate_job_id_accepts_sweep_form() {
        assert!(validate_job_id("opt").is_ok());
        assert!(validate_job_id("opt__compound=0__method=2").is_ok());
    }

    #[test]
    fn validate_job_id_rejects_invalid() {
        assert!(validate_job_id("opt/sub").is_err());
        assert!(validate_job_id("opt__compound=abc").is_err());
    }

    #[test]
    fn build_no_sweep_returns_step_id() {
        assert_eq!(build_job_id("opt", &[]), "opt");
    }

    #[test]
    fn build_with_sweep_encodes_axes() {
        assert_eq!(
            build_job_id("opt", &[("compound", 0), ("method", 2)]),
            "opt__compound=0__method=2"
        );
    }

    #[test]
    fn parse_round_trip_no_sweep() {
        let s = build_job_id("opt", &[]);
        let parts = parse_job_id(&s).unwrap();
        assert_eq!(parts.source_step_id, "opt");
        assert!(parts.axis_combo.is_empty());
    }

    #[test]
    fn parse_round_trip_with_sweep() {
        let s = build_job_id("opt", &[("compound", 0), ("method", 2)]);
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

    #[test]
    fn parse_axis_name_must_be_valid() {
        // axis 名に '=' は使えない (区切り文字なので)
        assert!(parse_job_id("opt__c/d=0").is_err());
    }
}
```

- [ ] **Step 2: `src/lib.rs` で公開**

```rust
pub mod jobid;

pub use jobid::{build_job_id, parse_job_id, validate_job_id, validate_step_id, JobIdParts};
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib jobid:: 2>&1 | tail -5`

Expected: `test result: ok. 11 passed`.

- [ ] **Step 4: コミット**

```bash
git add src/jobid.rs src/lib.rs
git commit -m "feat(jobid): validate / build / parse helpers

SP-2 spec §4 に対応。step_id / JobId の命名規約 (case [A-Za-z0-9_-]+
+ 予約名 + sweep encoding) を Rust で集約。Python 側は pyfunction 経由
で利用する。"
```

---

### Task 4: src/plan/ — ExperimentPlan + I/O

**Files:**
- Create: `src/plan/mod.rs`
- Create: `src/plan/io.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: `src/plan/mod.rs` を作成**

```rust
//! Experiment plan sidecar — per-job params 永続化 (SP-3 が bash render で使う)。

use std::collections::BTreeMap;

use gaussian_job_shared::entities::workflow::JobId;

pub mod io;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentPlan {
    /// Map key は D2 `JobId` newtype。value は任意の TOML 値。
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
    use gaussian_job_shared::entities::workflow::JobId;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn sample_plan() -> ExperimentPlan {
        let mut params = BTreeMap::new();
        params.insert("route".into(), toml::Value::String("# B3LYP".into()));
        params.insert("nproc".into(), toml::Value::Integer(16));
        let mut jobs = BTreeMap::new();
        jobs.insert(JobId::from("opt__c=0"), params);
        ExperimentPlan { jobs }
    }

    #[test]
    fn round_trip_preserves_params() {
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

    #[test]
    fn round_trip_preserves_multiple_jobs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.toml");
        let mut jobs = BTreeMap::new();
        for i in 0..3 {
            let jid = JobId::from(format!("opt__c={i}"));
            let mut params = BTreeMap::new();
            params.insert("idx".into(), toml::Value::Integer(i as i64));
            jobs.insert(jid, params);
        }
        let p = ExperimentPlan { jobs };
        write_plan(&path, &p).unwrap();
        let back = read_plan(&path).unwrap();
        assert_eq!(back.jobs.len(), 3);
    }

    #[test]
    fn deny_unknown_fields_rejects_extra_top_level() {
        let bad = r#"
extra = "field"

[jobs."opt"]
route = "# x"
"#;
        let result: Result<ExperimentPlan, _> = toml::from_str(bad);
        assert!(result.is_err());
    }

    #[test]
    fn atomic_rename_replaces_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("plan.toml");
        std::fs::write(&path, "existing = 1").unwrap();
        let p = sample_plan();
        write_plan(&path, &p).unwrap();
        let back = read_plan(&path).unwrap();
        assert_eq!(back.jobs.len(), 1);
    }
}
```

- [ ] **Step 3: `src/lib.rs` で plan モジュールを公開**

```rust
pub mod plan;

pub use plan::io::{read_plan, write_plan};
pub use plan::ExperimentPlan;
```

- [ ] **Step 4: テスト通過確認**

Run: `cargo test --lib plan:: 2>&1 | tail -5`

Expected: `test result: ok. 5 passed`.

- [ ] **Step 5: コミット**

```bash
git add src/plan/ src/lib.rs
git commit -m "feat(plan): ExperimentPlan + atomic rename I/O

SP-2 spec §5 に対応。BTreeMap<JobId, BTreeMap<String, toml::Value>>
で per-job params を保持。SP-3 が bash render で使う sidecar。"
```

---

### Task 5: src/path.rs — plan_toml / experiment_toml getter

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

Run: `cargo test --lib plan_toml_path 2>&1 | tail -5`

Expected: 失敗。

- [ ] **Step 2: getter を実装**

`src/path.rs` の `impl PathResolver { ... }` 内に追加:

```rust
pub fn plan_toml(&self, flow_uuid: &Uuid) -> PathBuf {
    self.flow_dir(flow_uuid).join("plan.toml")
}

/// 将来、ユーザーが experiment authoring の Python script を flow dir に保存
/// したい場合の慣用 path。SP-2 では使わない (SP-2 は experiment.toml DSL を
/// 実装しないため)。
pub fn experiment_toml(&self, flow_uuid: &Uuid) -> PathBuf {
    self.flow_dir(flow_uuid).join("experiment.toml")
}
```

- [ ] **Step 3: テスト通過確認**

Run: `cargo test --lib path:: 2>&1 | tail -5`

Expected: 全 pass。

- [ ] **Step 4: コミット**

```bash
git add src/path.rs
git commit -m "feat(path): plan_toml() + experiment_toml() resolvers

SP-2 spec §6 に対応。experiment_toml() は SP-2 で使わないが、
将来用に確保。"
```

---

### Task 6: src/py_export/jobid.rs + plan.rs

**Files:**
- Create: `src/py_export/jobid.rs`
- Create: `src/py_export/plan.rs`
- Modify: `src/py_export/mod.rs`

- [ ] **Step 1: `src/py_export/jobid.rs`**

```rust
//! Python 公開: jobid helpers.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3_stub_gen::derive::gen_stub_pyfunction;

use crate::jobid;

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn validate_step_id(s: &str) -> PyResult<String> {
    jobid::validate_step_id(s)
        .map(|x| x.to_string())
        .map_err(crate::py_export::error::to_py_err)
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn validate_job_id(s: &str) -> PyResult<String> {
    jobid::validate_job_id(s)
        .map(|x| x.to_string())
        .map_err(crate::py_export::error::to_py_err)
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn build_job_id(source_step_id: &str, axis_combo: Vec<(String, usize)>) -> String {
    let refs: Vec<(&str, usize)> = axis_combo.iter().map(|(s, i)| (s.as_str(), *i)).collect();
    jobid::build_job_id(source_step_id, &refs)
}

#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn parse_job_id<'py>(py: Python<'py>, s: &str) -> PyResult<Bound<'py, PyDict>> {
    let parts = jobid::parse_job_id(s).map_err(crate::py_export::error::to_py_err)?;
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

- [ ] **Step 2: `src/py_export/plan.rs`**

```rust
//! Python 公開: ExperimentPlan + I/O.

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
    #[new]
    fn new<'py>(py: Python<'py>, jobs: Bound<'py, pyo3::types::PyDict>) -> PyResult<Self> {
        use std::collections::BTreeMap;
        use gaussian_job_shared::entities::workflow::JobId;
        let mut out_jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>> = BTreeMap::new();
        for (k, v) in jobs.iter() {
            let jid_str: String = k.extract()?;
            let params_dict: Bound<'_, pyo3::types::PyDict> = v.downcast_into()?;
            let mut params: BTreeMap<String, toml::Value> = BTreeMap::new();
            for (pk, pv) in params_dict.iter() {
                let key: String = pk.extract()?;
                let val: toml::Value = pythonize::depythonize(&pv)?;
                params.insert(key, val);
            }
            out_jobs.insert(JobId::from(jid_str), params);
        }
        Ok(PyExperimentPlan {
            inner: ExperimentPlan { jobs: out_jobs },
        })
    }

    #[getter]
    fn jobs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let dict = pyo3::types::PyDict::new(py);
        for (k, params) in &self.inner.jobs {
            let pdict = pyo3::types::PyDict::new(py);
            for (pk, pv) in params {
                let py_value = pythonize::pythonize(py, pv)?;
                pdict.set_item(pk, py_value)?;
            }
            // D2 JobId(pub String) の内部 String を Python dict key として使う
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
```

- [ ] **Step 3: `src/py_export/mod.rs` で register**

```rust
pub mod jobid;
pub mod plan;

// register 関数内:
m.add_function(wrap_pyfunction!(jobid::validate_step_id, m)?)?;
m.add_function(wrap_pyfunction!(jobid::validate_job_id, m)?)?;
m.add_function(wrap_pyfunction!(jobid::build_job_id, m)?)?;
m.add_function(wrap_pyfunction!(jobid::parse_job_id, m)?)?;
m.add_class::<plan::PyExperimentPlan>()?;
m.add_function(wrap_pyfunction!(plan::read_plan, m)?)?;
m.add_function(wrap_pyfunction!(plan::write_plan, m)?)?;
```

- [ ] **Step 4: ビルド + テスト**

Run: `cargo build --features pyo3,pythonize,stub_gen 2>&1 | tail -5 && cargo test --lib 2>&1 | tail -5`

Expected: 成功。

- [ ] **Step 5: コミット**

```bash
git add src/py_export/
git commit -m "feat(py): jobid helpers + PyExperimentPlan pyfunctions

SP-2 spec §8 に対応。Python authoring で必要な validate / build / parse
+ ExperimentPlan I/O を公開。expand_experiment は提供しない (DSL なし)。"
```

---

### Task 7: src/lib.rs + python/job_manager/__init__.py — re-exports

**Files:**
- Modify: `src/lib.rs`
- Modify: `python/job_manager/__init__.py`

- [ ] **Step 1: `src/lib.rs` を確認** (Task 3, 4 で既に追加済み)

```rust
pub mod jobid;
pub mod plan;

pub use jobid::{build_job_id, parse_job_id, validate_job_id, validate_step_id, JobIdParts};
pub use plan::io::{read_plan, write_plan};
pub use plan::ExperimentPlan;
```

`JobManagerError` も SP-1 で既に re-export されているか確認、無ければ追加。

- [ ] **Step 2: Python 側 re-export**

`python/job_manager/__init__.py` の SP-1 既存 import の隣に追加:

```python
from job_manager._job_manager_core import (
    # ... SP-1 既存 import ...
    # SP-2: jobid
    validate_step_id,
    validate_job_id,
    build_job_id,
    parse_job_id,
    # SP-2: plan
    ExperimentPlan,
    read_plan,
    write_plan,
)

__all__ = [
    # ... SP-1 既存 ...
    "validate_step_id",
    "validate_job_id",
    "build_job_id",
    "parse_job_id",
    "ExperimentPlan",
    "read_plan",
    "write_plan",
]
```

- [ ] **Step 3: maturin で Python 側ビルド + smoke**

Run: `uv run maturin develop 2>&1 | tail -3 && uv run python -c 'from job_manager import build_job_id, parse_job_id, ExperimentPlan; print("ok")'`

Expected: `ok`.

- [ ] **Step 4: コミット**

```bash
git add src/lib.rs python/job_manager/__init__.py
git commit -m "feat(api): re-export SP-2 jobid + plan public surface"
```

---

### Task 8: tests/integration_plan.rs — Rust integration

**Files:**
- Create: `tests/integration_plan.rs`

- [ ] **Step 1: integration test を書く**

`tests/integration_plan.rs`:

```rust
//! Integration test for SP-2 minimal scope:
//! - JobId helpers (validate / build / parse)
//! - ExperimentPlan I/O (round-trip)
//! - PathResolver getters (plan_toml / experiment_toml)
//! - JobFlow + ExperimentPlan を Python authoring と同じ手順で構築

use std::collections::BTreeMap;

use chrono::Utc;
use gaussian_job_shared::entities::workflow::{Job, JobEdge, JobFlow, JobId, JobSpec, Program};
use slurm_async_runner::entities::slurm::{DependencyType, SlurmJobConfig};
use tempfile::tempdir;
use uuid::Uuid;

use job_manager::{
    build_job_id, parse_job_id, read_plan, validate_step_id, write_plan, ExperimentPlan,
    PathResolver,
};

#[test]
fn jobid_round_trip() {
    let s = build_job_id("opt", &[("compound", 0), ("method", 2)]);
    assert_eq!(s, "opt__compound=0__method=2");
    let parts = parse_job_id(&s).unwrap();
    assert_eq!(parts.source_step_id, "opt");
    assert_eq!(parts.axis_combo, vec![("compound", 0), ("method", 2)]);
}

#[test]
fn validate_step_id_rejects_reserved() {
    assert!(validate_step_id("flow").is_err());
    assert!(validate_step_id("opt").is_ok());
}

#[test]
fn build_and_persist_plan_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("plan.toml");

    let mut params = BTreeMap::new();
    params.insert("route".into(), toml::Value::String("# B3LYP".into()));
    params.insert("nproc".into(), toml::Value::Integer(16));
    let mut jobs = BTreeMap::new();
    jobs.insert(JobId::from(build_job_id("opt", &[("compound", 0)])), params);

    let plan = ExperimentPlan { jobs };
    write_plan(&path, &plan).unwrap();
    let back = read_plan(&path).unwrap();
    assert_eq!(back.jobs.len(), 1);
}

#[test]
fn python_authoring_pattern_works_in_rust() {
    // Mimics spec §1.1 Python authoring flow in Rust.
    let compounds = vec!["benzene", "toluene", "p-xylene"];
    let methods = vec![("b3lyp", "B3LYP"), ("m062x", "M06-2X")];

    let mut jobs: BTreeMap<JobId, Job> = BTreeMap::new();
    let mut params: BTreeMap<JobId, BTreeMap<String, toml::Value>> = BTreeMap::new();

    for (i, c) in compounds.iter().enumerate() {
        for (j, (_name, route)) in methods.iter().enumerate() {
            // opt
            let opt_id = JobId::from(build_job_id(
                "opt",
                &[("compound", i), ("method", j)],
            ));
            jobs.insert(
                opt_id.clone(),
                Job {
                    spec: JobSpec {
                        program: Program::from("g16"),
                        config: SlurmJobConfig::default(),
                        body: String::new(),
                    },
                    parents: vec![],
                },
            );
            let mut p = BTreeMap::new();
            p.insert("route".into(), toml::Value::String(format!("# {route}/6-31G* opt")));
            p.insert("compound".into(), toml::Value::String((*c).into()));
            params.insert(opt_id.clone(), p);

            // freq (pair_by_axes parent: opt)
            let freq_id = JobId::from(build_job_id(
                "freq",
                &[("compound", i), ("method", j)],
            ));
            jobs.insert(
                freq_id.clone(),
                Job {
                    spec: JobSpec {
                        program: Program::from("g16"),
                        config: SlurmJobConfig::default(),
                        body: String::new(),
                    },
                    parents: vec![JobEdge {
                        from: opt_id.clone(),
                        kind: DependencyType::AfterOk,
                    }],
                },
            );
            let mut p = BTreeMap::new();
            p.insert("route".into(), toml::Value::String(format!("# {route}/6-31G* freq")));
            p.insert("compound".into(), toml::Value::String((*c).into()));
            params.insert(freq_id, p);
        }
    }

    let flow = JobFlow {
        uuid: Uuid::now_v7(),
        created_at: Utc::now(),
        tags: BTreeMap::from([("calc_type".to_string(), "opt+freq".to_string())]),
        jobs,
    };
    let plan = ExperimentPlan { jobs: params };

    // 3 compounds × 2 methods × 2 steps = 12 jobs
    assert_eq!(flow.jobs.len(), 12);
    assert_eq!(plan.jobs.len(), 12);
    let flow_keys: std::collections::BTreeSet<_> = flow.jobs.keys().collect();
    let plan_keys: std::collections::BTreeSet<_> = plan.jobs.keys().collect();
    assert_eq!(flow_keys, plan_keys, "flow.toml と plan.toml の JobId 集合は一致");

    // freq の parent は対応する opt
    let freq_id = JobId::from("freq__compound=1__method=0");
    let opt_id = JobId::from("opt__compound=1__method=0");
    let freq_job = &flow.jobs[&freq_id];
    assert_eq!(freq_job.parents.len(), 1);
    assert_eq!(freq_job.parents[0].from, opt_id);

    // 全 JobId が parse できる (規約に従う)
    for jid in flow.jobs.keys() {
        let parts = parse_job_id(&jid.0).unwrap();
        assert!(parts.source_step_id == "opt" || parts.source_step_id == "freq");
        assert_eq!(parts.axis_combo.len(), 2);
    }
}

#[test]
fn pathresolver_plan_toml_round_trip() {
    let dir = tempdir().unwrap();
    let resolver = PathResolver::new(dir.path().to_path_buf());
    let uuid = Uuid::now_v7();
    std::fs::create_dir_all(resolver.flow_dir(&uuid)).unwrap();

    let mut jobs = BTreeMap::new();
    jobs.insert(JobId::from("opt"), BTreeMap::new());
    let plan = ExperimentPlan { jobs };
    write_plan(&resolver.plan_toml(&uuid), &plan).unwrap();

    let back = read_plan(&resolver.plan_toml(&uuid)).unwrap();
    assert_eq!(back.jobs.len(), 1);
}
```

- [ ] **Step 2: テスト実行**

Run: `cargo test --test integration_plan 2>&1 | tail -10`

Expected: `test result: ok. 5 passed`.

- [ ] **Step 3: コミット**

```bash
git add tests/integration_plan.rs
git commit -m "test(integration): jobid + plan + Python-authoring pattern (12-job)

SP-2 spec §9.2 に対応。grammar DSL を使わず Rust で
JobFlow + ExperimentPlan を直接構築する Python authoring パターンを
end-to-end で検証。"
```

---

### Task 9: python/tests/test_jobid.py + test_plan.py

**Files:**
- Create: `python/tests/test_jobid.py`
- Create: `python/tests/test_plan.py`

- [ ] **Step 1: `python/tests/test_jobid.py`**

```python
"""Python E2E for SP-2 jobid helpers."""

from __future__ import annotations

import pytest

from job_manager import build_job_id, parse_job_id, validate_step_id, validate_job_id


def test_validate_step_id_ok():
    assert validate_step_id("opt") == "opt"
    assert validate_step_id("opt-1") == "opt-1"
    assert validate_step_id("Step_2") == "Step_2"


def test_validate_step_id_rejects_reserved():
    for name in ["flow", "plan", "experiment", "derived", "status"]:
        with pytest.raises(ValueError):
            validate_step_id(name)


def test_validate_step_id_rejects_invalid_chars():
    for bad in ["opt=1", "opt/sub", "", "opt space"]:
        with pytest.raises(ValueError):
            validate_step_id(bad)


def test_build_no_sweep():
    assert build_job_id("opt", []) == "opt"


def test_build_with_sweep():
    assert (
        build_job_id("opt", [("compound", 0), ("method", 2)])
        == "opt__compound=0__method=2"
    )


def test_parse_round_trip():
    s = build_job_id("opt", [("compound", 0), ("method", 2)])
    parts = parse_job_id(s)
    assert parts["source_step_id"] == "opt"
    assert parts["axis_combo"] == [("compound", 0), ("method", 2)]


def test_parse_rejects_malformed():
    with pytest.raises(ValueError):
        parse_job_id("opt__compound=abc")
    with pytest.raises(ValueError):
        parse_job_id("opt__nothing")
    with pytest.raises(ValueError):
        parse_job_id("")


def test_validate_job_id_accepts_sweep_form():
    assert validate_job_id("opt__compound=0__method=2") == "opt__compound=0__method=2"
```

- [ ] **Step 2: `python/tests/test_plan.py`** (spec §1.1 の Python authoring パターン)

```python
"""Python E2E for SP-2 plan + authoring pattern (spec §1.1)."""

from __future__ import annotations

import tempfile
from itertools import product
from pathlib import Path
from uuid import uuid4

import pytest

from job_manager import (
    ExperimentPlan,
    build_job_id,
    parse_job_id,
    read_plan,
    write_plan,
    PathResolver,
)


def test_experiment_plan_construct_and_jobs_getter():
    plan = ExperimentPlan({
        "opt__compound=0": {"route": "# B3LYP/6-31G* opt", "nproc": 16},
        "opt__compound=1": {"route": "# B3LYP/6-31G* opt", "nproc": 16},
    })
    jobs = plan.jobs
    assert len(jobs) == 2
    assert jobs["opt__compound=0"]["route"] == "# B3LYP/6-31G* opt"
    assert jobs["opt__compound=0"]["nproc"] == 16


def test_plan_round_trip():
    plan = ExperimentPlan({
        "opt__c=0": {"route": "# r0", "nproc": 16},
        "opt__c=1": {"route": "# r1", "nproc": 16},
    })
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "plan.toml"
        write_plan(str(p), plan)
        back = read_plan(str(p))
        assert len(back.jobs) == 2
        assert back.jobs["opt__c=0"]["route"] == "# r0"


def test_authoring_pattern_12_jobs():
    """spec §1.1 の Python authoring パターン (sweep + parent in pure Python)."""
    compounds = ["benzene", "toluene", "p-xylene"]
    methods = [
        {"name": "b3lyp", "route": "B3LYP"},
        {"name": "m062x", "route": "M06-2X"},
    ]

    params: dict[str, dict] = {}
    for (i, c), (j, m) in product(enumerate(compounds), enumerate(methods)):
        opt_id = build_job_id("opt", [("compound", i), ("method", j)])
        params[opt_id] = {
            "route": f"# {m['route']}/6-31G* opt",
            "compound": c,
            "nproc": 16,
        }
        freq_id = build_job_id("freq", [("compound", i), ("method", j)])
        params[freq_id] = {
            "route": f"# {m['route']}/6-31G* freq",
            "compound": c,
            "nproc": 16,
        }

    plan = ExperimentPlan(params)
    assert len(plan.jobs) == 12

    # 各 JobId が規約に従う
    for jid in plan.jobs:
        parts = parse_job_id(jid)
        assert parts["source_step_id"] in ("opt", "freq")
        assert len(parts["axis_combo"]) == 2

    # round-trip
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "plan.toml"
        write_plan(str(p), plan)
        back = read_plan(str(p))
        assert len(back.jobs) == 12


def test_pathresolver_plan_toml():
    with tempfile.TemporaryDirectory() as d:
        resolver = PathResolver(d)
        from uuid import uuid4
        uid = uuid4()
        path = resolver.plan_toml(str(uid))
        # path はまだ存在しないが、parent dir を含むはず
        assert "plan.toml" in str(path)
        assert str(uid) in str(path)


def test_pathresolver_experiment_toml_reserved_for_future():
    """experiment_toml() getter は SP-2 では使わないが、将来用に公開済み。"""
    with tempfile.TemporaryDirectory() as d:
        resolver = PathResolver(d)
        from uuid import uuid4
        uid = uuid4()
        path = resolver.experiment_toml(str(uid))
        assert "experiment.toml" in str(path)
```

- [ ] **Step 3: maturin で Python パッケージを再ビルド**

Run: `uv run maturin develop 2>&1 | tail -3`

Expected: 成功。

- [ ] **Step 4: テスト実行**

Run: `uv run pytest python/tests/test_jobid.py python/tests/test_plan.py -v 2>&1 | tail -25`

Expected: 全 pass。

- [ ] **Step 5: コミット**

```bash
git add python/tests/test_jobid.py python/tests/test_plan.py
git commit -m "test(py): jobid + plan + spec §1.1 authoring pattern"
```

---

### Task 10: 仕上げ — fmt / clippy / stub_gen / coverage / README

**Files:**
- Modify: `README.md`
- Generated: `python/job_manager/_job_manager_core/__init__.pyi`

- [ ] **Step 1: cargo fmt**

Run: `cargo fmt && cargo fmt --check`

Expected: 整形差分なし。

- [ ] **Step 2: cargo clippy**

Run: `cargo clippy --all-features -- -D warnings 2>&1 | tail -10`

Expected: 警告 0。

- [ ] **Step 3: stub_gen**

Run: `cargo run --bin stub_gen --features stub_gen 2>&1 | tail -5 && uv run ruff format python/job_manager/_job_manager_core/__init__.pyi`

Expected: 新しい SP-2 関数 (`validate_step_id` 等) が `.pyi` に出現。`crate::grammar` 関連の関数は **無いこと** を確認。

- [ ] **Step 4: cargo llvm-cov でカバレッジ**

Run: `cargo llvm-cov --fail-under-lines 80 --all-features 2>&1 | tail -5`

Expected: ≥ 80%。

- [ ] **Step 5: README.md に SP-2 capability セクション追加**

`README.md` の SP-1 capability セクションの後ろに追加:

```markdown
## SP-2 (plan + jobid helpers) capabilities

- `ExperimentPlan` — per-job params sidecar (SP-3 が bash render で使う)
- `read_plan(path)` / `write_plan(path, plan)` — `plan.toml` atomic rename I/O
- `build_job_id(step_id, axis_combo)` — JobId 文字列を組み立てる
- `parse_job_id(s)` — JobId を `{source_step_id, axis_combo}` に分解
- `validate_step_id(s)` / `validate_job_id(s)` — 命名規約検証
- `PathResolver.plan_toml(&uuid)` / `.experiment_toml(&uuid)` — path 解決

**experiment.toml DSL は SP-2 に含まない。** sweep / placeholder / parent 解決はユーザーが Python (itertools / f-string / `JobEdge` 直接構築) で書く。spec §1.1 にサンプルあり。
```

- [ ] **Step 6: 最終全テスト**

Run: `cargo test --all-features 2>&1 | tail -5 && uv run pytest python/tests 2>&1 | tail -5`

Expected: 両方 pass。

- [ ] **Step 7: コミット**

```bash
git add README.md python/job_manager/_job_manager_core/__init__.pyi
git commit -m "chore(release): finalize SP-2 (plan + jobid, no DSL)"
```

---

### Task 11: PR 作成

- [ ] **Step 1: push して PR を開く**

```bash
git push -u origin feat/sp2-impl
gh pr create --base main --title "feat(sp2): plan sidecar + jobid helpers (no experiment.toml DSL)" --body "$(cat <<'EOF'
## Summary

SP-2 minimal — \`experiment.toml\` DSL を **実装しない** 案 B を採用。
SP-3 (submit + CLI) が必要とする最小機能のみを実装する。

## 前提

- D2 (\`gaussian-job-shared2\`) の \`JobFlow.work_dir\` 撤廃 PR が merged
- SP-1 follow-up PR (work_dir 参照を PathResolver 経由に置換) が merged
- A1 (\`slurm-async-runner2\`) は不可侵
- D2 newtype (\`JobId\` / \`Program\` / \`CalcType\`) は保持・import 利用

## 主な追加

- \`crate::plan::ExperimentPlan\` — per-job params 永続化
- \`crate::plan::io::{read_plan, write_plan}\` — atomic rename I/O
- \`crate::jobid::{validate_step_id, validate_job_id, build_job_id, parse_job_id, JobIdParts}\`
- \`crate::JobManagerError\` に JobId* バリアント (Invalid/Reserved/ParseError)
- \`PathResolver::plan_toml(&uuid)\` / \`experiment_toml(&uuid)\` getter
- Python: 上記すべての pyfunction / pyclass

## 主に追加しないもの

- \`experiment.toml\` schema parser
- \`\${...}\` placeholder 展開
- sweep 展開アルゴリズム (\`[[axis]]\` / \`itertools.product\`)
- parent 解決 DSL (pair / fanout / reduce_over)
- legacy 形状検出
- \`expand_experiment\` 公開 API

これらはユーザーが Python (itertools, f-string, JobEdge 直接構築) で書く。
spec §1.1 にサンプルあり。

## 設計

詳細は spec v5 (\`docs/superpowers/specs/2026-05-12-job-manager-sp2-design.md\`) を参照。

## Test plan

- [ ] cargo test --all-features 通過
- [ ] cargo clippy --all-features -- -D warnings 通過
- [ ] cargo fmt --check 通過
- [ ] uv run maturin develop 成功
- [ ] uv run pytest python/tests 通過
- [ ] cargo llvm-cov ≥ 80%
- [ ] integration_plan.rs (5 tests) 全 pass
- [ ] test_jobid.py + test_plan.py 全 pass
- [ ] \`git ls-files src/grammar/\` が空 (DSL 不実装の確証)
EOF
)"
```

---

## Capabilities (SP-2 完了時)

```
job_manager::ExperimentPlan
job_manager::read_plan(path) -> Result<ExperimentPlan, JobManagerError>
job_manager::write_plan(path, &plan) -> Result<(), JobManagerError>
job_manager::build_job_id(step, combo) -> String
job_manager::parse_job_id(s) -> Result<JobIdParts<'_>, JobManagerError>
job_manager::validate_step_id(s) -> Result<&str, JobManagerError>
job_manager::validate_job_id(s) -> Result<&str, JobManagerError>
job_manager::PathResolver::plan_toml(&uuid) -> PathBuf
job_manager::PathResolver::experiment_toml(&uuid) -> PathBuf
```

Python:

```python
from job_manager import (
    ExperimentPlan, read_plan, write_plan,
    build_job_id, parse_job_id, validate_step_id, validate_job_id,
    PathResolver,
)
from gaussian_job_shared import JobFlow, JobId, Job, JobSpec, Program, JobEdge
```

---

## Out of scope (deferred to SP-3 or out of project)

- experiment.toml DSL (案 B 採用、Rust で実装しない方針)
- `common.toml` 読み込み + `SlurmJobConfig` 合成 (SP-3)
- `JobSpec.body` の bash render (SP-3)
- A1 `SbatchManager` 経由の sbatch 投入 (SP-3)
- CLI (`run` / `submit` / `show` / `tick` / `search`) (SP-3)

---

## Self-Review

### Spec coverage check

spec (v5) の各セクションに対応するタスク:

| Spec section | Task |
|---|---|
| §1 背景 | 説明のみ |
| §1.1 Python authoring 例 | Task 8 (Rust integration) + Task 9 (Python E2E) で検証 |
| §1.2 SP-2 スコープ | 全タスク |
| §2 採用アプローチ (案 B) | 全タスク |
| §2.3 TOML 形式と Rust 型の対応 | Task 4 (plan I/O) で deny_unknown_fields 確認 |
| §3 必須 D2 変更 (work_dir 撤廃) | Phase 0 (P0.1-P0.5) + Phase 1 (P1.1-P1.4) |
| §4 JobId 命名規約 | Task 3 (jobid.rs) + Task 6 (pyfunctions) |
| §5 ExperimentPlan | Task 4 (plan/mod.rs + io.rs) |
| §6 FS レイアウト | Task 5 (PathResolver getters) |
| §7 Rust モジュール | Task 2-7 全体 |
| §8 Python API | Task 6 + Task 7 |
| §9 テスト計画 | Task 8 + Task 9 |
| §10 リスク | 各タスクで mitigate |
| §11 完了基準 | Task 10 + Task 11 で確認 |

### Type consistency

- `JobId` / `Program` / `CalcType` は **D2 から import** して使用
- `JobFlow.work_dir` フィールドは v4 で **撤廃** (Phase 0)
- `ExperimentPlan.jobs: BTreeMap<JobId, BTreeMap<String, toml::Value>>` で全タスク一貫
- `JobIdParts<'a>` は `parse_job_id` の戻り値、`source_step_id: &'a str` + `axis_combo: Vec<(&'a str, usize)>`
- `validate_*` は OK で `&str` (入力をそのまま) を返し、NG で error
- `build_job_id(&str, &[(&str, usize)]) -> String` — D2 newtype 包装は呼び側
- Rust に **`crate::grammar` モジュールは存在しない** (案 B 採用の構造的確証)

### Placeholder scan

- 全タスクで実コードが提示されている (TBD/TODO なし)
- 全エラーメッセージ・テスト名は具体的
- 型 / 関数名はタスク間で一致
- 全ステップに具体的な command / expected output / commit message が含まれる

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-12-job-manager-sp2.md`.

**実行方法 2 択:**

1. **Subagent-Driven (推奨)** — 各タスクごとに fresh subagent をディスパッチし、レビュー → 次タスクの反復。各 Task の責務が `src/<file>.rs` 単位で明確なので、Task 境界での文脈リセットと相性がよい。

2. **Inline Execution** — このセッション内で executing-plans でバッチ実行。チェックポイントでレビュー。

**どちらで進めますか?**

- Subagent-Driven の場合: `superpowers:subagent-driven-development` skill を起動
- Inline の場合: `superpowers:executing-plans` skill を起動
