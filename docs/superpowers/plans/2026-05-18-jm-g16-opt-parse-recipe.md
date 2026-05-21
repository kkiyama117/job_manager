# jm new g16-opt-parse レシピ(案A v1)Implementation Plan

> **Status (2026-05-20):** Historical(全 16 タスク完了 = PR #27 + PR #28 merged、
> (a) interim 確定)。本 plan は **2026-05-18 時点の実装 plan** をそのまま保存している
> historical record。embed された `run.py` / `parse.py` template 抜粋(L657/L662 /
> L1121/L1126 周辺)は作成時点の `gaussian_compute_runtime` swap-in 例
> (`python -m gaussian_compute_runtime <step> --config <abs gem toml>` 等)を含むが、
> 実 CLI 形とは drift がある。**現行の真値**は次を参照(issue #34 cross-ref):
> - **Runtime CLI 実形 / 安定契約**: `docs/superpowers/specs/2026-05-20-gaussian-compute-runtime-audit.md`
>   (PR #37 merged) — `run-g16` / `parse-results` は D-α v0.2.0 で BROKEN(B-α migration 待ち)、
>   γ(`consume-parent-results`)のみ swap-in 可。実フラグは `--config <path>`。
> - **現行 recipes の `# REPLACE_ME` コメント**: `src/recipes/assets/{g16_opt,parse_g16_out}/`
>   (PR #38 merged で audit §13 整合済) — plan embed snippet の現在形。
> - **v2 設計差分(R3' → R4)**: `docs/superpowers/specs/2026-05-20-jm-recipe-v2-design.md`(PR #31 merged)。
>
> plan body は**書き換えない**(historical preservation)。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `jm new g16-opt-parse` 一発で、kudpc で「g16 構造最適化 →afterok→ 結果検証」を回す編集可能・自己完結な job-manager flow 一式(flow.toml/plan.toml + `scripts/*.bash`/`run.py`/`parse.py` + `input/main.gjf`)を生成する。

**Architecture:** `src/recipes/`(pyo3 非依存・純 Rust)に二層レジストリ `JobTemplate`/`FlowRecipe` を新設。共有シェルプリアンブルは上流 `_base.bash.j2` を embed し minijinja で描画。`run.py`/`parse.py`/`main.gjf` は asset テンプレ + sentinel 置換。launcher/scratch_root/g16_cmd は plan.toml の recipe param に留め、既存 render の `JM_PARAM_*` 経路で batch.bash に焼く(render/A1/D2 変更ゼロ)。`blank` は既存 `jm new` 出力とバイト同値で温存。

**Tech Stack:** Rust nightly edition 2024 / `minijinja`(新規 crate dep) / `toml` / `uuid` v7 / `chrono` / `clap` / 生成 Python は純 stdlib(parse のみ `cclib`) / `assert_cmd` 統合テスト / `pytest` smoke。

**Spec:** `docs/superpowers/specs/2026-05-18-jm-g16-opt-parse-recipe-design.md`(案A)。

**Branching:** 実装ブランチは本プラン文書が載る `docs/jm-g16-opt-parse-altdesign` の上にスタック(CLAUDE.md「PRs: stack on the closest parent branch」)。例: `git switch -c feat/jm-g16-opt-parse-recipe docs/jm-g16-opt-parse-altdesign`。各タスクで Conventional Commits の per-task commit。

**CI gate(各 commit 前に最低限、最終タスクで全実行):**
```
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo build --bin jm --no-default-features \
  && cargo test --all-features \
  && uv run pytest python/tests -v
```

---

## File Structure

新規:
- `src/recipes/mod.rs` — 公開 re-export、`recipe_registry()`、`find_flow()`、`--param` パース、`--list`/`--describe` 整形
- `src/recipes/job.rs` — `RecipeParam`/`RecipeParamType`/`GeneratedFile`/`JobArtifacts`/`JobCtx`/`RecipeError`、`JobTemplate`/`FlowRecipe` trait、`PreambleOpts`/`base_preamble()`
- `src/recipes/flow.rs` — `assemble()`(real recipe 用)
- `src/recipes/jobs/mod.rs`、`src/recipes/jobs/g16_opt.rs`、`src/recipes/jobs/parse_g16_out.rs`
- `src/recipes/flows/mod.rs`、`src/recipes/flows/blank.rs`(legacy 移設)、`src/recipes/flows/g16_opt_parse.rs`
- `src/recipes/xyz.rs` — 純 Rust `.xyz` パーサ
- `src/recipes/assets/_base.bash.j2`
- `src/recipes/assets/_base.bash.expected`(バイト同値回帰フィクスチャ)
- `src/recipes/assets/g16_opt/main.gjf.tmpl`、`src/recipes/assets/g16_opt/run.py.tmpl`
- `src/recipes/assets/parse_g16_out/parse.py.tmpl`
- `tests/integration_new_recipes.rs`
- `python/tests/test_recipe_run_py.py`、`python/tests/test_recipe_parse_py.py`、`python/tests/_recipe_fixtures/g16_ok.out`

変更:
- `Cargo.toml` — `[dependencies]` に `minijinja`
- `src/lib.rs` — `pub mod recipes;` + re-export
- `src/bin/jm.rs` — `Cmd::New` 拡張、`cmd_new` を recipes ディスパッチに改修、`build_flow_template`/`build_plan_template`/`parse_tag` とその tests を `flows/blank.rs` へ移設
- `README.md` / `docs/toml-reference.md` / `CLAUDE.md` — `jm new <recipe>` 追記(最終タスク)

各ファイルは単一責務(型と trait は `job.rs`、組立ロジックは `flow.rs`、各テンプレは独立 asset)。`src/recipes/` は 800 行/file を超えないよう job/flow 単位で分割。

---

## Task 1: Cargo.toml に minijinja を追加し no-default-features ビルドを確認

**Files:**
- Modify: `Cargo.toml`(`[dependencies]` の `uuid = ...` 行直後)

- [ ] **Step 1: minijinja 依存を追加**

`Cargo.toml` の `[dependencies]` で `uuid = { version = "1.23", features = ["serde", "v7"] }` の直後に追加:

```toml
uuid = { version = "1.23", features = ["serde", "v7"] }

# Pure-Rust Jinja2-compatible template engine for the recipe shared
# preamble (src/recipes/assets/_base.bash.j2). C/Python/pyo3 非依存 —
# `jm --no-default-features` の libpython 非リンク契約に抵触しない。
minijinja = "2"
```

- [ ] **Step 2: no-default-features ビルド確認**

Run: `cargo build --bin jm --no-default-features`
Expected: PASS(警告可・エラー無)。minijinja が pyo3/libpython を引き込まないことの確認。

- [ ] **Step 3: 全機能ビルド確認**

Run: `cargo build --all-features`
Expected: PASS。

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: add minijinja for recipe shared preamble rendering"
```

---

## Task 2: `src/recipes/` モジュール骨格 + コア型・trait

**Files:**
- Create: `src/recipes/job.rs`
- Create: `src/recipes/mod.rs`
- Modify: `src/lib.rs:19`(`pub mod walk;` 直後に `pub mod recipes;`)、`src/lib.rs:44`(re-export 追加)
- Test: `src/recipes/job.rs` の `#[cfg(test)] mod tests`

- [ ] **Step 1: `src/recipes/job.rs` を型定義 + テスト付きで作成**

```rust
//! Recipe 二層モデルの型と trait。pyo3 非依存・純粋(I/O なし)。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// `--param` 値の型タグ。`RecipeParam::default` は常に文字列で持ち、
/// 検証時にこの型へパースできるかだけを見る。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipeParamType {
    Str,
    Int,
    Float,
    Bool,
    Path,
}

/// JobTemplate が宣言する単一パラメータ。すべて `&'static`。
#[derive(Debug, Clone, Copy)]
pub struct RecipeParam {
    pub name: &'static str,
    pub ty: RecipeParamType,
    pub default: &'static str,
    pub help: &'static str,
}

/// scaffold が生成する1ファイル。`relpath` は flow_dir 相対
/// (例 `"opt/scripts/run.py"`)。`unix_mode` = `Some(0o755)` で実行ビット。
#[derive(Debug, Clone)]
pub struct GeneratedFile {
    pub relpath: PathBuf,
    pub contents: String,
    pub unix_mode: Option<u32>,
}

/// JobTemplate::instantiate の出力。flow.toml/plan.toml 片 + サイドカー。
#[derive(Debug, Clone)]
pub struct JobArtifacts {
    /// flow.toml `[jobs.<id>] program`(論理分類値。`jm ls --program` 用)。
    pub program: String,
    /// flow.toml `[jobs.<id>] body`。R3': `bash scripts/<id>.bash` のみ(cd 無し)。
    /// job dir は run.py/parse.py 冒頭の絶対 `JOB_DIR` 定数で解決(cwd 非依存)。
    pub body: String,
    /// flow.toml `[jobs.<id>.config] time_limit`。
    pub time_limit: Option<String>,
    /// plan.toml `[jobs.<id>]` テーブル。
    pub plan_params: BTreeMap<String, toml::Value>,
    /// `scripts/<id>.bash` / `scripts/run.py` 等。relpath は "<id>/..." 名前空間。
    pub sidecars: Vec<GeneratedFile>,
}

/// instantiate に渡す解決済みコンテキスト。
pub struct JobCtx<'a> {
    /// flow 内の JobId(例 `"opt"`)。
    pub job_id: &'a str,
    /// 解決済み param(name -> 文字列値。default 適用後)。
    pub params: &'a BTreeMap<String, String>,
    /// 論理 input 名 -> flow_dir 相対パス(例 `"../opt/output/main.out"`)。
    pub inputs: &'a BTreeMap<String, String>,
    pub uuid: &'a uuid::Uuid,
    pub created_at: &'a str,
    /// 絶対 `<root>/<uuid>`。R3' で `flow_dir_abs.join(job_id)` を run.py/parse.py の
    /// `{{JOB_DIR}}` sentinel へ swap-in する絶対 job dir の親。
    pub flow_dir_abs: &'a Path,
}

#[derive(Debug, thiserror::Error)]
pub enum RecipeError {
    #[error("unknown flow recipe {0:?}; available: {1}")]
    UnknownFlow(String, String),
    #[error("unknown job template {0:?}; available: {1}")]
    UnknownJob(String, String),
    #[error("unknown --param {job}.{param}; {job} accepts: {available}")]
    UnknownParam {
        job: String,
        param: String,
        available: String,
    },
    #[error("--param {job}.{param}={value:?}: expected {ty}")]
    BadParamType {
        job: String,
        param: String,
        value: String,
        ty: String,
    },
    #[error("invalid --param syntax {0:?}: expected <JobId>.<param>=<value>")]
    BadParamSyntax(String),
    #[error("input_coordinate source not found: {0}")]
    InputCoordinateMissing(PathBuf),
    #[error("xyz parse error: {0}")]
    XyzParse(String),
}

/// Job 層テンプレート。`instantiate` は純粋(I/O なし)。
pub trait JobTemplate: Send + Sync {
    fn name(&self) -> &'static str;
    fn params(&self) -> &'static [RecipeParam];
    /// 論理 input 名(親 output を wiring で受ける)。
    fn inputs(&self) -> &'static [&'static str];
    /// (論理 output 名, flow_dir 相対の self 出力パス)。
    fn outputs(&self) -> &'static [(&'static str, &'static str)];
    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, RecipeError>;
}

/// Flow 層レシピ。scaffold 可能な単位。
pub trait FlowRecipe: Send + Sync {
    fn name(&self) -> &'static str;
    fn summary(&self) -> &'static str;
    /// (JobId, JobTemplate 名)。
    fn nodes(&self) -> &'static [(&'static str, &'static str)];
    /// (from JobId, to JobId, kind 例 "afterok")。
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)];
    /// (consumer JobId, consumer input 名, producer JobId, producer output 名)。
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)];
}

/// `base_preamble()` の入力。サイト固有値のみ可変。
pub struct PreambleOpts<'a> {
    pub conda_env: &'a str,
    pub module_block: &'a str,
    pub body_block: &'a str,
    pub pixi_manifest: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipe_param_type_is_copy_and_eq() {
        let a = RecipeParamType::Int;
        let b = a;
        assert_eq!(a, b);
        assert_ne!(RecipeParamType::Str, RecipeParamType::Path);
    }

    #[test]
    fn generated_file_carries_mode_and_relpath() {
        let f = GeneratedFile {
            relpath: PathBuf::from("opt/scripts/run.py"),
            contents: "print('x')\n".into(),
            unix_mode: Some(0o755),
        };
        assert_eq!(f.relpath, PathBuf::from("opt/scripts/run.py"));
        assert_eq!(f.unix_mode, Some(0o755));
    }

    #[test]
    fn recipe_error_messages_are_actionable() {
        let e = RecipeError::BadParamSyntax("opt.charge".into());
        assert!(e.to_string().contains("expected <JobId>.<param>=<value>"));
    }
}
```

- [ ] **Step 2: `src/recipes/mod.rs` を最小骨格で作成**

```rust
//! `jm new <flow-recipe>` の二層レシピ(Job 層 / Flow 層)。
//!
//! pyo3 非依存・純 Rust(`minijinja`/`toml`/`uuid`/`chrono`/std のみ)。
//! `jm --no-default-features` でクリーンビルドされる。

pub mod job;

pub use job::{
    FlowRecipe, GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeError,
    RecipeParam, RecipeParamType,
};
```

- [ ] **Step 3: `src/lib.rs` にモジュール登録 + re-export**

`src/lib.rs:19` の `pub mod walk;` 直後に追加:

```rust
pub mod walk;
pub mod recipes;
```

`src/lib.rs:44` の `pub use walk::walk_flows;` 直後に追加:

```rust
pub use walk::walk_flows;
pub use recipes::{FlowRecipe, JobTemplate, RecipeError};
```

- [ ] **Step 4: テスト実行**

Run: `cargo test --all-features recipes::job::tests`
Expected: PASS(3 tests)。

- [ ] **Step 5: no-default-features ビルド確認**

Run: `cargo build --bin jm --no-default-features`
Expected: PASS。

- [ ] **Step 6: Commit**

```bash
git add src/recipes/job.rs src/recipes/mod.rs src/lib.rs
git commit -m "feat(recipes): add two-layer JobTemplate/FlowRecipe core types"
```

---

## Task 3: `_base.bash.j2` asset + `base_preamble()`(minijinja)

**Files:**
- Create: `src/recipes/assets/_base.bash.j2`
- Create: `src/recipes/assets/_base.bash.expected`
- Modify: `src/recipes/job.rs`(`base_preamble()` 実装 + テスト)
- Modify: `src/recipes/mod.rs`(`base_preamble` re-export)

- [ ] **Step 1: `src/recipes/assets/_base.bash.j2` を作成**

`{% raw %}` で囲った conda-reset 区間は学習スキル pixi-conda-stack-reset と同一固定文字列(param 化しない)。`#SBATCH` は含めない。

```jinja
#!/bin/bash
set -euo pipefail
{% raw %}
# --- reset inherited conda activation state (pixi-conda-stack-reset) ---
# A SLURM job inherits the submitter's shell env; a half-activated conda
# stack there breaks `conda activate` here. Fully unwind it first.
unset -f conda 2>/dev/null || true
for _v in $(compgen -v | grep -E '^CONDA_' || true); do
  unset "$_v" 2>/dev/null || true
done
unset _v 2>/dev/null || true
{% endraw %}
source "$(conda info --base)/etc/profile.d/conda.sh"
. /usr/share/Modules/init/bash
{{ module_block }}
conda activate {{ conda_env }}
{%- if pixi_manifest %}
eval "$(pixi shell-hook --manifest-path {{ pixi_manifest }})"
{%- endif %}
{{ body_block }}
echo "JOB DONE"
exit 0
```

- [ ] **Step 2: 失敗するテストを書く(`src/recipes/job.rs` の `mod tests` に追加)**

```rust
    #[test]
    fn base_preamble_matches_expected_fixture_for_g16_opt() {
        let out = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore gaussian_A -f",
            body_block: "python scripts/run.py",
            pixi_manifest: "",
        });
        let expected = include_str!("assets/_base.bash.expected");
        assert_eq!(out, expected, "base_preamble drifted from fixture");
    }

    #[test]
    fn base_preamble_omits_pixi_hook_when_manifest_empty() {
        let out = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore default -f",
            body_block: "python scripts/parse.py",
            pixi_manifest: "",
        });
        assert!(!out.contains("pixi shell-hook"), "got:\n{out}");
        assert!(out.contains("module restore default -f"));
        assert!(out.contains("conda activate analysis"));
    }

    #[test]
    fn base_preamble_includes_pixi_hook_when_manifest_set() {
        let out = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore gaussian_A -f",
            body_block: "python scripts/run.py",
            pixi_manifest: "/work/pixi.toml",
        });
        assert!(out.contains("pixi shell-hook --manifest-path /work/pixi.toml"));
    }

    #[test]
    fn base_preamble_has_no_sbatch_and_resets_conda() {
        let out = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore gaussian_A -f",
            body_block: "python scripts/run.py",
            pixi_manifest: "",
        });
        assert!(!out.contains("#SBATCH"), "preamble must not carry #SBATCH");
        assert!(out.contains("unset -f conda"));
        assert!(out.contains("CONDA_"));
        assert!(out.trim_end().ends_with("exit 0"));
    }
```

- [ ] **Step 3: テスト実行(失敗確認)**

Run: `cargo test --all-features base_preamble`
Expected: FAIL(`base_preamble` 未定義 / `_base.bash.expected` 不在)。

- [ ] **Step 4: `base_preamble()` を実装(`src/recipes/job.rs`、`mod tests` の直前)**

```rust
/// 上流 `_base.bash.j2` を embed し minijinja で描画した共有プリアンブル。
/// `#SBATCH` は含まない(SbatchCmd 領域)。公開シグネチャは不変契約。
pub fn base_preamble(o: &PreambleOpts<'_>) -> String {
    const TEMPLATE: &str = include_str!("assets/_base.bash.j2");
    let mut env = minijinja::Environment::new();
    // bash は空白/改行に敏感 → lstrip/trim を無効化し template の
    // whitespace-control 記法(`{%- -%}`)だけで制御する。
    env.set_lstrip_blocks(false);
    env.set_trim_blocks(false);
    env.add_template("_base", TEMPLATE)
        .expect("embedded _base.bash.j2 is a static, valid template");
    let tmpl = env.get_template("_base").expect("template was just added");
    tmpl.render(minijinja::context! {
        conda_env => o.conda_env,
        module_block => o.module_block,
        body_block => o.body_block,
        pixi_manifest => o.pixi_manifest,
    })
    .expect("static template + string context cannot fail to render")
}
```

- [ ] **Step 5: 期待フィクスチャを生成して固定**

`src/recipes/job.rs` の `mod tests` に一時テストを追加:

```rust
    #[test]
    fn _dump_fixture() {
        let out = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore gaussian_A -f",
            body_block: "python scripts/run.py",
            pixi_manifest: "",
        });
        std::fs::write("src/recipes/assets/_base.bash.expected", out).unwrap();
    }
```

Run: `cargo test --all-features _dump_fixture`
Expected: PASS。`src/recipes/assets/_base.bash.expected` が生成される。
その後 **`_dump_fixture` テストを削除**(コミット対象は `_base.bash.expected` のみ)。レビュー用に期待値の確定形を明記する:

```
#!/bin/bash
set -euo pipefail

# --- reset inherited conda activation state (pixi-conda-stack-reset) ---
# A SLURM job inherits the submitter's shell env; a half-activated conda
# stack there breaks `conda activate` here. Fully unwind it first.
unset -f conda 2>/dev/null || true
for _v in $(compgen -v | grep -E '^CONDA_' || true); do
  unset "$_v" 2>/dev/null || true
done
unset _v 2>/dev/null || true

source "$(conda info --base)/etc/profile.d/conda.sh"
. /usr/share/Modules/init/bash
module restore gaussian_A -f
conda activate analysis
python scripts/run.py
echo "JOB DONE"
exit 0
```

生成物がこの確定形と異なる場合は `_base.bash.j2` の `{%- -%}` を調整して一致させる(fixture が契約)。

- [ ] **Step 6: 全プリアンブルテスト実行**

Run: `cargo test --all-features base_preamble`
Expected: PASS(4 tests)。

- [ ] **Step 7: `mod.rs` に re-export 追加**

```rust
pub use job::{
    base_preamble, FlowRecipe, GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts,
    RecipeError, RecipeParam, RecipeParamType,
};
```

- [ ] **Step 8: no-default-features ビルド + Commit**

Run: `cargo build --bin jm --no-default-features` → PASS

```bash
git add src/recipes/assets/_base.bash.j2 src/recipes/assets/_base.bash.expected src/recipes/job.rs src/recipes/mod.rs
git commit -m "feat(recipes): base_preamble() renders embedded _base.bash.j2 via minijinja"
```

---

## Task 4: 純 Rust `.xyz` パーサ(`src/recipes/xyz.rs`)

**Files:**
- Create: `src/recipes/xyz.rs`
- Modify: `src/recipes/mod.rs`(`pub mod xyz;`)
- Test: `src/recipes/xyz.rs` の `#[cfg(test)] mod tests`

- [ ] **Step 1: `src/recipes/xyz.rs` を未実装関数 + テストで作成**

```rust
//! 最小・依存ゼロの XYZ 座標パーサ。`jm new` は化学ライブラリを持たない
//! ので、g16 gjf の geometry block を作るのに必要な最小機能だけを実装。

/// XYZ を Gaussian gjf geometry block へ変換する。
/// 形式: 1行目=原子数, 2行目=コメント, 以降 `Elem x y z`(f64)。
/// 出力は1原子1行 `{sym} {x:.6} {y:.6} {z:.6}`(末尾改行なし)。
pub fn xyz_to_geometry_block(src: &str) -> Result<String, String> {
    let _ = src;
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_atom_xyz() {
        let xyz = "2\nwater fragment\nO  0.0 0.0 0.117\nH 0.0 0.757 -0.467\n";
        let block = xyz_to_geometry_block(xyz).unwrap();
        assert_eq!(
            block,
            "O 0.000000 0.000000 0.117000\nH 0.000000 0.757000 -0.467000"
        );
    }

    #[test]
    fn rejects_atom_count_mismatch() {
        let err = xyz_to_geometry_block("3\nc\nO 0 0 0\nH 0 0 1\n").unwrap_err();
        assert!(err.contains("atom count"), "got: {err}");
    }

    #[test]
    fn rejects_bad_header() {
        let err = xyz_to_geometry_block("notanumber\nc\nO 0 0 0\n").unwrap_err();
        assert!(err.contains("first line"), "got: {err}");
    }

    #[test]
    fn rejects_malformed_atom_line() {
        let err = xyz_to_geometry_block("1\nc\nO 0.0 nope 0.0\n").unwrap_err();
        assert!(err.contains("coordinate"), "got: {err}");
    }

    #[test]
    fn rejects_empty() {
        let err = xyz_to_geometry_block("").unwrap_err();
        assert!(err.contains("first line"), "got: {err}");
    }
}
```

- [ ] **Step 2: `src/recipes/mod.rs` に登録**

```rust
pub mod job;
pub mod xyz;
```

- [ ] **Step 3: テスト実行(失敗確認)**

Run: `cargo test --all-features recipes::xyz`
Expected: FAIL(`unimplemented!()` panic)。

- [ ] **Step 4: `xyz_to_geometry_block` を実装(関数本体を置換)**

```rust
pub fn xyz_to_geometry_block(src: &str) -> Result<String, String> {
    let mut lines = src.lines();
    let count: usize = lines
        .next()
        .ok_or_else(|| "xyz: first line (atom count) missing".to_string())?
        .trim()
        .parse()
        .map_err(|_| "xyz: first line must be an integer atom count".to_string())?;
    lines
        .next()
        .ok_or_else(|| "xyz: comment line (line 2) missing".to_string())?;

    let mut out: Vec<String> = Vec::with_capacity(count);
    for (i, raw) in lines.enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        let mut it = raw.split_whitespace();
        let sym = it
            .next()
            .ok_or_else(|| format!("xyz: atom line {} empty", i + 3))?;
        let parse_coord = |o: Option<&str>, axis: &str| -> Result<f64, String> {
            o.ok_or_else(|| format!("xyz: atom line {} missing {axis} coordinate", i + 3))?
                .parse::<f64>()
                .map_err(|_| format!("xyz: atom line {} has non-numeric {axis} coordinate", i + 3))
        };
        let x = parse_coord(it.next(), "x")?;
        let y = parse_coord(it.next(), "y")?;
        let z = parse_coord(it.next(), "z")?;
        out.push(format!("{sym} {x:.6} {y:.6} {z:.6}"));
    }
    if out.len() != count {
        return Err(format!(
            "xyz: atom count mismatch — header says {count}, found {}",
            out.len()
        ));
    }
    Ok(out.join("\n"))
}
```

- [ ] **Step 5: テスト実行(成功確認)**

Run: `cargo test --all-features recipes::xyz`
Expected: PASS(5 tests)。

- [ ] **Step 6: Commit**

```bash
git add src/recipes/xyz.rs src/recipes/mod.rs
git commit -m "feat(recipes): pure-Rust xyz -> gjf geometry block parser"
```

---

## Task 5: `g16_opt` asset テンプレ(main.gjf / run.py)

**Files:**
- Create: `src/recipes/assets/g16_opt/main.gjf.tmpl`
- Create: `src/recipes/assets/g16_opt/run.py.tmpl`

sentinel 方式(`{{NAME}}` を `str::replace`。minijinja は使わない — run.py が `{...}` を多用するためデリミタ衝突回避。spec §4.1)。

- [ ] **Step 1: `src/recipes/assets/g16_opt/main.gjf.tmpl`(`%rwf` 無し、末尾改行1つ)**

```
%nprocshared={{NPROC}}
%mem={{MEM}}
%chk=main.chk
{{ROUTE}}

{{COMPOUND}}

{{CHARGE}} {{MULTIPLICITY}}
{{GEOMETRY_BLOCK}}
{{EXTRA_INPUT}}
```

- [ ] **Step 2: `src/recipes/assets/g16_opt/run.py.tmpl`(`run_g16` 写経、純 stdlib・sentinel は `{{JOB_DIR}}` の 1 個のみ)**

R3':`JOB_DIR = "{{JOB_DIR}}"` を冒頭に置き、Task 6 instantiate が `flow_dir_abs.join(job_id)` の Python エスケープ済み絶対パスへ `str::replace("{{JOB_DIR}}", ...)` する。`os.getcwd()` は使わない(SLURM cwd 非決定性に非依存=参照 `run-g16` と同性質)。`{{JOB_DIR}}` は通常文字列リテラル(f-string ではない)内なので Python の `{...}` とは衝突しない。

```python
#!/usr/bin/env python3
# Generated by `jm new g16-opt-parse`. Self-contained port of
# gaussian_compute_runtime.run_g16 (stdlib only: subprocess/shutil/os/sys).
# DO NOT import cclib or any Group B/C/D package here.
#
# REPLACE_ME: if this site has the gem stack installed, you may replace
# this entire script body with:
#   python -m gaussian_compute_runtime run-g16 --config <abs gem toml>
import os
import shutil
import subprocess
import sys

# R3': scaffold (`jm new`) swaps {{JOB_DIR}} for the absolute job dir at
# generate time. This script never reads os.getcwd(), so it is immune to
# SLURM's nondeterministic submit cwd / spool-copy (same property as the
# reference `run-g16`, which resolves everything from --config/--uuid).
JOB_DIR = "{{JOB_DIR}}"

TASK = "main"


def log(msg):
    print(f"[run.py] {msg}", flush=True)


def main():
    job_dir = JOB_DIR  # R3': scaffold-baked absolute path; cwd-independent.
    g16 = os.environ.get("JM_PARAM_G16_CMD", "g16")
    launcher = os.environ.get("JM_PARAM_LAUNCHER", "")
    flow_uuid = os.environ.get("JM_FLOW_UUID", "noflow")
    job_id = os.environ.get("JM_JOB_ID", "nojob")
    scratch_root = os.environ.get("JM_PARAM_SCRATCH_ROOT", "") or os.path.join(
        job_dir, ".scratch"
    )
    scratch = os.path.join(scratch_root, flow_uuid, job_id)

    in_dir = os.path.join(job_dir, "input")
    out_dir = os.path.join(job_dir, "output")
    os.makedirs(out_dir, exist_ok=True)

    # --- prepare_inputs: input/ -> scratch/ ---
    os.makedirs(scratch, exist_ok=True)
    if os.path.isdir(in_dir):
        shutil.copytree(in_dir, scratch, dirs_exist_ok=True)
    log(f"prepared inputs into scratch={scratch}")

    # --- (optional) overwrite %nprocshared/%mem from SLURM allocation ---
    gjf = os.path.join(scratch, f"{TASK}.gjf")
    cpus = os.environ.get("SLURM_CPUS_PER_TASK")
    mem_mb = os.environ.get("SLURM_MEM_PER_NODE")  # MB, if set
    if os.path.isfile(gjf) and (cpus or mem_mb):
        lines = []
        with open(gjf, "r") as fh:
            for ln in fh:
                low = ln.strip().lower()
                if cpus and low.startswith("%nprocshared="):
                    lines.append(f"%nprocshared={cpus}\n")
                elif mem_mb and low.startswith("%mem="):
                    lines.append(f"%mem={mem_mb}MB\n")
                else:
                    lines.append(ln)
        with open(gjf, "w") as fh:
            fh.writelines(lines)
        log(f"applied SLURM allocation (cpus={cpus}, mem_mb={mem_mb})")

    rc = 1
    copy_failed = False
    try:
        argv = ([launcher] if launcher else []) + [g16, f"{TASK}.gjf", f"{TASK}.out"]
        log(f"exec: {argv} (cwd={scratch})")
        try:
            rc = subprocess.run(argv, cwd=scratch).returncode
        except FileNotFoundError:
            # Never silently exit 0: an afterok parse step would treat an
            # empty .out as success.
            sys.stderr.write(f"error: failed to launch {argv[0]}\n")
            rc = 2
    finally:
        if os.path.isdir(scratch):
            for name in os.listdir(scratch):
                if name == f"{TASK}.out" or name.endswith((".chk", ".log")):
                    try:
                        shutil.copy2(
                            os.path.join(scratch, name), os.path.join(out_dir, name)
                        )
                    except OSError as e:
                        copy_failed = True
                        sys.stderr.write(f"warn: copy_results {name}: {e}\n")
        log("copied results back to output/")

    if rc != 0:
        sys.exit(rc)  # g16 rc has top precedence
    if copy_failed:
        sys.exit(3)
    sys.exit(0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Python 構文チェック**

Run: `python3 -c "import ast; ast.parse(open('src/recipes/assets/g16_opt/run.py.tmpl').read())"`
Expected: 例外なし(終了コード0)。唯一の sentinel `{{JOB_DIR}}` は通常文字列リテラル `JOB_DIR = "{{JOB_DIR}}"` の内側なので、未置換のテンプレでも素の Python として valid(置換後も valid)。

- [ ] **Step 4: Commit**

```bash
git add src/recipes/assets/g16_opt/main.gjf.tmpl src/recipes/assets/g16_opt/run.py.tmpl
git commit -m "feat(recipes): g16_opt assets — main.gjf (no %rwf) + self-contained run.py"
```

---

## Task 6: `g16_opt` JobTemplate 実装

**Files:**
- Create: `src/recipes/jobs/mod.rs`
- Create: `src/recipes/jobs/g16_opt.rs`
- Create: `src/recipes/jobs/parse_g16_out.rs`(placeholder。Task 8 で実装)
- Modify: `src/recipes/mod.rs`(`pub mod jobs;`)
- Modify: `Cargo.toml`(`[dev-dependencies]` に `tempfile` が無ければ追加)
- Test: `src/recipes/jobs/g16_opt.rs` の `#[cfg(test)] mod tests`

- [ ] **Step 1: `src/recipes/jobs/mod.rs` を作成(parse は Task 8 までコメントアウト)**

```rust
pub mod g16_opt;
// pub mod parse_g16_out; // implemented in Task 8

pub use g16_opt::G16Opt;
```

- [ ] **Step 1b: `src/recipes/jobs/parse_g16_out.rs` placeholder**

```rust
//! Placeholder — full impl in plan Task 8.
```

- [ ] **Step 2: `src/recipes/jobs/g16_opt.rs` をテスト + 実装で作成**

```rust
//! JobTemplate `g16_opt` — g16 構造最適化1ステップ。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::recipes::job::{
    base_preamble, GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeError,
    RecipeParam, RecipeParamType,
};
use crate::recipes::xyz::xyz_to_geometry_block;

pub struct G16Opt;

const PARAMS: &[RecipeParam] = &[
    RecipeParam { name: "route", ty: RecipeParamType::Str, default: "#p opt b3lyp/6-31g(d)", help: "Gaussian route line" },
    RecipeParam { name: "charge", ty: RecipeParamType::Int, default: "0", help: "total charge" },
    RecipeParam { name: "multiplicity", ty: RecipeParamType::Int, default: "1", help: "spin multiplicity" },
    RecipeParam { name: "extra_input", ty: RecipeParamType::Str, default: "", help: "input appended after geometry" },
    RecipeParam { name: "nproc", ty: RecipeParamType::Int, default: "8", help: "scaffold %nprocshared (run.py overrides from SLURM)" },
    RecipeParam { name: "mem", ty: RecipeParamType::Str, default: "8GB", help: "scaffold %mem (run.py overrides from SLURM)" },
    RecipeParam { name: "compound", ty: RecipeParamType::Str, default: "REPLACE_ME-INCHIKEY", help: "InChIKey; gjf title + [tags].compound" },
    RecipeParam { name: "g16_cmd", ty: RecipeParamType::Str, default: "g16", help: "Gaussian binary -> JM_PARAM_G16_CMD" },
    RecipeParam { name: "conda_env", ty: RecipeParamType::Str, default: "analysis", help: "conda activate <env>" },
    RecipeParam { name: "module_profile", ty: RecipeParamType::Str, default: "gaussian_A", help: "module restore <profile> -f" },
    RecipeParam { name: "pixi_manifest", ty: RecipeParamType::Path, default: "", help: "empty = skip pixi hook" },
    RecipeParam { name: "launcher", ty: RecipeParamType::Str, default: "srun", help: "empty = bare (no srun)" },
    RecipeParam { name: "scratch_root", ty: RecipeParamType::Path, default: "", help: "empty = <job_dir>/.scratch fallback" },
    RecipeParam { name: "input_coordinate", ty: RecipeParamType::Path, default: "", help: ".xyz/.mol2 copied into <id>/input/ by cmd_new" },
];

fn pv<'a>(ctx: &'a JobCtx<'_>, k: &str) -> &'a str {
    ctx.params.get(k).map(|s| s.as_str()).unwrap_or_default()
}

/// R3': `JOB_DIR = "{{JOB_DIR}}"` の二重引用符内へ差し込む Python 文字列リテラル
/// 内容のエスケープ(`\` と `"` のみ。POSIX パスに改行はまず無いが念のため `\n` も)。
/// 周囲の引用符はテンプレ側 (`"{{JOB_DIR}}"`) が持つ。
fn py_escape(p: &Path) -> String {
    p.to_string_lossy()
        .replace('\\', r"\\")
        .replace('"', "\\\"")
        .replace('\n', r"\n")
}

/// param 値を宣言型に応じた `toml::Value` へ(検証は assemble 済み前提。
/// パース失敗時は文字列フォールバックで panic しない)。
fn typed_toml(ty: RecipeParamType, v: &str) -> toml::Value {
    match ty {
        RecipeParamType::Int => v
            .parse::<i64>()
            .map(toml::Value::Integer)
            .unwrap_or_else(|_| toml::Value::String(v.to_string())),
        RecipeParamType::Float => v
            .parse::<f64>()
            .map(toml::Value::Float)
            .unwrap_or_else(|_| toml::Value::String(v.to_string())),
        RecipeParamType::Bool => v
            .parse::<bool>()
            .map(toml::Value::Boolean)
            .unwrap_or_else(|_| toml::Value::String(v.to_string())),
        RecipeParamType::Str | RecipeParamType::Path => toml::Value::String(v.to_string()),
    }
}

impl JobTemplate for G16Opt {
    fn name(&self) -> &'static str {
        "g16_opt"
    }
    fn params(&self) -> &'static [RecipeParam] {
        PARAMS
    }
    fn inputs(&self) -> &'static [&'static str] {
        &[]
    }
    fn outputs(&self) -> &'static [(&'static str, &'static str)] {
        &[("gaussian_out", "output/main.out")]
    }

    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, RecipeError> {
        let job_id = ctx.job_id;

        let geometry_block = match pv(ctx, "input_coordinate") {
            "" => "<GEOMETRY: REPLACE_ME — Elem x y z を1原子1行>".to_string(),
            path if path.to_ascii_lowercase().ends_with(".xyz") => {
                let src = std::fs::read_to_string(path)
                    .map_err(|_| RecipeError::InputCoordinateMissing(PathBuf::from(path)))?;
                xyz_to_geometry_block(&src).map_err(RecipeError::XyzParse)?
            }
            _ => "<GEOMETRY: REPLACE_ME — non-xyz coordinate copied to input/; fill manually>"
                .to_string(),
        };

        let gjf = include_str!("../assets/g16_opt/main.gjf.tmpl")
            .replace("{{NPROC}}", pv(ctx, "nproc"))
            .replace("{{MEM}}", pv(ctx, "mem"))
            .replace("{{ROUTE}}", pv(ctx, "route"))
            .replace("{{COMPOUND}}", pv(ctx, "compound"))
            .replace("{{CHARGE}}", pv(ctx, "charge"))
            .replace("{{MULTIPLICITY}}", pv(ctx, "multiplicity"))
            .replace("{{GEOMETRY_BLOCK}}", &geometry_block)
            .replace("{{EXTRA_INPUT}}", pv(ctx, "extra_input"));

        let abs_job_dir = ctx.flow_dir_abs.join(job_id);
        let run_py = include_str!("../assets/g16_opt/run.py.tmpl")
            .replace("{{JOB_DIR}}", &py_escape(&abs_job_dir)); // R3': cwd-independent

        let module_block = format!("module restore {} -f", pv(ctx, "module_profile"));
        let bash = base_preamble(&PreambleOpts {
            conda_env: pv(ctx, "conda_env"),
            module_block: &module_block,
            body_block: "python scripts/run.py",
            pixi_manifest: pv(ctx, "pixi_manifest"),
        });

        let nsp = |rel: &str| PathBuf::from(format!("{job_id}/{rel}"));
        let sidecars = vec![
            GeneratedFile { relpath: nsp(&format!("scripts/{job_id}.bash")), contents: bash, unix_mode: Some(0o755) },
            GeneratedFile { relpath: nsp("scripts/run.py"), contents: run_py, unix_mode: Some(0o755) },
            GeneratedFile { relpath: nsp("input/main.gjf"), contents: gjf, unix_mode: None },
        ];

        let mut plan_params = BTreeMap::new();
        for rp in PARAMS {
            if rp.name == "input_coordinate" {
                continue; // scaffold 時消費のみ。plan.toml には出さない。
            }
            plan_params.insert(rp.name.to_string(), typed_toml(rp.ty, pv(ctx, rp.name)));
        }

        // R3': body は薄起動子のみ。cd 無し(job dir は run.py の JOB_DIR 絶対定数)。
        let body = format!("bash scripts/{job_id}.bash\n");

        Ok(JobArtifacts {
            program: "g16".to_string(),
            body,
            time_limit: Some("48:00:00".to_string()),
            plan_params,
            sidecars,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with<'a>(
        params: &'a BTreeMap<String, String>,
        inputs: &'a BTreeMap<String, String>,
        uuid: &'a uuid::Uuid,
        flow_dir: &'a Path,
    ) -> JobCtx<'a> {
        JobCtx {
            job_id: "opt",
            params,
            inputs,
            uuid,
            created_at: "2026-05-18T00:00:00Z",
            flow_dir_abs: flow_dir,
        }
    }

    fn default_params() -> BTreeMap<String, String> {
        PARAMS
            .iter()
            .map(|p| (p.name.to_string(), p.default.to_string()))
            .collect()
    }

    #[test]
    fn instantiate_emits_r3prime_body_and_sidecars() {
        let params = default_params();
        let inputs = BTreeMap::new();
        let uuid = uuid::Uuid::now_v7();
        let flow_dir = Path::new("/work/root/01999999-0000-7000-8000-000000000000");
        let a = G16Opt
            .instantiate(&ctx_with(&params, &inputs, &uuid, flow_dir))
            .unwrap();

        assert_eq!(a.program, "g16");
        assert_eq!(a.time_limit.as_deref(), Some("48:00:00"));
        // R3': body has NO cd — just the thin launcher.
        assert_eq!(a.body, "bash scripts/opt.bash\n");
        assert!(!a.body.contains("cd "), "R3': body must not cd");

        let bash = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/opt.bash"))
            .unwrap();
        assert_eq!(bash.unix_mode, Some(0o755));
        assert!(bash.contents.contains("module restore gaussian_A -f"));
        assert!(bash.contents.contains("conda activate analysis"));
        assert!(bash.contents.contains("python scripts/run.py"));
        assert!(!bash.contents.contains("srun"), "srun lives in run.py");

        let runpy = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/run.py"))
            .unwrap();
        assert_eq!(runpy.unix_mode, Some(0o755));
        // R3': absolute JOB_DIR baked in, no {{JOB_DIR}} sentinel left,
        // os.getcwd() never used (cwd-independent like the reference run-g16).
        assert!(runpy.contents.contains(
            "JOB_DIR = \"/work/root/01999999-0000-7000-8000-000000000000/opt\""
        ));
        assert!(!runpy.contents.contains("{{JOB_DIR}}"), "sentinel must be swapped");
        assert!(!runpy.contents.contains("os.getcwd()"), "R3': cwd-independent");
        assert!(runpy.contents.contains("subprocess.run(argv, cwd=scratch)"));
        assert!(runpy.contents.contains("finally:"));
        assert!(runpy.contents.contains("failed to launch"));
        assert!(runpy.contents.contains("REPLACE_ME"));
        assert!(!runpy.contents.contains("import cclib"));

        let gjf = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("input/main.gjf"))
            .unwrap();
        assert!(!gjf.contents.contains("%rwf"));
        assert!(!gjf.contents.contains("{{"));
        assert!(gjf.contents.contains("0 1"));
        assert!(gjf.contents.contains("REPLACE_ME"));
    }

    #[test]
    fn instantiate_injects_xyz_geometry() {
        let dir = tempfile::tempdir().unwrap();
        let xyz = dir.path().join("mol.xyz");
        std::fs::write(&xyz, "1\ncomment\nO 0.0 0.0 0.0\n").unwrap();
        let mut params = default_params();
        params.insert("input_coordinate".into(), xyz.to_string_lossy().into_owned());
        params.insert("charge".into(), "1".into());
        let inputs = BTreeMap::new();
        let uuid = uuid::Uuid::now_v7();
        let a = G16Opt
            .instantiate(&ctx_with(&params, &inputs, &uuid, Path::new("/r/u")))
            .unwrap();
        let gjf = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("input/main.gjf"))
            .unwrap();
        assert!(gjf.contents.contains("O 0.000000 0.000000 0.000000"));
        assert!(gjf.contents.contains("1 1"));
    }

    #[test]
    fn instantiate_errors_on_missing_xyz() {
        let mut params = default_params();
        params.insert("input_coordinate".into(), "/no/such.xyz".into());
        let inputs = BTreeMap::new();
        let uuid = uuid::Uuid::now_v7();
        let err = G16Opt
            .instantiate(&ctx_with(&params, &inputs, &uuid, Path::new("/r/u")))
            .unwrap_err();
        assert!(matches!(err, RecipeError::InputCoordinateMissing(_)));
    }

    #[test]
    fn plan_params_exclude_input_coordinate_and_type_ints() {
        let params = default_params();
        let inputs = BTreeMap::new();
        let uuid = uuid::Uuid::now_v7();
        let a = G16Opt
            .instantiate(&ctx_with(&params, &inputs, &uuid, Path::new("/r/u")))
            .unwrap();
        assert!(!a.plan_params.contains_key("input_coordinate"));
        assert_eq!(a.plan_params.get("charge"), Some(&toml::Value::Integer(0)));
        assert_eq!(
            a.plan_params.get("launcher"),
            Some(&toml::Value::String("srun".into()))
        );
    }
}
```

- [ ] **Step 3: `tempfile` dev-dep を確認/追加**

Run: `grep -n 'tempfile' Cargo.toml`
Expected: `[dev-dependencies]` に `tempfile`。無ければ `Cargo.toml` `[dev-dependencies]` に `tempfile = "3"` を追加。

- [ ] **Step 4: `src/recipes/mod.rs` に jobs 登録**

```rust
pub mod job;
pub mod jobs;
pub mod xyz;
```

- [ ] **Step 5: テスト実行**

Run: `cargo test --all-features recipes::jobs::g16_opt`
Expected: PASS(4 tests)。RED ならフォーマット/型差を修正。

- [ ] **Step 6: no-default-features + clippy**

Run: `cargo build --bin jm --no-default-features` → PASS
Run: `cargo clippy --all-targets --all-features -- -D warnings` → PASS

- [ ] **Step 7: Commit**

```bash
git add src/recipes/jobs/ src/recipes/mod.rs Cargo.toml
git commit -m "feat(recipes): g16_opt JobTemplate (R3' cwd-independent body, base_preamble, run.py, gjf)"
```

---

## Task 7: `parse_g16_out` asset(parse.py)

**Files:**
- Create: `src/recipes/assets/parse_g16_out/parse.py.tmpl`

- [ ] **Step 1: `src/recipes/assets/parse_g16_out/parse.py.tmpl`(`parse_results` 写経)**

sentinel は 2 個:`{{JOB_DIR}}`(R3':scaffold が絶対 job dir を swap-in。cwd 非依存)と `{{INPUT_REL}}`(wiring が `../opt/output/main.out` に解決)。入力・出力とも `JOB_DIR` 基準で絶対化し `os.getcwd()` を使わない。

```python
#!/usr/bin/env python3
# Generated by `jm new g16-opt-parse`. Self-contained port of
# gaussian_compute_runtime.parse_results (cclib + stdlib only). Writes a
# curated output/result.json. status は job-manager Lifecycle/tick 権威。
#
# REPLACE_ME: if this site has the gem stack installed, you may replace
# this script with:
#   python -m gaussian_compute_runtime parse-results --config <abs gem toml>
import json
import math
import os
import sys
import tempfile

SCHEMA = "jm-recipe/1"
# R3': scaffold swaps these at generate time. parse.py never reads
# os.getcwd(), so SLURM's nondeterministic submit cwd cannot break it
# (same cwd-independence as the reference run-g16 / parse-results).
JOB_DIR = "{{JOB_DIR}}"
INPUT_REL = "{{INPUT_REL}}"


def fail(code, msg):
    sys.stderr.write(f"error: {msg}\n")
    sys.exit(code)


def atomic_write_json(path, obj):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".result.", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(obj, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main():
    try:
        import cclib  # noqa: F401
        from cclib.io import ccread
    except Exception:
        fail(2, "cclib not importable")

    # R3': resolve the wiring-relative input against the baked absolute
    # JOB_DIR (e.g. <...>/parse + ../opt/output/main.out -> <...>/opt/...).
    src = os.path.normpath(os.path.join(JOB_DIR, INPUT_REL))
    if not os.path.isfile(src):
        fail(1, f"gaussian out not found: {src}")

    try:
        data = ccread(src)
    except Exception as e:
        fail(1, f"cclib could not parse {src}: {e}")
    if data is None:
        fail(1, f"cclib returned no data for {src}")

    meta = getattr(data, "metadata", {}) or {}
    if not bool(meta.get("success", False)):
        fail(1, "gaussian run did not terminate normally")

    optdone = getattr(data, "optdone", None)
    converged = True if optdone is None else bool(optdone)
    if not converged:
        fail(1, "geometry optimization did not converge")

    scf = getattr(data, "scfenergies", None)
    if scf is None or len(scf) == 0 or not math.isfinite(float(scf[-1])):
        fail(1, "final SCF energy missing or non-finite")

    natom = int(getattr(data, "natom", 0) or 0)
    result = {
        "schema": SCHEMA,
        "converged": converged,
        "scf_energy": float(scf[-1]),
        "n_atoms": natom,
        "source": os.path.abspath(src),
    }
    out_path = os.path.join(JOB_DIR, "output", "result.json")
    try:
        atomic_write_json(out_path, result)
    except OSError as e:
        fail(3, f"could not write {out_path}: {e}")

    # TODO(jm recipe): write derived/main.mol2 for multi-step g16 chaining
    # (out of scope for v1 opt->parse).
    print(
        f"[parse.py] OK converged={converged} E={result['scf_energy']} N={natom}",
        flush=True,
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Python 構文チェック(sentinel 仮置換)**

Run:
```bash
python3 -c "import ast; ast.parse(open('src/recipes/assets/parse_g16_out/parse.py.tmpl').read().replace('{{JOB_DIR}}','/tmp/u/parse').replace('{{INPUT_REL}}','../opt/output/main.out'))"
```
Expected: 例外なし(終了コード0)。

- [ ] **Step 3: Commit**

```bash
git add src/recipes/assets/parse_g16_out/parse.py.tmpl
git commit -m "feat(recipes): parse_g16_out asset — cclib parse.py -> curated result.json"
```

---

## Task 8: `parse_g16_out` JobTemplate 実装

**Files:**
- Modify: `src/recipes/jobs/parse_g16_out.rs`(placeholder を実装で全置換)
- Modify: `src/recipes/jobs/mod.rs`(`parse_g16_out` を有効化)
- Test: `src/recipes/jobs/parse_g16_out.rs` の `#[cfg(test)] mod tests`

- [ ] **Step 1: `src/recipes/jobs/parse_g16_out.rs` を実装 + テストで全置換**

```rust
//! JobTemplate `parse_g16_out` — 軽量 post。cclib で .out を検証し
//! output/result.json を書く。srun/巨大 scratch 無し → launcher/
//! scratch_root param 不要。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::recipes::job::{
    base_preamble, GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeError,
    RecipeParam, RecipeParamType,
};

pub struct ParseG16Out;

const PARAMS: &[RecipeParam] = &[
    RecipeParam { name: "conda_env", ty: RecipeParamType::Str, default: "analysis", help: "conda activate <env>" },
    RecipeParam { name: "pixi_manifest", ty: RecipeParamType::Path, default: "", help: "empty = skip pixi hook" },
];

fn pv<'a>(ctx: &'a JobCtx<'_>, k: &str) -> &'a str {
    ctx.params.get(k).map(|s| s.as_str()).unwrap_or_default()
}

/// R3': `JOB_DIR = "{{JOB_DIR}}"` の二重引用符内へ差し込む Python 文字列
/// リテラル内容のエスケープ(周囲の引用符はテンプレ側が持つ)。
fn py_escape(p: &Path) -> String {
    p.to_string_lossy()
        .replace('\\', r"\\")
        .replace('"', "\\\"")
        .replace('\n', r"\n")
}

impl JobTemplate for ParseG16Out {
    fn name(&self) -> &'static str {
        "parse_g16_out"
    }
    fn params(&self) -> &'static [RecipeParam] {
        PARAMS
    }
    fn inputs(&self) -> &'static [&'static str] {
        &["gaussian_out"]
    }
    fn outputs(&self) -> &'static [(&'static str, &'static str)] {
        &[("result_json", "output/result.json")]
    }

    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, RecipeError> {
        let job_id = ctx.job_id;
        let input_rel = ctx
            .inputs
            .get("gaussian_out")
            .cloned()
            .unwrap_or_else(|| "../opt/output/main.out".to_string());

        let abs_job_dir = ctx.flow_dir_abs.join(job_id);
        let parse_py = include_str!("../assets/parse_g16_out/parse.py.tmpl")
            .replace("{{JOB_DIR}}", &py_escape(&abs_job_dir)) // R3': cwd-independent
            .replace("{{INPUT_REL}}", &input_rel);

        let bash = base_preamble(&PreambleOpts {
            conda_env: pv(ctx, "conda_env"),
            module_block: "module restore default -f",
            body_block: "python scripts/parse.py",
            pixi_manifest: pv(ctx, "pixi_manifest"),
        });

        let nsp = |rel: &str| PathBuf::from(format!("{job_id}/{rel}"));
        let sidecars = vec![
            GeneratedFile { relpath: nsp(&format!("scripts/{job_id}.bash")), contents: bash, unix_mode: Some(0o755) },
            GeneratedFile { relpath: nsp("scripts/parse.py"), contents: parse_py, unix_mode: Some(0o755) },
        ];

        let mut plan_params = BTreeMap::new();
        for rp in PARAMS {
            plan_params.insert(
                rp.name.to_string(),
                toml::Value::String(pv(ctx, rp.name).to_string()),
            );
        }

        // R3': body は薄起動子のみ。cd 無し(入出力は parse.py の JOB_DIR 絶対定数)。
        let body = format!("bash scripts/{job_id}.bash\n");

        Ok(JobArtifacts {
            program: "python".to_string(),
            body,
            time_limit: Some("01:00:00".to_string()),
            plan_params,
            sidecars,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(
        params: &'a BTreeMap<String, String>,
        inputs: &'a BTreeMap<String, String>,
        uuid: &'a uuid::Uuid,
    ) -> JobCtx<'a> {
        JobCtx {
            job_id: "parse",
            params,
            inputs,
            uuid,
            created_at: "2026-05-18T00:00:00Z",
            flow_dir_abs: Path::new("/r/u"),
        }
    }

    #[test]
    fn instantiate_wires_input_and_emits_parse_py() {
        let params: BTreeMap<String, String> = PARAMS
            .iter()
            .map(|p| (p.name.to_string(), p.default.to_string()))
            .collect();
        let mut inputs = BTreeMap::new();
        inputs.insert("gaussian_out".into(), "../opt/output/main.out".into());
        let uuid = uuid::Uuid::now_v7();
        let a = ParseG16Out.instantiate(&ctx(&params, &inputs, &uuid)).unwrap();

        assert_eq!(a.program, "python");
        assert_eq!(a.time_limit.as_deref(), Some("01:00:00"));
        // R3': body has NO cd.
        assert_eq!(a.body, "bash scripts/parse.bash\n");
        assert!(!a.body.contains("cd "), "R3': body must not cd");

        let bash = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/parse.bash"))
            .unwrap();
        assert!(bash.contents.contains("module restore default -f"));
        assert!(bash.contents.contains("python scripts/parse.py"));

        let py = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/parse.py"))
            .unwrap();
        assert_eq!(py.unix_mode, Some(0o755));
        // R3': absolute JOB_DIR baked, sentinels swapped, cwd-independent.
        assert!(py.contents.contains("JOB_DIR = \"/r/u/parse\""));
        assert!(!py.contents.contains("{{JOB_DIR}}"));
        assert!(!py.contents.contains("os.getcwd()"), "R3': cwd-independent");
        assert!(py.contents.contains("../opt/output/main.out"));
        assert!(!py.contents.contains("{{INPUT_REL}}"));
        assert!(py.contents.contains("cclib"));
        assert!(py.contents.contains("result.json"));
        assert!(py.contents.contains("TODO(jm recipe): write derived/main.mol2"));
        assert!(py.contents.contains("REPLACE_ME"));
    }
}
```

- [ ] **Step 2: `src/recipes/jobs/mod.rs` を有効化**

```rust
pub mod g16_opt;
pub mod parse_g16_out;

pub use g16_opt::G16Opt;
pub use parse_g16_out::ParseG16Out;
```

- [ ] **Step 3: テスト + ビルド**

Run: `cargo test --all-features recipes::jobs::parse_g16_out`
Expected: PASS(1 test)。
Run: `cargo build --bin jm --no-default-features`
Expected: PASS。

- [ ] **Step 4: Commit**

```bash
git add src/recipes/jobs/parse_g16_out.rs src/recipes/jobs/mod.rs
git commit -m "feat(recipes): parse_g16_out JobTemplate (wires parent gaussian_out)"
```

---

## Task 9: JobFlow 実型パス確定

**Files:**
- 調査のみ(編集なし)

> Task 10 の `assemble()` テストで生成 flow.toml を実 `JobFlow` 型へ `toml::from_str` する。型パスと field 名を先に確定する。

- [ ] **Step 1: `read_flow` が使う型と field を確認**

Run: `grep -rn "JobFlow\|fn read_flow\|\.parents\|\.jobs\b\|from:\|kind:" src/persistence/ src/flow.rs | head -30`
Expected: `JobFlow` の正準パス(例 `gaussian_job_shared::entities::workflow::JobFlow` か `crate::flow::FlowRun` 経由か)と `jobs`/`parents`/`from`/`kind` の正確な型・field 名を把握。

- [ ] **Step 2: 確定結果をメモ**

確定したパス・field 名を本タスクのコメントとして控え、Task 10 のテスト(`flow_toml_parses_as_jobflow_*`)で正確に使う。例: `use gaussian_job_shared::entities::workflow::JobFlow;` / `flow.jobs: BTreeMap<String, JobSpec>` / `JobSpec.parents: Vec<JobEdge>` / `JobEdge { from, kind }`。`kind` が enum なら `format!("{:?}", e.kind)` 比較、文字列なら直接比較。

(コミット不要 — 調査タスク。Task 10 にマージ。)

---

## Task 10: `FlowRecipe g16-opt-parse` + `assemble()`

**Files:**
- Create: `src/recipes/flows/mod.rs`
- Create: `src/recipes/flows/g16_opt_parse.rs`
- Create: `src/recipes/flows/blank.rs`(placeholder。Task 12 で実装)
- Create: `src/recipes/flow.rs`
- Modify: `src/recipes/mod.rs`(`pub mod flow; pub mod flows;` + re-export)
- Test: `src/recipes/flow.rs` の `#[cfg(test)] mod tests`

- [ ] **Step 1: `src/recipes/flows/g16_opt_parse.rs`**

```rust
//! FlowRecipe `g16-opt-parse` — opt --afterok--> parse。

use crate::recipes::job::FlowRecipe;

pub struct G16OptParse;

impl FlowRecipe for G16OptParse {
    fn name(&self) -> &'static str {
        "g16-opt-parse"
    }
    fn summary(&self) -> &'static str {
        "g16 geometry optimization -> afterok -> cclib result.json (self-contained, kudpc)"
    }
    fn nodes(&self) -> &'static [(&'static str, &'static str)] {
        &[("opt", "g16_opt"), ("parse", "parse_g16_out")]
    }
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)] {
        &[("opt", "parse", "afterok")]
    }
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)] {
        &[("parse", "gaussian_out", "opt", "gaussian_out")]
    }
}
```

- [ ] **Step 2: `src/recipes/flows/mod.rs` + blank placeholder**

`src/recipes/flows/mod.rs`:

```rust
pub mod blank; // legacy migration in Task 12
pub mod g16_opt_parse;

pub use g16_opt_parse::G16OptParse;
```

`src/recipes/flows/blank.rs`:

```rust
//! Placeholder — legacy blank migration in plan Task 12.
```

- [ ] **Step 3: `src/recipes/flow.rs` を実装 + テストで作成**

> `JobFlow` 型パス/field は Task 9 で確定済みのものを使う。下記テストの `gaussian_job_shared::entities::workflow::JobFlow` と `parents[0].from/.kind` は確定値に合わせて修正すること。

```rust
//! Real FlowRecipe の組立。nodes -> JobTemplate::instantiate ->
//! flow.toml / plan.toml 片 + サイドカー。blank は対象外(Task 12)。

use std::collections::BTreeMap;

use crate::recipes::job::{FlowRecipe, GeneratedFile, JobCtx, JobTemplate, RecipeError};
use crate::recipes::jobs::{G16Opt, ParseG16Out};

pub fn job_template(name: &str) -> Result<Box<dyn JobTemplate>, RecipeError> {
    match name {
        "g16_opt" => Ok(Box::new(G16Opt)),
        "parse_g16_out" => Ok(Box::new(ParseG16Out)),
        other => Err(RecipeError::UnknownJob(
            other.to_string(),
            "g16_opt, parse_g16_out".to_string(),
        )),
    }
}

fn resolve_params(
    job_id: &str,
    tmpl: &dyn JobTemplate,
    raw: &BTreeMap<(String, String), String>,
) -> Result<BTreeMap<String, String>, RecipeError> {
    use crate::recipes::job::RecipeParamType::*;
    let mut out = BTreeMap::new();
    for rp in tmpl.params() {
        out.insert(rp.name.to_string(), rp.default.to_string());
    }
    for ((j, name), val) in raw {
        if j != job_id {
            continue;
        }
        let rp = tmpl
            .params()
            .iter()
            .find(|rp| rp.name == name)
            .ok_or_else(|| RecipeError::UnknownParam {
                job: job_id.to_string(),
                param: name.clone(),
                available: tmpl
                    .params()
                    .iter()
                    .map(|rp| rp.name)
                    .collect::<Vec<_>>()
                    .join(", "),
            })?;
        let ok = match rp.ty {
            Int => val.parse::<i64>().is_ok(),
            Float => val.parse::<f64>().is_ok(),
            Bool => val.parse::<bool>().is_ok(),
            Str | Path => true,
        };
        if !ok {
            return Err(RecipeError::BadParamType {
                job: job_id.to_string(),
                param: name.clone(),
                value: val.clone(),
                ty: format!("{:?}", rp.ty),
            });
        }
        out.insert(name.clone(), val.clone());
    }
    Ok(out)
}

/// `assemble` の戻り。
pub struct Assembled {
    pub flow_toml: String,
    pub plan_toml: String,
    pub sidecars: Vec<GeneratedFile>,
    /// `--param opt.input_coordinate` の (JobId, src path)。空なら None。
    pub input_coordinate: Option<(String, std::path::PathBuf)>,
}

pub fn assemble(
    recipe: &dyn FlowRecipe,
    raw_params: &BTreeMap<(String, String), String>,
    tags: &BTreeMap<String, String>,
    uuid: &uuid::Uuid,
    created_at: &str,
    abs_flow_dir: &std::path::Path,
) -> Result<Assembled, RecipeError> {
    // 1. wiring -> consumer JobId -> (input名 -> 相対パス)。
    let mut inputs_by_job: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for (consumer, in_name, producer, out_name) in recipe.wiring() {
        let ptmpl_name = recipe
            .nodes()
            .iter()
            .find(|(jid, _)| jid == producer)
            .map(|(_, t)| *t)
            .ok_or_else(|| {
                RecipeError::UnknownJob((*producer).to_string(), "recipe node".to_string())
            })?;
        let ptmpl = job_template(ptmpl_name)?;
        let rel = ptmpl
            .outputs()
            .iter()
            .find(|(o, _)| o == out_name)
            .map(|(_, p)| *p)
            .ok_or_else(|| {
                RecipeError::UnknownJob(
                    format!("{producer}.{out_name}"),
                    "producer output".to_string(),
                )
            })?;
        inputs_by_job
            .entry((*consumer).to_string())
            .or_default()
            .insert((*in_name).to_string(), format!("../{producer}/{rel}"));
    }

    let mut flow_jobs = String::new();
    let mut plan_jobs = String::new();
    let mut sidecars: Vec<GeneratedFile> = Vec::new();
    let mut input_coordinate: Option<(String, std::path::PathBuf)> = None;

    for (job_id, tmpl_name) in recipe.nodes() {
        let tmpl = job_template(tmpl_name)?;
        let params = resolve_params(job_id, tmpl.as_ref(), raw_params)?;

        if let Some(ic) = params.get("input_coordinate") {
            if !ic.is_empty() {
                input_coordinate = Some((job_id.to_string(), std::path::PathBuf::from(ic)));
            }
        }

        let empty = BTreeMap::new();
        let inputs = inputs_by_job.get(*job_id).unwrap_or(&empty);
        let ctx = JobCtx {
            job_id,
            params: &params,
            inputs,
            uuid,
            created_at,
            flow_dir_abs: abs_flow_dir,
        };
        let art = tmpl.instantiate(&ctx)?;
        sidecars.extend(art.sidecars);

        flow_jobs.push_str(&format!(
            "[jobs.{job_id}]\nprogram = {}\nbody = \"\"\"{}\"\"\"\n",
            toml::Value::String(art.program.clone()),
            art.body
        ));
        for (from, to, kind) in recipe.edges() {
            if to == job_id {
                flow_jobs.push_str(&format!(
                    "\n[[jobs.{job_id}.parents]]\nfrom = {}\nkind = {}\n",
                    toml::Value::String((*from).to_string()),
                    toml::Value::String((*kind).to_string())
                ));
            }
        }
        flow_jobs.push_str(&format!("\n[jobs.{job_id}.config]\npartition = \"REPLACE_ME\"\n"));
        if let Some(tl) = &art.time_limit {
            flow_jobs.push_str(&format!("time_limit = {}\n", toml::Value::String(tl.clone())));
        }
        flow_jobs.push('\n');

        plan_jobs.push_str(&format!("[jobs.{job_id}]\n"));
        for (k, v) in &art.plan_params {
            plan_jobs.push_str(&format!("{k} = {v}\n"));
        }
        plan_jobs.push('\n');
    }

    let mut tag_lines = String::new();
    tag_lines.push_str(&format!(
        "recipe = {}\n",
        toml::Value::String(recipe.name().to_string())
    ));
    for (k, v) in tags {
        tag_lines.push_str(&format!("{k} = {}\n", toml::Value::String(v.clone())));
    }

    let flow_toml = format!(
        "# Generated by `jm new {}` on {created_at}.\n# Schema: gaussian_job_shared::entities::workflow::JobFlow (deny_unknown_fields)\n\nuuid       = \"{uuid}\"\ncreated_at = \"{created_at}\"\n\n[tags]\n{tag_lines}\n{flow_jobs}",
        recipe.name()
    );
    let plan_toml = format!(
        "# Generated by `jm new {}`. Per-JobId params surface in batch.bash\n# as JM_PARAM_<UPPER_NAME>. Schema: job_manager::plan::ExperimentPlan.\n\n{plan_jobs}",
        recipe.name()
    );

    Ok(Assembled {
        flow_toml,
        plan_toml,
        sidecars,
        input_coordinate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipes::flows::G16OptParse;

    fn assemble_default() -> Assembled {
        let raw = BTreeMap::new();
        let mut tags = BTreeMap::new();
        tags.insert("compound".to_string(), "REPLACE_ME-INCHIKEY".to_string());
        let uuid = uuid::Uuid::now_v7();
        assemble(
            &G16OptParse,
            &raw,
            &tags,
            &uuid,
            "2026-05-18T00:00:00Z",
            std::path::Path::new("/work/root/01999999-0000-7000-8000-0000000000ab"),
        )
        .unwrap()
    }

    #[test]
    fn flow_toml_parses_as_jobflow_with_afterok_edge() {
        // NOTE: Task 9 で確定した JobFlow 実型パスに置換すること。
        let a = assemble_default();
        let flow: gaussian_job_shared::entities::workflow::JobFlow =
            toml::from_str(&a.flow_toml).expect("flow.toml must parse as JobFlow");
        let ids: std::collections::BTreeSet<_> = flow.jobs.keys().cloned().collect();
        assert!(ids.contains("opt") && ids.contains("parse") && ids.len() == 2);
        let parse = &flow.jobs["parse"];
        assert_eq!(parse.parents.len(), 1);
        assert_eq!(parse.parents[0].from, "opt");
        assert!(format!("{:?}", parse.parents[0].kind)
            .to_lowercase()
            .contains("afterok"));
    }

    #[test]
    fn plan_toml_parses_and_keysets_match() {
        let a = assemble_default();
        let plan: crate::plan::ExperimentPlan =
            toml::from_str(&a.plan_toml).expect("plan.toml must parse as ExperimentPlan");
        let flow: gaussian_job_shared::entities::workflow::JobFlow =
            toml::from_str(&a.flow_toml).unwrap();
        let flow_ids: std::collections::BTreeSet<_> = flow.jobs.keys().cloned().collect();
        let plan_ids: std::collections::BTreeSet<_> = plan.jobs.keys().cloned().collect();
        assert_eq!(flow_ids, plan_ids, "flow JobId set must equal plan key set");
    }

    #[test]
    fn config_partition_is_replace_me_and_times_set() {
        let a = assemble_default();
        assert_eq!(a.flow_toml.matches("partition = \"REPLACE_ME\"").count(), 2);
        assert!(a.flow_toml.contains("time_limit = \"48:00:00\""));
        assert!(a.flow_toml.contains("time_limit = \"01:00:00\""));
    }

    #[test]
    fn parse_input_wired_relative_to_opt_output() {
        let a = assemble_default();
        let py = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("parse/scripts/parse.py"))
            .unwrap();
        assert!(py.contents.contains("../opt/output/main.out"));
    }

    #[test]
    fn r3prime_no_cd_in_body_and_run_py_has_absolute_job_dir() {
        let a = assemble_default();
        // R3': flow.toml body must NOT contain a cd anchor.
        assert!(
            !a.flow_toml.contains("cd "),
            "R3': flow.toml body must not cd; got:\n{}",
            a.flow_toml
        );
        assert!(a.flow_toml.contains("bash scripts/opt.bash"));
        // The absolute job dir is baked into opt/scripts/run.py instead.
        let runpy = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("opt/scripts/run.py"))
            .unwrap();
        assert!(runpy.contents.contains(
            "JOB_DIR = \"/work/root/01999999-0000-7000-8000-0000000000ab/opt\""
        ));
        assert!(!runpy.contents.contains("os.getcwd()"), "R3': cwd-independent");
    }

    #[test]
    fn unknown_param_is_rejected() {
        let mut raw = BTreeMap::new();
        raw.insert(("opt".to_string(), "nope".to_string()), "1".to_string());
        let uuid = uuid::Uuid::now_v7();
        let err = assemble(
            &G16OptParse,
            &raw,
            &BTreeMap::new(),
            &uuid,
            "2026-05-18T00:00:00Z",
            std::path::Path::new("/r/u"),
        )
        .unwrap_err();
        assert!(matches!(err, RecipeError::UnknownParam { .. }));
    }
}
```

- [ ] **Step 4: `src/recipes/mod.rs` に登録**

```rust
pub mod flow;
pub mod flows;
pub mod job;
pub mod jobs;
pub mod xyz;

pub use flow::{assemble, Assembled};
pub use flows::G16OptParse;
```

- [ ] **Step 5: テスト実行**

Run: `cargo test --all-features recipes::flow`
Expected: PASS(6 tests)。`toml::from_str::<JobFlow>` が失敗する場合、生成 `body` の三重引用符・改行・`[jobs.*.config]` の TOML 妥当性を修正(Task 9 で確認した型に厳密に合わせる)。

- [ ] **Step 6: no-default-features + clippy + Commit**

Run: `cargo build --bin jm --no-default-features` → PASS
Run: `cargo clippy --all-targets --all-features -- -D warnings` → PASS

```bash
git add src/recipes/flow.rs src/recipes/flows/ src/recipes/mod.rs
git commit -m "feat(recipes): FlowRecipe g16-opt-parse + assemble() (flow/plan/sidecars)"
```

---

## Task 11: registry / `--param` パース / `--list` / `--describe`

**Files:**
- Modify: `src/recipes/mod.rs`(`recipe_registry`/`find_flow`/`parse_param_arg`/`render_list`/`render_describe` + tests)

- [ ] **Step 1: `src/recipes/mod.rs` 末尾に実装 + テストを追記**

```rust
use std::collections::BTreeMap;

use crate::recipes::flows::G16OptParse;

/// scaffold 可能な real レシピ(`blank` は legacy バイト同値経路で
/// cmd_new が特別扱いするため registry には含めない。`--list` には別途出す)。
pub fn recipe_registry() -> Vec<Box<dyn FlowRecipe>> {
    vec![Box::new(G16OptParse)]
}

pub fn find_flow(name: &str) -> Result<Box<dyn FlowRecipe>, RecipeError> {
    match name {
        "g16-opt-parse" => Ok(Box::new(G16OptParse)),
        other => Err(RecipeError::UnknownFlow(
            other.to_string(),
            "blank, g16-opt-parse".to_string(),
        )),
    }
}

/// `--param <JobId>.<param>=<value>` を ((job,param) -> value) に。
pub fn parse_param_arg(
    raw: &str,
    out: &mut BTreeMap<(String, String), String>,
) -> Result<(), RecipeError> {
    let (lhs, value) = raw
        .split_once('=')
        .ok_or_else(|| RecipeError::BadParamSyntax(raw.to_string()))?;
    let (job, param) = lhs
        .split_once('.')
        .ok_or_else(|| RecipeError::BadParamSyntax(raw.to_string()))?;
    if job.is_empty() || param.is_empty() {
        return Err(RecipeError::BadParamSyntax(raw.to_string()));
    }
    out.insert((job.to_string(), param.to_string()), value.to_string());
    Ok(())
}

/// `jm new --list`。
pub fn render_list() -> String {
    let mut s = String::from("available flow recipes:\n");
    s.push_str("  blank          legacy 2-job echo DAG (byte-identical to `jm new`)\n");
    for r in recipe_registry() {
        s.push_str(&format!("  {:<14} {}\n", r.name(), r.summary()));
    }
    s
}

/// `jm new <recipe> --describe`。
pub fn render_describe(name: &str) -> Result<String, RecipeError> {
    if name == "blank" {
        return Ok("blank: legacy 2-job step1->step2 echo DAG. No params. \
                   Output is byte-identical to bare `jm new`.\n"
            .to_string());
    }
    let r = find_flow(name)?;
    let mut s = format!("{} — {}\n", r.name(), r.summary());
    s.push_str("nodes:\n");
    for (jid, t) in r.nodes() {
        s.push_str(&format!("  {jid} ({t})\n"));
    }
    s.push_str("edges:\n");
    for (f, t, k) in r.edges() {
        s.push_str(&format!("  {f} -> {t} [{k}]\n"));
    }
    s.push_str("params (--param <JobId>.<name>=<value>):\n");
    for (jid, tname) in r.nodes() {
        let tmpl = crate::recipes::flow::job_template(tname)?;
        for rp in tmpl.params() {
            let ty = format!("{:?}", rp.ty);
            s.push_str(&format!(
                "  {jid}.{name:<16} {ty:<6} default={default:?}  {help}\n",
                name = rp.name,
                ty = ty,
                default = rp.default,
                help = rp.help
            ));
        }
    }
    Ok(s)
}

#[cfg(test)]
mod registry_tests {
    use super::*;

    #[test]
    fn parse_param_splits_job_param_value() {
        let mut m = BTreeMap::new();
        parse_param_arg("opt.charge=1", &mut m).unwrap();
        assert_eq!(m.get(&("opt".into(), "charge".into())).unwrap(), "1");
    }

    #[test]
    fn parse_param_keeps_later_equals_in_value() {
        let mut m = BTreeMap::new();
        parse_param_arg("opt.route=#p opt=tight b3lyp", &mut m).unwrap();
        assert_eq!(
            m.get(&("opt".into(), "route".into())).unwrap(),
            "#p opt=tight b3lyp"
        );
    }

    #[test]
    fn parse_param_rejects_missing_dot_or_equals() {
        let mut m = BTreeMap::new();
        assert!(parse_param_arg("optcharge=1", &mut m).is_err());
        assert!(parse_param_arg("opt.charge", &mut m).is_err());
    }

    #[test]
    fn find_flow_unknown_lists_candidates() {
        let err = find_flow("nope").unwrap_err();
        assert!(err.to_string().contains("blank, g16-opt-parse"));
    }

    #[test]
    fn list_includes_blank_and_g16_opt_parse() {
        let l = render_list();
        assert!(l.contains("blank"));
        assert!(l.contains("g16-opt-parse"));
    }

    #[test]
    fn describe_g16_opt_parse_lists_params() {
        let d = render_describe("g16-opt-parse").unwrap();
        assert!(d.contains("opt.route"));
        assert!(d.contains("parse.conda_env"));
        assert!(d.contains("opt -> parse [afterok]"));
    }

    #[test]
    fn describe_blank_has_no_params() {
        let d = render_describe("blank").unwrap();
        assert!(d.contains("No params"));
    }
}
```

- [ ] **Step 2: テスト・clippy**

Run: `cargo test --all-features recipes::registry_tests`
Expected: PASS(7 tests)。
Run: `cargo clippy --all-targets --all-features -- -D warnings` → PASS

- [ ] **Step 3: no-default-features + Commit**

Run: `cargo build --bin jm --no-default-features` → PASS

```bash
git add src/recipes/mod.rs
git commit -m "feat(recipes): registry, --param parser, --list/--describe"
```

---

## Task 12: `blank` legacy 移設(バイト同値)

**Files:**
- Modify: `src/recipes/flows/blank.rs`(placeholder を実装で全置換)
- Modify: `src/recipes/mod.rs`(`pub use flows::blank;`)
- Test: `src/recipes/flows/blank.rs` の `#[cfg(test)] mod tests`

> jm.rs からの削除は Task 13 で行う(本タスクは移設先を作るのみ。両所に一時的に同じ関数が存在しても OK — Task 13 で旧側を消す)。

- [ ] **Step 1: `src/recipes/flows/blank.rs` に `src/bin/jm.rs:476-574` の本文をそのまま移設**

```rust
//! `blank` FlowRecipe — legacy 2-job echo DAG。**既存 `jm new` 出力と
//! バイト同値**(非分解・サイドカー/プリアンブル/R3' 焼込定数 無し)。`jm new` ≡
//! `jm new blank`。

use std::collections::BTreeMap;

pub fn parse_tag(raw: &str) -> anyhow::Result<(String, String)> {
    match raw.split_once('=') {
        Some(("", _)) => anyhow::bail!("invalid --tag: empty key in {raw:?}"),
        Some((k, _))
            if !k
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') =>
        {
            anyhow::bail!(
                "invalid --tag: key {k:?} has non-bare-key characters (only A-Za-z0-9_- allowed)"
            )
        }
        Some((k, v)) => Ok((k.to_string(), v.to_string())),
        None => anyhow::bail!("invalid --tag: expected key=value, got {raw:?}"),
    }
}

pub fn build_plan_template() -> String {
    "\
# Generated by `jm new`. Per-JobId params surface in batch.bash as
# `JM_PARAM_<UPPER_NAME>`.
# Schema: job_manager::plan::ExperimentPlan (deny_unknown_fields)

[jobs.step1]
note = \"TODO: replace with real render params\"

[jobs.step2]
note = \"TODO: replace with real render params\"
"
    .to_string()
}

pub fn build_flow_template(
    uuid: &uuid::Uuid,
    created_at: &str,
    tags: &BTreeMap<String, String>,
) -> String {
    let mut tag_lines = String::new();
    if tags.is_empty() {
        tag_lines.push_str("# free-form key=value tags; populate via `jm new --tag k=v`\n");
    } else {
        for (k, v) in tags {
            let v_toml = toml::Value::String(v.clone()).to_string();
            tag_lines.push_str(&format!("{k} = {v_toml}\n"));
        }
    }
    format!(
        "\
# Generated by `jm new` on {created_at}.
# Schema: gaussian_job_shared::entities::workflow::JobFlow (deny_unknown_fields)
#   uuid          UUID v7 — MUST equal the parent directory name
#   created_at    RFC3339 UTC
#   jobs.<JobId>  JobSpec (program/body/config) + parents[]

uuid       = \"{uuid}\"
created_at = \"{created_at}\"

[tags]
{tag_lines}
# --- step 1: replace `program` / `body` with the real workload ---
[jobs.step1]
program = \"echo\"
body    = \"echo \\\"[step1] flow=$JM_FLOW_UUID job=$JM_JOB_ID\\\"\\n\"

[jobs.step1.config]
# `jm new` does NOT create common.toml, so `partition` is written here
# explicitly. REPLACE_ME makes `jm render` succeed but real `jm submit`
# fail fast with \"invalid partition: REPLACE_ME\" until you set a real
# partition (sinfo -s). Alternatively create <root>/common.toml with a
# [slurm_default] partition and delete this line to inherit it.
partition = \"REPLACE_ME\"

# --- step 2: runs only if step1 exits 0 ---
[jobs.step2]
program = \"echo\"
body    = \"echo \\\"[step2] flow=$JM_FLOW_UUID job=$JM_JOB_ID\\\"\\n\"

[[jobs.step2.parents]]
from = \"step1\"
kind = \"afterok\"

[jobs.step2.config]
partition = \"REPLACE_ME\"
"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tag_splits_on_first_equals() {
        assert_eq!(parse_tag("a=b").unwrap(), ("a".into(), "b".into()));
    }
    #[test]
    fn parse_tag_keeps_later_equals_in_value() {
        assert_eq!(parse_tag("a=b=c").unwrap(), ("a".into(), "b=c".into()));
    }
    #[test]
    fn parse_tag_rejects_missing_equals() {
        assert!(parse_tag("abc")
            .unwrap_err()
            .to_string()
            .contains("expected key=value"));
    }
    #[test]
    fn parse_tag_rejects_empty_key() {
        assert!(parse_tag("=v").unwrap_err().to_string().contains("empty key"));
    }
    #[test]
    fn parse_tag_rejects_non_bare_key() {
        for bad in ["my key=v", "my.key=v", "k!=v"] {
            assert!(parse_tag(bad)
                .unwrap_err()
                .to_string()
                .contains("non-bare-key"));
        }
    }
    #[test]
    fn blank_flow_template_parses_as_jobflow() {
        // Task 9 で確定した JobFlow 実型に置換すること。
        let u = uuid::Uuid::now_v7();
        let f = build_flow_template(&u, "2026-05-18T00:00:00Z", &BTreeMap::new());
        let _: gaussian_job_shared::entities::workflow::JobFlow =
            toml::from_str(&f).expect("blank flow.toml must parse");
        assert!(f.contains("[jobs.step1]") && f.contains("[jobs.step2]"));
        assert_eq!(f.matches("partition = \"REPLACE_ME\"").count(), 2);
    }
}
```

- [ ] **Step 2: `src/recipes/mod.rs` に re-export 追加**

```rust
pub use flows::{blank, G16OptParse};
```

- [ ] **Step 3: テスト**

Run: `cargo test --all-features recipes::flows::blank`
Expected: PASS(6 tests)。`cargo build --bin jm --no-default-features` → PASS。

- [ ] **Step 4: Commit**

```bash
git add src/recipes/flows/blank.rs src/recipes/mod.rs
git commit -m "refactor(recipes): move legacy blank templates into flows/blank.rs"
```

---

## Task 13: `jm.rs` の `Cmd::New` 拡張 + `cmd_new` 改修 + legacy 削除

**Files:**
- Modify: `src/bin/jm.rs`(`Cmd::New` 46-53、`main` match 90-96、`cmd_new` 420-469、legacy 関数 471-574 + その tests 削除)
- Test: 統合は Task 15(本タスクは後方互換スモークで確認)

- [ ] **Step 1: `Cmd::New` を拡張(`src/bin/jm.rs:46-53` を置換)**

```rust
    /// Scaffold a new flow from a recipe (default `blank` = legacy 2-job
    /// echo DAG). `jm new g16-opt-parse --param opt.charge=1`.
    New {
        /// Flow recipe name. Omitted = `blank`.
        recipe: Option<String>,
        /// Repeatable `<JobId>.<param>=<value>`.
        #[arg(long = "param", value_name = "JOBID.PARAM=VALUE")]
        params: Vec<String>,
        /// Repeatable. KEY=VALUE pairs written into flow.toml [tags].
        #[arg(long = "tag", value_name = "KEY=VALUE")]
        tags: Vec<String>,
        /// Print only the created `<root>/<uuid>` path to stdout.
        #[arg(long)]
        print_path: bool,
        /// List available recipes and exit.
        #[arg(long)]
        list: bool,
        /// Describe the given recipe and exit.
        #[arg(long)]
        describe: bool,
    },
```

- [ ] **Step 2: `main` match arm を置換(`src/bin/jm.rs:90-96`)**

```rust
        Cmd::New {
            ref recipe,
            ref params,
            ref tags,
            print_path,
            list,
            describe,
        } => {
            if list {
                print!("{}", job_manager::recipes::render_list());
                return Ok(());
            }
            if describe {
                let name = recipe.as_deref().unwrap_or("blank");
                print!("{}", job_manager::recipes::render_describe(name)?);
                return Ok(());
            }
            let root = resolve_root(&cli)?;
            cmd_new(&root, recipe.as_deref(), params, tags, print_path).await
        }
```

- [ ] **Step 3: `cmd_new` を全置換(`src/bin/jm.rs:420-469`)**

```rust
async fn cmd_new(
    root: &std::path::Path,
    recipe: Option<&str>,
    params: &[String],
    tags: &[String],
    print_path: bool,
) -> anyhow::Result<()> {
    use job_manager::persistence::PathResolver;
    use job_manager::recipes::flows::blank;

    let recipe_name = recipe.unwrap_or("blank");

    let mut tag_map = BTreeMap::new();
    for raw in tags {
        let (k, v) = blank::parse_tag(raw)?;
        tag_map.insert(k, v);
    }

    let uuid = uuid::Uuid::now_v7();
    let resolver = PathResolver::new(root);
    let flow_dir = resolver.flow_dir(&uuid);
    if flow_dir.exists() {
        anyhow::bail!("flow dir already exists: {}", flow_dir.display());
    }
    tokio::fs::create_dir_all(&flow_dir).await?;
    // R3' invariant: the JOB_DIR baked into run.py/parse.py MUST be absolute,
    // otherwise it would be re-resolved against SLURM's nondeterministic cwd
    // at runtime — exactly the failure R3' eliminates. `--root` may be
    // relative, so absolutize here (no symlink resolution: keeps login↔compute
    // mounts stable per spec §5.1). std::path::absolute is stable on the
    // pinned nightly/edition-2024 toolchain.
    let flow_dir_abs =
        std::path::absolute(&flow_dir).unwrap_or_else(|_| flow_dir.clone());
    let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let rollback = || {
        let _ = std::fs::remove_dir_all(&flow_dir);
    };

    if recipe_name == "blank" {
        let flow_str = blank::build_flow_template(&uuid, &created_at, &tag_map);
        let plan_str = blank::build_plan_template();
        let write_both = || -> std::io::Result<()> {
            atomic_write_str(&resolver.flow_toml(&uuid), &flow_str)?;
            atomic_write_str(&resolver.plan_toml(&uuid), &plan_str)?;
            Ok(())
        };
        if let Err(e) = write_both() {
            rollback();
            return Err(anyhow::Error::new(e).context(format!(
                "failed to write boilerplate under {}",
                flow_dir.display()
            )));
        }
    } else {
        let flow_recipe = match job_manager::recipes::find_flow(recipe_name) {
            Ok(r) => r,
            Err(e) => {
                rollback();
                return Err(anyhow::anyhow!(e));
            }
        };
        let mut raw_params = BTreeMap::new();
        for p in params {
            if let Err(e) = job_manager::recipes::parse_param_arg(p, &mut raw_params) {
                rollback();
                return Err(anyhow::anyhow!(e));
            }
        }
        let assembled = match job_manager::recipes::assemble(
            flow_recipe.as_ref(),
            &raw_params,
            &tag_map,
            &uuid,
            &created_at,
            &flow_dir_abs, // R3': absolute -> baked JOB_DIR is cwd-independent
        ) {
            Ok(a) => a,
            Err(e) => {
                rollback();
                return Err(anyhow::anyhow!(e));
            }
        };

        let do_writes = || -> std::io::Result<()> {
            atomic_write_str(&resolver.flow_toml(&uuid), &assembled.flow_toml)?;
            atomic_write_str(&resolver.plan_toml(&uuid), &assembled.plan_toml)?;
            for f in &assembled.sidecars {
                let dst = flow_dir.join(&f.relpath);
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                atomic_write_str(&dst, &f.contents)?;
                #[cfg(unix)]
                if let Some(mode) = f.unix_mode {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(mode))?;
                }
            }
            if let Some((job_id, src)) = &assembled.input_coordinate {
                if !src.exists() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("input_coordinate not found: {}", src.display()),
                    ));
                }
                let base = src.file_name().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "input_coordinate has no file name",
                    )
                })?;
                let dst_dir = flow_dir.join(job_id).join("input");
                std::fs::create_dir_all(&dst_dir)?;
                std::fs::copy(src, dst_dir.join(base))?;
            }
            Ok(())
        };
        if let Err(e) = do_writes() {
            rollback();
            return Err(anyhow::Error::new(e).context(format!(
                "failed to scaffold recipe {recipe_name} under {}",
                flow_dir.display()
            )));
        }
    }

    if print_path {
        println!("{}", flow_dir.display());
    } else {
        println!("created flow {uuid} (recipe: {recipe_name})");
        println!("  {}", resolver.flow_toml(&uuid).display());
        println!("  {}", resolver.plan_toml(&uuid).display());
        println!(
            "next: edit flow.toml/plan.toml, then `jm --root {} render {uuid}`",
            root.display()
        );
    }
    Ok(())
}
```

- [ ] **Step 4: legacy 関数 + tests を `jm.rs` から削除**

`src/bin/jm.rs` から `parse_tag`(471-493)・`build_plan_template`(495-510)・`build_flow_template`(512-574)を削除。`#[cfg(test)] mod tests` 内の `parse_tag_*` 5 テストも削除(`flows/blank.rs` 移設済)。`atomic_write_str` とその test は **残す**(両経路で使用)。`use std::collections::{BTreeMap, BTreeSet};` は `BTreeMap` を cmd_new で使うので残す(`BTreeSet` 不使用なら clippy 指摘に従い整理)。

- [ ] **Step 5: ビルド + 全テスト回帰**

Run: `cargo build --bin jm --no-default-features` → PASS
Run: `cargo test --all-features` → PASS(削除テスト分は flows/blank.rs 側に移動済で差し引きゼロ)

- [ ] **Step 6: 後方互換スモーク**

Run:
```bash
ROOT=$(mktemp -d)
cargo run --quiet --bin jm --no-default-features -- --root "$ROOT" new --print-path
cat "$ROOT"/*/flow.toml
```
Expected: `<ROOT>/<uuid>` 1行 + 旧 blank 内容(step1/step2 echo、`partition = "REPLACE_ME"` ×2)。

- [ ] **Step 7: clippy + Commit**

Run: `cargo clippy --all-targets --all-features -- -D warnings` → PASS

```bash
git add src/bin/jm.rs
git commit -m "feat(jm): jm new <recipe> dispatch (blank legacy + recipes::assemble)"
```

---

## Task 14: Python smoke — run.py / parse.py アルゴリズム回帰

**Files:**
- Create: `python/tests/test_recipe_run_py.py`
- Create: `python/tests/test_recipe_parse_py.py`
- Create: `python/tests/_recipe_fixtures/g16_ok.out`

- [ ] **Step 1: `python/tests/test_recipe_run_py.py`**

```python
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUN_TMPL = REPO / "src/recipes/assets/g16_opt/run.py.tmpl"


def _materialize(tmp: Path) -> Path:
    job = tmp / "job"
    (job / "input").mkdir(parents=True)
    (job / "input" / "main.gjf").write_text(
        "%nprocshared=8\n%mem=8GB\n%chk=main.chk\n#p opt\n\nt\n\n0 1\nH 0 0 0\n\n"
    )
    scripts = job / "scripts"
    scripts.mkdir()
    # R3': scaffold bakes the absolute job dir; the smoke harness does the
    # same {{JOB_DIR}} swap-in here so run.py is cwd-independent under test.
    (scripts / "run.py").write_text(
        RUN_TMPL.read_text().replace("{{JOB_DIR}}", str(job))
    )
    return job


def _stub_bin(tmp: Path, name: str, script: str) -> None:
    b = tmp / "bin"
    b.mkdir(exist_ok=True)
    p = b / name
    p.write_text("#!/bin/bash\n" + script)
    p.chmod(0o755)


def _run(job: Path, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "scripts/run.py"],
        cwd=job,
        env=env,
        capture_output=True,
        text=True,
    )


def base_env(tmp: Path) -> dict:
    e = dict(os.environ)
    e["PATH"] = f"{tmp / 'bin'}:{e['PATH']}"
    e["JM_FLOW_UUID"] = "flowu"
    e["JM_JOB_ID"] = "opt"
    e["JM_PARAM_SCRATCH_ROOT"] = str(tmp / "scratch")
    e["JM_PARAM_LAUNCHER"] = ""
    e["JM_PARAM_G16_CMD"] = "g16"
    return e


def test_success_order_prepare_run_copy(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(tmp_path, "g16", 'echo "ok" > main.out\nexit 0\n')
    cp = _run(job, base_env(tmp_path))
    assert cp.returncode == 0, cp.stderr
    assert (job / "output" / "main.out").read_text().strip() == "ok"


def test_g16_nonzero_propagates_and_still_copies(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(tmp_path, "g16", 'echo "partial" > main.out\nexit 7\n')
    cp = _run(job, base_env(tmp_path))
    assert cp.returncode == 7  # g16 rc has top precedence
    assert (job / "output" / "main.out").read_text().strip() == "partial"


def test_missing_g16_does_not_exit_zero(tmp_path):
    job = _materialize(tmp_path)
    env = base_env(tmp_path)
    env["JM_PARAM_G16_CMD"] = "definitely-not-on-path-xyz"
    cp = _run(job, env)
    assert cp.returncode != 0
    assert "failed to launch" in cp.stderr


def test_launcher_prefixes_argv(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(
        tmp_path,
        "srun",
        'echo "$@" > "$SRUN_ARGS_FILE"\nshift_cmd="${@: -3}"\nexit 0\n',
    )
    _stub_bin(tmp_path, "g16", "exit 0\n")
    env = base_env(tmp_path)
    env["JM_PARAM_LAUNCHER"] = "srun"
    env["SRUN_ARGS_FILE"] = str(tmp_path / "srun_args.txt")
    cp = _run(job, env)
    assert cp.returncode == 0, cp.stderr
    assert "g16 main.gjf main.out" in (tmp_path / "srun_args.txt").read_text()


def test_scratch_root_empty_falls_back_to_dot_scratch(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(tmp_path, "g16", "echo ok > main.out\nexit 0\n")
    env = base_env(tmp_path)
    env["JM_PARAM_SCRATCH_ROOT"] = ""
    cp = _run(job, env)
    assert cp.returncode == 0, cp.stderr
    assert (job / ".scratch" / "flowu" / "opt" / "main.out").exists()
```

- [ ] **Step 2: `python/tests/test_recipe_parse_py.py`**

```python
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PARSE_TMPL = REPO / "src/recipes/assets/parse_g16_out/parse.py.tmpl"
FIX = Path(__file__).resolve().parent / "_recipe_fixtures" / "g16_ok.out"


def _materialize(tmp: Path, input_rel: str) -> Path:
    job = tmp / "parse"
    (job / "scripts").mkdir(parents=True)
    # R3': bake absolute JOB_DIR (mirrors scaffold). An absolute input_rel
    # (e.g. the fixture) wins over JOB_DIR via os.path.join semantics.
    body = (
        PARSE_TMPL.read_text()
        .replace("{{JOB_DIR}}", str(job))
        .replace("{{INPUT_REL}}", input_rel)
    )
    (job / "scripts" / "parse.py").write_text(body)
    return job


def _run(job: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "scripts/parse.py"],
        cwd=job,
        capture_output=True,
        text=True,
    )


def test_cclib_missing_exits_2(tmp_path):
    job = _materialize(tmp_path, "missing.out")
    body = (job / "scripts" / "parse.py").read_text().replace(
        "import cclib  # noqa: F401", "raise ImportError('forced')"
    )
    (job / "scripts" / "parse.py").write_text(body)
    cp = _run(job)
    assert cp.returncode == 2
    assert "cclib not importable" in cp.stderr


def test_valid_out_writes_result_json(tmp_path):
    pytest.importorskip("cclib")
    job = _materialize(tmp_path, str(FIX))
    cp = _run(job)
    assert cp.returncode == 0, cp.stderr
    res = json.loads((job / "output" / "result.json").read_text())
    assert res["schema"] == "jm-recipe/1"
    assert res["converged"] is True
    assert res["n_atoms"] >= 1
    assert isinstance(res["scf_energy"], float)


def test_missing_input_exits_1(tmp_path):
    pytest.importorskip("cclib")
    job = _materialize(tmp_path, "nope.out")
    cp = _run(job)
    assert cp.returncode == 1
```

- [ ] **Step 3: cclib パース可能な最小 `.out` フィクスチャを用意**

```bash
mkdir -p python/tests/_recipe_fixtures
REF=$(cat /tmp/gaussian_ref_path.txt 2>/dev/null || true)
if [ -n "$REF" ] && [ -f "$REF/examples/replica/ROSDSFDQCJNGOL-UHFFFAOYSA-O/main.out" ]; then
  cp "$REF/examples/replica/ROSDSFDQCJNGOL-UHFFFAOYSA-O/main.out" python/tests/_recipe_fixtures/g16_ok.out
else
  D=$(mktemp -d); gh repo clone miyake-ken/GAUSSIAN_repo "$D" -- --depth 1
  cp "$D/examples/replica/ROSDSFDQCJNGOL-UHFFFAOYSA-O/main.out" python/tests/_recipe_fixtures/g16_ok.out
fi
uv run python -c "from cclib.io import ccread; d=ccread('python/tests/_recipe_fixtures/g16_ok.out'); print(d.metadata.get('success'), getattr(d,'optdone',None), len(d.scfenergies))"
```
Expected: 末尾 `python -c` が `True ... <非空 scfenergies>` を出す(success=True、optdone truthy、scfenergies>0)。違えば別の収束済み `.out` を選ぶ。テキストファイルなので LFS 不要。

- [ ] **Step 4: テスト実行**

Run: `uv run pytest python/tests/test_recipe_run_py.py python/tests/test_recipe_parse_py.py -v`
Expected: run_py 5 PASS。parse_py は cclib 有なら 3 PASS、無なら `cclib_missing` 1 PASS + 2 skip。

- [ ] **Step 5: Commit**

```bash
git add python/tests/test_recipe_run_py.py python/tests/test_recipe_parse_py.py python/tests/_recipe_fixtures/g16_ok.out
git commit -m "test(recipes): python smoke for generated run.py/parse.py algorithms"
```

---

## Task 15: 統合テスト `tests/integration_new_recipes.rs`

**Files:**
- Create: `tests/integration_new_recipes.rs`
- Modify: `Cargo.toml`(`[dev-dependencies]` に `assert_cmd`/`predicates`/`tempfile` が無ければ追加)

- [ ] **Step 1: dev-deps 確認**

Run: `grep -nE 'assert_cmd|predicates|tempfile|walkdir' Cargo.toml`
Expected: `[dev-dependencies]` に `assert_cmd`/`predicates`/`tempfile`。無いものを追加(`assert_cmd = "2"`, `predicates = "3"`, `tempfile = "3"`)。`walkdir` は使わず自前再帰関数を使う(下記)。

- [ ] **Step 2: `tests/integration_new_recipes.rs` を作成**

```rust
//! `jm new <recipe>` の end-to-end。MockExecutor 不要(render まで)。
//! live SLURM 不要。

use assert_cmd::Command;
use std::fs;
use std::path::{Path, PathBuf};

fn jm() -> Command {
    let mut c = Command::cargo_bin("jm").unwrap();
    c.arg("--root");
    c
}

fn walk(root: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = fs::read_dir(root) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push(p);
            }
        }
    }
}

fn scaffold_print_path(root: &tempfile::TempDir, extra: &[&str]) -> PathBuf {
    let mut c = jm();
    c.arg(root.path()).arg("new").args(extra).arg("--print-path");
    let out = c.assert().success();
    let p = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    PathBuf::from(p.trim().to_string())
}

fn normalize(s: &str) -> String {
    s.lines()
        .filter(|l| {
            !l.starts_with("uuid")
                && !l.starts_with("created_at")
                && !l.starts_with("# Generated by")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn list_and_describe_do_not_scaffold() {
    let root = tempfile::tempdir().unwrap();
    jm().arg(root.path())
        .args(["new", "--list"])
        .assert()
        .success()
        .stdout(predicates::str::contains("g16-opt-parse"))
        .stdout(predicates::str::contains("blank"));
    jm().arg(root.path())
        .args(["new", "g16-opt-parse", "--describe"])
        .assert()
        .success()
        .stdout(predicates::str::contains("opt -> parse [afterok]"));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn scaffold_g16_opt_parse_writes_all_files() {
    let root = tempfile::tempdir().unwrap();
    let dir = scaffold_print_path(&root, &["g16-opt-parse", "--param", "opt.charge=1"]);

    let flow = fs::read_to_string(dir.join("flow.toml")).unwrap();
    assert!(flow.contains("[jobs.opt]") && flow.contains("[jobs.parse]"));
    // R3': flow.toml body has NO cd; absolute job dir is baked into run.py.
    assert!(!flow.contains("cd "), "R3': flow.toml must not cd; got:\n{flow}");
    assert!(flow.contains("bash scripts/opt.bash"));

    let gjf = fs::read_to_string(dir.join("opt/input/main.gjf")).unwrap();
    assert!(gjf.contains("1 1"));
    assert!(!gjf.contains("%rwf"));

    let optbash = fs::read_to_string(dir.join("opt/scripts/opt.bash")).unwrap();
    assert!(optbash.contains("unset -f conda"));
    assert!(optbash.contains("module restore gaussian_A -f"));
    assert!(optbash.contains("python scripts/run.py"));
    assert!(!optbash.contains("srun"));

    assert!(dir.join("opt/scripts/run.py").exists());
    assert!(dir.join("parse/scripts/parse.py").exists());

    // R3': the absolute job dir is baked into run.py/parse.py, sentinel
    // swapped, and os.getcwd() never used (cwd-independent like run-g16).
    let runpy = fs::read_to_string(dir.join("opt/scripts/run.py")).unwrap();
    assert!(runpy.contains(&format!(
        "JOB_DIR = \"{}/opt\"",
        dir.display()
    )));
    assert!(!runpy.contains("{{JOB_DIR}}"));
    assert!(!runpy.contains("os.getcwd()"));
    let parsepy = fs::read_to_string(dir.join("parse/scripts/parse.py")).unwrap();
    assert!(parsepy.contains(&format!(
        "JOB_DIR = \"{}/parse\"",
        dir.display()
    )));
    assert!(!parsepy.contains("{{JOB_DIR}}"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let m = fs::metadata(dir.join("opt/scripts/run.py")).unwrap();
        assert_eq!(m.permissions().mode() & 0o777, 0o755);
    }
}

#[test]
fn r3prime_job_dir_is_absolute_even_with_relative_root() {
    // R3' invariant regression: a relative `--root` must still bake an
    // ABSOLUTE JOB_DIR (otherwise it re-resolves against SLURM's cwd).
    let root = tempfile::tempdir().unwrap();
    let mut c = jm();
    c.current_dir(root.path()) // cwd = tempdir; pass `--root .` (relative)
        .arg(".")
        .arg("new")
        .arg("g16-opt-parse")
        .arg("--print-path");
    let out = c.assert().success();
    let printed = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let dir = PathBuf::from(printed.trim());

    let runpy_path = if dir.is_absolute() {
        dir.join("opt/scripts/run.py")
    } else {
        root.path().join(&dir).join("opt/scripts/run.py")
    };
    let runpy = fs::read_to_string(&runpy_path).unwrap();
    // The JOB_DIR literal must start with the absolute tempdir prefix and
    // must not be the relative "./..." form.
    let job_dir_line = runpy
        .lines()
        .find(|l| l.starts_with("JOB_DIR = "))
        .expect("run.py must define JOB_DIR");
    let baked = job_dir_line
        .trim_start_matches("JOB_DIR = \"")
        .trim_end_matches('"');
    assert!(
        Path::new(baked).is_absolute(),
        "R3': baked JOB_DIR must be absolute, got {baked:?}"
    );
    assert!(baked.ends_with("/opt"), "got {baked:?}");
    assert!(!baked.starts_with("./") && !baked.starts_with("."), "got {baked:?}");
}

#[test]
fn input_coordinate_xyz_is_copied_and_injected() {
    let root = tempfile::tempdir().unwrap();
    let xyz = root.path().join("mol.xyz");
    fs::write(&xyz, "1\nc\nO 0.0 0.0 0.0\n").unwrap();
    let dir = scaffold_print_path(
        &root,
        &[
            "g16-opt-parse",
            "--param",
            &format!("opt.input_coordinate={}", xyz.display()),
        ],
    );
    assert!(dir.join("opt/input/mol.xyz").exists());
    let gjf = fs::read_to_string(dir.join("opt/input/main.gjf")).unwrap();
    assert!(gjf.contains("O 0.000000 0.000000 0.000000"));
}

#[test]
fn missing_input_coordinate_fails_and_rolls_back() {
    let root = tempfile::tempdir().unwrap();
    jm().arg(root.path())
        .args([
            "new",
            "g16-opt-parse",
            "--param",
            "opt.input_coordinate=/no/such/file.xyz",
        ])
        .assert()
        .failure();
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[test]
fn scaffold_then_render_succeeds_and_bakes_jm_params() {
    let root = tempfile::tempdir().unwrap();
    let dir = scaffold_print_path(&root, &["g16-opt-parse"]);
    let uuid = dir.file_name().unwrap().to_string_lossy().into_owned();

    jm().arg(root.path()).args(["doctor", &uuid]).assert().success();
    jm().arg(root.path()).args(["render", &uuid]).assert().success();

    let mut files = Vec::new();
    walk(&dir, &mut files);
    let baked = files.iter().any(|p| {
        p.ends_with("batch.bash")
            && fs::read_to_string(p)
                .map(|s| s.contains("export JM_PARAM_LAUNCHER='srun'"))
                .unwrap_or(false)
    });
    assert!(baked, "render must bake export JM_PARAM_LAUNCHER='srun'");
}

#[test]
fn bare_jm_new_is_byte_identical_to_blank() {
    let ra = tempfile::tempdir().unwrap();
    let rb = tempfile::tempdir().unwrap();
    let pa = scaffold_print_path(&ra, &[]);
    let pb = scaffold_print_path(&rb, &["blank"]);
    let fa = normalize(&fs::read_to_string(pa.join("flow.toml")).unwrap());
    let fb = normalize(&fs::read_to_string(pb.join("flow.toml")).unwrap());
    assert_eq!(fa, fb);
}
```

- [ ] **Step 3: テスト実行**

Run: `cargo test --test integration_new_recipes`
Expected: PASS(6 tests)。`scaffold_then_render...` 失敗時は、`jm render` が common.toml 不在・`partition=REPLACE_ME` でも通る(既存 blank と同仕様)ことを確認し、`.jm/<JobId>/batch.bash` の実パスを `grep -rn "batch.bash\|fn render_batch_bash\|atomic_write_batch_bash" src/render src/runner` で確認して `walk` 探索を合わせる。

- [ ] **Step 4: Commit**

```bash
git add tests/integration_new_recipes.rs Cargo.toml
git commit -m "test(recipes): integration — jm new g16-opt-parse e2e + blank parity"
```

---

## Task 16: ドキュメント追記 + フル CI ゲート + PR

**Files:**
- Modify: `README.md`、`docs/toml-reference.md`、`CLAUDE.md`

- [ ] **Step 1: README に `jm new <recipe>` 節を追記**

`README.md` の `jm` CLI 説明付近に追加:

```markdown
### `jm new` — flow scaffolding

`jm --root <ROOT> new` mints a UUID v7 and writes editable boilerplate.

- `jm new` (or `jm new blank`) — legacy 2-job echo DAG (`step1 -> step2`, afterok).
- `jm new g16-opt-parse [--param opt.charge=1] [--param opt.input_coordinate=mol.xyz]`
  — self-contained kudpc g16 optimization → afterok → cclib result.json.
  Generates `flow.toml`/`plan.toml` plus `<job>/scripts/{<job>.bash,run.py|parse.py}`
  and `opt/input/main.gjf`. Cluster knobs (`launcher`, `scratch_root`, `g16_cmd`)
  are plan.toml params surfaced to the scripts as `JM_PARAM_*`; set
  `partition`/`resource_spec` via `[jobs.*.config]` or `<root>/common.toml`.
- `jm new --list` / `jm new <recipe> --describe` — introspection, no scaffold.
```

- [ ] **Step 2: `docs/toml-reference.md` にサイドカー/`JM_PARAM_*` 段落を追記**

`docs/toml-reference.md` 末尾に、生成 `scripts/` レイアウト・`launcher`/`scratch_root`/`g16_cmd` が `JM_PARAM_*` として render される旨・`# REPLACE_ME` swap-in を 1 段落で記述(spec §4.2/§7 要約)。

- [ ] **Step 3: `CLAUDE.md` の `jm` 行を更新(軽微)**

`CLAUDE.md` の `./target/debug/jm --root /work {render|submit ...}` 行に `new` の recipe 形を追記:
`{render|submit [--dry-run]|tick|show|doctor|ls {...}|new [<recipe>] [--param ...]} <flow_uuid>`。

- [ ] **Step 4: フル CI ゲート**

Run:
```bash
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo build --bin jm --no-default-features \
  && cargo test --all-features \
  && uv run pytest python/tests -v
```
Expected: 全 PASS。落ちたら該当タスクに戻り修正、緑になるまで再実行。

- [ ] **Step 5: spec ↔ 実装 最終突合**

`docs/superpowers/specs/2026-05-18-jm-g16-opt-parse-recipe-design.md` §2/§5/§7/§9/§10 を読み返し、`jm new`/`blank`/`g16-opt-parse`/`--param`/`--list`/`--describe`/`--print-path`/`--tag`/`input_coordinate`/**R3'(body cd 無し・run.py/parse.py に絶対 `JOB_DIR`・cwd 非依存)**/`JM_PARAM_*`/`%rwf` 無し/`# REPLACE_ME`/`result.json` スキーマ/`blank` バイト同値 が全てテストで担保されていることを確認(欠落あればタスク追加)。

- [ ] **Step 6: Commit + PR スタック**

```bash
git add README.md docs/toml-reference.md CLAUDE.md
git commit -m "docs: document jm new g16-opt-parse recipe + JM_PARAM_* knobs"

git push -u origin feat/jm-g16-opt-parse-recipe
gh pr create --base docs/jm-g16-opt-parse-altdesign \
  --title "feat: jm new g16-opt-parse recipe (案A v1)" \
  --body "Implements docs/superpowers/specs/2026-05-18-jm-g16-opt-parse-recipe-design.md (案A). v1 job-manager-only; launcher/scratch_root as JM_PARAM_*; D2 deferred to v2. Independent of rev.7 — user merges manually."
```

---

## Self-Review

**1. Spec coverage(spec §ごと → 担当 Task):**
- §2 Goal 1(`jm new [recipe] --param/--tag/--print-path/--list/--describe`)→ Task 11/13/15
- §2 Goal 2(二層 registry)→ Task 2/10/11
- §2 Goal 3(doctor-clean, render 通過)→ Task 15(`doctor`+`render` アサート)
- §2 Goal 4(run.py/parse.py 写経 + `# REPLACE_ME`)→ Task 5/7/14
- §2 Goal 5(v1 内完結 / `JM_PARAM_*` / 上流変更ゼロ)→ Task 6/8/13(render 改変なし)/15
- §2 Goal 6(rollback)→ Task 13 + Task 15 `missing_input...rolls_back`
- §2 Goal 7(**R3'**:body cd 無し・run.py/parse.py に絶対 `JOB_DIR`・cwd 非依存 / シグネチャ不変)→ Task 5/6/7/8/10 + Task 14/15(JOB_DIR 焼込・`!cd`・`!os.getcwd()` アサート)
- §4.1 base_preamble/minijinja/`{% raw %}`→ Task 3
- §5.2 scratch 順序 prepare→g16(cwd=scratch)→finally copy→ Task 14
- §7 g16_opt/parse_g16_out/flow→ Task 6/8/10
- §8 blank バイト同値→ Task 12 + Task 15 `bare_jm_new_is_byte_identical_to_blank`
- §9 エラー表→ Task 6/13/14(各エラー経路にテスト)
- §10 テスト3層→ Task 6-15
- §11 `--no-default-features`/原子書込/0755→ 各 Task の no-default ビルド step + Task 13 set_permissions

ギャップ無し(`derived/main.mol2` は spec で v1 非対象=`# TODO`、Task 8/14 で存在のみ確認)。

**2. Placeholder scan:** 「Placeholder — implemented in Task N」(Task 6 Step1b / Task 10 Step2 blank / Task 12)は段階導入で各々後続タスクで実体化。`{{...}}`/`REPLACE_ME`/`# TODO derived/main.mol2` は生成物の意図的 sentinel。`JobFlow` 実型パス未確定点は **Task 9 を専用調査タスクとして分離し、依存する Task 10/12 のテストに「Task 9 確定値に置換」と明記**(プラン内の未解決を残さない)。

**3. Type consistency:** `JobCtx`/`JobArtifacts`/`GeneratedFile`/`RecipeError`/`FlowRecipe`/`JobTemplate`/`PreambleOpts`(Task 2 定義)を Task 3/6/8/10/13 で同一シグネチャ使用。`assemble()`→`Assembled{flow_toml,plan_toml,sidecars,input_coordinate}`(Task 10)を Task 13 が一致して消費。`base_preamble(&PreambleOpts)`→`String`(Task 3)を Task 6/8 が一致使用。helper 名は各ファイル内 `pv`/`py_escape`(R3':run.py/parse.py の `{{JOB_DIR}}` へ差す Python 文字列リテラル内容エスケープ。g16_opt.rs/parse_g16_out.rs 各々モジュール private に同名定義・衝突なし)/`typed_toml` で衝突なし。`job_template`(Task 10 で `pub`)を Task 11 `render_describe` が使用 — 整合。

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-18-jm-g16-opt-parse-recipe.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — fresh subagent per task, two-stage review between tasks, fast iteration.

**2. Inline Execution** — execute tasks in this session via executing-plans, batch with checkpoints.

**Which approach?**
