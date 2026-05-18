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
#[derive(Debug)]
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
#[derive(Debug)]
pub struct PreambleOpts<'a> {
    pub conda_env: &'a str,
    pub module_block: &'a str,
    pub body_block: &'a str,
    pub pixi_manifest: &'a str,
}

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

#[cfg(test)]
mod tests {
    use super::*;

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
