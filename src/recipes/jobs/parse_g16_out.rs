//! JobTemplate `parse_g16_out` — 軽量 post。cclib で .out を検証し
//! output/result.json を書く。srun/巨大 scratch 無し → launcher/
//! scratch_root param 不要。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::recipes::job::{
    GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeError, RecipeParam,
    RecipeParamType, base_preamble,
};

pub struct ParseG16Out;

const PARAMS: &[RecipeParam] = &[
    RecipeParam {
        name: "conda_env",
        ty: RecipeParamType::Str,
        default: "analysis",
        help: "conda activate <env>",
    },
    RecipeParam {
        name: "pixi_manifest",
        ty: RecipeParamType::Path,
        default: "",
        help: "empty = skip pixi hook",
    },
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
            GeneratedFile {
                relpath: nsp(&format!("scripts/{job_id}.bash")),
                contents: bash,
                unix_mode: Some(0o755),
            },
            GeneratedFile {
                relpath: nsp("scripts/parse.py"),
                contents: parse_py,
                unix_mode: Some(0o755),
            },
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
        let a = ParseG16Out
            .instantiate(&ctx(&params, &inputs, &uuid))
            .unwrap();

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
        assert!(
            py.contents
                .contains("TODO(jm recipe): write derived/main.mol2")
        );
        assert!(py.contents.contains("REPLACE_ME"));
    }
}
