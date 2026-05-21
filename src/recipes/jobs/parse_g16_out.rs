//! JobTemplate `parse_g16_out` — 軽量 post。cclib で .out を検証し
//! output/result.json を書く。srun/巨大 scratch 無し → launcher/
//! scratch_root param 不要。

use std::collections::BTreeMap;
use std::path::PathBuf;

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

        // v2 R4: parse.py reads `os.environ["JM_JOB_DIR"]`; only the relative
        // wiring path (INPUT_REL) is swapped at scaffold time. No absolute path
        // baked, so the flow folder stays portable.
        let parse_py = include_str!("../assets/parse_g16_out/parse.py.tmpl")
            .replace("{{INPUT_REL}}", &input_rel);

        // v2 R4: launch parse.py via the render-time `$JM_JOB_DIR` env var
        // (bash-expanded at job runtime). cwd-independent, no absolute path baked.
        let parse_py_invocation = "python \"$JM_JOB_DIR/scripts/parse.py\"".to_string();
        let bash = base_preamble(&PreambleOpts {
            conda_env: pv(ctx, "conda_env"),
            module_block: "module restore default -f",
            body_block: &parse_py_invocation,
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

        // v2 R4: body launches via `$JM_JOB_DIR` (bash-expanded at runtime).
        // The literal is stored in flow.toml — no absolute path baked.
        let body = format!("bash \"$JM_JOB_DIR/scripts/{job_id}.bash\"\n");

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
        // R4: body has NO cd and launches via the render-time $JM_JOB_DIR env
        // var — no absolute path baked.
        assert_eq!(a.body, "bash \"$JM_JOB_DIR/scripts/parse.bash\"\n");
        assert!(!a.body.contains("cd "), "R4: body must not cd");
        assert!(
            !a.body.contains("/r/u/"),
            "R4: body must not bake an absolute flow_dir path"
        );

        let bash = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/parse.bash"))
            .unwrap();
        assert!(bash.contents.contains("module restore default -f"));
        assert!(
            bash.contents
                .contains("python \"$JM_JOB_DIR/scripts/parse.py\"")
        );

        let py = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/parse.py"))
            .unwrap();
        assert_eq!(py.unix_mode, Some(0o755));
        // R4: JOB_DIR read from env, INPUT_REL swapped, no absolute path baked.
        assert!(py.contents.contains("JOB_DIR = os.environ[\"JM_JOB_DIR\"]"));
        assert!(!py.contents.contains("{{JOB_DIR}}"));
        assert!(
            !py.contents.contains("/r/u/"),
            "R4: no absolute flow_dir path baked into parse.py"
        );
        assert!(!py.contents.contains("os.getcwd()"), "R4: cwd-independent");
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
