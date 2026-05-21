//! JobTemplate `g16_opt` — g16 構造最適化1ステップ。

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::recipes::job::{
    GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeError, RecipeParam,
    RecipeParamType, base_preamble,
};
use crate::recipes::xyz::xyz_to_geometry_block;

pub struct G16Opt;

const PARAMS: &[RecipeParam] = &[
    RecipeParam {
        name: "route",
        ty: RecipeParamType::Str,
        default: "#p opt b3lyp/6-31g(d)",
        help: "Gaussian route line",
    },
    RecipeParam {
        name: "charge",
        ty: RecipeParamType::Int,
        default: "0",
        help: "total charge",
    },
    RecipeParam {
        name: "multiplicity",
        ty: RecipeParamType::Int,
        default: "1",
        help: "spin multiplicity",
    },
    RecipeParam {
        name: "extra_input",
        ty: RecipeParamType::Str,
        default: "",
        help: "input appended after geometry",
    },
    RecipeParam {
        name: "nproc",
        ty: RecipeParamType::Int,
        default: "8",
        help: "scaffold %nprocshared (run.py overrides from SLURM)",
    },
    RecipeParam {
        name: "mem",
        ty: RecipeParamType::Str,
        default: "8GB",
        help: "scaffold %mem (run.py overrides from SLURM)",
    },
    RecipeParam {
        name: "compound",
        ty: RecipeParamType::Str,
        default: "REPLACE_ME-INCHIKEY",
        help: "InChIKey; gjf title + [tags].compound",
    },
    RecipeParam {
        name: "g16_cmd",
        ty: RecipeParamType::Str,
        default: "g16",
        help: "Gaussian binary -> JM_PARAM_G16_CMD",
    },
    RecipeParam {
        name: "conda_env",
        ty: RecipeParamType::Str,
        default: "analysis",
        help: "conda activate <env>",
    },
    RecipeParam {
        name: "module_profile",
        ty: RecipeParamType::Str,
        default: "gaussian_A",
        help: "module restore <profile> -f",
    },
    RecipeParam {
        name: "pixi_manifest",
        ty: RecipeParamType::Path,
        default: "",
        help: "empty = skip pixi hook",
    },
    RecipeParam {
        name: "launcher",
        ty: RecipeParamType::Str,
        default: "srun",
        help: "empty = bare (no srun)",
    },
    RecipeParam {
        name: "scratch_root",
        ty: RecipeParamType::Path,
        default: "",
        help: "empty = <job_dir>/.scratch fallback",
    },
    RecipeParam {
        name: "input_coordinate",
        ty: RecipeParamType::Path,
        default: "",
        help: ".xyz/.mol2 copied into <id>/input/ by cmd_new",
    },
];

fn pv<'a>(ctx: &'a JobCtx<'_>, k: &str) -> &'a str {
    ctx.params.get(k).map(|s| s.as_str()).unwrap_or_default()
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

        // v2 R4: run.py reads `os.environ["JM_JOB_DIR"]` (exported by
        // batch.bash at render time) — no scaffold-baked absolute path, so the
        // template is embedded verbatim and the flow folder stays portable.
        let run_py = include_str!("../assets/g16_opt/run.py.tmpl").to_string();

        let module_block = format!("module restore {} -f", pv(ctx, "module_profile"));
        // v2 R4: launch run.py via the render-time `$JM_JOB_DIR` env var (bash
        // expands it at job runtime to the re-rendered absolute path). cwd-
        // independent without baking any absolute path into the scaffold.
        let run_py_invocation = "python \"$JM_JOB_DIR/scripts/run.py\"".to_string();
        let bash = base_preamble(&PreambleOpts {
            conda_env: pv(ctx, "conda_env"),
            module_block: &module_block,
            body_block: &run_py_invocation,
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
                relpath: nsp("scripts/run.py"),
                contents: run_py,
                unix_mode: Some(0o755),
            },
            GeneratedFile {
                relpath: nsp("input/main.gjf"),
                contents: gjf,
                unix_mode: None,
            },
        ];

        let mut plan_params = BTreeMap::new();
        for rp in PARAMS {
            if rp.name == "input_coordinate" {
                continue; // scaffold 時消費のみ。plan.toml には出さない。
            }
            plan_params.insert(rp.name.to_string(), typed_toml(rp.ty, pv(ctx, rp.name)));
        }

        // v2 R4: body launches via `$JM_JOB_DIR` (bash-expanded at job
        // runtime). The literal `$JM_JOB_DIR` is stored in flow.toml (no
        // absolute path baked), satisfying the R4 portability invariant.
        let body = format!("bash \"$JM_JOB_DIR/scripts/{job_id}.bash\"\n");

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
    ) -> JobCtx<'a> {
        JobCtx {
            job_id: "opt",
            params,
            inputs,
            uuid,
            created_at: "2026-05-18T00:00:00Z",
        }
    }

    fn default_params() -> BTreeMap<String, String> {
        PARAMS
            .iter()
            .map(|p| (p.name.to_string(), p.default.to_string()))
            .collect()
    }

    #[test]
    fn instantiate_emits_r4_body_and_sidecars() {
        let params = default_params();
        let inputs = BTreeMap::new();
        let uuid = uuid::Uuid::now_v7();
        let a = G16Opt
            .instantiate(&ctx_with(&params, &inputs, &uuid))
            .unwrap();

        assert_eq!(a.program, "g16");
        assert_eq!(a.time_limit.as_deref(), Some("48:00:00"));
        // R4: body has NO cd and launches via the render-time $JM_JOB_DIR env
        // var — no absolute path baked into the scaffold (folder-portable).
        assert_eq!(a.body, "bash \"$JM_JOB_DIR/scripts/opt.bash\"\n");
        assert!(!a.body.contains("cd "), "R4: body must not cd");
        assert!(
            !a.body.contains("/work/root/"),
            "R4: body must not bake an absolute flow_dir path"
        );

        let bash = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/opt.bash"))
            .unwrap();
        assert_eq!(bash.unix_mode, Some(0o755));
        assert!(bash.contents.contains("module restore gaussian_A -f"));
        assert!(bash.contents.contains("conda activate analysis"));
        assert!(
            bash.contents
                .contains("python \"$JM_JOB_DIR/scripts/run.py\"")
        );
        assert!(!bash.contents.contains("srun"), "srun lives in run.py");

        let runpy = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/run.py"))
            .unwrap();
        assert_eq!(runpy.unix_mode, Some(0o755));
        // R4: JOB_DIR is read from the environment, no {{JOB_DIR}} sentinel
        // and no absolute path baked, os.getcwd() never used.
        assert!(
            runpy
                .contents
                .contains("JOB_DIR = os.environ[\"JM_JOB_DIR\"]")
        );
        assert!(
            !runpy.contents.contains("{{JOB_DIR}}"),
            "sentinel must be gone"
        );
        assert!(
            !runpy.contents.contains("/work/root/"),
            "R4: no absolute flow_dir path baked into run.py"
        );
        assert!(
            !runpy.contents.contains("os.getcwd()"),
            "R4: cwd-independent"
        );
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
        params.insert(
            "input_coordinate".into(),
            xyz.to_string_lossy().into_owned(),
        );
        params.insert("charge".into(), "1".into());
        let inputs = BTreeMap::new();
        let uuid = uuid::Uuid::now_v7();
        let a = G16Opt
            .instantiate(&ctx_with(&params, &inputs, &uuid))
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
            .instantiate(&ctx_with(&params, &inputs, &uuid))
            .unwrap_err();
        assert!(matches!(err, RecipeError::InputCoordinateMissing(_)));
    }

    #[test]
    fn plan_params_exclude_input_coordinate_and_type_ints() {
        let params = default_params();
        let inputs = BTreeMap::new();
        let uuid = uuid::Uuid::now_v7();
        let a = G16Opt
            .instantiate(&ctx_with(&params, &inputs, &uuid))
            .unwrap();
        assert!(!a.plan_params.contains_key("input_coordinate"));
        assert_eq!(a.plan_params.get("charge"), Some(&toml::Value::Integer(0)));
        assert_eq!(
            a.plan_params.get("launcher"),
            Some(&toml::Value::String("srun".into()))
        );
    }
}
