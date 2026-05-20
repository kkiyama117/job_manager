//! JobTemplate `g16_opt` — g16 構造最適化1ステップ。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

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
        // R3' + (b): SLURM job cwd is pinned to `<flow_dir>/<job_id>/` by
        // FlowRunner::submit setting per-job `SbatchCmd.chdir`, so a thin
        // relative launcher resolves. run.py / parse.py remain
        // cwd-independent internally via the baked absolute `JOB_DIR`
        // constant — this is the R3' invariant the (a) interim fix
        // (PR #27 / issue #29) protected by absolute body paths; (b)
        // hands that responsibility back to the submit layer.
        let run_py_invocation = "python scripts/run.py".to_string();
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

        // (b): body is a thin RELATIVE launcher. The SLURM job cwd is
        // pinned to `<flow_dir>/<job_id>/` by FlowRunner::submit via
        // per-job `SbatchCmd.chdir`, so `scripts/<id>.bash` resolves
        // (issue #29 / PR #27 H1 revoke of the interim (a) fix).
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
        // (b): body is a thin RELATIVE launcher; the SLURM job cwd is
        // pinned to `<flow_dir>/<job_id>/` by FlowRunner::submit via
        // per-job `SbatchCmd.chdir` (issue #29 / PR #27 H1 revoke of (a)).
        assert_eq!(a.body, "bash scripts/opt.bash\n");
        assert!(!a.body.contains("cd "), "R3': body must not cd");
        assert!(
            !a.body.contains(" \"/"),
            "(b) revoke (a): body must not use an absolute launcher path"
        );

        let bash = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/opt.bash"))
            .unwrap();
        assert_eq!(bash.unix_mode, Some(0o755));
        assert!(bash.contents.contains("module restore gaussian_A -f"));
        assert!(bash.contents.contains("conda activate analysis"));
        assert!(bash.contents.contains("python scripts/run.py"));
        assert!(
            !bash.contents.contains("python \"/"),
            "(b) revoke (a): run.py must be launched by a relative path"
        );
        assert!(!bash.contents.contains("srun"), "srun lives in run.py");

        let runpy = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("scripts/run.py"))
            .unwrap();
        assert_eq!(runpy.unix_mode, Some(0o755));
        // R3': absolute JOB_DIR baked in, no {{JOB_DIR}} sentinel left,
        // os.getcwd() never used (cwd-independent like the reference run-g16).
        assert!(
            runpy
                .contents
                .contains("JOB_DIR = \"/work/root/01999999-0000-7000-8000-000000000000/opt\"")
        );
        assert!(
            !runpy.contents.contains("{{JOB_DIR}}"),
            "sentinel must be swapped"
        );
        assert!(
            !runpy.contents.contains("os.getcwd()"),
            "R3': cwd-independent"
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
