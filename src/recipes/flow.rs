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
#[derive(Debug)]
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

        if let Some(ic) = params.get("input_coordinate")
            && !ic.is_empty()
        {
            input_coordinate = Some((job_id.to_string(), std::path::PathBuf::from(ic)));
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
        flow_jobs.push_str(&format!(
            "\n[jobs.{job_id}.config]\npartition = \"REPLACE_ME\"\n"
        ));
        if let Some(tl) = &art.time_limit {
            flow_jobs.push_str(&format!(
                "time_limit = {}\n",
                toml::Value::String(tl.clone())
            ));
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
        use gaussian_job_shared::entities::workflow::JobId;
        use slurm_async_runner::entities::slurm::DependencyType;

        let a = assemble_default();
        let flow: gaussian_job_shared::entities::workflow::JobFlow =
            toml::from_str(&a.flow_toml).expect("flow.toml must parse as JobFlow");

        // Verify exactly the two expected job ids are present.
        assert!(
            flow.jobs.contains_key(&JobId::from("opt")),
            "jobs must contain 'opt'"
        );
        assert!(
            flow.jobs.contains_key(&JobId::from("parse")),
            "jobs must contain 'parse'"
        );
        assert_eq!(flow.jobs.len(), 2, "must have exactly 2 jobs");

        // Verify the afterok edge on 'parse'.
        let parse = &flow.jobs[&JobId::from("parse")];
        assert_eq!(parse.parents.len(), 1);
        assert_eq!(parse.parents[0].from, JobId::from("opt"));
        assert_eq!(parse.parents[0].kind, DependencyType::AfterOk);
    }

    #[test]
    fn plan_toml_parses_and_keysets_match() {
        let a = assemble_default();
        let plan: crate::plan::ExperimentPlan =
            toml::from_str(&a.plan_toml).expect("plan.toml must parse as ExperimentPlan");
        let flow: gaussian_job_shared::entities::workflow::JobFlow =
            toml::from_str(&a.flow_toml).unwrap();

        // Compare via normalized String sets so JobId vs String key-type difference
        // does not prevent compilation — the assertion intent (same id set) is preserved.
        let flow_ids: std::collections::BTreeSet<String> =
            flow.jobs.keys().map(|k| k.to_string()).collect();
        let plan_ids: std::collections::BTreeSet<String> =
            plan.jobs.keys().map(|k| k.to_string()).collect();
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
        assert!(
            !a.flow_toml.contains("cd "),
            "R3': flow.toml body must not cd; got:\n{}",
            a.flow_toml
        );
        assert!(a.flow_toml.contains("bash scripts/opt.bash"));
        let runpy = a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("opt/scripts/run.py"))
            .unwrap();
        assert!(
            runpy
                .contents
                .contains("JOB_DIR = \"/work/root/01999999-0000-7000-8000-0000000000ab/opt\"")
        );
        assert!(
            !runpy.contents.contains("os.getcwd()"),
            "R3': cwd-independent"
        );
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
