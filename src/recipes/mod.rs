//! `jm new <flow-recipe>` の二層レシピ(Job 層 / Flow 層)。
//!
//! pyo3 非依存・純 Rust(`minijinja`/`toml`/`uuid`/`chrono`/std のみ)。
//! `jm --no-default-features` でクリーンビルドされる。

pub mod flow;
pub mod flows;
pub mod job;
pub mod jobs;
pub mod xyz;

pub use flow::{Assembled, assemble};
pub use flows::G16OptParse;
pub use job::{
    FlowRecipe, GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeError,
    RecipeParam, RecipeParamType, base_preamble,
};

use std::collections::BTreeMap;

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
        let result = find_flow("nope");
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("blank, g16-opt-parse"));
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
