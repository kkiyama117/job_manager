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
    ExperimentPlan, PathResolver, build_job_id, parse_job_id, read_plan, validate_step_id,
    write_plan,
};

/// `SlurmJobConfig` does not derive `Default` (the `partition` field is
/// required), so tests build a minimal config explicitly. Mirrors the
/// `sample_config()` helper used in upstream D2 tests.
fn sample_config() -> SlurmJobConfig {
    SlurmJobConfig {
        partition: "long".to_string(),
        time_limit: None,
        log_stdout: None,
        log_stderr: None,
        comment: None,
        job_name: None,
        array_spec: None,
        dependency: None,
        mail_user: None,
        mail_types: None,
        resource_spec: None,
    }
}

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
    let compounds = ["benzene", "toluene", "p-xylene"];
    let methods = [("b3lyp", "B3LYP"), ("m062x", "M06-2X")];

    let mut jobs: BTreeMap<JobId, Job> = BTreeMap::new();
    let mut params: BTreeMap<JobId, BTreeMap<String, toml::Value>> = BTreeMap::new();

    for (i, c) in compounds.iter().enumerate() {
        for (j, (_name, route)) in methods.iter().enumerate() {
            // opt
            let opt_id = JobId::from(build_job_id("opt", &[("compound", i), ("method", j)]));
            jobs.insert(
                opt_id.clone(),
                Job {
                    spec: JobSpec {
                        program: Program::from("g16"),
                        config: sample_config(),
                        body: String::new(),
                    },
                    parents: vec![],
                },
            );
            let mut p = BTreeMap::new();
            p.insert(
                "route".into(),
                toml::Value::String(format!("# {route}/6-31G* opt")),
            );
            p.insert("compound".into(), toml::Value::String((*c).into()));
            params.insert(opt_id.clone(), p);

            // freq (pair_by_axes parent: opt)
            let freq_id = JobId::from(build_job_id("freq", &[("compound", i), ("method", j)]));
            jobs.insert(
                freq_id.clone(),
                Job {
                    spec: JobSpec {
                        program: Program::from("g16"),
                        config: sample_config(),
                        body: String::new(),
                    },
                    parents: vec![JobEdge {
                        from: opt_id.clone(),
                        kind: DependencyType::AfterOk,
                    }],
                },
            );
            let mut p = BTreeMap::new();
            p.insert(
                "route".into(),
                toml::Value::String(format!("# {route}/6-31G* freq")),
            );
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
    assert_eq!(
        flow_keys, plan_keys,
        "flow.toml と plan.toml の JobId 集合は一致"
    );

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
