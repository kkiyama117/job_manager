//! `jm` — job-manager CLI.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "jm", about = "job-manager CLI")]
struct Cli {
    #[arg(long, global = true)]
    root: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Render batch.bash + .flow.effective.toml. With --effective-only the
    /// batch.bash files are NOT touched, only the snapshot is refreshed.
    Render {
        target: String,
        #[arg(long)]
        effective_only: bool,
    },
    /// Submit to SLURM (or DryRun).
    Submit {
        target: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Show flow + per-job status.
    Show { target: String },
    /// Query SLURM and update .status.toml.
    Tick { target: String },
    /// Cross-flow search.
    Search {
        #[arg(long)]
        program: Option<String>,
    },
    /// Validate TOML files + structural invariants under --root.
    Doctor { target: Option<String> },
    /// Scaffold a new flow: mint a UUID v7, create <root>/<uuid>/, and
    /// write flow.toml + plan.toml boilerplate (a 2-job step1->step2 DAG).
    New {
        /// Repeatable. KEY=VALUE pairs written into flow.toml [tags].
        #[arg(long = "tag", value_name = "KEY=VALUE")]
        tags: Vec<String>,
        /// Print only the created `<root>/<uuid>` path to stdout.
        #[arg(long)]
        print_path: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Render {
            ref target,
            effective_only,
        } => {
            let root = resolve_root(&cli)?;
            cmd_render(&root, target, effective_only).await
        }
        Cmd::Submit {
            ref target,
            dry_run,
        } => {
            let root = resolve_root(&cli)?;
            cmd_submit(&root, target, dry_run).await
        }
        Cmd::Show { ref target } => {
            let root = resolve_root(&cli)?;
            cmd_show(&root, target).await
        }
        Cmd::Tick { ref target } => {
            let root = resolve_root(&cli)?;
            cmd_tick(&root, target).await
        }
        Cmd::Search { ref program } => {
            let root = resolve_root(&cli)?;
            cmd_search(&root, program.as_deref()).await
        }
        Cmd::Doctor { ref target } => {
            let root = resolve_root(&cli)?;
            cmd_doctor(&root, target.as_deref()).await
        }
        Cmd::New {
            ref tags,
            print_path,
        } => {
            let root = resolve_root(&cli)?;
            cmd_new(&root, tags, print_path).await
        }
    }
}

fn resolve_root(cli: &Cli) -> anyhow::Result<PathBuf> {
    let raw = if let Some(p) = &cli.root {
        p.clone()
    } else if let Ok(p) = std::env::var("JM_ROOT") {
        PathBuf::from(p)
    } else {
        anyhow::bail!("--root or JM_ROOT must be set");
    };
    // Resolve `..` and symlinks. The root must exist on disk before any
    // command runs anyway, so failing here gives a clearer error than
    // letting downstream I/O hit a phantom path.
    std::fs::canonicalize(&raw)
        .map_err(|e| anyhow::anyhow!("failed to canonicalize root {}: {e}", raw.display()))
}

fn parse_target(_root: &std::path::Path, target: &str) -> anyhow::Result<uuid::Uuid> {
    let p = std::path::Path::new(target);
    if p.is_absolute() {
        let last = p
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid path"))?;
        return uuid::Uuid::parse_str(last).map_err(|e| anyhow::anyhow!("invalid uuid: {e}"));
    }
    uuid::Uuid::parse_str(target).map_err(|e| anyhow::anyhow!("invalid uuid: {e}"))
}

async fn cmd_render(
    root: &std::path::Path,
    target: &str,
    effective_only: bool,
) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::{PathResolver, write_flow_effective};
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::DryRunExecutor;
    use job_manager::slurm::querier::InMemoryQuerier;
    use std::collections::HashMap;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;

    if effective_only {
        let path = resolver.flow_effective_toml(&uuid);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        write_flow_effective(&path, &fr.flow)?;
        println!("updated .flow.effective.toml for {}", uuid);
        return Ok(());
    }

    let runner = FlowRunner::new(
        Box::new(DryRunExecutor),
        Box::new(InMemoryQuerier::new(HashMap::new())),
        &resolver,
    );
    runner.render_only(&fr).await?;
    println!("rendered {} jobs in {}", fr.flow.jobs.len(), uuid);
    Ok(())
}

async fn cmd_submit(root: &std::path::Path, target: &str, dry_run: bool) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::{DryRunExecutor, Executor, SbatchExecutor};
    use job_manager::slurm::querier::{InMemoryQuerier, Querier, SlurmQuerier};
    use slurm_async_runner::SlurmManager;
    use std::collections::HashMap;
    use std::sync::Arc;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;
    let (exec, querier): (Box<dyn Executor>, Box<dyn Querier>) = if dry_run {
        (
            Box::new(DryRunExecutor),
            Box::new(InMemoryQuerier::new(HashMap::new())),
        )
    } else {
        (
            Box::new(SbatchExecutor),
            Box::new(SlurmQuerier::new(Arc::new(SlurmManager::default()))),
        )
    };
    let runner = FlowRunner::new(exec, querier, &resolver);
    let jobids = runner.submit(&fr, dry_run).await?;
    println!("submitted {} jobs", jobids.len());
    for (jid, j) in jobids {
        println!("  {} -> {}", jid.0, j);
    }
    Ok(())
}

async fn cmd_show(root: &std::path::Path, target: &str) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::{PathResolver, read_job_run};

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::load_effective(&resolver, uuid)?;
    println!("flow {} ({} jobs)", uuid, fr.flow.jobs.len());
    for jid in fr.flow.jobs.keys() {
        let p = resolver.status_file(&uuid, jid);
        let label = if p.exists() {
            let r = read_job_run(&p)?;
            match r.slurm_jobid {
                Some(j) => format!("{:?} (slurm_jobid={j})", r.lifecycle),
                None => format!("{:?}", r.lifecycle),
            }
        } else {
            "<pending>".to_string()
        };
        println!("  {}  {}", jid.0, label);
    }
    Ok(())
}

async fn cmd_tick(root: &std::path::Path, target: &str) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::DryRunExecutor;
    use job_manager::slurm::querier::SlurmQuerier;
    use slurm_async_runner::SlurmManager;
    use std::sync::Arc;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::load_effective(&resolver, uuid)?;
    let manager = Arc::new(SlurmManager::default());
    let querier = SlurmQuerier::new(manager);
    let runner = FlowRunner::new(Box::new(DryRunExecutor), Box::new(querier), &resolver);
    let result = runner.tick(&fr).await?;
    println!(
        "tick complete: {} transitions evaluated",
        result.transitions.len()
    );
    Ok(())
}

async fn cmd_doctor(root: &std::path::Path, target: Option<&str>) -> anyhow::Result<()> {
    use job_manager::doctor::{DoctorScope, run_doctor};

    let scope = match target {
        Some(t) => DoctorScope::Flow(parse_target(root, t)?),
        None => DoctorScope::All,
    };
    let report = run_doctor(root, &scope)?;
    print!("{report}");
    if report.has_fail() {
        anyhow::bail!(
            "doctor found {} error(s)",
            report.count(job_manager::Severity::Fail)
        );
    }
    Ok(())
}

async fn cmd_search(root: &std::path::Path, program: Option<&str>) -> anyhow::Result<()> {
    use futures::StreamExt;
    use job_manager::persistence::{PathResolver, read_common};
    use job_manager::walk::walk_flows;
    use std::sync::Arc;

    let resolver = PathResolver::new(root);
    let common_path = resolver.common_toml();
    let common = if common_path.exists() {
        read_common(&common_path)?
    } else {
        tracing::info!(
            common_path = %common_path.display(),
            "common.toml not found under root; falling back to synth_empty_common for jm search"
        );
        job_manager::persistence::synth_empty_common()
    };

    let s = walk_flows(root, Arc::new(common));
    let mut s = std::pin::pin!(s);
    while let Some(item) = s.next().await {
        let flow = item?;
        if let Some(p) = program
            && !flow.jobs.values().any(|j| j.spec.program.0 == p)
        {
            continue;
        }
        println!("{}\t{}", flow.uuid, flow.created_at);
    }
    Ok(())
}

async fn cmd_new(root: &std::path::Path, tags: &[String], print_path: bool) -> anyhow::Result<()> {
    use job_manager::persistence::PathResolver;

    let mut tag_map = BTreeMap::new();
    for raw in tags {
        let (k, v) = parse_tag(raw)?;
        tag_map.insert(k, v); // last value wins on duplicate key
    }

    let uuid = uuid::Uuid::now_v7();
    let resolver = PathResolver::new(root);
    let flow_dir = resolver.flow_dir(&uuid);

    if flow_dir.exists() {
        anyhow::bail!("flow dir already exists: {}", flow_dir.display());
    }
    tokio::fs::create_dir_all(&flow_dir).await?;

    let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let flow_str = build_flow_template(&uuid, &created_at, &tag_map);
    let plan_str = build_plan_template();

    // Roll the freshly-created dir back if either write fails, so a
    // half-written flow never lingers under <root>.
    let write_both = || -> std::io::Result<()> {
        atomic_write_str(&resolver.flow_toml(&uuid), &flow_str)?;
        atomic_write_str(&resolver.plan_toml(&uuid), &plan_str)?;
        Ok(())
    };
    if let Err(e) = write_both() {
        let _ = std::fs::remove_dir_all(&flow_dir);
        return Err(anyhow::Error::new(e).context(format!(
            "failed to write boilerplate under {}",
            flow_dir.display()
        )));
    }

    if print_path {
        println!("{}", flow_dir.display());
    } else {
        println!("created flow {uuid}");
        println!("  {}", resolver.flow_toml(&uuid).display());
        println!("  {}", resolver.plan_toml(&uuid).display());
        println!(
            "next: edit flow.toml, then `jm --root {} render {uuid}`",
            root.display()
        );
    }
    Ok(())
}

/// Split a `--tag KEY=VALUE` argument on the first `=`. The value may
/// itself contain `=`. The key must be a TOML bare key
/// (`[A-Za-z0-9_-]`, non-empty) so it can be written into `flow.toml`'s
/// `[tags]` table without quoting; this fails fast here rather than as a
/// cryptic `jm render` TOML error later.
fn parse_tag(raw: &str) -> anyhow::Result<(String, String)> {
    match raw.split_once('=') {
        Some(("", _)) => {
            anyhow::bail!("invalid --tag: empty key in {raw:?}")
        }
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

/// The `plan.toml` boilerplate. Static — every JobId in the flow
/// template has a matching `[jobs.*]` table here.
fn build_plan_template() -> String {
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

/// The `flow.toml` boilerplate: a 2-job `step1 -> step2` (afterok) DAG.
///
/// `partition = "REPLACE_ME"` is written explicitly because `jm new`
/// does not create `common.toml`; without it `jm render` would fail
/// with `PartitionMissing`. REPLACE_ME lets `render` succeed while real
/// `submit` fails fast until the user edits it.
fn build_flow_template(
    uuid: &uuid::Uuid,
    created_at: &str,
    tags: &BTreeMap<String, String>,
) -> String {
    let mut tag_lines = String::new();
    if tags.is_empty() {
        tag_lines.push_str("# free-form key=value tags; populate via `jm new --tag k=v`\n");
    } else {
        for (k, v) in tags {
            // Keys are TOML bare-key-safe in practice (CLI-provided);
            // values are TOML-escaped via the string serializer.
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

/// Atomic write for `jm new`'s generated files. `persistence::atomic_write`
/// is `pub(crate)` and unreachable from this binary crate, so this is a
/// minimal local equivalent: write to a `<filename>.<pid>.tmp` sibling,
/// fsync, rename over `path`, and clean the tmp on failure. `jm new` never
/// writes the same path concurrently, so PID alone is a sufficient tmp
/// discriminator. The tmp name is built by *appending* (not
/// `Path::with_extension`, which would replace `.toml` and produce
/// `flow.<pid>.tmp`), matching the CLAUDE.md `<name>.<pid>.tmp` convention.
fn atomic_write_str(path: &std::path::Path, body: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut tmp_name = path
        .file_name()
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "path has no file name")
        })?
        .to_os_string();
    tmp_name.push(format!(".{}.tmp", std::process::id()));
    let tmp = path.with_file_name(tmp_name);
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tag_splits_on_first_equals() {
        assert_eq!(
            parse_tag("a=b").unwrap(),
            ("a".to_string(), "b".to_string())
        );
    }

    #[test]
    fn parse_tag_keeps_later_equals_in_value() {
        assert_eq!(
            parse_tag("a=b=c").unwrap(),
            ("a".to_string(), "b=c".to_string())
        );
    }

    #[test]
    fn parse_tag_rejects_missing_equals() {
        let err = parse_tag("abc").unwrap_err();
        assert!(err.to_string().contains("expected key=value"), "got: {err}");
    }

    #[test]
    fn parse_tag_rejects_empty_key() {
        let err = parse_tag("=v").unwrap_err();
        assert!(err.to_string().contains("empty key"), "got: {err}");
    }

    #[test]
    fn parse_tag_rejects_non_bare_key() {
        for bad in ["my key=v", "my.key=v", "k!=v"] {
            let err = parse_tag(bad).unwrap_err();
            assert!(
                err.to_string().contains("non-bare-key"),
                "got: {err} for {bad:?}"
            );
        }
    }

    #[test]
    fn plan_template_parses_as_experiment_plan() {
        use job_manager::plan::ExperimentPlan;

        let s = build_plan_template();
        let plan: ExperimentPlan =
            toml::from_str(&s).expect("plan template must parse as ExperimentPlan");

        let keys: std::collections::BTreeSet<String> =
            plan.jobs.keys().map(|j| j.0.clone()).collect();
        assert_eq!(
            keys,
            ["step1", "step2"].iter().map(|s| s.to_string()).collect()
        );
    }

    #[test]
    fn flow_template_parses_with_two_step_dag_and_partition() {
        use gaussian_job_shared::entities::workflow::JobFlow;

        let uuid = uuid::Uuid::now_v7();
        let created = "2026-05-16T00:00:00Z";
        let s = build_flow_template(&uuid, created, &BTreeMap::new());

        let flow: JobFlow =
            toml::from_str(&s).expect("flow template must parse directly as JobFlow");

        assert_eq!(flow.uuid, uuid);
        let ids: std::collections::BTreeSet<String> =
            flow.jobs.keys().map(|j| j.0.clone()).collect();
        assert_eq!(
            ids,
            ["step1", "step2"].iter().map(|s| s.to_string()).collect()
        );

        // step2 depends on step1 via afterok.
        let step2 = flow
            .jobs
            .get(&gaussian_job_shared::entities::workflow::JobId(
                "step2".into(),
            ))
            .expect("step2 present");
        assert_eq!(step2.parents.len(), 1);
        assert_eq!(step2.parents[0].from.0, "step1");

        // partition is present (REPLACE_ME) on both jobs so render won't hit
        // PartitionMissing when common.toml is absent.
        for (jid, job) in &flow.jobs {
            assert_eq!(
                job.spec.config.partition, "REPLACE_ME",
                "job {} must carry REPLACE_ME partition",
                jid.0
            );
        }
    }

    #[test]
    fn flow_template_renders_tags_section() {
        use gaussian_job_shared::entities::workflow::JobFlow;

        let uuid = uuid::Uuid::now_v7();
        let mut tags = BTreeMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("owner".to_string(), "a=b".to_string()); // value with '='

        let s = build_flow_template(&uuid, "2026-05-16T00:00:00Z", &tags);
        let flow: JobFlow = toml::from_str(&s).expect("tagged flow template parses");

        assert_eq!(flow.tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(flow.tags.get("owner").map(String::as_str), Some("a=b"));
    }

    #[test]
    fn atomic_write_str_creates_file_with_exact_contents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.toml");

        atomic_write_str(&path, "hello = 1\n").expect("write ok");

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello = 1\n");
        // No leftover .tmp sibling.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "tmp file not cleaned: {leftovers:?}");
    }
}
