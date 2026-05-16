//! `jm` — job-manager CLI.

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

/// Split a `--tag KEY=VALUE` argument on the first `=`. The value may
/// itself contain `=`. Empty keys are rejected so a stray `=v` cannot
/// produce an unnamed tag.
#[cfg_attr(not(test), allow(dead_code))]
fn parse_tag(raw: &str) -> anyhow::Result<(String, String)> {
    match raw.split_once('=') {
        Some(("", _)) => {
            anyhow::bail!("invalid --tag: empty key in {raw:?}")
        }
        Some((k, v)) => Ok((k.to_string(), v.to_string())),
        None => anyhow::bail!("invalid --tag: expected key=value, got {raw:?}"),
    }
}

/// The `plan.toml` boilerplate. Static — every JobId in the flow
/// template has a matching `[jobs.*]` table here.
#[cfg_attr(not(test), allow(dead_code))]
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
}
