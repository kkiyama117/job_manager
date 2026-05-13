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
    /// Render batch.bash only.
    Run { target: String },
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
        root: PathBuf,
        #[arg(long)]
        program: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run { ref target } => {
            let root = resolve_root(&cli)?;
            cmd_run(&root, target).await
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
        Cmd::Search {
            ref root,
            ref program,
        } => cmd_search(root, program.as_deref()).await,
    }
}

fn resolve_root(cli: &Cli) -> anyhow::Result<PathBuf> {
    if let Some(p) = &cli.root {
        return Ok(p.clone());
    }
    if let Ok(p) = std::env::var("JM_ROOT") {
        return Ok(PathBuf::from(p));
    }
    anyhow::bail!("--root or JM_ROOT must be set")
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

async fn cmd_run(root: &std::path::Path, target: &str) -> anyhow::Result<()> {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;
    use job_manager::runner::flow::FlowRunner;
    use job_manager::slurm::executor::DryRunExecutor;
    use job_manager::slurm::querier::InMemoryQuerier;
    use std::collections::HashMap;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;
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
    use job_manager::slurm::querier::InMemoryQuerier;
    use std::collections::HashMap;

    let resolver = PathResolver::new(root);
    let uuid = parse_target(root, target)?;
    let fr = FlowRun::read(&resolver, uuid)?;
    let exec: Box<dyn Executor> = if dry_run {
        Box::new(DryRunExecutor)
    } else {
        Box::new(SbatchExecutor)
    };
    let runner = FlowRunner::new(
        exec,
        Box::new(InMemoryQuerier::new(HashMap::new())),
        &resolver,
    );
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
    let fr = FlowRun::read(&resolver, uuid)?;
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
    let fr = FlowRun::read(&resolver, uuid)?;
    let manager = Arc::new(SlurmManager::default());
    let querier = SlurmQuerier::new(manager);
    let runner = FlowRunner::new(Box::new(DryRunExecutor), Box::new(querier), &resolver);
    runner.tick(&fr).await?;
    println!("tick complete");
    Ok(())
}

async fn cmd_search(root: &std::path::Path, program: Option<&str>) -> anyhow::Result<()> {
    use futures::StreamExt;
    use job_manager::walk::walk_flows;

    let s = walk_flows(root);
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
