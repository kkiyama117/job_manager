//! `jm` — job-manager CLI.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

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
    /// Validate TOML files + structural invariants under --root.
    Doctor { target: Option<String> },
    /// Cross-flow status listing (read-only; no SLURM).
    Ls {
        #[command(subcommand)]
        view: LsView,
    },
    /// Scaffold a new flow from a recipe (default `blank` = legacy 2-job
    /// echo DAG). `jm new g16-opt-parse --param opt.charge=1`.
    New {
        /// Flow recipe name. Omitted = `blank`.
        recipe: Option<String>,
        /// Repeatable `<JobId>.<param>=<value>`.
        #[arg(long = "param", value_name = "JOBID.PARAM=VALUE")]
        params: Vec<String>,
        /// Repeatable. KEY=VALUE pairs written into flow.toml [tags].
        #[arg(long = "tag", value_name = "KEY=VALUE")]
        tags: Vec<String>,
        /// Print only the created `<root>/<uuid>` path to stdout.
        #[arg(long)]
        print_path: bool,
        /// List available recipes and exit.
        #[arg(long)]
        list: bool,
        /// Describe the given recipe and exit.
        #[arg(long)]
        describe: bool,
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
        Cmd::Ls { ref view } => {
            let root = resolve_root(&cli)?;
            cmd_ls(&root, view).await
        }
        Cmd::Doctor { ref target } => {
            let root = resolve_root(&cli)?;
            cmd_doctor(&root, target.as_deref()).await
        }
        Cmd::New {
            ref recipe,
            ref params,
            ref tags,
            print_path,
            list,
            describe,
        } => {
            if list {
                print!("{}", job_manager::recipes::render_list());
                return Ok(());
            }
            if describe {
                let name = recipe.as_deref().unwrap_or("blank");
                print!("{}", job_manager::recipes::render_describe(name)?);
                return Ok(());
            }
            let root = resolve_root(&cli)?;
            cmd_new(&root, recipe.as_deref(), params, tags, print_path).await
        }
    }
}

#[derive(Subcommand)]
enum LsView {
    /// One row per job across all flows.
    Jobs {
        #[command(flatten)]
        filter: FilterArgs,
        #[command(flatten)]
        fmt: FmtArgs,
    },
    /// One row per flow (aggregated status).
    Flows {
        #[command(flatten)]
        filter: FilterArgs,
        #[command(flatten)]
        fmt: FmtArgs,
    },
    /// Flow → job tree. No arg = all flows matching the filters;
    /// FLOW_UUID = that one flow (filters then select forest membership
    /// only — a flow's tree always shows its full job DAG).
    Tree {
        target: Option<String>,
        #[command(flatten)]
        filter: FilterArgs,
    },
}

#[derive(Args, Debug)]
struct FilterArgs {
    /// Filter by program name (exact match).
    #[arg(long)]
    program: Option<String>,
    /// Repeatable KEY=VALUE; all must match.
    #[arg(long = "tag", value_name = "KEY=VALUE")]
    tag: Vec<String>,
    /// Comma-separated: pd,q,r,ok,f,sk or long names (case-insensitive).
    #[arg(long)]
    status: Option<String>,
    /// flow uuid prefix (case-insensitive).
    #[arg(long)]
    flow: Option<String>,
    /// Only flows created at/after this RFC3339 datetime.
    #[arg(long)]
    created_after: Option<String>,
    /// Only flows created at/before this RFC3339 datetime.
    #[arg(long)]
    created_before: Option<String>,
    /// Filter by SLURM job id (matches a job's recorded slurm_jobid).
    #[arg(long)]
    slurm_jobid: Option<u64>,
    /// Filter by job id.
    #[arg(long)]
    job: Option<String>,
    /// Maximum number of result rows.
    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Args, Debug)]
struct FmtArgs {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    no_header: bool,
}

fn build_filter(a: &FilterArgs) -> anyhow::Result<job_manager::SearchFilter> {
    use gaussian_job_shared::entities::workflow::{JobId, Program};

    let mut tags = BTreeMap::new();
    for raw in &a.tag {
        let (k, v) = job_manager::recipes::flows::blank::parse_tag(raw)?;
        tags.insert(k, v);
    }
    let status = match &a.status {
        Some(s) => job_manager::parse_status_set(s).map_err(|e| anyhow::anyhow!(e))?,
        None => BTreeSet::new(),
    };
    let parse_dt = |s: &str| -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
        Ok(chrono::DateTime::parse_from_rfc3339(s)
            .map_err(|e| anyhow::anyhow!("invalid RFC3339 datetime {s:?}: {e}"))?
            .with_timezone(&chrono::Utc))
    };
    Ok(job_manager::SearchFilter {
        program: a.program.clone().map(Program::from),
        tags,
        status,
        flow_uuid_prefix: a.flow.clone(),
        created_after: a.created_after.as_deref().map(parse_dt).transpose()?,
        created_before: a.created_before.as_deref().map(parse_dt).transpose()?,
        slurm_jobid: a.slurm_jobid,
        job_id: a.job.clone().map(JobId::from),
    })
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

async fn cmd_ls(root: &std::path::Path, view: &LsView) -> anyhow::Result<()> {
    use job_manager::persistence::{PathResolver, read_common};
    use std::sync::Arc;

    let resolver = PathResolver::new(root);
    let common_path = resolver.common_toml();
    let common = if common_path.exists() {
        read_common(&common_path)?
    } else {
        job_manager::persistence::synth_empty_common()
    };
    let common = Arc::new(common);

    match view {
        LsView::Jobs { filter, fmt } => {
            let f = build_filter(filter)?;
            let collected = job_manager::listing::collect(root, common, &f).await?;
            let rows = job_manager::listing::job_rows(&collected, &f, filter.limit);
            if fmt.json {
                println!("{}", job_manager::listing::format_jobs_json(&rows)?);
            } else {
                print!(
                    "{}",
                    job_manager::listing::format_jobs_table(&rows, fmt.no_header)
                );
            }
        }
        LsView::Flows { filter, fmt } => {
            let f = build_filter(filter)?;
            let collected = job_manager::listing::collect(root, common, &f).await?;
            let rows = job_manager::listing::flow_rows(&collected, &f, filter.limit);
            if fmt.json {
                println!("{}", job_manager::listing::format_flows_json(&rows)?);
            } else {
                print!(
                    "{}",
                    job_manager::listing::format_flows_table(&rows, fmt.no_header)
                );
            }
        }
        LsView::Tree { target, filter } => {
            let f = build_filter(filter)?;
            let collected = job_manager::listing::collect(root, common, &f).await?;
            let selected: Vec<&job_manager::listing::CollectedFlow> = match target {
                Some(t) => {
                    let uuid = parse_target(root, t)?;
                    let sel: Vec<&job_manager::listing::CollectedFlow> =
                        collected.iter().filter(|c| c.flow.uuid == uuid).collect();
                    if sel.is_empty() {
                        // Read-only filter: keep exit 0 (no flow on disk is not
                        // an error), but tell the user nothing matched —
                        // otherwise a typo'd uuid is silently indistinguishable
                        // from an empty flow.
                        eprintln!("jm ls tree: no flow matched {uuid}");
                    }
                    sel
                }
                None => job_manager::listing::matched_flows(&collected, &f, filter.limit),
            };
            print!("{}", job_manager::listing::format_tree(&selected));
        }
    }
    Ok(())
}

async fn cmd_new(
    root: &std::path::Path,
    recipe: Option<&str>,
    params: &[String],
    tags: &[String],
    print_path: bool,
) -> anyhow::Result<()> {
    use job_manager::persistence::PathResolver;
    use job_manager::recipes::flows::blank;

    let recipe_name = recipe.unwrap_or("blank");

    let mut tag_map = BTreeMap::new();
    for raw in tags {
        let (k, v) = blank::parse_tag(raw)?;
        tag_map.insert(k, v);
    }

    let uuid = uuid::Uuid::now_v7();
    let resolver = PathResolver::new(root);
    let flow_dir = resolver.flow_dir(&uuid);
    if flow_dir.exists() {
        anyhow::bail!("flow dir already exists: {}", flow_dir.display());
    }
    tokio::fs::create_dir_all(&flow_dir).await?;
    let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let rollback = || {
        if let Err(e) = std::fs::remove_dir_all(&flow_dir) {
            eprintln!(
                "warning: rollback failed to remove {}: {e}",
                flow_dir.display()
            );
        }
    };

    if recipe_name == "blank" {
        let flow_str = blank::build_flow_template(&uuid, &created_at, &tag_map);
        let plan_str = blank::build_plan_template();
        let write_both = || -> std::io::Result<()> {
            atomic_write_str(&resolver.flow_toml(&uuid), &flow_str)?;
            atomic_write_str(&resolver.plan_toml(&uuid), &plan_str)?;
            Ok(())
        };
        if let Err(e) = write_both() {
            rollback();
            return Err(anyhow::Error::new(e).context(format!(
                "failed to write boilerplate under {}",
                flow_dir.display()
            )));
        }
    } else {
        // v2 R4: the scaffold bakes no absolute path. JM_JOB_DIR is exported by
        // batch.bash at render time, so cmd_new no longer needs to absolutize
        // the flow dir for `assemble` (paths resolve from the env at job runtime).
        let flow_recipe = match job_manager::recipes::find_flow(recipe_name) {
            Ok(r) => r,
            Err(e) => {
                rollback();
                return Err(anyhow::anyhow!(e));
            }
        };
        let mut raw_params = BTreeMap::new();
        for p in params {
            if let Err(e) = job_manager::recipes::parse_param_arg(p, &mut raw_params) {
                rollback();
                return Err(anyhow::anyhow!(e));
            }
        }
        let assembled = match job_manager::recipes::assemble(
            flow_recipe.as_ref(),
            &raw_params,
            &tag_map,
            &uuid,
            &created_at,
        ) {
            Ok(a) => a,
            Err(e) => {
                rollback();
                return Err(anyhow::anyhow!(e));
            }
        };

        let do_writes = || -> std::io::Result<()> {
            atomic_write_str(&resolver.flow_toml(&uuid), &assembled.flow_toml)?;
            atomic_write_str(&resolver.plan_toml(&uuid), &assembled.plan_toml)?;
            for f in &assembled.sidecars {
                let dst = flow_dir.join(&f.relpath);
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                atomic_write_str(&dst, &f.contents)?;
                #[cfg(unix)]
                if let Some(mode) = f.unix_mode {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&dst, std::fs::Permissions::from_mode(mode))?;
                }
            }
            if let Some((job_id, src)) = &assembled.input_coordinate {
                if !src.exists() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("input_coordinate not found: {}", src.display()),
                    ));
                }
                let base = src.file_name().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "input_coordinate has no file name",
                    )
                })?;
                let dst_dir = flow_dir.join(job_id).join("input");
                std::fs::create_dir_all(&dst_dir)?;
                std::fs::copy(src, dst_dir.join(base))?;
            }
            Ok(())
        };
        if let Err(e) = do_writes() {
            rollback();
            return Err(anyhow::Error::new(e).context(format!(
                "failed to scaffold recipe {recipe_name} under {}",
                flow_dir.display()
            )));
        }
    }

    if print_path {
        println!("{}", flow_dir.display());
    } else {
        println!("created flow {uuid} (recipe: {recipe_name})");
        println!("  {}", resolver.flow_toml(&uuid).display());
        println!("  {}", resolver.plan_toml(&uuid).display());
        println!(
            "next: edit flow.toml/plan.toml, then `jm --root {} render {uuid}`",
            root.display()
        );
    }
    Ok(())
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
    fn cli_parses_ls_jobs_with_filters() {
        let cli = Cli::try_parse_from([
            "jm",
            "--root",
            "/tmp/x",
            "ls",
            "jobs",
            "--status",
            "running,F",
            "--program",
            "g16",
            "--no-header",
        ])
        .expect("parse ls jobs");
        match cli.cmd {
            Cmd::Ls {
                view: LsView::Jobs { filter, fmt },
            } => {
                assert_eq!(filter.status.as_deref(), Some("running,F"));
                assert_eq!(filter.program.as_deref(), Some("g16"));
                assert!(fmt.no_header);
                assert!(!fmt.json);
            }
            _ => panic!("expected ls jobs"),
        }
    }

    #[test]
    fn cli_parses_ls_tree_optional_target() {
        let cli =
            Cli::try_parse_from(["jm", "--root", "/tmp/x", "ls", "tree"]).expect("parse ls tree");
        match cli.cmd {
            Cmd::Ls {
                view: LsView::Tree { target, .. },
            } => assert!(target.is_none()),
            _ => panic!("expected ls tree"),
        }
    }

    #[test]
    fn build_filter_parses_status_tag_dates() {
        let fa = FilterArgs {
            program: Some("g16".into()),
            tag: vec!["env=prod".into()],
            status: Some("ok,running".into()),
            flow: Some("0199".into()),
            created_after: Some("2026-05-16T00:00:00Z".into()),
            created_before: None,
            slurm_jobid: Some(42),
            job: Some("step1".into()),
            limit: Some(10),
        };
        let f = build_filter(&fa).expect("build_filter ok");
        assert_eq!(f.status.len(), 2);
        assert_eq!(f.tags.get("env").map(String::as_str), Some("prod"));
        assert!(f.created_after.is_some());
        assert_eq!(f.slurm_jobid, Some(42));
    }

    #[test]
    fn build_filter_rejects_bad_status() {
        let fa = FilterArgs {
            program: None,
            tag: vec![],
            status: Some("nope".into()),
            flow: None,
            created_after: None,
            created_before: None,
            slurm_jobid: None,
            job: None,
            limit: None,
        };
        assert!(build_filter(&fa).is_err());
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

    #[test]
    fn cli_parses_ls_flows_with_json() {
        let cli = Cli::try_parse_from(["jm", "--root", "/tmp/x", "ls", "flows", "--json"])
            .expect("parse ls flows");
        match cli.cmd {
            Cmd::Ls {
                view: LsView::Flows { fmt, .. },
            } => assert!(fmt.json),
            _ => panic!("expected ls flows"),
        }
    }
}
