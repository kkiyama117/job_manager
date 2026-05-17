# jm new recipes — Plan C: `jm new` CLI + render-time resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the Plan B `recipes` library into the `jm new` CLI (recipe positional, `--param`, `--list`, `--describe`, `input_coordinate` copy + `.xyz` splice, rollback), resolve `JM_LAUNCHER`/`JM_SCRATCH_ROOT` at render time from the Plan A D2 fields (4-case/3-case precedence, additive `render_batch_bash` sibling — public signature unchanged), delete the now-duplicated jm.rs boilerplate, and ship the end-to-end feature with integration + python-smoke tests, docs, and a green CI gate.

**Architecture:** `Cmd::New` gains four clap fields; `cmd_new` routes `blank` through Plan B's byte-identical `blank_flow_toml`/`blank_plan_toml` (no sidecars — keeps `tests/integration_new.rs` green) and every other FlowRecipe through `recipes::assemble()`, writing each `GeneratedFile` atomically with the existing `atomic_write_str` + a 0755 chmod for `*.bash`/`*.py`, rolling the flow dir back on any failure. `input_coordinate` is the only post-`assemble` I/O: copy the file into `<jid>/input/`, and for `.xyz` splice `recipes::parse_xyz()` output into the already-written `main.gjf` (replacing the geometry sentinel). Launcher/scratch_root resolution lives in a **pure** `render::resolve_runtime_ctx` (4-case launcher, 3-case scratch_root — spec §5.5/§5.6) consumed by a new additive `render::render_batch_bash_with_ctx`; the existing public+PyO3 `render_batch_bash` delegates to it with an empty ctx so its output and signature are byte/ABI-identical (no `.pyi` drift, R1 honored). Both `FlowRunner::submit` and `FlowRunner::render_only` call the ctx-aware renderer with `resolve_runtime_ctx(fr.common.as_ref(), params)`.

**Tech Stack:** Rust (edition 2024, nightly), `clap` 4 derive, `assert_cmd`/`predicates`, `tempfile`; `uv`/`pytest` for python smoke; the Plan A D2 fields + Plan B `recipes` module.

**Spec:** `docs/superpowers/specs/2026-05-16-jm-new-domain-recipes-design.md` rev.6 — §3, §4 (`cmd_new`), §5.1/§5.5/§5.6, §7, §8, §9, §10, §11.

**Depends on:** Plan A executed (D2 `CommonConfig.launcher` / `DirectoryConfig.scratch_root` exist; `read_common`/`synth_empty_common` carry them) **and** Plan B executed (`recipes` module: `assemble`, `find_flow`, `flow_registry`, `format_list`, `format_describe`, `parse_xyz`, `flows::blank::{blank_flow_toml,blank_plan_toml}`, `RecipeError`).

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `src/bin/jm.rs` | Modify | `Cmd::New` clap fields; `main` dispatch; `cmd_new` rewrite; delete `build_flow_template`/`build_plan_template` + their unit tests; keep `atomic_write_str`/`parse_tag` |
| `src/render/mod.rs` | Modify | add pure `resolve_runtime_ctx` + additive `render_batch_bash_with_ctx`; old `render_batch_bash` delegates (output unchanged) |
| `src/runner/flow.rs` | Modify | both `render_batch_bash` call sites (submit + render_only) → `render_batch_bash_with_ctx` with resolved ctx |
| `src/lib.rs` | Modify | re-export `render_batch_bash_with_ctx`, `resolve_runtime_ctx` |
| `tests/integration_new_recipes.rs` | Create | end-to-end `jm new <recipe>` / `--list` / `--describe` / `input_coordinate` / render `JM_*` |
| `tests/integration_new.rs` | Verify only | byte-identity regression — must stay green unedited |
| `python/tests/test_recipe_scripts.py` | Create | smoke the generated `run.py` / `parse.py` |
| `README.md`, `docs/API.md`, `CLAUDE.md` | Modify | document `jm new <recipe>` surface (CLAUDE.md is gitignored — edit on disk for local accuracy, not committed) |

---

## Task 1: `Cmd::New` clap fields + dispatch + `cmd_new` signature

**Files:**
- Modify: `src/bin/jm.rs` (`Cmd::New` ~lines 44-53; `main` match arm ~90-96; `cmd_new` signature ~420)
- Test: `src/bin/jm.rs` inline (`cli_parses_*`)

- [ ] **Step 1: Write a failing CLI-parse test** — add to `src/bin/jm.rs` `mod tests`:

```rust
    #[test]
    fn cli_parses_new_recipe_param_list_describe() {
        let cli = Cli::try_parse_from([
            "jm", "--root", "/tmp/x", "new", "g16-opt-parse",
            "--param", "opt.charge=1", "--param", "opt.route=#p opt=b3lyp",
            "--tag", "k=v",
        ])
        .expect("parse new with recipe+params");
        match cli.cmd {
            Cmd::New { ref recipe, ref params, ref tags, print_path, list, describe } => {
                assert_eq!(recipe.as_deref(), Some("g16-opt-parse"));
                assert_eq!(params.len(), 2);
                assert_eq!(params[0], "opt.charge=1");
                assert_eq!(tags, &vec!["k=v".to_string()]);
                assert!(!print_path && !list && !describe);
            }
            _ => panic!("expected Cmd::New"),
        }

        let l = Cli::try_parse_from(["jm", "--root", "/tmp/x", "new", "--list"])
            .expect("parse new --list");
        match l.cmd {
            Cmd::New { recipe, list, .. } => {
                assert!(recipe.is_none());
                assert!(list);
            }
            _ => panic!("expected Cmd::New"),
        }
    }
```

- [ ] **Step 2: Run → fail**

Run: `cargo test --bin jm --no-default-features cli_parses_new_recipe 2>&1 | tail -8`
Expected: COMPILE FAIL — `Cmd::New` has no fields `recipe`/`params`/`list`/`describe`.

- [ ] **Step 3: Extend `Cmd::New`** — replace the `New { … }` variant (jm.rs ~44-53) with:

```rust
    /// Scaffold a new flow. No recipe = `blank` (legacy 2-job DAG).
    /// `jm new g16-opt-parse --param opt.charge=1` scaffolds a domain flow.
    New {
        /// FlowRecipe name (positional, optional). Omitted = `blank`.
        recipe: Option<String>,
        /// Repeatable `--param <JobId>.<param>=<value>` overrides.
        #[arg(long = "param", value_name = "JOBID.PARAM=VALUE")]
        params: Vec<String>,
        /// Repeatable. KEY=VALUE pairs written into flow.toml [tags].
        #[arg(long = "tag", value_name = "KEY=VALUE")]
        tags: Vec<String>,
        /// Print only the created `<root>/<uuid>` path to stdout.
        #[arg(long)]
        print_path: bool,
        /// List available flow recipes and exit (no scaffold).
        #[arg(long)]
        list: bool,
        /// Describe the given recipe's nodes/params and exit (no scaffold).
        #[arg(long)]
        describe: bool,
    },
```

- [ ] **Step 4: Update the `main` dispatch** — replace the `Cmd::New { ref tags, print_path } => { … }` arm (~90-96) with:

```rust
        Cmd::New {
            ref recipe,
            ref params,
            ref tags,
            print_path,
            list,
            describe,
        } => {
            let root = resolve_root(&cli)?;
            cmd_new(&root, recipe.as_deref(), params, tags, print_path, list, describe).await
        }
```

- [ ] **Step 5: Stub `cmd_new` new signature** — change `cmd_new`'s signature only (full body in Task 2) so the crate compiles & the parse test passes. Replace the `async fn cmd_new(root, tags, print_path)` line with:

```rust
async fn cmd_new(
    root: &std::path::Path,
    recipe: Option<&str>,
    params: &[String],
    tags: &[String],
    print_path: bool,
    list: bool,
    describe: bool,
) -> anyhow::Result<()> {
```

and at the very top of the existing body add (Task 2 replaces the whole body):

```rust
    let _ = (recipe, params, list, describe); // wired in Task 2
```

- [ ] **Step 6: Run → pass**

Run: `cargo test --bin jm --no-default-features cli_parses_new_recipe 2>&1 | tail -8`
Expected: PASS — `cli_parses_new_recipe_param_list_describe` ok; crate compiles. (Run only the named test here; whole `cargo test --bin jm` may not build until Task 2/4 — expected.)

- [ ] **Step 7: Commit**

```bash
git add src/bin/jm.rs
git commit -m "feat(jm): Cmd::New gains recipe/--param/--list/--describe"
```

---

## Task 2: `cmd_new` rewrite — recipe routing + assemble + write + rollback

**Files:**
- Modify: `src/bin/jm.rs` (`cmd_new` body)
- Test: `src/bin/jm.rs` inline + Task 7 integration

- [ ] **Step 1: Replace the entire `cmd_new` body** (everything between the signature `{` and its closing `}`) with:

```rust
    use job_manager::persistence::PathResolver;
    use job_manager::recipes;

    if list {
        print!("{}", recipes::format_list());
        return Ok(());
    }

    let recipe_name = recipe.unwrap_or("blank");
    let flow_recipe = recipes::find_flow(recipe_name).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown recipe {recipe_name:?}; available: {}",
            recipes::flow_registry()
                .iter()
                .map(|r| r.name())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })?;

    if describe {
        print!(
            "{}",
            recipes::format_describe(recipe_name).map_err(|e| anyhow::anyhow!(e))?
        );
        return Ok(());
    }

    let mut tag_map = std::collections::BTreeMap::new();
    for raw in tags {
        let (k, v) = parse_tag(raw)?;
        tag_map.insert(k, v);
    }

    let uuid = uuid::Uuid::now_v7();
    let resolver = PathResolver::new(root);
    let flow_dir = resolver.flow_dir(&uuid);
    if flow_dir.exists() {
        anyhow::bail!("flow dir already exists: {}", flow_dir.display());
    }
    let created_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    tokio::fs::create_dir_all(&flow_dir).await?;

    // Everything that can fail after mkdir is wrapped so the half-written
    // flow dir is rolled back (matches the legacy `jm new` contract).
    let result: anyhow::Result<Vec<std::path::PathBuf>> = (|| {
        let mut written: Vec<std::path::PathBuf> = Vec::new();

        if recipe_name == "blank" {
            let flow_str =
                recipes::flows::blank::blank_flow_toml(&uuid, &created_at, &tag_map);
            let plan_str = recipes::flows::blank::blank_plan_toml();
            let fp = resolver.flow_toml(&uuid);
            let pp = resolver.plan_toml(&uuid);
            atomic_write_str(&fp, &flow_str)?;
            atomic_write_str(&pp, &plan_str)?;
            written.push(fp);
            written.push(pp);
            return Ok(written);
        }

        let files = recipes::assemble(
            flow_recipe.as_ref(),
            params,
            &tag_map,
            &uuid,
            &created_at,
            &flow_dir,
        )
        .map_err(|e| anyhow::anyhow!(e))?;

        for gf in &files {
            let dest = flow_dir.join(&gf.relpath);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            atomic_write_str(&dest, &gf.contents)?;
            #[cfg(unix)]
            if let Some(mode) = gf.unix_mode {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(mode))?;
            }
            written.push(dest);
        }

        // input_coordinate post-processing (the only post-assemble I/O).
        for raw in params {
            let Some((key, src)) = raw.split_once('=') else {
                continue;
            };
            let Some((jid, pname)) = key.split_once('.') else {
                continue;
            };
            if pname != "input_coordinate" || src.is_empty() {
                continue;
            }
            let src_path = std::path::Path::new(src);
            if !src_path.is_file() {
                anyhow::bail!("input_coordinate {src:?}: not found");
            }
            let base = src_path
                .file_name()
                .ok_or_else(|| anyhow::anyhow!("input_coordinate {src:?}: no file name"))?;
            let input_dir = flow_dir.join(jid).join("input");
            std::fs::create_dir_all(&input_dir)?;
            let copied = input_dir.join(base);
            std::fs::copy(src_path, &copied)?;
            written.push(copied.clone());

            if src_path.extension().and_then(|e| e.to_str()) == Some("xyz") {
                let xyz = std::fs::read_to_string(src_path)?;
                let geom = recipes::parse_xyz(&xyz)
                    .map_err(|e| anyhow::anyhow!(e))?
                    .join("\n");
                let gjf_path = input_dir.join("main.gjf");
                let gjf = std::fs::read_to_string(&gjf_path)?;
                let spliced: String = gjf
                    .lines()
                    .map(|l| {
                        if l.contains("<GEOMETRY: REPLACE_ME") {
                            geom.clone()
                        } else {
                            l.to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                atomic_write_str(&gjf_path, &(spliced + "\n"))?;
            }
        }
        Ok(written)
    })();

    let written = match result {
        Ok(w) => w,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&flow_dir);
            return Err(e.context(format!(
                "failed to scaffold flow under {}",
                flow_dir.display()
            )));
        }
    };

    if print_path {
        println!("{}", flow_dir.display());
    } else {
        println!("created flow {uuid} from recipe {recipe_name}");
        for p in &written {
            println!("  {}", p.display());
        }
        println!(
            "next: edit inputs as needed, then `jm --root {} render {uuid}`",
            root.display()
        );
    }
    Ok(())
}
```

Remove the temporary `let _ = (recipe, params, list, describe);` line from Task 1 Step 5 (the real body uses them).

- [ ] **Step 2: Add `cmd_new`-routing unit tests** — add to `src/bin/jm.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn cmd_new_blank_is_structurally_legacy() {
        let dir = tempfile::tempdir().unwrap();
        cmd_new(dir.path(), None, &[], &[], false, false, false)
            .await
            .unwrap();
        let uuid_dir = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .find(|e| e.path().is_dir())
            .unwrap()
            .path();
        let flow = std::fs::read_to_string(uuid_dir.join("flow.toml")).unwrap();
        let plan = std::fs::read_to_string(uuid_dir.join("plan.toml")).unwrap();
        assert!(flow.contains("[jobs.step1]") && flow.contains("[jobs.step2]"));
        assert!(flow.contains(r#"partition = "REPLACE_ME""#));
        assert!(plan.contains("[jobs.step1]") && plan.contains("[jobs.step2]"));
        assert!(!uuid_dir.join("step1").exists(), "blank: no recipe sidecars");
    }

    #[tokio::test]
    async fn cmd_new_unknown_recipe_errors_with_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let e = cmd_new(dir.path(), Some("nope"), &[], &[], false, false, false)
            .await
            .unwrap_err();
        let m = e.to_string();
        assert!(m.contains("unknown recipe") && m.contains("g16-opt-parse"));
    }
```

- [ ] **Step 3: Run**

Run: `cargo test --bin jm --no-default-features cmd_new_ 2>&1 | tail -12`
Expected: PASS — `cmd_new_blank_is_structurally_legacy`, `cmd_new_unknown_recipe_errors_with_candidates`.

- [ ] **Step 4: Commit**

```bash
git add src/bin/jm.rs
git commit -m "feat(jm): cmd_new routes blank vs assemble; input_coordinate copy + .xyz splice; rollback"
```

---

## Task 3: delete the now-duplicated jm.rs boilerplate

**Files:**
- Modify: `src/bin/jm.rs` (delete `build_flow_template`, `build_plan_template`, + their unit tests; keep `atomic_write_str`, `parse_tag`)

- [ ] **Step 1: Delete the two functions**

Remove `fn build_plan_template() -> String { … }` (~495-510) and `fn build_flow_template(uuid, created_at, tags) -> String { … }` (~512-574) entirely. Replaced by `recipes::flows::blank::{blank_plan_toml, blank_flow_toml}` (Plan B), now called from `cmd_new`.

- [ ] **Step 2: Delete their unit tests**

In `mod tests`, delete `plan_template_parses_as_experiment_plan`, `flow_template_parses_with_two_step_dag_and_partition`, `flow_template_renders_tags_section` (equivalent coverage now in `src/recipes/flows/blank.rs`). Keep all `parse_tag_*`, `atomic_write_str_*`, `cli_parses_*`, `build_filter_*`, and the new `cmd_new_*` tests.

- [ ] **Step 3: Build — compiler confirms no dangling refs**

Run: `cargo build --no-default-features 2>&1 | tail -5`
Expected: PASS — `Finished`. Any `cannot find function build_*_template` names a stray ref; the only legit caller was the old `cmd_new` (replaced in Task 2).

- [ ] **Step 4: Run jm bin tests**

Run: `cargo test --bin jm --no-default-features 2>&1 | tail -15`
Expected: PASS — remaining jm.rs unit tests all ok.

- [ ] **Step 5: Commit**

```bash
git add src/bin/jm.rs
git commit -m "refactor(jm): drop build_*_template (moved to recipes::flows::blank)"
```

---

## Task 4: render — pure `resolve_runtime_ctx` (4-case / 3-case)

**Files:**
- Modify: `src/render/mod.rs`
- Test: `src/render/mod.rs` (inline)

- [ ] **Step 1: Write the failing tests** — append to `src/render/mod.rs` `mod tests`:

```rust
    #[test]
    fn resolve_runtime_ctx_launcher_4_cases() {
        use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
        use slurm_async_runner::entities::slurm::SlurmJobConfig;
        use std::collections::BTreeMap;
        use std::path::PathBuf;

        fn common(launcher: Option<&str>) -> CommonConfig {
            CommonConfig {
                slurm_default: SlurmJobConfig {
                    partition: "p".into(),
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
                },
                directories: DirectoryConfig {
                    project_root: PathBuf::from("/w"),
                    scratch_root: None,
                },
                launcher: launcher.map(String::from),
            }
        }
        let mut pm: BTreeMap<String, toml::Value> = BTreeMap::new();

        pm.insert("launcher".into(), toml::Value::String("mpirun".into()));
        let ctx = resolve_runtime_ctx(Some(&common(Some("srun"))), &pm);
        assert_eq!(ctx.get("JM_LAUNCHER").map(String::as_str), Some("mpirun"));

        pm.insert("launcher".into(), toml::Value::String(String::new()));
        let ctx = resolve_runtime_ctx(Some(&common(Some("srun"))), &pm);
        assert_eq!(ctx.get("JM_LAUNCHER").map(String::as_str), Some("srun"));

        let ctx = resolve_runtime_ctx(Some(&common(Some(""))), &pm);
        assert_eq!(ctx.get("JM_LAUNCHER").map(String::as_str), Some(""));

        let ctx = resolve_runtime_ctx(Some(&common(None)), &pm);
        assert_eq!(ctx.get("JM_LAUNCHER").map(String::as_str), Some("srun"));
        let ctx = resolve_runtime_ctx(None, &pm);
        assert_eq!(ctx.get("JM_LAUNCHER").map(String::as_str), Some("srun"));
    }

    #[test]
    fn resolve_runtime_ctx_scratch_3_cases() {
        use gaussian_job_shared::config::common::{CommonConfig, DirectoryConfig};
        use slurm_async_runner::entities::slurm::SlurmJobConfig;
        use std::collections::BTreeMap;
        use std::path::PathBuf;

        fn common(scratch: Option<&str>) -> CommonConfig {
            CommonConfig {
                slurm_default: SlurmJobConfig {
                    partition: "p".into(),
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
                },
                directories: DirectoryConfig {
                    project_root: PathBuf::from("/w"),
                    scratch_root: scratch.map(PathBuf::from),
                },
                launcher: None,
            }
        }
        let mut pm: BTreeMap<String, toml::Value> = BTreeMap::new();

        pm.insert(
            "scratch_root".into(),
            toml::Value::String("/flow/scratch".into()),
        );
        let ctx = resolve_runtime_ctx(Some(&common(Some("/cluster/scratch"))), &pm);
        assert_eq!(
            ctx.get("JM_SCRATCH_ROOT").map(String::as_str),
            Some("/flow/scratch")
        );

        pm.insert("scratch_root".into(), toml::Value::String(String::new()));
        let ctx = resolve_runtime_ctx(Some(&common(Some("/cluster/scratch"))), &pm);
        assert_eq!(
            ctx.get("JM_SCRATCH_ROOT").map(String::as_str),
            Some("/cluster/scratch")
        );

        let ctx = resolve_runtime_ctx(Some(&common(None)), &pm);
        assert_eq!(ctx.get("JM_SCRATCH_ROOT").map(String::as_str), Some(""));
    }
```

- [ ] **Step 2: Run → fail**

Run: `cargo test --lib --no-default-features render::tests::resolve_runtime_ctx 2>&1 | tail -6`
Expected: COMPILE FAIL — `cannot find function \`resolve_runtime_ctx\``.

- [ ] **Step 3: Implement** — add to `src/render/mod.rs` (after `render_batch_bash`, before `#[cfg(test)]`):

```rust
use gaussian_job_shared::config::common::CommonConfig;

/// Resolve the render-time runtime context exported into batch.bash:
/// `JM_LAUNCHER` (spec §5.5, 4 cases) and `JM_SCRATCH_ROOT` (spec §5.6,
/// 3 cases). Pure. `common` = materialized `CommonConfig`; `params` =
/// the job's plan.toml param map. A non-empty plan param overrides
/// common; an empty/absent plan param defers to common. Launcher absent
/// from common ⇒ hardcoded `"srun"` (KUDPC default); scratch_root absent
/// ⇒ empty (run.py falls back to `<job_dir>/.scratch`).
pub fn resolve_runtime_ctx(
    common: Option<&CommonConfig>,
    params: &BTreeMap<String, toml::Value>,
) -> BTreeMap<String, String> {
    let param_str = |k: &str| -> Option<String> {
        match params.get(k) {
            Some(toml::Value::String(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        }
    };

    let launcher = if let Some(p) = param_str("launcher") {
        p
    } else {
        match common.and_then(|c| c.launcher.as_deref()) {
            Some(v) => v.to_string(), // Some("") => "" (bare); Some("srun") => "srun"
            None => "srun".to_string(),
        }
    };

    let scratch = if let Some(p) = param_str("scratch_root") {
        p
    } else {
        common
            .and_then(|c| c.directories.scratch_root.as_ref())
            .map(|p| p.display().to_string())
            .unwrap_or_default()
    };

    let mut m = BTreeMap::new();
    m.insert("JM_LAUNCHER".to_string(), launcher);
    m.insert("JM_SCRATCH_ROOT".to_string(), scratch);
    m
}
```

(`BTreeMap` is already imported at the top of `src/render/mod.rs` via `use std::collections::BTreeMap;`.)

- [ ] **Step 4: Run → pass + commit**

Run: `cargo test --lib --no-default-features render::tests::resolve_runtime_ctx 2>&1 | tail -8`
Expected: PASS — `resolve_runtime_ctx_launcher_4_cases`, `resolve_runtime_ctx_scratch_3_cases` ok.
```bash
git add src/render/mod.rs
git commit -m "feat(render): pure resolve_runtime_ctx (launcher 4-case / scratch 3-case)"
```

---

## Task 5: render — additive `render_batch_bash_with_ctx` (old fn output unchanged)

**Files:**
- Modify: `src/render/mod.rs`, `src/lib.rs`
- Test: `src/render/mod.rs` (inline)

- [ ] **Step 1: Write failing tests** — append to `src/render/mod.rs` `mod tests`:

```rust
    #[test]
    fn empty_ctx_is_byte_identical_to_render_batch_bash() {
        use crate::jobid::JobIdParts;
        use gaussian_job_shared::entities::workflow::JobId;
        use std::collections::BTreeMap;
        use uuid::Uuid;

        let u = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
        let jid = JobId("opt__a=0".into());
        let parts = JobIdParts {
            source_step_id: "opt",
            axis_combo: vec![("a", 0)],
        };
        let mut params: BTreeMap<String, toml::Value> = BTreeMap::new();
        params.insert("route".into(), toml::Value::String("#p".into()));
        let body = "bash scripts/opt.bash\n";

        let old = render_batch_bash(&u, &jid, &parts, &params, body);
        let new_empty =
            render_batch_bash_with_ctx(&u, &jid, &parts, &params, &BTreeMap::new(), body);
        assert_eq!(old, new_empty, "empty ctx must reproduce legacy output");
    }

    #[test]
    fn nonempty_ctx_emits_resolved_runtime_block_before_body() {
        use crate::jobid::JobIdParts;
        use gaussian_job_shared::entities::workflow::JobId;
        use std::collections::BTreeMap;
        use uuid::Uuid;

        let u = Uuid::parse_str("01997cdc-0000-7000-8000-000000000000").unwrap();
        let jid = JobId("opt".into());
        let parts = JobIdParts {
            source_step_id: "opt",
            axis_combo: vec![],
        };
        let params: BTreeMap<String, toml::Value> = BTreeMap::new();
        let mut ctx: BTreeMap<String, String> = BTreeMap::new();
        ctx.insert("JM_LAUNCHER".into(), "srun".into());
        ctx.insert("JM_SCRATCH_ROOT".into(), String::new());
        let body = "bash scripts/opt.bash\n";

        let out = render_batch_bash_with_ctx(&u, &jid, &parts, &params, &ctx, body);
        let i_launcher = out.find("export JM_LAUNCHER='srun'").expect("JM_LAUNCHER");
        let i_scratch = out.find("export JM_SCRATCH_ROOT=''").expect("JM_SCRATCH_ROOT");
        let i_body = out.find("bash scripts/opt.bash").unwrap();
        assert!(i_launcher < i_body && i_scratch < i_body);
    }
```

- [ ] **Step 2: Run → fail**

Run: `cargo test --lib --no-default-features render::tests 2>&1 | grep -E 'ctx|error\[' | tail -6`
Expected: COMPILE FAIL — `cannot find function \`render_batch_bash_with_ctx\``.

- [ ] **Step 3: Refactor `render_batch_bash` to delegate** — in `src/render/mod.rs`, replace the existing `pub fn render_batch_bash(...) -> String { … }` (the whole function) with these two functions:

```rust
pub fn render_batch_bash(
    flow_uuid: &Uuid,
    jid: &JobId,
    parts: &JobIdParts<'_>,
    params: &BTreeMap<String, toml::Value>,
    body: &str,
) -> String {
    render_batch_bash_with_ctx(flow_uuid, jid, parts, params, &BTreeMap::new(), body)
}

/// Like `render_batch_bash` but also emits a `# --- job-manager runtime
/// context (resolved) ---` block of POSIX-quoted `export K=V` lines
/// AFTER the `JM_PARAM_*` block and BEFORE the body, when `runtime_ctx`
/// is non-empty. With an empty `runtime_ctx` the output is byte-identical
/// to the pre-existing `render_batch_bash` (regression-locked).
pub fn render_batch_bash_with_ctx(
    flow_uuid: &Uuid,
    jid: &JobId,
    parts: &JobIdParts<'_>,
    params: &BTreeMap<String, toml::Value>,
    runtime_ctx: &BTreeMap<String, String>,
    body: &str,
) -> String {
    let mut s = String::new();
    s.push_str("#!/bin/bash\n");
    s.push_str("# Generated by job_manager SP-3. Do not edit; regenerated on every `jm run`.\n");
    s.push_str("\n# --- job-manager runtime context ---\n");
    s.push_str(&format!(
        "export JM_FLOW_UUID={}\n",
        quote_for_bash(&flow_uuid.to_string())
    ));
    s.push_str(&format!("export JM_JOB_ID={}\n", quote_for_bash(&jid.0)));
    for (axis, idx) in &parts.axis_combo {
        let key = sanitize_var_name(axis);
        s.push_str(&format!(
            "export JM_AXIS_{}={}\n",
            key,
            quote_for_bash(&idx.to_string())
        ));
    }
    s.push_str("\n# --- plan.toml params ---\n");
    for (k, v) in params {
        let key = sanitize_var_name(k);
        let val = toml_value_to_string(v);
        s.push_str(&format!("export JM_PARAM_{}={}\n", key, quote_for_bash(&val)));
    }
    if !runtime_ctx.is_empty() {
        s.push_str("\n# --- job-manager runtime context (resolved) ---\n");
        for (k, v) in runtime_ctx {
            s.push_str(&format!("export {}={}\n", k, quote_for_bash(v)));
        }
    }
    s.push_str("\n# --- user body (JobSpec.body) ---\n");
    s.push_str(body);
    if !body.ends_with('\n') {
        s.push('\n');
    }
    s
}
```

(The body of `render_batch_bash_with_ctx` is the verbatim former `render_batch_bash` body with one added gated block. For empty `runtime_ctx` the byte sequence is unchanged ⇒ existing render tests + `tests/integration_*` stay green.)

- [ ] **Step 4: Run → pass; existing render tests green**

Run: `cargo test --lib --no-default-features render:: 2>&1 | tail -15`
Expected: PASS — pre-existing `render_batch_bash_produces_expected_sections`, `render_batch_bash_escapes_single_quote_in_param`, plus new `empty_ctx_is_byte_identical_to_render_batch_bash`, `nonempty_ctx_emits_resolved_runtime_block_before_body`, and Task 4's `resolve_runtime_ctx_*`.

- [ ] **Step 5: Verify PyO3 / `.pyi` unchanged**

Run: `cargo run --bin stub_gen 2>&1 | tail -2 && git status --porcelain python/job_manager/_job_manager_core 2>&1 | tail -3`
Expected: stub_gen runs; **no `.pyi` change** (`render_batch_bash` signature unchanged; `render_batch_bash_with_ctx` is NOT added to `src/py_export/` — spec §11). A `.pyi` diff means `src/py_export/render.rs` referenced the new fn — it must not.

- [ ] **Step 6: Re-export + commit**

In `src/lib.rs` change `pub use render::render_batch_bash;` to:
```rust
pub use render::{render_batch_bash, render_batch_bash_with_ctx, resolve_runtime_ctx};
```
```bash
git add src/render/mod.rs src/lib.rs
git commit -m "feat(render): additive render_batch_bash_with_ctx; old fn delegates (output/ABI unchanged)"
```

---

## Task 6: wire resolved ctx into both `FlowRunner` render call sites

**Files:**
- Modify: `src/runner/flow.rs` (`submit` render loop + `render_only`)
- Test: `src/runner/flow.rs` (inline) + `tests/integration_sp3.rs` regression

- [ ] **Step 1: Locate both call sites**

Run: `grep -n 'render_batch_bash' src/runner/flow.rs`
Expected: 3 hits — the `use crate::render::render_batch_bash;` import, one call in `submit` (~245), one call in `render_only`.

- [ ] **Step 2: Write a failing regression test** — append to `src/runner/flow.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn submit_dry_run_bakes_jm_launcher_default_srun_without_common() {
        use crate::persistence::PathResolver;
        use crate::slurm::executor::DryRunExecutor;
        use crate::slurm::querier::InMemoryQuerier;
        use std::collections::HashMap;

        let dir = tempfile::tempdir().unwrap();
        let resolver = PathResolver::new(dir.path());
        let mut fr = crate::flow::run::tests::fr_with_2_jobs();
        fr.flow_uuid = uuid::Uuid::now_v7();
        fr.flow.uuid = fr.flow_uuid;
        let runner = FlowRunner::new(
            Box::new(DryRunExecutor),
            Box::new(InMemoryQuerier::new(HashMap::new())),
            &resolver,
        );
        runner.submit(&fr, true).await.unwrap();
        let jid = fr.topological_order().unwrap()[0].clone();
        let bb = std::fs::read_to_string(resolver.batch_bash(&fr.flow_uuid, &jid)).unwrap();
        assert!(bb.contains("export JM_LAUNCHER='srun'"), "got:\n{bb}");
        assert!(bb.contains("export JM_SCRATCH_ROOT=''"));
    }
```

(`crate::flow::run::tests::fr_with_2_jobs()` is `pub(crate)` and unchanged by Plan A/B; its jobs carry no `launcher`/`scratch_root` plan params and `common: None` ⇒ `resolve_runtime_ctx(None, {})` ⇒ `JM_LAUNCHER=srun`, `JM_SCRATCH_ROOT=""`. `resolver.batch_bash` is the existing PathResolver accessor used elsewhere in this module.)

- [ ] **Step 3: Run → fail**

Run: `cargo test --lib --no-default-features runner::flow::tests::submit_dry_run_bakes_jm_launcher 2>&1 | tail -8`
Expected: FAIL — assertion fails (`JM_LAUNCHER` absent; the call site still uses `render_batch_bash` with no ctx).

- [ ] **Step 4: Switch both call sites**

In `src/runner/flow.rs`:
- Change the import `use crate::render::render_batch_bash;` to:
  ```rust
  use crate::render::{render_batch_bash_with_ctx, resolve_runtime_ctx};
  ```
- In `submit`, replace the line
  `let script_content = render_batch_bash(&fr.flow_uuid, jid, &parts, params, &job.spec.body);`
  with:
  ```rust
  let ctx = resolve_runtime_ctx(fr.common.as_ref(), params);
  let script_content = render_batch_bash_with_ctx(
      &fr.flow_uuid,
      jid,
      &parts,
      params,
      &ctx,
      &job.spec.body,
  );
  ```
- In `render_only` (Step 1 hit on the call, not the import), apply the **same** transformation: insert a `let ctx = resolve_runtime_ctx(fr.common.as_ref(), <its params binding>);` line immediately before its `render_batch_bash(...)` call and swap that call to `render_batch_bash_with_ctx(<same first 4 args>, &ctx, <same body arg>)`. Use whatever local bindings `render_only` already passes to `render_batch_bash` (same arg expressions, only inserting `&ctx` before the body arg). Do not otherwise alter `render_only`.

- [ ] **Step 5: Run → pass + regressions**

Run: `cargo test --lib --no-default-features runner::flow 2>&1 | tail -15 && cargo test --test integration_sp3 --no-default-features 2>&1 | tail -8`
Expected: PASS — new `submit_dry_run_bakes_jm_launcher_default_srun_without_common`, all pre-existing `runner::flow` tests, and `tests/integration_sp3.rs` unchanged-green (the runtime-context block is additive; sp3 fixtures have no `launcher` plan param ⇒ they now also get `JM_LAUNCHER='srun'` baked, which does not touch their lifecycle/transition assertions).

- [ ] **Step 6: Commit**

```bash
git add src/runner/flow.rs
git commit -m "feat(runner): resolve+export JM_LAUNCHER/JM_SCRATCH_ROOT at render (submit + render_only)"
```

---

## Task 7: end-to-end integration tests

**Files:**
- Create: `tests/integration_new_recipes.rs`
- Verify (no edit): `tests/integration_new.rs`

- [ ] **Step 1: Create `tests/integration_new_recipes.rs`**

```rust
//! End-to-end tests for `jm new <recipe>` (Plan C).

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

fn sole_flow_uuid(root: &std::path::Path) -> String {
    let e: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .filter_map(|x| x.ok())
        .filter(|x| x.path().is_dir())
        .collect();
    assert_eq!(e.len(), 1, "expected exactly one flow dir");
    e[0].file_name().to_string_lossy().into_owned()
}

#[test]
fn jm_new_list_and_describe_do_not_scaffold() {
    let dir = tempdir().unwrap();
    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", dir.path().to_str().unwrap(), "new", "--list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("g16-opt-parse"));
    Command::cargo_bin("jm")
        .unwrap()
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "new",
            "g16-opt-parse",
            "--describe",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("opt.route"));
    let n = std::fs::read_dir(dir.path()).unwrap().count();
    assert_eq!(n, 0, "--list/--describe must not create a flow dir");
}

#[test]
fn jm_new_g16_opt_parse_scaffolds_and_renders() {
    let dir = tempdir().unwrap();
    Command::cargo_bin("jm")
        .unwrap()
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "new",
            "g16-opt-parse",
            "--param",
            "opt.charge=1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("created flow"));

    let u = sole_flow_uuid(dir.path());
    let base = std::fs::canonicalize(dir.path()).unwrap().join(&u);
    for rel in [
        "flow.toml",
        "plan.toml",
        "opt/scripts/opt.bash",
        "opt/scripts/run.py",
        "opt/input/main.gjf",
        "parse/scripts/parse.bash",
        "parse/scripts/parse.py",
    ] {
        assert!(base.join(rel).exists(), "missing {rel}");
    }
    let gjf = std::fs::read_to_string(base.join("opt/input/main.gjf")).unwrap();
    assert!(gjf.contains("1 1"), "charge=1 multiplicity=1 line");
    assert!(!gjf.contains("%rwf"));
    let optbash = std::fs::read_to_string(base.join("opt/scripts/opt.bash")).unwrap();
    assert!(optbash.contains("module restore gaussian_A -f"));
    assert!(optbash.contains("python scripts/run.py"));
    let flow = std::fs::read_to_string(base.join("flow.toml")).unwrap();
    assert!(
        flow.contains(&format!("{}/opt", base.display())),
        "R3 absolute cd to opt job dir"
    );

    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", dir.path().to_str().unwrap(), "doctor", &u])
        .assert()
        .success();

    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", dir.path().to_str().unwrap(), "render", &u])
        .assert()
        .success();
    let opt_batch = base.join(".jm/opt/batch.bash");
    let bb = std::fs::read_to_string(&opt_batch)
        .unwrap_or_else(|_| panic!("missing {}", opt_batch.display()));
    assert!(bb.contains("export JM_LAUNCHER='srun'"));
    assert!(bb.contains("export JM_SCRATCH_ROOT=''"));
}

#[test]
fn jm_new_input_coordinate_xyz_is_copied_and_spliced() {
    let dir = tempdir().unwrap();
    let xyz = dir.path().join("mol.xyz");
    std::fs::write(&xyz, "2\nethyne-ish\nC 0.0 0.0 0.0\nH 0.0 0.0 1.07\n").unwrap();
    Command::cargo_bin("jm")
        .unwrap()
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "new",
            "g16-opt-parse",
            "--param",
            &format!("opt.input_coordinate={}", xyz.display()),
        ])
        .assert()
        .success();
    let u = sole_flow_uuid(dir.path());
    let base = std::fs::canonicalize(dir.path()).unwrap().join(&u);
    assert!(base.join("opt/input/mol.xyz").exists(), "coord copied");
    let gjf = std::fs::read_to_string(base.join("opt/input/main.gjf")).unwrap();
    assert!(!gjf.contains("REPLACE_ME"), "geometry sentinel replaced");
    assert!(gjf.contains("C "), "carbon geometry spliced");
    assert!(gjf.contains("1.070000"), "z coord formatted");
}

#[test]
fn jm_new_input_coordinate_missing_src_fails_and_rolls_back() {
    let dir = tempdir().unwrap();
    Command::cargo_bin("jm")
        .unwrap()
        .args([
            "--root",
            dir.path().to_str().unwrap(),
            "new",
            "g16-opt-parse",
            "--param",
            "opt.input_coordinate=/no/such/file.xyz",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
    let n = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .count();
    assert_eq!(n, 0, "flow dir rolled back on failure");
}

#[test]
fn jm_new_blank_is_backward_compatible() {
    let dir = tempdir().unwrap();
    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", dir.path().to_str().unwrap(), "new"])
        .assert()
        .success()
        .stdout(predicate::str::contains("created flow"));
    let u = sole_flow_uuid(dir.path());
    let base = std::fs::canonicalize(dir.path()).unwrap().join(&u);
    assert!(base.join("flow.toml").exists());
    assert!(base.join("plan.toml").exists());
    assert!(!base.join("step1").exists(), "blank has no recipe sidecars");
    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", dir.path().to_str().unwrap(), "render", &u])
        .assert()
        .success()
        .stdout(predicate::str::contains("rendered 2 jobs"));
}

#[test]
fn jm_new_unknown_recipe_lists_candidates() {
    let dir = tempdir().unwrap();
    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", dir.path().to_str().unwrap(), "new", "bogus"])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("unknown recipe")
                .and(predicate::str::contains("g16-opt-parse")),
        );
}
```

- [ ] **Step 2: Run the new suite**

Run: `cargo test --test integration_new_recipes --no-default-features 2>&1 | tail -15`
Expected: PASS — all 6 tests. (The `.jm/opt/batch.bash` path follows CLAUDE.md PathResolver `batch.bash = <root>/<uuid>/.jm/<JobId>/batch.bash`. If the actual layout differs, adjust the `opt_batch` join to match `PathResolver::batch_bash`; the asserted content is the contract.)

- [ ] **Step 3: Verify the byte-identity gate stays green (unedited)**

Run: `cargo test --test integration_new --no-default-features 2>&1 | tail -10`
Expected: PASS — `jm_new_creates_flow_and_plan_and_renders`, `jm_new_print_path_emits_only_the_dir`, `jm_new_writes_tag_into_flow`, `jm_new_rejects_malformed_tag` all green **without editing `tests/integration_new.rs`**. This proves `jm new` (no recipe) is byte-compatible with the legacy behavior. If any fails, the `blank_*` text in `src/recipes/flows/blank.rs` (Plan B) drifted from the legacy template — fix the drift there, not the test.

- [ ] **Step 4: Commit**

```bash
git add tests/integration_new_recipes.rs
git commit -m "test(jm): e2e jm new recipes (list/describe/g16-opt-parse/input_coordinate/blank-compat)"
```

---

## Task 8: python smoke for the generated `run.py` / `parse.py`

**Files:**
- Create: `python/tests/test_recipe_scripts.py`

- [ ] **Step 1: Create `python/tests/test_recipe_scripts.py`**

```python
"""Smoke the generated run.py / parse.py from `jm new g16-opt-parse`.

run.py is exercised with g16 stubbed (JM_PARAM_G16_CMD points at a fake
that writes main.out), asserting the prepare->run(cwd=scratch)->finally
copy contract + non-silent launch failure. parse.py is checked for its
cclib-missing exit code (cclib is not a test dependency).
"""
import os
import subprocess
import sys
from pathlib import Path


def _jm_bin() -> str:
    exe = os.environ.get("CARGO_BIN_EXE_jm")
    if exe:
        return exe
    return str(Path(__file__).resolve().parents[2] / "target" / "debug" / "jm")


def _scaffold(tmp_path: Path) -> Path:
    root = tmp_path / "root"
    root.mkdir()
    r = subprocess.run(
        [_jm_bin(), "--root", str(root), "new", "g16-opt-parse"],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
    return next(p for p in root.iterdir() if p.is_dir())


def test_run_py_prepares_runs_and_copies_back(tmp_path: Path):
    job = _scaffold(tmp_path)
    opt = job / "opt"
    fake = tmp_path / "fakeg16"
    fake.write_text("#!/bin/bash\necho 'Normal termination' > main.out\nexit 0\n")
    fake.chmod(0o755)
    env = dict(os.environ)
    env["JM_PARAM_G16_CMD"] = str(fake)
    env["JM_LAUNCHER"] = ""  # bare (no srun in CI)
    env["JM_SCRATCH_ROOT"] = str(tmp_path / "scratch")
    env["JM_FLOW_UUID"] = job.name
    env["JM_JOB_ID"] = "opt"
    p = subprocess.run(
        [sys.executable, "scripts/run.py"],
        cwd=opt,
        env=env,
        capture_output=True,
        text=True,
    )
    assert p.returncode == 0, p.stderr
    out = opt / "output" / "main.out"
    assert out.is_file(), "copy_results ran"
    assert "Normal termination" in out.read_text()


def test_run_py_missing_binary_is_nonzero_not_silent(tmp_path: Path):
    job = _scaffold(tmp_path)
    opt = job / "opt"
    env = dict(os.environ)
    env["JM_PARAM_G16_CMD"] = "definitely-not-a-real-binary-xyzzy"
    env["JM_LAUNCHER"] = ""
    env["JM_SCRATCH_ROOT"] = str(tmp_path / "scratch")
    env["JM_FLOW_UUID"] = job.name
    env["JM_JOB_ID"] = "opt"
    p = subprocess.run(
        [sys.executable, "scripts/run.py"],
        cwd=opt,
        env=env,
        capture_output=True,
        text=True,
    )
    assert p.returncode != 0, "missing g16 must NOT silently succeed"
    assert "failed to launch" in p.stderr


def test_parse_py_without_cclib_exits_2(tmp_path: Path):
    job = _scaffold(tmp_path)
    parse = job / "parse"
    p = subprocess.run(
        [sys.executable, "scripts/parse.py"],
        cwd=parse,
        env=dict(os.environ),
        capture_output=True,
        text=True,
    )
    if p.returncode == 2:
        assert "cclib not importable" in p.stderr
    else:
        # cclib happens to be installed: the .out is missing => exit 1.
        assert p.returncode == 1, (p.returncode, p.stderr)
```

- [ ] **Step 2: Build the `jm` binary the smoke shells out to**

Run: `cargo build --no-default-features --bin jm 2>&1 | tail -2`
Expected: `Finished`.

- [ ] **Step 3: Run the smoke**

Run: `uv run pytest python/tests/test_recipe_scripts.py -v 2>&1 | tail -12`
Expected: PASS — 3 tests ok.

- [ ] **Step 4: Commit**

```bash
git add python/tests/test_recipe_scripts.py
git commit -m "test(python): smoke generated run.py/parse.py (prepare/copy/exit, cclib-missing)"
```

---

## Task 9: docs + full CI gate + push

**Files:**
- Modify: `README.md`, `docs/API.md`, `CLAUDE.md` (CLAUDE.md gitignored — on-disk only)

- [ ] **Step 1: Update `README.md`** — in the `jm` CLI usage section, replace the `jm new` line/paragraph with:

```markdown
### `jm new [<flow-recipe>]`

- `jm --root <root> new` — legacy 2-job `step1 -> step2` boilerplate (unchanged).
- `jm --root <root> new --list` — list available flow recipes.
- `jm --root <root> new <recipe> --describe` — show a recipe's nodes + typed params.
- `jm --root <root> new g16-opt-parse --param opt.charge=1 [--param opt.input_coordinate=mol.xyz]`
  — scaffold a Gaussian opt → (afterok) cclib-validate flow. Edit the
  generated `<uuid>/opt/scripts/run.py` / `input/main.gjf`, then `jm render`.

Launcher (`srun`) and scratch root resolve at `jm render`/`submit` time
from `<root>/common.toml` (`launcher`, `[directories] scratch_root`),
overridable per-flow via `--param <job>.launcher=` / `.scratch_root=`.
```

- [ ] **Step 2: Update `docs/API.md`** — add a `## recipes` section listing the public surface with one-line descriptions matching the doc comments: `recipes::assemble`, `FlowRecipe`/`JobTemplate` traits, `flow_registry`, `find_flow`, `format_list`, `format_describe`, `parse_xyz`, `base_preamble`, `render::resolve_runtime_ctx`, `render::render_batch_bash_with_ctx`. State explicitly: `render_batch_bash` is unchanged and remains the PyO3-exported entry; the new functions are Rust-only.

- [ ] **Step 3: Update `CLAUDE.md`** (on-disk only — globally gitignored) — in `## Common commands` `jm CLI` cheatsheet, extend the usage line to `... {render|submit|tick|show|doctor|ls|new} ...` showing `new [recipe] [--param k=v] [--list|--describe]`, and add one sentence: "recipe sidecars (`scripts/run.py`/`parse.py`) reproduce `gaussian_compute_runtime.run_g16`/`parse_results` in self-contained stdlib; launcher/scratch_root resolve at render from `common.toml`."

- [ ] **Step 4: Full CLAUDE.md CI gate**

Run:
```bash
cargo fmt --check && \
cargo clippy --all-targets --all-features -- -D warnings && \
cargo test --all-features 2>&1 | tail -30 && \
uv run pytest python/tests -v 2>&1 | tail -12
```
Expected: fmt clean; clippy no warnings; **all** Rust suites `ok` — notably `tests/integration_new` (byte-identity, unedited), `tests/integration_new_recipes` (new), `tests/integration_sp3`/`integration_effective_isolation`/`integration_walk`/`integration_listing` (unchanged-green; the additive runtime block does not alter their assertions), `render::*`, `runner::flow::*`, `recipes::*`, jm bin tests; pytest all pass incl. `test_recipe_scripts.py`.

- [ ] **Step 5: Verify `.pyi` did not drift**

Run: `cargo run --bin stub_gen 2>&1 | tail -1 && uv run ruff format python/ >/dev/null 2>&1; git status --porcelain python/job_manager/_job_manager_core 2>&1 | tail -3`
Expected: no modified `.pyi` (PyO3 surface unchanged). A `.pyi` change ⇒ investigate `src/py_export/` (nothing there should reference the new fns).

- [ ] **Step 6: If fmt failed, format + amend**

Run: `cargo fmt && git add -u && git commit --amend --no-edit` (only if Step 4 fmt failed).

- [ ] **Step 7: Commit docs + push**

```bash
git add README.md docs/API.md
git commit -m "docs: jm new <recipe> surface + recipes API"
git branch --show-current && git push 2>&1 | tail -2
```
(`CLAUDE.md` intentionally NOT `git add`-ed — globally gitignored.)

- [ ] **Step 8: Plan C done — verify end-to-end exit criteria**

- `jm new` (no recipe) byte-compatible with legacy (`tests/integration_new` green, unedited).
- `jm new g16-opt-parse --param opt.charge=1` scaffolds doctor-clean flow with sidecars; `jm doctor` exit 0; `jm render` exit 0 + `batch.bash` carries `export JM_LAUNCHER='srun'` + `export JM_SCRATCH_ROOT=''`.
- `--param opt.input_coordinate=<.xyz>` copies + splices geometry; missing src ⇒ fail + rollback.
- `--list`/`--describe` print, no scaffold.
- `resolve_runtime_ctx` 4/3-case unit-tested; `render_batch_bash` output & PyO3 signature unchanged (`.pyi` no drift); submit & render_only both export resolved ctx.
- `build_*_template` deleted from jm.rs; no dangling refs.
- python smoke proves run.py prepare/copy/exit-precedence + non-silent launch failure; parse.py cclib-missing ⇒ exit 2.
- Full CI gate green.

---

## Self-Review

**1. Spec coverage (rev.6 §3, §4 cmd_new, §5.1/§5.5/§5.6, §7, §8, §9, §10, §11):**
- §3 CLI form / `Cmd::New` extension / `cmd_new(root, recipe, params, tags, print_path, list, describe)` → Tasks 1,2. ✓
- §4 `cmd_new` sequence (list→resolve→describe→tag→uuid→collision→created_at→assemble→input_coordinate→write/chmod→rollback→output/print_path) → Task 2. ✓
- §5.1 R3 baked by `assemble` (Plan B); Task 7 asserts the absolute `cd` in flow.toml. ✓
- §5.5/§5.6 4-case/3-case render-time, additive renderer, `render_batch_bash` unchanged, both submit+render_only → Tasks 4,5,6. ✓
- §7 generated tree + gjf no `%rwf` + `1 1` → Task 7. ✓
- §8 blank byte-identical / backward compat → Task 2 routing + Task 3 dedup + Task 7 `jm_new_blank_is_backward_compatible` + unedited `tests/integration_new.rs` (Task 7 Step 3). ✓
- §9 errors (unknown recipe+candidates; input_coordinate not-found; rollback) → Tasks 2,7. ✓
- §10 tests (unit render/cmd_new; integration; core-invariance `integration_sp3`; python smoke) → Tasks 4,5,6,7,8. ✓
- §11 (PyO3/`.pyi`/`render_batch_bash` unchanged; Rust-only additions; CLAUDE.md gitignored; Conventional Commits/per-task) → Tasks 5,9. ✓

**2. Placeholder scan:** Every code block is complete; commands carry expected output. Task 6 Step 4's render_only edit is bounded (Step 1 grep yields the exact call; transformation is mechanical: same args + inserted `&ctx`). Task 1 Step 5's temporary `let _ = (...)` has an explicit removal in Task 2 Step 1. No "TBD"/"handle errors"/"etc.". ✓

**3. Type consistency:** `cmd_new(&Path, Option<&str>, &[String], &[String], bool, bool, bool)` identical in Task 1 Step 5, the dispatch (Task 1 Step 4), the body (Task 2). `recipes::{assemble, find_flow, flow_registry, format_list, format_describe, parse_xyz, flows::blank::{blank_flow_toml,blank_plan_toml}, RecipeError}` match Plan B's public surface. `assemble(flow_recipe.as_ref(), params, &tag_map, &uuid, &created_at, &flow_dir)` matches Plan B `assemble(&dyn FlowRecipe, &[String], &BTreeMap<String,String>, &Uuid, &str, &Path)` (`flow_recipe: Box<dyn FlowRecipe>` → `.as_ref()`). `resolve_runtime_ctx(Option<&CommonConfig>, &BTreeMap<String,toml::Value>) -> BTreeMap<String,String>` and `render_batch_bash_with_ctx(&Uuid,&JobId,&JobIdParts,&BTreeMap<String,toml::Value>,&BTreeMap<String,String>,&str)->String` consistent across render/mod.rs (def), lib.rs (re-export), runner/flow.rs (call). `fr.common: Option<CommonConfig>` (flow/run.rs, untouched) → `.as_ref()`; `params = fr.params_of(jid)? : &BTreeMap<String,toml::Value>`. CommonConfig literal in Task 4 tests carries `scratch_root`/`launcher` (Plan A fields) — consistent. ✓

No blocking issues.

---

## Execution Handoff

All three plans complete and committed under `docs/superpowers/plans/`:
- `2026-05-17-jm-new-recipes-planA-d2-launcher-scratch.md`
- `2026-05-17-jm-new-recipes-planB-recipes-core.md`
- `2026-05-17-jm-new-recipes-planC-cli-render.md`

Execution order **A → B → C** (C depends on A's D2 fields + B's recipes module; B depends on A for branch linearity). Two execution options:

1. **Subagent-Driven (recommended)** — `superpowers:subagent-driven-development`: fresh subagent per task, two-stage review between tasks, fast iteration. Plan A Task 1 has a cross-repo (D2) owner PR-merge gate before Plan A Task 2.
2. **Inline Execution** — `superpowers:executing-plans`: batch execution with checkpoints in this session.

Which approach (start at Plan A Task 1)?
