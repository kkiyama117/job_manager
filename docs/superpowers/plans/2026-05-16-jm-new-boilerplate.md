# `jm new` Boilerplate Generator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `jm new` subcommand that mints a fresh UUID v7, creates `<root>/<uuid>/`, and writes commented `flow.toml` + `plan.toml` boilerplate (a 2-job `step1 → step2` DAG) so a new flow can be started in one command.

**Architecture:** Pure template-builder helpers (`build_flow_template`, `build_plan_template`, `parse_tag`) take no I/O and return `String`/parsed values, unit-tested in `src/bin/jm.rs`. `cmd_new` orchestrates: parse `--tag`, mint `Uuid::now_v7()`, collision-check via `PathResolver::flow_dir().exists()`, `create_dir_all`, write both files via a local `atomic_write_str` (PID-suffixed tmp + rename — the lib's `persistence::atomic_write` is `pub(crate)` and unreachable from the `jm` binary crate), rolling back the created dir on any write failure. `common.toml` is deliberately untouched (v1 Non-goal); the template carries `partition = "REPLACE_ME"` so `jm render` succeeds while real `jm submit` fails fast until edited.

**Tech Stack:** Rust 2024 (nightly), `clap` (derive), `uuid` v7, `chrono` RFC3339, `toml`, `tokio::fs`, `assert_cmd` + `predicates` + `tempfile` for tests. Build/test the `jm` binary with `--no-default-features` (no libpython linkage; `cmd_new` touches no pyo3).

**Spec:** `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `src/bin/jm.rs` | CLI entry. Add `Cmd::New` variant, `cmd_new()`, pure helpers `parse_tag`, `build_flow_template`, `build_plan_template`, local `atomic_write_str`, and a `#[cfg(test)] mod tests`. | Modify |
| `tests/integration_new.rs` | `assert_cmd` end-to-end: `jm new` creates files, round-trips through `FlowRun::read`, then `jm render` succeeds; `--print-path` and `--tag` behaviors. | Create |

No other files change. `Cargo.toml` already has every dependency (`uuid` v7, `chrono`, `toml`, `assert_cmd`, `predicates`, `tempfile`).

---

## Conventions for every task

- Build/test the binary **without default features** (CLAUDE.md `jm` linker note):
  `cargo test --bin jm --no-default-features <...>` and `cargo test --test integration_new --no-default-features <...>`.
- Run `cargo fmt` before every commit. Run `cargo clippy --bin jm --no-default-features -- -D warnings` before the final task's commit.
- Conventional Commits, one commit per task.

---

## Task 1: `parse_tag` helper + unit tests

**Files:**
- Modify: `src/bin/jm.rs` (add helper near the bottom, before any `#[cfg(test)]`)
- Test: `src/bin/jm.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Add the failing test module**

Append to the very end of `src/bin/jm.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tag_splits_on_first_equals() {
        assert_eq!(parse_tag("a=b").unwrap(), ("a".to_string(), "b".to_string()));
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
        assert!(
            err.to_string().contains("expected key=value"),
            "got: {err}"
        );
    }

    #[test]
    fn parse_tag_rejects_empty_key() {
        let err = parse_tag("=v").unwrap_err();
        assert!(err.to_string().contains("empty key"), "got: {err}");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin jm --no-default-features parse_tag -- --nocapture`
Expected: FAIL — `cannot find function 'parse_tag' in this scope`.

- [ ] **Step 3: Implement `parse_tag`**

Add to `src/bin/jm.rs` (above the `#[cfg(test)]` module, after the existing functions):

```rust
/// Split a `--tag KEY=VALUE` argument on the first `=`. The value may
/// itself contain `=`. Empty keys are rejected so a stray `=v` cannot
/// produce an unnamed tag.
fn parse_tag(raw: &str) -> anyhow::Result<(String, String)> {
    match raw.split_once('=') {
        Some((k, _)) if k.is_empty() => {
            anyhow::bail!("invalid --tag: empty key in {raw:?}")
        }
        Some((k, v)) => Ok((k.to_string(), v.to_string())),
        None => anyhow::bail!("invalid --tag: expected key=value, got {raw:?}"),
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin jm --no-default-features parse_tag -- --nocapture`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/bin/jm.rs
git commit -m "feat(jm): add parse_tag helper for jm new --tag parsing"
```

---

## Task 2: `build_plan_template` + unit test

**Files:**
- Modify: `src/bin/jm.rs`
- Test: `src/bin/jm.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Add the failing test**

Add inside `mod tests` in `src/bin/jm.rs`:

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin jm --no-default-features plan_template -- --nocapture`
Expected: FAIL — `cannot find function 'build_plan_template' in this scope`.

- [ ] **Step 3: Implement `build_plan_template`**

Add to `src/bin/jm.rs` (above `#[cfg(test)]`):

```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin jm --no-default-features plan_template -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/bin/jm.rs
git commit -m "feat(jm): add build_plan_template for jm new"
```

---

## Task 3: `build_flow_template` (no tags) + unit test

**Files:**
- Modify: `src/bin/jm.rs`
- Test: `src/bin/jm.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Add the failing test**

Add inside `mod tests`:

```rust
#[test]
fn flow_template_parses_with_two_step_dag_and_partition() {
    use gaussian_job_shared::entities::workflow::JobFlow;
    use std::collections::BTreeMap;

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
        .get(&gaussian_job_shared::entities::workflow::JobId("step2".into()))
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin jm --no-default-features flow_template_parses -- --nocapture`
Expected: FAIL — `cannot find function 'build_flow_template' in this scope`.

- [ ] **Step 3: Implement `build_flow_template`**

Add to `src/bin/jm.rs` (above `#[cfg(test)]`). The `tags` parameter is wired now; the empty-map branch emits a comment line so this task's test (empty map) passes, and Task 4 locks the non-empty branch:

```rust
use std::collections::BTreeMap;

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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin jm --no-default-features flow_template_parses -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/bin/jm.rs
git commit -m "feat(jm): add build_flow_template (2-job DAG, REPLACE_ME partition)"
```

---

## Task 4: `--tag` rendering into `[tags]` + unit test

**Files:**
- Modify: `src/bin/jm.rs` (no code change to `build_flow_template` if Task 3's tag branch is correct — this task locks the behavior with a test)
- Test: `src/bin/jm.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Add the test**

Add inside `mod tests`:

```rust
#[test]
fn flow_template_renders_tags_section() {
    use gaussian_job_shared::entities::workflow::JobFlow;
    use std::collections::BTreeMap;

    let uuid = uuid::Uuid::now_v7();
    let mut tags = BTreeMap::new();
    tags.insert("env".to_string(), "prod".to_string());
    tags.insert("owner".to_string(), "a=b".to_string()); // value with '='

    let s = build_flow_template(&uuid, "2026-05-16T00:00:00Z", &tags);
    let flow: JobFlow = toml::from_str(&s).expect("tagged flow template parses");

    assert_eq!(flow.tags.get("env").map(String::as_str), Some("prod"));
    assert_eq!(flow.tags.get("owner").map(String::as_str), Some("a=b"));
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test --bin jm --no-default-features flow_template_renders_tags -- --nocapture`
Expected: PASS (the tag branch was implemented in Task 3 Step 3).

- [ ] **Step 3: (only if Step 2 failed) Fix `build_flow_template` tag branch**

If it FAILS with a TOML parse error on the `[tags]` section, ensure the non-empty branch reads exactly:

```rust
for (k, v) in tags {
    let v_toml = toml::Value::String(v.clone()).to_string();
    tag_lines.push_str(&format!("{k} = {v_toml}\n"));
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin jm --no-default-features flow_template -- --nocapture`
Expected: PASS (all `flow_template*` tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/bin/jm.rs
git commit -m "test(jm): lock --tag rendering into flow.toml [tags] section"
```

---

## Task 5: `atomic_write_str` helper + unit test

**Files:**
- Modify: `src/bin/jm.rs`
- Test: `src/bin/jm.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Add the failing test**

Add inside `mod tests`:

```rust
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
```

`tempfile` is a dev-dependency (already in `Cargo.toml`), available in `#[cfg(test)]`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --bin jm --no-default-features atomic_write_str -- --nocapture`
Expected: FAIL — `cannot find function 'atomic_write_str' in this scope`.

- [ ] **Step 3: Implement `atomic_write_str`**

Add to `src/bin/jm.rs` (above `#[cfg(test)]`):

```rust
/// Atomic write for `jm new`'s generated files. `persistence::atomic_write`
/// is `pub(crate)` and unreachable from this binary crate, so this is a
/// minimal local equivalent: write to a PID-suffixed tmp, fsync, rename
/// over `path`, and clean the tmp on failure. `jm new` never writes the
/// same path concurrently, so PID alone is a sufficient tmp discriminator.
fn atomic_write_str(path: &std::path::Path, body: &str) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --bin jm --no-default-features atomic_write_str -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add src/bin/jm.rs
git commit -m "feat(jm): add local atomic_write_str (pub(crate) lib helper unreachable)"
```

---

## Task 6: Wire `Cmd::New` + `cmd_new` into the CLI

**Files:**
- Modify: `src/bin/jm.rs` — add `New` variant to `enum Cmd`, a match arm in `main`, and `async fn cmd_new`.

- [ ] **Step 1: Add the `New` variant to `enum Cmd`**

In `src/bin/jm.rs`, inside `enum Cmd { ... }`, add after the `Search { ... }` variant:

```rust
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
```

- [ ] **Step 2: Add the match arm in `main`**

In `main`, inside `match cli.cmd { ... }`, add after the `Cmd::Search { ... }` arm:

```rust
        Cmd::New {
            ref tags,
            print_path,
        } => {
            let root = resolve_root(&cli)?;
            cmd_new(&root, tags, print_path).await
        }
```

- [ ] **Step 3: Implement `cmd_new`**

Add to `src/bin/jm.rs` (after `cmd_search`, before the pure helpers `parse_tag` / `build_*`):

```rust
async fn cmd_new(
    root: &std::path::Path,
    tags: &[String],
    print_path: bool,
) -> anyhow::Result<()> {
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
```

- [ ] **Step 4: Build and run the full unit test suite**

Run: `cargo test --bin jm --no-default-features -- --nocapture`
Expected: PASS — all helper tests from Tasks 1–5 still green, binary compiles with the new subcommand.

- [ ] **Step 5: Manual smoke check**

Run:
```bash
cargo build --bin jm --no-default-features
T=$(mktemp -d)
./target/debug/jm --root "$T" new --tag env=dev
ls "$T"/*/flow.toml "$T"/*/plan.toml
./target/debug/jm --root "$T" render "$(ls "$T")"
rm -rf "$T"
```
Expected: `created flow <uuid>` printed, both files listed, `rendered 2 jobs in <uuid>` printed (render succeeds — REPLACE_ME partition is a valid string at read time).

- [ ] **Step 6: Commit**

```bash
cargo fmt
cargo clippy --bin jm --no-default-features -- -D warnings
git add src/bin/jm.rs
git commit -m "feat(jm): add 'jm new' subcommand to scaffold flow.toml + plan.toml"
```

---

## Task 7: End-to-end integration test

**Files:**
- Create: `tests/integration_new.rs`

- [ ] **Step 1: Write the integration test**

Create `tests/integration_new.rs`:

```rust
//! End-to-end tests for `jm new`.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

/// Read the single flow dir name (the minted uuid) under `root`.
fn sole_flow_uuid(root: &std::path::Path) -> String {
    let entries: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    assert_eq!(entries.len(), 1, "expected exactly one flow dir");
    entries[0].file_name().to_string_lossy().into_owned()
}

#[test]
fn jm_new_creates_flow_and_plan_and_renders() {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;

    let dir = tempdir().unwrap();

    // jm new
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root").arg(dir.path()).arg("new");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("created flow"));

    let uuid_str = sole_flow_uuid(dir.path());
    let uuid = uuid::Uuid::parse_str(&uuid_str).expect("flow dir name is a uuid");

    let resolver = PathResolver::new(dir.path());
    assert!(resolver.flow_toml(&uuid).exists(), "flow.toml missing");
    assert!(resolver.plan_toml(&uuid).exists(), "plan.toml missing");

    // Round-trips through FlowRun::read (no common.toml -> synth fallback;
    // REPLACE_ME partition keeps it off the PartitionMissing path).
    let fr = FlowRun::read(&resolver, uuid).expect("FlowRun::read should succeed");
    assert_eq!(fr.flow.jobs.len(), 2);

    // jm render must succeed on the generated boilerplate.
    let mut render = Command::cargo_bin("jm").unwrap();
    render
        .arg("--root")
        .arg(dir.path())
        .arg("render")
        .arg(&uuid_str);
    render
        .assert()
        .success()
        .stdout(predicate::str::contains("rendered 2 jobs"));
}

#[test]
fn jm_new_print_path_emits_only_the_dir() {
    let dir = tempdir().unwrap();

    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root")
        .arg(dir.path())
        .arg("new")
        .arg("--print-path");
    let out = cmd.assert().success().get_output().stdout.clone();
    let printed = String::from_utf8(out).unwrap();
    let printed = printed.trim();

    let uuid_str = sole_flow_uuid(dir.path());
    let expected = std::fs::canonicalize(dir.path()).unwrap().join(&uuid_str);
    assert_eq!(
        std::path::Path::new(printed),
        expected,
        "print-path output should be exactly <root>/<uuid>"
    );
    assert!(
        !printed.contains("created flow"),
        "print-path must not emit the human banner"
    );
}

#[test]
fn jm_new_writes_tag_into_flow() {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;

    let dir = tempdir().unwrap();
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root")
        .arg(dir.path())
        .arg("new")
        .arg("--tag")
        .arg("env=prod");
    cmd.assert().success();

    let uuid_str = sole_flow_uuid(dir.path());
    let uuid = uuid::Uuid::parse_str(&uuid_str).unwrap();
    let resolver = PathResolver::new(dir.path());
    let fr = FlowRun::read(&resolver, uuid).unwrap();
    assert_eq!(fr.flow.tags.get("env").map(String::as_str), Some("prod"));
}

#[test]
fn jm_new_rejects_malformed_tag() {
    let dir = tempdir().unwrap();
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root")
        .arg(dir.path())
        .arg("new")
        .arg("--tag")
        .arg("notakeyvalue");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("expected key=value"));
    // No flow dir should have been created (tag is parsed before mkdir).
    let dirs: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    assert!(dirs.is_empty(), "no flow dir on tag-parse failure");
}
```

Note: `--tag` is parsed *before* `create_dir_all` in `cmd_new`, so the malformed-tag case never creates a dir — the "no flow dir" assertion holds.

- [ ] **Step 2: Run the integration test**

Run: `cargo test --test integration_new --no-default-features -- --nocapture`
Expected: PASS (Task 6 is committed, so `jm new` exists). If FAIL, go to Step 3.

- [ ] **Step 3: Fix root causes (if any test failed)**

Fix in `src/bin/jm.rs` (not the test) unless the test encodes a wrong expectation. Known gotchas:
- `--print-path` comparison: `resolve_root` canonicalizes, so the expected path uses `std::fs::canonicalize(dir.path())` joined with uuid (already in the test).
- `render` stdout text: the pre-existing `cmd_render` prints `rendered {n} jobs in {uuid}`; the predicate uses substring `"rendered 2 jobs"`. Do **not** change `cmd_render`'s wording.
Re-run Step 2 until green.

- [ ] **Step 4: Run the focused gate**

Run:
```bash
cargo fmt --check \
  && cargo clippy --bin jm --test integration_new --no-default-features -- -D warnings \
  && cargo test --bin jm --test integration_new --no-default-features
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add tests/integration_new.rs
git commit -m "test(jm): add end-to-end integration_new (create, round-trip, render, --tag, --print-path)"
```

---

## Task 8: Full CI gate + docs touch

**Files:**
- Modify: `README.md` (only if it documents the `jm` subcommand list — add a one-line `jm new` entry mirroring existing style). If `README.md` has no `jm` subcommand list, skip the README edit.

- [ ] **Step 1: Locate the jm subcommand docs**

Run: `grep -n "jm render\|jm submit\|jm tick\|jm search" README.md docs/development.md docs/API.md 2>/dev/null`
Expected: find where subcommands are listed (or no match).

- [ ] **Step 2: Add the `jm new` line**

If a subcommand list/usage block exists, add a `jm new` entry adjacent to the `jm render` line, matching the surrounding format exactly. Reference wording (adapt to the file's actual style — do not invent a new format):

```
jm --root <root> new [--tag k=v]... [--print-path]   # scaffold flow.toml + plan.toml under a fresh uuid
```

If no such list exists in any doc, make no doc edit and record that in Step 4's commit message.

- [ ] **Step 3: Run the full project CI gate**

Run (the documented gate plus the `jm`-only no-default-features path):
```bash
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo clippy --bin jm --no-default-features -- -D warnings \
  && cargo test --all-features \
  && cargo test --bin jm --test integration_new --no-default-features
```
Expected: all green. Also run `uv run pytest python/tests -v` if `uv` is available (unaffected — `cmd_new` adds no Python surface — but the documented gate includes it).

- [ ] **Step 4: Commit**

If `README.md` (or another doc) was edited in Step 2:
```bash
git add README.md
git commit -m "docs: document jm new subcommand"
```
If no doc file was edited, skip this commit entirely — the feature is complete at Task 7 and Task 8 was verification only.

---

## Self-Review (completed during planning)

**1. Spec coverage:**
- CLI shape `jm new [--tag]... [--print-path]` → Task 6 (`Cmd::New`, match arm, `cmd_new`).
- UUID v7 mint + collision check (`flow_dir.exists()` → bail) → Task 6 Step 3.
- `create_dir_all` then write flow.toml + plan.toml → Task 6 Step 3.
- Template = 2-job DAG with `partition = "REPLACE_ME"` → Task 3 (+ Task 2 plan).
- `--tag k=v` into `[tags]`, dup last-wins, missing `=` errors, empty key errors → Task 1 (parse) + Task 4 (render) + Task 7 (e2e).
- `atomic_write_str` local helper (lib `atomic_write` is `pub(crate)`) → Task 5.
- Rollback created dir on write failure → Task 6 Step 3 (`remove_dir_all`).
- `--print-path` emits only `<root>/<uuid>` → Task 6 Step 3 + Task 7.
- Error handling table (root, collision, tag, write-fail, mkdir-fail) → Tasks 1/5/6 + Task 7 malformed-tag case.
- Tests: unit (helpers) Tasks 1–5; integration (round-trip via `FlowRun::read`, render, print-path, tag) Task 7. `MockExecutor`/`InMemoryQuerier` not needed — `cmd_render` uses `DryRunExecutor` internally; no live SLURM.
- `--no-default-features` build (no libpython) honored in every build/test command.

**2. Placeholder scan:** No TBD/TODO-as-gap. The literal `note = "TODO: ..."` strings are intentional *template payload*, not plan gaps. Every code step shows complete code.

**3. Type consistency:** `build_flow_template(&Uuid, &str, &BTreeMap<String,String>) -> String`, `build_plan_template() -> String`, `parse_tag(&str) -> anyhow::Result<(String,String)>`, `atomic_write_str(&Path, &str) -> io::Result<()>` — names and signatures identical across Tasks 1–7 and the `cmd_new` call sites. `Cmd::New { tags: Vec<String>, print_path: bool }` matches the `main` arm and `cmd_new(&root, tags, print_path)`. Import paths match the live codebase (`gaussian_job_shared::entities::workflow::{JobFlow,JobId}`, `job_manager::plan::ExperimentPlan`, `job_manager::persistence::PathResolver`, `job_manager::flow::FlowRun`) — verified against `tests/cli_smoke.rs` and `src/flow/run.rs`.

**4. Ambiguity:** `--print-path` path equality resolved by canonicalizing `dir.path()` in the test (matches `resolve_root`). `cmd_render` stdout wording (`rendered {n} jobs in {uuid}`) is pre-existing and left unchanged; the Task 7 predicate matches a stable substring. `BTreeMap` is imported once via the `use std::collections::BTreeMap;` added in Task 3 Step 3 and reused by `cmd_new` in Task 6 (same module scope) — if rustc reports an unused/duplicate import during Task 6, keep the single top-level `use` from Task 3 and remove any local re-import.
