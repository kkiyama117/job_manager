# jm new recipes — Plan A: D2 `launcher` / `scratch_root` fields Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two optional cluster-invariant config fields to upstream D2 (`gaussian_job_shared`) — `CommonConfig.launcher: Option<String>` and `DirectoryConfig.scratch_root: Option<PathBuf>` — re-pin job-manager to the new D2 commit, fix every Rust struct-literal construction site that the new fields break, and prove the fields round-trip through `read_common` / `synth_empty_common`.

**Architecture:** D2 is a separate git repo consumed by job-manager via `git = "..."` (no `rev` pin; Cargo.lock is the canonical pin — currently `00c645e445baebb40f57f5ff783bacd994765499`). The two fields carry `#[serde(default)]` so existing `common.toml` files (and `examples/`) keep parsing (`Option<T>` = absent ⇒ `None`). `#[serde(deny_unknown_fields)]` on both structs is unaffected (it rejects *extra* keys, not *missing defaulted* ones). Because they are non-`Default` struct fields, every `CommonConfig { … }` / `DirectoryConfig { … }` **literal** in job-manager stops compiling until the new fields are added — the Rust compiler is the exhaustive checklist for that cascade. This plan is the prerequisite for Plan B (`src/recipes/`) and Plan C (`jm new` CLI + render-time resolution); it ships working software on its own (job-manager compiles and all tests pass with the new fields available but not yet consumed by feature code).

**Tech Stack:** Rust (edition 2024, nightly), `serde`, `toml` 1.1, `cargo`, the `gaussian_job_shared` (D2) upstream repo at `https://github.com/kkiyama117/gaussian_job_shared.git`.

**Spec:** `docs/superpowers/specs/2026-05-16-jm-new-domain-recipes-design.md` rev.6 — §2 Goal 9, §5.5, §5.6, §11 (D2 coordinated change).

---

## Cross-repo note (read before starting)

D2 lives in its **own repository**, not in this tree. The locked source is checked out read-only under
`~/.local/share/cargo/git/checkouts/gaussian_job_shared-e8f9e7768e9e33cd/00c645e/` — **do not edit that**; cargo overwrites it.
Task 1 makes the change in a fresh clone of the D2 repo, pushes a branch, opens and merges a PR to D2's default branch (`main`). Tasks 2–5 happen in this job-manager repo and **require the D2 change to be on D2 `main`** (Task 2's `cargo update` re-pins to D2 `main` HEAD). You own `kkiyama117/gaussian_job_shared`, so self-merging the D2 PR is expected.

`Cargo.toml` deliberately has **no `rev =`** on `gaussian_job_shared` (the same-URL-trap note in `Cargo.toml:52-58` / CLAUDE.md). Do **not** add one. Do **not** add a `[patch]` table. Re-pinning is `cargo update -p gaussian_job_shared` + committing `Cargo.lock`.

---

## File Structure

| File | Repo | Responsibility | Task |
|---|---|---|---|
| `src/config/common.rs` | **D2** (`gaussian_job_shared`) | Add `launcher` to `CommonConfig`, `scratch_root` to `DirectoryConfig`, + D2 unit tests | 1 |
| `Cargo.lock` | job-manager | Re-pin `gaussian_job_shared` to the new D2 commit | 2 |
| `src/persistence/common.rs` | job-manager | `synth_empty_common()` + `tests::sample()` literal fix; new round-trip tests | 3, 4 |
| `src/walk.rs` | job-manager | literal fix (`CommonConfig`/`DirectoryConfig`) | 3 |
| `src/persistence/flow.rs` | job-manager | literal fix (`tests::sample_common`) | 3 |
| `src/flow/run.rs` | job-manager | literal fix (`tests::read_constructs_from_disk_with_common`) | 3 |
| `src/doctor/checks.rs` | job-manager | literal fix (`tests::common_with_logs`) | 3 |
| `tests/integration_listing.rs` | job-manager | literal fix | 3 |
| `tests/integration_effective_isolation.rs` | job-manager | literal fix | 3 |
| `tests/integration_walk.rs` | job-manager | literal fix | 3 |

The compiler enumerates any site missed by the explicit list above (Task 3 gate = `cargo build` green).

---

## Task 1: D2 — add `launcher` / `scratch_root` with serde defaults + tests

**Files (in the D2 repo, NOT this tree):**
- Modify: `src/config/common.rs` (the whole file is short — current content shown below)

- [ ] **Step 1: Clone D2 and branch**

Run:
```bash
cd /tmp && rm -rf gjs-work && \
git clone https://github.com/kkiyama117/gaussian_job_shared.git gjs-work && \
cd /tmp/gjs-work && git checkout -b feat/common-launcher-scratch-root
```
Expected: clone succeeds, on branch `feat/common-launcher-scratch-root`.

- [ ] **Step 2: Write the failing D2 tests**

The current `src/config/common.rs` `mod tests` has `sample()`, `serde_round_trip`, `deny_unknown_fields_rejects_extra_top_level`. Replace the entire `#[cfg(test)] mod tests { … }` block in `/tmp/gjs-work/src/config/common.rs` with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use slurm_async_runner::entities::slurm::SlurmJobConfig;

    fn sample() -> CommonConfig {
        CommonConfig {
            slurm_default: SlurmJobConfig {
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
            },
            directories: DirectoryConfig {
                project_root: PathBuf::from("/work"),
                scratch_root: None,
            },
            launcher: None,
        }
    }

    #[test]
    fn serde_round_trip() {
        let original = sample();
        let toml_str = toml::to_string(&original).unwrap();
        let restored: CommonConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(
            restored.slurm_default.partition,
            original.slurm_default.partition
        );
        assert_eq!(
            restored.directories.project_root,
            original.directories.project_root
        );
    }

    #[test]
    fn deny_unknown_fields_rejects_extra_top_level() {
        let bad = r#"
[slurm_default]
partition = "long"

[directories]
project_root = "/work"

[bogus]
key = "value"
"#;
        let result: Result<CommonConfig, _> = toml::from_str(bad);
        assert!(result.is_err());
    }

    #[test]
    fn launcher_absent_is_none() {
        let s = r#"
[slurm_default]
partition = "long"

[directories]
project_root = "/work"
"#;
        let c: CommonConfig = toml::from_str(s).unwrap();
        assert_eq!(c.launcher, None);
        assert_eq!(c.directories.scratch_root, None);
    }

    #[test]
    fn launcher_and_scratch_root_parse_when_present() {
        let s = r#"
launcher = "srun"

[slurm_default]
partition = "long"

[directories]
project_root = "/work"
scratch_root = "/LARGE0/scratch"
"#;
        let c: CommonConfig = toml::from_str(s).unwrap();
        assert_eq!(c.launcher.as_deref(), Some("srun"));
        assert_eq!(
            c.directories.scratch_root,
            Some(PathBuf::from("/LARGE0/scratch"))
        );
    }

    #[test]
    fn empty_launcher_string_is_preserved() {
        let s = r#"
launcher = ""

[slurm_default]
partition = "long"

[directories]
project_root = "/work"
"#;
        let c: CommonConfig = toml::from_str(s).unwrap();
        assert_eq!(c.launcher.as_deref(), Some(""));
    }
}
```

- [ ] **Step 3: Run D2 tests to verify they fail**

Run: `cd /tmp/gjs-work && cargo test --lib config::common 2>&1 | tail -20`
Expected: COMPILE FAIL — `error[E0560]: struct \`CommonConfig\` has no field named \`launcher\`` and `… \`DirectoryConfig\` has no field named \`scratch_root\``.

- [ ] **Step 4: Add the two fields**

In `/tmp/gjs-work/src/config/common.rs`, replace the two struct definitions (top of file) with:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommonConfig {
    /// Set default arguments of slurm_config
    pub slurm_default: SlurmJobConfig,
    /// Set config of directory.
    pub directories: DirectoryConfig,
    /// Optional cluster-wide job launcher prefix (e.g. `"srun"`).
    /// Absent ⇒ `None`; downstream consumers resolve at render time.
    /// `Some("")` is a meaningful value ("this cluster runs bare").
    #[serde(default)]
    pub launcher: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryConfig {
    /// Root of all project data.
    pub project_root: PathBuf,
    /// Optional node-local scratch root (HPC `tmp_root` analog).
    /// Absent ⇒ `None`; consumers pick a fallback.
    #[serde(default)]
    pub scratch_root: Option<PathBuf>,
}
```

(`#[serde(default)]` on a field is compatible with the struct-level `#[serde(deny_unknown_fields)]`: the former allows the key to be *missing*, the latter rejects *extra* keys. `toml` omits `None` `Option` keys on serialize, exactly as it already does for `SlurmJobConfig`'s many `Option` fields — so `serde_round_trip` stays green.)

- [ ] **Step 5: Run D2 tests to verify they pass**

Run: `cd /tmp/gjs-work && cargo test --lib config::common 2>&1 | tail -15`
Expected: PASS — `test result: ok.` covering `serde_round_trip`, `deny_unknown_fields_rejects_extra_top_level`, `launcher_absent_is_none`, `launcher_and_scratch_root_parse_when_present`, `empty_launcher_string_is_preserved`.

- [ ] **Step 6: Run the full D2 test suite (no regressions elsewhere in D2)**

Run: `cd /tmp/gjs-work && cargo test 2>&1 | tail -15`
Expected: all D2 tests `ok`. If another D2 module constructs `DirectoryConfig`/`CommonConfig` literals, the compiler will name it — add `scratch_root: None,` / `launcher: None,` there too, then re-run.

- [ ] **Step 7: Commit and push the D2 branch**

Run:
```bash
cd /tmp/gjs-work && git add src/config/common.rs && \
git commit -m "feat(config): add optional CommonConfig.launcher and DirectoryConfig.scratch_root

Both #[serde(default)] Option fields so existing common.toml keeps
parsing. Consumed by job-manager's jm-new recipe render path." && \
git push -u origin feat/common-launcher-scratch-root
```
Expected: branch pushed.

- [ ] **Step 8: Open and merge the D2 PR to `main`**

Run:
```bash
cd /tmp/gjs-work && \
gh pr create --fill --base main --head feat/common-launcher-scratch-root && \
gh pr merge --squash --delete-branch
```
Expected: PR created and merged to `main`. Capture the new `main` commit SHA:
```bash
cd /tmp/gjs-work && git fetch origin main && git rev-parse origin/main
```
Record this SHA — Task 2 verifies `Cargo.lock` moves to it.

---

## Task 2: job-manager — re-pin `Cargo.lock` to the new D2 commit

**Files:**
- Modify: `Cargo.lock` (job-manager root; `cargo` rewrites it — do not hand-edit)

- [ ] **Step 1: Record the current locked D2 SHA**

Run: `cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/job-manager && grep -A1 'name = "gaussian_job_shared"' Cargo.lock | grep source`
Expected: `source = "git+https://github.com/kkiyama117/gaussian_job_shared.git#00c645e445baebb40f57f5ff783bacd994765499"`

- [ ] **Step 2: Update the lock to D2 `main` HEAD**

Run: `cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/job-manager && cargo update -p gaussian_job_shared 2>&1 | tail -3`
Expected: `Updating gaussian_job_shared v0.1.0 (...#00c645e...) -> (...#<new-sha>...)` where `<new-sha>` equals the SHA recorded in Task 1 Step 8.

- [ ] **Step 3: Verify the lock moved to the recorded SHA**

Run: `grep -A1 'name = "gaussian_job_shared"' Cargo.lock | grep source`
Expected: `source = "git+https://github.com/kkiyama117/gaussian_job_shared.git#<new-sha>"` (the Task 1 Step 8 SHA). If it did not move, the D2 PR is not yet on `main` — return to Task 1 Step 8.

- [ ] **Step 4: Commit the lock bump**

Run:
```bash
git add Cargo.lock && \
git commit -m "chore(deps): bump gaussian_job_shared for launcher/scratch_root fields"
```
Expected: commit created. (Do not push yet — Plan A pushes once at the end.)

---

## Task 3: Fix every `CommonConfig` / `DirectoryConfig` struct-literal site

The two new fields are non-`Default` struct fields, so **every literal `CommonConfig { … }` / `DirectoryConfig { … }` in this repo fails to compile** until `launcher: None,` / `scratch_root: None,` are added. The compiler is the exhaustive checklist; the explicit site list below is the known set.

**Files (known sites — verify with the grep in Step 1):**
- Modify: `src/persistence/common.rs` (`synth_empty_common()` ~line 41–58; `tests::sample()` ~line 114–131)
- Modify: `src/walk.rs` (~line 102–118)
- Modify: `src/persistence/flow.rs` (`tests::sample_common` ~line 224–240)
- Modify: `src/flow/run.rs` (`tests::read_constructs_from_disk_with_common` ~line 217–234)
- Modify: `src/doctor/checks.rs` (`tests::common_with_logs` ~line 371+)
- Modify: `tests/integration_listing.rs` (~line 22–37)
- Modify: `tests/integration_effective_isolation.rs` (~line 15–30)
- Modify: `tests/integration_walk.rs` (~line 19–34)

- [ ] **Step 1: Enumerate all literal sites (the checklist)**

Run:
```bash
cd /home/kiyama/programs/research/GAUSSIAN_repo_packages/job-manager && \
grep -rn 'CommonConfig {\|DirectoryConfig {' src tests --include='*.rs'
```
Expected: ~18 lines across the files listed above. This is the work list. (`synth_empty_common` and the `fn sample()`/`fn sample_common()`/`fn common_with_logs()` lines are function signatures, not literals — the literal is the `CommonConfig {` / `DirectoryConfig {` line itself.)

- [ ] **Step 2: Run the build to see the failures (the "failing test")**

Run: `cargo build --no-default-features 2>&1 | grep -E 'error\[E0063\]|missing.*(launcher|scratch_root)' | head`
Expected: FAIL — multiple `error[E0063]: missing field \`scratch_root\` in initializer of \`...DirectoryConfig\`` and `missing field \`launcher\` in initializer of \`...CommonConfig\``.

- [ ] **Step 3: Fix `src/persistence/common.rs` — `synth_empty_common()`**

In `src/persistence/common.rs`, the `synth_empty_common()` body currently ends:

```rust
        directories: DirectoryConfig {
            project_root: std::path::PathBuf::from("."),
        },
    }
}
```

Replace with:

```rust
        directories: DirectoryConfig {
            project_root: std::path::PathBuf::from("."),
            scratch_root: None,
        },
        launcher: None,
    }
}
```

- [ ] **Step 4: Fix `src/persistence/common.rs` — `tests::sample()`**

In the same file, `tests::sample()` ends:

```rust
            directories: DirectoryConfig {
                project_root: PathBuf::from("/work"),
            },
        }
    }
```

Replace with:

```rust
            directories: DirectoryConfig {
                project_root: PathBuf::from("/work"),
                scratch_root: None,
            },
            launcher: None,
        }
    }
```

- [ ] **Step 5: Fix the remaining six known sites mechanically**

For **each** of `src/walk.rs`, `src/persistence/flow.rs`, `src/flow/run.rs`, `src/doctor/checks.rs`, `tests/integration_listing.rs`, `tests/integration_effective_isolation.rs`, `tests/integration_walk.rs`: open the file at the grep line from Step 1 and, in every `CommonConfig { … }` literal add a `launcher: None,` field (last field, after `directories: …,`), and in every `DirectoryConfig { … }` literal add a `scratch_root: None,` field (after `project_root: …,`). These are all test/helper constructors; the only values are `None`. Example shape (applies to each):

```rust
        // before
        CommonConfig {
            slurm_default: SlurmJobConfig { /* … unchanged … */ },
            directories: DirectoryConfig {
                project_root: /* … unchanged … */,
            },
        }
        // after
        CommonConfig {
            slurm_default: SlurmJobConfig { /* … unchanged … */ },
            directories: DirectoryConfig {
                project_root: /* … unchanged … */,
                scratch_root: None,
            },
            launcher: None,
        }
```

- [ ] **Step 6: Build (no-default-features) to confirm the cascade is closed**

Run: `cargo build --no-default-features 2>&1 | tail -5`
Expected: PASS — `Finished`. If any `error[E0063]` remains, the compiler names the file:line; apply the same two-field addition there and re-run.

- [ ] **Step 7: Build with all features (pyo3 path constructs nothing new, but verify)**

Run: `cargo build --all-features 2>&1 | tail -3`
Expected: PASS — `Finished`.

- [ ] **Step 8: Commit the cascade fix**

Run:
```bash
git add src tests && \
git commit -m "refactor: thread launcher/scratch_root None through CommonConfig/DirectoryConfig literals"
```
Expected: commit created.

---

## Task 4: Prove the new fields round-trip through job-manager's persistence layer

**Files:**
- Test: `src/persistence/common.rs` (extend the existing `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the tests**

In `src/persistence/common.rs`, inside `#[cfg(test)] mod tests { … }`, add these three tests (after the existing `merge_*` tests, before the closing `}` of `mod tests`):

```rust
    #[test]
    fn synth_empty_common_has_none_launcher_and_scratch_root() {
        let c = synth_empty_common();
        assert_eq!(c.launcher, None);
        assert_eq!(c.directories.scratch_root, None);
    }

    #[test]
    fn read_common_parses_launcher_and_scratch_root() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("common.toml");
        std::fs::write(
            &p,
            r#"
launcher = "srun"

[slurm_default]
partition = "long"

[directories]
project_root = "/work"
scratch_root = "/LARGE0/scratch"
"#,
        )
        .unwrap();
        let c = read_common(&p).unwrap();
        assert_eq!(c.launcher.as_deref(), Some("srun"));
        assert_eq!(
            c.directories.scratch_root,
            Some(PathBuf::from("/LARGE0/scratch"))
        );
    }

    #[test]
    fn read_common_defaults_launcher_scratch_none_when_absent() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("common.toml");
        std::fs::write(
            &p,
            r#"
[slurm_default]
partition = "long"

[directories]
project_root = "/work"
"#,
        )
        .unwrap();
        let c = read_common(&p).unwrap();
        assert_eq!(c.launcher, None);
        assert_eq!(c.directories.scratch_root, None);
    }
```

(`tempdir`, `PathBuf`, `read_common`, `synth_empty_common` are already in scope in this test module via `use super::*;` plus the existing `use std::path::PathBuf;` / `use tempfile::tempdir;` at the top of `mod tests`.)

- [ ] **Step 2: Run to verify they pass**

Run: `cargo test --lib --no-default-features persistence::common 2>&1 | tail -15`
Expected: PASS — including `synth_empty_common_has_none_launcher_and_scratch_root`, `read_common_parses_launcher_and_scratch_root`, `read_common_defaults_launcher_scratch_none_when_absent`. (Not RED-first: Task 1 already added the fields upstream; this task locks job-manager-side behavior so Plan C can rely on it.)

- [ ] **Step 3: Commit**

Run:
```bash
git add src/persistence/common.rs && \
git commit -m "test(persistence): cover launcher/scratch_root parse + synth defaults"
```
Expected: commit created.

---

## Task 5: Full CI gate + push

- [ ] **Step 1: Run the CLAUDE.md CI gate**

Run:
```bash
cargo fmt --check && \
cargo clippy --all-targets --all-features -- -D warnings && \
cargo test --all-features 2>&1 | tail -20 && \
uv run pytest python/tests -v 2>&1 | tail -10
```
Expected: `cargo fmt` no diff; clippy `Finished` no warnings; `cargo test` all suites `ok` (notably `integration_listing`, `integration_effective_isolation`, `integration_walk`, `flow::run`, `persistence::common`, `persistence::flow`, `walk`, `doctor::checks`); pytest all pass (Plan A changes no Python, so this is a no-regression check).

- [ ] **Step 2: If `cargo fmt --check` reports a diff, format and amend**

Run: `cargo fmt && git add -u && git commit --amend --no-edit` (only if Step 1's fmt check failed; otherwise skip).

- [ ] **Step 3: Push the branch**

First confirm the branch: `git branch --show-current` (Plan A's commits stack on the rev.6 spec branch `docs/jm-new-recipes-spec` per CLAUDE.md "stacked PRs"; if a dedicated impl branch is preferred, `git checkout -b feat/jm-new-recipes-planA` before pushing).
Run: `git push 2>&1 | tail -2`
Expected: pushed to the current branch's upstream.

- [ ] **Step 4: Plan A done — verify exit criteria**

Confirm all true:
- D2 `main` carries `CommonConfig.launcher` + `DirectoryConfig.scratch_root` (`gh pr view` shows merged).
- `Cargo.lock` `gaussian_job_shared` source SHA == D2 `main` HEAD.
- `cargo build --no-default-features` and `--all-features` both green.
- `cargo test --all-features` green; `read_common` parses both fields; `synth_empty_common()` returns `None` for both.
- No `error[E0063]` anywhere (`grep -rn 'CommonConfig {\|DirectoryConfig {' src tests` sites all carry the two new fields).

---

## Self-Review

**1. Spec coverage (rev.6 §2 Goal 9, §5.5, §5.6, §11):**
- §2 Goal 9 "D2 1 coordinated PR: `CommonConfig.launcher: Option<String>` + `DirectoryConfig.scratch_root: Option<PathBuf>` (both `#[serde(default)]`)" → Task 1. ✓
- §11 "land it in the D2 repo, then bump the D2 rev here" (here = `Cargo.lock`, since `Cargo.toml` has no `rev` by design) → Task 2. ✓
- §11 "`synth_empty_common()` + 関連テスト追従" → Task 3 (synth + all literals), Task 4 (synth test). ✓
- §5.5/§5.6 only *define* the fields here; their *render-time resolution* (4-case / 3-case precedence, `JM_LAUNCHER`/`JM_SCRATCH_ROOT` export) is **Plan C**, explicitly out of Plan A scope. No gap — Plan A's job is to make the fields exist and be readable. ✓
- §5.5/§5.6 backward-compat ("既存 common.toml も `#[serde(default)]` でパース可") → Task 1 `launcher_absent_is_none`, Task 4 `read_common_defaults_launcher_scratch_none_when_absent`; `examples/` unchanged (serde default ⇒ still parse; `doctor_examples.rs` exercises them in Task 5). ✓

**2. Placeholder scan:** No "TBD"/"handle errors"/"similar to". Every code step shows full code; every command shows expected output. Task 3 Step 5 is mechanical-but-exhaustive (two named fields, `None` value, compiler-gated by Step 6) — not a placeholder. ✓

**3. Type consistency:** Field names/types identical everywhere: `CommonConfig.launcher: Option<String>` (`launcher: None`), `DirectoryConfig.scratch_root: Option<PathBuf>` (`scratch_root: None`). D2 `sample()` (Task 1 Step 2) and job-manager literals (Task 3) use the same field order (`directories` then `launcher`; `project_root` then `scratch_root`). `read_common`/`synth_empty_common` names match `src/persistence/common.rs` / `lib.rs` re-exports. ✓

No issues found.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-17-jm-new-recipes-planA-d2-launcher-scratch.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration. Note: Task 1 is cross-repo (D2) and ends with an owner PR-merge gate before Task 2.
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints.

Plans B and C are written after Plan A is approved/executed (B = `src/recipes/` core; C = `jm new` CLI + render-time resolution).

Which approach?
