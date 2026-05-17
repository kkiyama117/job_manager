# jm new recipes — Plan B: `src/recipes/` core library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pyo3-free `src/recipes/` library: traits (`JobTemplate`, `FlowRecipe`), value types, the `base_preamble()` port of gem's `_base.bash.j2`, a pure `.xyz` parser, the `g16_opt` / `parse_g16_out` JobTemplates with their `run.py` / `parse.py` / `main.gjf` assets, the `blank` / `g16-opt-parse` FlowRecipes, `flow::assemble()`, registries, `--param` parse/type-check, and `--list` / `--describe` formatters — all fully unit-tested, with **zero** changes to `src/bin/jm.rs` or the render path (those are Plan C).

**Architecture:** `src/recipes/` is a self-contained library module re-exported from `lib.rs`. It depends only on `uuid`, `chrono`, `toml`, and `std` (no `pyo3`; the generation path emits **TOML text + sidecar file contents**, never in-memory `JobFlow` structs — only the *tests* deserialize the emitted text back into `JobFlow`/`ExperimentPlan` to assert doctor-clean invariants). `JobTemplate::instantiate` is pure (no I/O); it returns `JobArtifacts` with the job's `program`, the thin flow.toml `body`, plan params, and `Vec<GeneratedFile>` sidecars (`scripts/<JobId>.bash` built via `base_preamble()`, plus `scripts/run.py`/`parse.py` and `input/main.gjf`). `assemble()` resolves a `FlowRecipe`'s nodes/edges/wiring into doctor-clean `flow.toml` + `plan.toml` text plus all sidecars, prepending the R3 absolute `cd` to each job body. File copy of `input_coordinate`, `.xyz` geometry splicing (the parser is here; the I/O is Plan C), and CLI wiring are **Plan C**. Plan B ships working software on its own: the library compiles under `--no-default-features` and its unit tests pass.

**Tech Stack:** Rust (edition 2024, nightly), `toml` 1.1, `uuid` v7, `chrono`, `thiserror`; generated assets are POSIX `bash` + Python 3 stdlib (`run.py`) + `cclib` (`parse.py`).

**Spec:** `docs/superpowers/specs/2026-05-16-jm-new-domain-recipes-design.md` rev.6 — §4, §4.0, §5.1 (R3 in assemble), §7, §8, §13.

**Depends on:** Plan A executed (D2 `launcher`/`scratch_root` fields exist). Not strictly required to *compile* Plan B (recipes never construct `CommonConfig`), but execute after Plan A so the branch stays linear.

---

## Spec refinements locked by this plan (resolve spec §4 illustration ambiguity)

1. **R3 cd lives in `assemble()`, not `instantiate`.** `JobTemplate::instantiate` returns `JobArtifacts.body = "bash scripts/<job_id>.bash\n"` (no cd). `assemble()` prepends `cd "<abs_flow_dir>/<job_id>" || exit 1\n`. This keeps `JobCtx` exactly as spec §4 lists (no `abs_flow_dir` in it) and centralizes R3 in one place (spec §4 assemble step 4).
2. **`JobCtx` fields:** `{ job_id: &str, params: &BTreeMap<String, toml::Value>, inputs: &BTreeMap<&'static str, String>, uuid: &Uuid, created_at: &str }` (spec §4 verbatim).
3. **`assemble()` signature:** `assemble(recipe: &dyn FlowRecipe, raw_params: &[String], tags: &BTreeMap<String,String>, uuid: &Uuid, created_at: &str, abs_flow_dir: &Path) -> Result<Vec<GeneratedFile>, RecipeError>`.
4. **`input_coordinate` at instantiate time:** instantiate is pure, no file I/O. `input/main.gjf` is always emitted with `{{geometry_block}}` = the REPLACE_ME sentinel. The `.xyz` splice + file copy is Plan C's `cmd_new`, which calls the pure `parse_xyz()` defined here.
5. **`program` field values:** `g16_opt` ⇒ `"g16"`, `parse_g16_out` ⇒ `"python"` (classification for `jm ls --program`; spec §7).

---

## File Structure

| File | Responsibility |
|---|---|
| `src/recipes/mod.rs` | module decls, public re-exports, `flow_registry`/`find_flow`/`find_job`, `parse_param_raw`/`typecheck_node`, `format_list`/`format_describe` |
| `src/recipes/job.rs` | `RecipeParamType`, `RecipeParam`, `RecipeError`, `GeneratedFile`, `JobArtifacts`, `JobCtx`, `JobTemplate` trait, `PreambleOpts`, `base_preamble()`, `parse_xyz()` |
| `src/recipes/flow.rs` | `FlowRecipe` trait, `assemble()` |
| `src/recipes/jobs/mod.rs` | `pub mod g16_opt; pub mod parse_g16_out;` |
| `src/recipes/jobs/g16_opt.rs` | `G16Opt` JobTemplate |
| `src/recipes/jobs/parse_g16_out.rs` | `ParseG16Out` JobTemplate |
| `src/recipes/flows/mod.rs` | `pub mod blank; pub mod g16_opt_parse;` |
| `src/recipes/flows/blank.rs` | `Blank` FlowRecipe + byte-identical 2-job text generator |
| `src/recipes/flows/g16_opt_parse.rs` | `G16OptParse` FlowRecipe |
| `src/recipes/assets/g16_opt/main.gjf.tmpl` | gjf template (`{{}}` placeholders, no `%rwf`) |
| `src/recipes/assets/g16_opt/run.py.tmpl` | `run_g16` reproduction (stdlib) |
| `src/recipes/assets/parse_g16_out/parse.py.tmpl` | `parse_results` reproduction (cclib) |
| `src/lib.rs` | add `pub mod recipes;` + re-exports (modify) |

---

## Task 1: module skeleton + core value types

**Files:**
- Create: `src/recipes/mod.rs`, `src/recipes/job.rs`
- Modify: `src/lib.rs`
- Test: `src/recipes/job.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Create `src/recipes/job.rs`**

```rust
//! Recipe value types + JobTemplate trait + base_preamble + xyz parser.
//! pyo3-free: std + toml + uuid + chrono only.

use std::collections::BTreeMap;
use std::path::PathBuf;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipeParamType {
    Str,
    Int,
    Float,
    Bool,
    Path,
}

#[derive(Debug, Clone, Copy)]
pub struct RecipeParam {
    pub name: &'static str,
    pub ty: RecipeParamType,
    pub default: &'static str,
    pub help: &'static str,
}

#[derive(Debug, thiserror::Error)]
pub enum RecipeError {
    #[error("unknown recipe {0:?}; available: {1}")]
    UnknownRecipe(String, String),
    #[error("invalid --param: expected <JobId>.<param>=<value>, got {0:?}")]
    BadParamSyntax(String),
    #[error("recipe {flow}: no node {jobid}; nodes: {nodes}")]
    UnknownNode {
        flow: String,
        jobid: String,
        nodes: String,
    },
    #[error("recipe {flow}: job {jobid}: unknown param {param:?}; params: {known}")]
    UnknownParam {
        flow: String,
        jobid: String,
        param: String,
        known: String,
    },
    #[error("recipe {flow}: job {jobid}: param {param:?} expects {ty:?}, got {value:?}")]
    TypeMismatch {
        flow: String,
        jobid: String,
        param: String,
        ty: RecipeParamType,
        value: String,
    },
    #[error("recipe definition bug: node {node:?} references unknown JobTemplate {tmpl:?}")]
    UnknownJobTemplate { node: String, tmpl: String },
    #[error("recipe definition bug: wiring {consumer}.{input} <- {producer}.{output}: {detail}")]
    WiringMismatch {
        consumer: String,
        input: String,
        producer: String,
        output: String,
        detail: String,
    },
    #[error("xyz parse error: {0}")]
    Xyz(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedFile {
    pub relpath: PathBuf,
    pub contents: String,
    pub unix_mode: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct JobArtifacts {
    pub program: String,
    /// flow.toml `jobs.<id>.body` WITHOUT the R3 cd (assemble prepends it).
    pub body: String,
    pub time_limit: Option<String>,
    pub plan_params: BTreeMap<String, toml::Value>,
    pub sidecars: Vec<GeneratedFile>,
}

pub struct JobCtx<'a> {
    pub job_id: &'a str,
    pub params: &'a BTreeMap<String, toml::Value>,
    /// input name -> resolved RELATIVE path (e.g. `../opt/output/main.out`).
    pub inputs: &'a BTreeMap<&'static str, String>,
    pub uuid: &'a Uuid,
    pub created_at: &'a str,
}

pub trait JobTemplate: Send + Sync {
    fn name(&self) -> &'static str;
    fn params(&self) -> &'static [RecipeParam];
    fn inputs(&self) -> &'static [&'static str];
    fn outputs(&self) -> &'static [(&'static str, &'static str)];
    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, RecipeError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipe_param_is_copy() {
        let p = RecipeParam {
            name: "x",
            ty: RecipeParamType::Int,
            default: "0",
            help: "h",
        };
        let q = p;
        assert_eq!(p.name, q.name);
        assert_eq!(q.ty, RecipeParamType::Int);
    }

    #[test]
    fn generated_file_eq() {
        let a = GeneratedFile {
            relpath: PathBuf::from("opt/scripts/opt.bash"),
            contents: "x".into(),
            unix_mode: Some(0o755),
        };
        assert_eq!(a, a.clone());
    }
}
```

- [ ] **Step 2: Create `src/recipes/mod.rs` (minimal — grows in later tasks)**

```rust
//! `jm new <flow-recipe>` two-layer recipe library. pyo3-free.

pub mod job;

pub use job::{
    GeneratedFile, JobArtifacts, JobCtx, JobTemplate, RecipeError, RecipeParam, RecipeParamType,
};
```

- [ ] **Step 3: Wire into `src/lib.rs`**

Add after `pub mod plan;`:
```rust
pub mod recipes;
```
Add after `pub use plan::ExperimentPlan;`:
```rust
pub use recipes::{
    GeneratedFile, JobArtifacts, JobCtx, JobTemplate, RecipeError, RecipeParam, RecipeParamType,
};
```

- [ ] **Step 4: Build + test**

Run: `cargo test --lib --no-default-features recipes::job 2>&1 | tail -10`
Expected: PASS — `recipe_param_is_copy`, `generated_file_eq` ok; compiles under `--no-default-features`.

- [ ] **Step 5: Commit**

```bash
git add src/recipes/mod.rs src/recipes/job.rs src/lib.rs
git commit -m "feat(recipes): core value types + JobTemplate trait + RecipeError"
```

---

## Task 2: `FlowRecipe` trait + `flow.rs` skeleton

**Files:**
- Create: `src/recipes/flow.rs`
- Modify: `src/recipes/mod.rs`, `src/lib.rs`
- Test: `src/recipes/flow.rs` (inline)

- [ ] **Step 1: Create `src/recipes/flow.rs`**

```rust
//! FlowRecipe trait + assemble() (assemble lands in Task 8).

pub trait FlowRecipe: Send + Sync {
    fn name(&self) -> &'static str;
    fn summary(&self) -> &'static str;
    /// (JobId, JobTemplate name)
    fn nodes(&self) -> &'static [(&'static str, &'static str)];
    /// (from JobId, to JobId, kind e.g. "afterok")
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)];
    /// (consumer JobId, input name, producer JobId, producer output name)
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)];
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dummy;
    impl FlowRecipe for Dummy {
        fn name(&self) -> &'static str {
            "dummy"
        }
        fn summary(&self) -> &'static str {
            "d"
        }
        fn nodes(&self) -> &'static [(&'static str, &'static str)] {
            &[("a", "g16_opt")]
        }
        fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)] {
            &[]
        }
        fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)] {
            &[]
        }
    }

    #[test]
    fn trait_object_dispatch() {
        let r: Box<dyn FlowRecipe> = Box::new(Dummy);
        assert_eq!(r.name(), "dummy");
        assert_eq!(r.nodes().len(), 1);
    }
}
```

- [ ] **Step 2: Re-export**

Rewrite `src/recipes/mod.rs`:
```rust
//! `jm new <flow-recipe>` two-layer recipe library. pyo3-free.

pub mod flow;
pub mod job;

pub use flow::FlowRecipe;
pub use job::{
    GeneratedFile, JobArtifacts, JobCtx, JobTemplate, RecipeError, RecipeParam, RecipeParamType,
};
```
Add `FlowRecipe` to the `src/lib.rs` `pub use recipes::{...}` list.

- [ ] **Step 3: Run + commit**

Run: `cargo test --lib --no-default-features recipes::flow 2>&1 | tail -8`
Expected: PASS — `trait_object_dispatch` ok.
```bash
git add src/recipes/flow.rs src/recipes/mod.rs src/lib.rs
git commit -m "feat(recipes): FlowRecipe trait"
```

---

## Task 3: `base_preamble()` — port of `_base.bash.j2`

**Files:**
- Modify: `src/recipes/job.rs`, `src/recipes/mod.rs`, `src/lib.rs`
- Test: `src/recipes/job.rs` (inline)

- [ ] **Step 1: Write failing tests** — append to `src/recipes/job.rs` `mod tests`:

```rust
    #[test]
    fn preamble_has_base_bash_j2_structure_in_order() {
        let s = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore gaussian_A -f",
            body_block: "python scripts/run.py",
            pixi_manifest: "",
        });
        let i_set = s.find("set -euo pipefail").unwrap();
        let i_unsetf = s.find("unset -f conda").unwrap();
        let i_condash = s.find("etc/profile.d/conda.sh").unwrap();
        let i_modinit = s.find("/usr/share/Modules/init/bash").unwrap();
        let i_modblk = s.find("module restore gaussian_A -f").unwrap();
        let i_act = s.find("conda activate analysis").unwrap();
        let i_body = s.find("python scripts/run.py").unwrap();
        let i_done = s.find(r#"echo "JOB DONE""#).unwrap();
        let i_exit = s.rfind("exit 0").unwrap();
        assert!(
            i_set < i_unsetf
                && i_unsetf < i_condash
                && i_condash < i_modinit
                && i_modinit < i_modblk
                && i_modblk < i_act
                && i_act < i_body
                && i_body < i_done
                && i_done < i_exit,
            "preamble order wrong:\n{s}"
        );
        assert!(s.starts_with("#!/bin/bash\n"));
        assert!(!s.contains("#SBATCH"), "must not emit #SBATCH (spec §2 non-goal)");
    }

    #[test]
    fn preamble_omits_pixi_hook_when_manifest_empty() {
        let s = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore default -f",
            body_block: "python scripts/parse.py",
            pixi_manifest: "",
        });
        assert!(!s.contains("pixi shell-hook"));
    }

    #[test]
    fn preamble_emits_pixi_hook_when_manifest_set() {
        let s = base_preamble(&PreambleOpts {
            conda_env: "analysis",
            module_block: "module restore default -f",
            body_block: "true",
            pixi_manifest: "/work/pixi.toml",
        });
        assert!(s.contains(r#"pixi shell-hook --manifest-path "/work/pixi.toml""#));
    }

    #[test]
    fn preamble_substitutes_env_module_body() {
        let s = base_preamble(&PreambleOpts {
            conda_env: "myenv",
            module_block: "module restore X -f",
            body_block: "echo hi",
            pixi_manifest: "",
        });
        assert!(s.contains("conda activate myenv"));
        assert!(s.contains("module restore X -f"));
        assert!(s.contains("echo hi"));
    }
```

- [ ] **Step 2: Run → fail**

Run: `cargo test --lib --no-default-features recipes::job::tests::preamble 2>&1 | tail -6`
Expected: COMPILE FAIL — `cannot find function \`base_preamble\`` / `struct \`PreambleOpts\``.

- [ ] **Step 3: Implement** — add to `src/recipes/job.rs` (after the `JobTemplate` trait, before `#[cfg(test)]`):

```rust
pub struct PreambleOpts<'a> {
    pub conda_env: &'a str,
    pub module_block: &'a str,
    pub body_block: &'a str,
    /// Empty ⇒ the pixi shell-hook block is omitted.
    pub pixi_manifest: &'a str,
}

/// Rust port of `_base.bash.j2`'s shell part (lines 13-70 — the `#SBATCH`
/// header is job-manager's SbatchCmd territory and is deliberately NOT
/// emitted here). The inherited conda-stack reset block is a fixed
/// load-bearing string (mirrors the learned `pixi-conda-stack-reset`
/// skill); only `conda_env` / `module_block` / `body_block` /
/// `pixi_manifest` vary.
pub fn base_preamble(o: &PreambleOpts<'_>) -> String {
    let pixi = if o.pixi_manifest.is_empty() {
        String::new()
    } else {
        format!(
            "\n# Activate the pixi environment so workspace CLIs are on PATH.\n\
             eval \"$(pixi shell-hook --manifest-path \"{}\")\"\n",
            o.pixi_manifest
        )
    };
    format!(
        r#"#!/bin/bash
# Generated by `jm new`. EDIT FREELY — this is your job logic.
set -euo pipefail

# Wipe conda activation state inherited from the parent (login) shell —
# both env vars and the `conda` shell function. A later `module restore`
# triggers unload hooks that call `conda deactivate`; a partially-corrupt
# inherited CONDA_PREFIX_<N> stack makes that abort with "non-consecutive
# CONDA_PREFIX_<number>". Resetting first guarantees a clean slate.
set +u
unset -f conda 2>/dev/null || true
for _v in $(env 2>/dev/null | awk -F= '/^CONDA_/{{print $1}}'); do
    unset "$_v" || true
done
unset _v
set -u

# Load `conda`, because `module` unload hooks may call `conda deactivate`.
set +u
source "$(conda info --base)/etc/profile.d/conda.sh"
set -u

# Reload the module system for this SLURM shell.
. /usr/share/Modules/init/bash

# Load module(s).
{module_block}

set +u
conda activate {conda_env}
set -u
{pixi}
# ----------------------------------------------------------------------------
# JOB BODY
# ----------------------------------------------------------------------------
{body_block}
echo "JOB DONE"
exit 0
"#,
        module_block = o.module_block,
        conda_env = o.conda_env,
        pixi = pixi,
        body_block = o.body_block,
    )
}
```

- [ ] **Step 4: Re-export + run**

Add `PreambleOpts, base_preamble` to `src/recipes/mod.rs` `pub use job::{...}` and `src/lib.rs`.
Run: `cargo test --lib --no-default-features recipes::job::tests::preamble 2>&1 | tail -10`
Expected: PASS — all four `preamble_*` ok.

- [ ] **Step 5: Commit**

```bash
git add src/recipes/job.rs src/recipes/mod.rs src/lib.rs
git commit -m "feat(recipes): base_preamble() — _base.bash.j2 shell port"
```

---

## Task 4: `parse_xyz()` — pure `.xyz` parser

**Files:**
- Modify: `src/recipes/job.rs`, `src/recipes/mod.rs`, `src/lib.rs`
- Test: `src/recipes/job.rs` (inline)

- [ ] **Step 1: Write failing tests** — append to `src/recipes/job.rs` `mod tests`:

```rust
    #[test]
    fn parse_xyz_valid_two_atoms() {
        let xyz = "2\nwater\nO   0.000000  0.000000  0.117300\nH   0.000000  0.757200 -0.469200\n";
        let lines = parse_xyz(xyz).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "O      0.000000     0.000000     0.117300");
        assert_eq!(lines[1], "H      0.000000     0.757200    -0.469200");
    }

    #[test]
    fn parse_xyz_rejects_count_mismatch() {
        let err = parse_xyz("3\nc\nO 0 0 0\nH 0 0 1\n").unwrap_err();
        assert!(matches!(err, RecipeError::Xyz(_)));
    }

    #[test]
    fn parse_xyz_rejects_bad_coord() {
        assert!(matches!(
            parse_xyz("1\nc\nO 0 zzz 0\n").unwrap_err(),
            RecipeError::Xyz(_)
        ));
    }

    #[test]
    fn parse_xyz_rejects_too_few_lines() {
        assert!(matches!(parse_xyz("1\n").unwrap_err(), RecipeError::Xyz(_)));
    }
```

- [ ] **Step 2: Run → fail**

Run: `cargo test --lib --no-default-features recipes::job::tests::parse_xyz 2>&1 | tail -6`
Expected: COMPILE FAIL — `cannot find function \`parse_xyz\``.

- [ ] **Step 3: Implement** — add to `src/recipes/job.rs` (after `base_preamble`):

```rust
/// Parse a standard `.xyz` (line 1 = atom count, line 2 = comment, then
/// `Element x y z`). Returns Gaussian-ready lines
/// `{sym:<2} {x:>12.6} {y:>12.6} {z:>12.6}`. Pure — no I/O, no chemistry lib.
pub fn parse_xyz(text: &str) -> Result<Vec<String>, RecipeError> {
    let mut lines = text.lines();
    let count_line = lines
        .next()
        .ok_or_else(|| RecipeError::Xyz("empty file".into()))?;
    let n: usize = count_line
        .trim()
        .parse()
        .map_err(|_| RecipeError::Xyz(format!("line 1 not an atom count: {count_line:?}")))?;
    lines
        .next()
        .ok_or_else(|| RecipeError::Xyz("missing comment line".into()))?;
    let mut out = Vec::with_capacity(n);
    for (i, raw) in lines.enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        let mut it = raw.split_whitespace();
        let sym = it
            .next()
            .ok_or_else(|| RecipeError::Xyz(format!("atom {} has no element", i + 1)))?;
        let mut coord = [0.0f64; 3];
        for (k, slot) in coord.iter_mut().enumerate() {
            let tok = it.next().ok_or_else(|| {
                RecipeError::Xyz(format!("atom {} missing coordinate {}", i + 1, k + 1))
            })?;
            *slot = tok.parse().map_err(|_| {
                RecipeError::Xyz(format!("atom {} coordinate {tok:?} not a number", i + 1))
            })?;
        }
        out.push(format!(
            "{:<2} {:>12.6} {:>12.6} {:>12.6}",
            sym, coord[0], coord[1], coord[2]
        ));
        if out.len() == n {
            break;
        }
    }
    if out.len() != n {
        return Err(RecipeError::Xyz(format!(
            "declared {n} atoms but found {}",
            out.len()
        )));
    }
    Ok(out)
}
```

- [ ] **Step 4: Re-export + run**

Add `parse_xyz` to `src/recipes/mod.rs` `pub use job::{...}` and `src/lib.rs`.
Run: `cargo test --lib --no-default-features recipes::job::tests::parse_xyz 2>&1 | tail -8`
Expected: PASS — 4 `parse_xyz_*` ok. (Asserts match `{:<2} {:>12.6}` output exactly.)

- [ ] **Step 5: Commit**

```bash
git add src/recipes/job.rs src/recipes/mod.rs src/lib.rs
git commit -m "feat(recipes): pure .xyz geometry parser"
```

---

## Task 5: `g16_opt` JobTemplate + assets

**Files:**
- Create: `src/recipes/assets/g16_opt/main.gjf.tmpl`, `src/recipes/assets/g16_opt/run.py.tmpl`, `src/recipes/jobs/mod.rs`, `src/recipes/jobs/g16_opt.rs`
- Modify: `src/recipes/mod.rs`
- Test: `src/recipes/jobs/g16_opt.rs` (inline)

- [ ] **Step 1: Create `src/recipes/assets/g16_opt/main.gjf.tmpl`** (exact bytes; NO `%rwf`):

```
%nprocshared={{nproc}}
%mem={{mem}}
%chk=main.chk
{{route}}

{{compound}}

{{charge}} {{multiplicity}}
{{geometry_block}}
{{extra_input}}
```

- [ ] **Step 2: Create `src/recipes/assets/g16_opt/run.py.tmpl`** (exact bytes — reproduces `run_g16`; no `{{}}`):

```python
#!/usr/bin/env python3
# Generated by `jm new` (recipe g16_opt). EDIT FREELY — this is your job.
# Reproduces gaussian_compute_runtime.run_g16 in pure stdlib (no Group C/D):
#   prepare(input/ -> scratch) -> [JM_LAUNCHER] g16 (cwd=scratch)
#   -> finally copy_results(scratch -> output/);  exit: g16 rc > copy rc.
# srun wraps ONLY the g16 subprocess (KUDPC); this orchestrator runs bare.
#
# REPLACE_ME: if this site has the gem stack installed, replace main()
# body with a subprocess call to:
#   python -m gaussian_compute_runtime run-g16 --config <abs gem job toml>
import os
import shutil
import subprocess
import sys

TASK = "main"


def main() -> int:
    job_dir = os.getcwd()  # R3: flow.toml body cd'd here (persistent job dir)
    g16 = os.environ.get("JM_PARAM_G16_CMD", "g16")
    launcher = os.environ.get("JM_LAUNCHER", "")  # resolved at render time
    scratch_root = os.environ.get("JM_SCRATCH_ROOT", "") or os.path.join(
        job_dir, ".scratch"
    )
    flow_uuid = os.environ.get("JM_FLOW_UUID", "flow")
    job_id = os.environ.get("JM_JOB_ID", "job")
    scratch = os.path.join(scratch_root, flow_uuid, job_id)

    src_input = os.path.join(job_dir, "input")
    out_dir = os.path.join(job_dir, "output")

    try:
        os.makedirs(scratch, exist_ok=True)
        if not os.path.isdir(src_input):
            print(f"error: input dir not found: {src_input}", file=sys.stderr)
            return 2
        shutil.copytree(src_input, scratch, dirs_exist_ok=True)
    except OSError as e:
        print(f"error: prepare_inputs failed: {e}", file=sys.stderr)
        return 3

    argv = ([launcher] if launcher else []) + [g16, f"{TASK}.gjf", f"{TASK}.out"]

    rc = 0
    copy_failed = False
    try:
        try:
            proc = subprocess.run(argv, cwd=scratch, check=False)
            rc = proc.returncode
        except FileNotFoundError as e:
            # srun/g16 not on PATH (typically a failed `module restore`).
            # Never return 0 — an afterok post must not treat a missing
            # g16 as success and run on an empty .out.
            print(f"error: failed to launch {argv[0]}: {e}", file=sys.stderr)
            rc = 2
    finally:
        # copy_results: scratch -> output/ even on g16 failure, so a
        # partial .out is retrievable for inspection.
        try:
            os.makedirs(out_dir, exist_ok=True)
            for name in sorted(os.listdir(scratch)):
                if (
                    name == "main.out"
                    or name == "main.chk"
                    or name.endswith(".log")
                ):
                    shutil.copy2(
                        os.path.join(scratch, name), os.path.join(out_dir, name)
                    )
        except OSError as e:
            copy_failed = True
            print(f"error: copy_results failed: {e}", file=sys.stderr)

    if rc != 0:
        return rc  # g16 rc takes priority over a copy failure
    return 3 if copy_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 3: Create `src/recipes/jobs/mod.rs`** (only g16_opt now; parse_g16_out added Task 6 Step 4):

```rust
pub mod g16_opt;
```

- [ ] **Step 4: Create `src/recipes/jobs/g16_opt.rs`**

```rust
//! `g16_opt` JobTemplate — Gaussian geometry optimization.

use std::collections::BTreeMap;

use crate::recipes::job::{
    JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeParam, RecipeParamType, base_preamble,
};
use crate::recipes::GeneratedFile;

const GJF_TMPL: &str = include_str!("../assets/g16_opt/main.gjf.tmpl");
const RUN_PY_TMPL: &str = include_str!("../assets/g16_opt/run.py.tmpl");

const GEOMETRY_SENTINEL: &str =
    "<GEOMETRY: REPLACE_ME — one atom per line: Element x y z; if the route \
     contains geom=connectivity, add a blank line then the connectivity block>";

pub struct G16Opt;

const PARAMS: &[RecipeParam] = &[
    RecipeParam { name: "route", ty: RecipeParamType::Str, default: "#p opt b3lyp/6-31g(d)", help: "Gaussian route line" },
    RecipeParam { name: "charge", ty: RecipeParamType::Int, default: "0", help: "total charge" },
    RecipeParam { name: "multiplicity", ty: RecipeParamType::Int, default: "1", help: "spin multiplicity" },
    RecipeParam { name: "extra_input", ty: RecipeParamType::Str, default: "", help: "extra input after geometry" },
    RecipeParam { name: "nproc", ty: RecipeParamType::Int, default: "8", help: "scaffold %nprocshared (run.py may override from SLURM env)" },
    RecipeParam { name: "mem", ty: RecipeParamType::Str, default: "8GB", help: "scaffold %mem" },
    RecipeParam { name: "compound", ty: RecipeParamType::Str, default: "REPLACE_ME-INCHIKEY", help: "InChIKey; gjf title + [tags].compound" },
    RecipeParam { name: "g16_cmd", ty: RecipeParamType::Str, default: "g16", help: "Gaussian binary -> JM_PARAM_G16_CMD" },
    RecipeParam { name: "conda_env", ty: RecipeParamType::Str, default: "analysis", help: "conda env for the preamble" },
    RecipeParam { name: "module_profile", ty: RecipeParamType::Str, default: "gaussian_A", help: "module restore <profile> -f" },
    RecipeParam { name: "pixi_manifest", ty: RecipeParamType::Path, default: "", help: "empty = no pixi hook" },
    RecipeParam { name: "launcher", ty: RecipeParamType::Str, default: "", help: "per-flow launcher override; empty = defer to common.toml -> srun" },
    RecipeParam { name: "scratch_root", ty: RecipeParamType::Path, default: "", help: "per-flow scratch override; empty = defer to common.toml -> .scratch" },
    RecipeParam { name: "input_coordinate", ty: RecipeParamType::Path, default: "", help: "molecule coord file (.xyz/.mol2); cmd_new copies into <JobId>/input/" },
];

fn pstr(p: &BTreeMap<String, toml::Value>, k: &str) -> String {
    match p.get(k) {
        Some(toml::Value::String(s)) => s.clone(),
        Some(toml::Value::Integer(i)) => i.to_string(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

impl JobTemplate for G16Opt {
    fn name(&self) -> &'static str {
        "g16_opt"
    }
    fn params(&self) -> &'static [RecipeParam] {
        PARAMS
    }
    fn inputs(&self) -> &'static [&'static str] {
        &[]
    }
    fn outputs(&self) -> &'static [(&'static str, &'static str)] {
        &[("gaussian_out", "output/main.out")]
    }
    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, crate::recipes::RecipeError> {
        let p = ctx.params;
        let gjf = GJF_TMPL
            .replace("{{nproc}}", &pstr(p, "nproc"))
            .replace("{{mem}}", &pstr(p, "mem"))
            .replace("{{route}}", &pstr(p, "route"))
            .replace("{{compound}}", &pstr(p, "compound"))
            .replace("{{charge}}", &pstr(p, "charge"))
            .replace("{{multiplicity}}", &pstr(p, "multiplicity"))
            .replace("{{geometry_block}}", GEOMETRY_SENTINEL)
            .replace("{{extra_input}}", &pstr(p, "extra_input"));

        let bash = base_preamble(&PreambleOpts {
            conda_env: &pstr(p, "conda_env"),
            module_block: &format!("module restore {} -f", pstr(p, "module_profile")),
            body_block: "python scripts/run.py",
            pixi_manifest: &pstr(p, "pixi_manifest"),
        });

        let jid = ctx.job_id;
        let mut plan_params: BTreeMap<String, toml::Value> = BTreeMap::new();
        for rp in PARAMS {
            if rp.name == "input_coordinate" {
                continue; // copied by cmd_new (Plan C), not a render param
            }
            if let Some(v) = p.get(rp.name) {
                plan_params.insert(rp.name.to_string(), v.clone());
            }
        }

        Ok(JobArtifacts {
            program: "g16".to_string(),
            body: format!("bash scripts/{jid}.bash\n"),
            time_limit: Some("48:00:00".to_string()),
            plan_params,
            sidecars: vec![
                GeneratedFile {
                    relpath: format!("{jid}/scripts/{jid}.bash").into(),
                    contents: bash,
                    unix_mode: Some(0o755),
                },
                GeneratedFile {
                    relpath: format!("{jid}/scripts/run.py").into(),
                    contents: RUN_PY_TMPL.to_string(),
                    unix_mode: Some(0o755),
                },
                GeneratedFile {
                    relpath: format!("{jid}/input/main.gjf").into(),
                    contents: gjf,
                    unix_mode: None,
                },
            ],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn defaults() -> BTreeMap<String, toml::Value> {
        let mut m = BTreeMap::new();
        for rp in PARAMS {
            m.insert(
                rp.name.to_string(),
                match rp.ty {
                    RecipeParamType::Int => {
                        toml::Value::Integer(rp.default.parse().unwrap_or(0))
                    }
                    _ => toml::Value::String(rp.default.to_string()),
                },
            );
        }
        m
    }

    fn run(jid: &str, p: &BTreeMap<String, toml::Value>) -> JobArtifacts {
        let inp = BTreeMap::new();
        let u = Uuid::nil();
        let ctx = JobCtx {
            job_id: jid,
            params: p,
            inputs: &inp,
            uuid: &u,
            created_at: "2026-05-17T00:00:00Z",
        };
        G16Opt.instantiate(&ctx).unwrap()
    }

    #[test]
    fn program_body_sidecars() {
        let a = run("opt", &defaults());
        assert_eq!(a.program, "g16");
        assert_eq!(a.body, "bash scripts/opt.bash\n");
        assert_eq!(a.time_limit.as_deref(), Some("48:00:00"));
        let names: Vec<String> = a
            .sidecars
            .iter()
            .map(|f| f.relpath.to_string_lossy().into_owned())
            .collect();
        assert!(names.contains(&"opt/scripts/opt.bash".to_string()));
        assert!(names.contains(&"opt/scripts/run.py".to_string()));
        assert!(names.contains(&"opt/input/main.gjf".to_string()));
        for f in &a.sidecars {
            if f.relpath.extension().is_some_and(|e| e == "bash" || e == "py") {
                assert_eq!(f.unix_mode, Some(0o755));
            }
        }
    }

    #[test]
    fn bash_has_preamble_calls_run_py_no_srun() {
        let a = run("opt", &defaults());
        let bash = &a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("opt.bash"))
            .unwrap()
            .contents;
        assert!(bash.contains("module restore gaussian_A -f"));
        assert!(bash.contains("conda activate analysis"));
        assert!(bash.contains("python scripts/run.py"));
        assert!(!bash.contains("srun"), "bash must not wrap srun (run.py does)");
        assert!(bash.contains("unset -f conda"));
    }

    #[test]
    fn gjf_no_rwf_sentinel_geometry_filled_params() {
        let mut p = defaults();
        p.insert("charge".into(), toml::Value::Integer(1));
        p.insert("multiplicity".into(), toml::Value::Integer(2));
        let a = run("opt", &p);
        let gjf = &a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("main.gjf"))
            .unwrap()
            .contents;
        assert!(!gjf.contains("%rwf"));
        assert!(gjf.contains("%chk=main.chk"));
        assert!(gjf.contains("1 2"));
        assert!(gjf.contains("REPLACE_ME"));
        assert!(!gjf.contains("{{"));
    }

    #[test]
    fn run_py_reproduces_run_g16_shape() {
        let a = run("opt", &defaults());
        let r = &a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("run.py"))
            .unwrap()
            .contents;
        assert!(r.contains("shutil.copytree(src_input, scratch"));
        assert!(r.contains("cwd=scratch"));
        assert!(r.contains("finally:"));
        assert!(r.contains("JM_LAUNCHER"));
        assert!(r.contains("JM_SCRATCH_ROOT"));
        assert!(r.contains("failed to launch"));
        assert!(r.contains("return rc"));
        assert!(r.contains("REPLACE_ME"));
        assert!(!r.contains("gaussian_job_shared"));
        assert!(!r.contains("import cclib"));
    }

    #[test]
    fn plan_params_exclude_input_coordinate() {
        let a = run("opt", &defaults());
        assert!(!a.plan_params.contains_key("input_coordinate"));
        assert!(a.plan_params.contains_key("route"));
        assert!(a.plan_params.contains_key("launcher"));
        assert!(a.plan_params.contains_key("scratch_root"));
    }
}
```

- [ ] **Step 5: Wire module + run**

Add `pub mod jobs;` to `src/recipes/mod.rs` (after `pub mod job;`).
Run: `cargo test --lib --no-default-features recipes::jobs::g16_opt 2>&1 | tail -15`
Expected: PASS — 5 `g16_opt` tests ok.

- [ ] **Step 6: Commit**

```bash
git add src/recipes/jobs src/recipes/assets/g16_opt src/recipes/mod.rs
git commit -m "feat(recipes): g16_opt JobTemplate + gjf/run.py assets"
```

---

## Task 6: `parse_g16_out` JobTemplate + asset

**Files:**
- Create: `src/recipes/assets/parse_g16_out/parse.py.tmpl`, `src/recipes/jobs/parse_g16_out.rs`
- Modify: `src/recipes/jobs/mod.rs`
- Test: `src/recipes/jobs/parse_g16_out.rs` (inline)

- [ ] **Step 1: Create `src/recipes/assets/parse_g16_out/parse.py.tmpl`** (exact bytes — reproduces `parse_results`; `{{inputs.gaussian_out}}` filled by assemble):

```python
#!/usr/bin/env python3
# Generated by `jm new` (recipe parse_g16_out). EDIT FREELY.
# Reproduces gaussian_compute_runtime.parse_results via cclib (no Group C
# dep): parse the finished .out, validate, write a curated result.json.
# Light post step — runs bare like gem's gaussian_post.bash.j2.
#
# REPLACE_ME: if this site has the gem stack installed, replace main()
# body with a subprocess call to:
#   python -m gaussian_compute_runtime parse-results --config <abs gem toml>
import json
import math
import os
import sys
import tempfile

# Resolved by the recipe wiring to the producer's .out (relative path).
GAUSSIAN_OUT = "{{inputs.gaussian_out}}"


def _atomic_write_json(path, obj):
    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="." + os.path.basename(path) + ".", dir=parent)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main() -> int:
    try:
        import cclib
    except ImportError as e:
        print(f"error: cclib not importable: {e}", file=sys.stderr)
        return 2

    src = GAUSSIAN_OUT
    if not os.path.isfile(src):
        print(f"error: gaussian .out not found: {src}", file=sys.stderr)
        return 1
    try:
        data = cclib.io.ccread(src)
    except Exception as e:
        print(f"error: cclib could not parse {src}: {e}", file=sys.stderr)
        return 1
    if data is None:
        print(f"error: cclib returned no data for {src}", file=sys.stderr)
        return 1

    metadata = getattr(data, "metadata", {}) or {}
    if not metadata.get("success", False):
        print("error: gaussian run did not terminate normally", file=sys.stderr)
        return 1

    optdone = getattr(data, "optdone", None)
    if optdone is not None and optdone is not True:
        print("error: optimization did not converge", file=sys.stderr)
        return 1

    scfenergies = getattr(data, "scfenergies", None)
    if scfenergies is None or len(scfenergies) == 0:
        print("error: no SCF energy parsed", file=sys.stderr)
        return 1
    scf = float(scfenergies[-1])
    if not math.isfinite(scf):
        print("error: final SCF energy is not finite", file=sys.stderr)
        return 1

    result = {
        "schema": "jm-recipe/1",
        "source": os.path.abspath(src),
        "converged": optdone is True or optdone is None,
        "scf_energy_ev": scf,
        "n_atoms": int(getattr(data, "natom", 0)),
    }
    target = os.path.join(os.getcwd(), "output", "result.json")
    try:
        _atomic_write_json(target, result)
    except OSError as e:
        print(f"error: could not write {target}: {e}", file=sys.stderr)
        return 3

    # TODO(jm recipe): write derived/main.mol2 for multi-step g16 chains
    # (parent geometry handoff). Not needed for v1 opt->parse.
    print(f"[parse] {src} -> {target}  scf={scf} eV")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Create `src/recipes/jobs/parse_g16_out.rs`**

```rust
//! `parse_g16_out` JobTemplate — cclib validation of a finished g16 .out.

use std::collections::BTreeMap;

use crate::recipes::job::{
    JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeParam, RecipeParamType, base_preamble,
};
use crate::recipes::GeneratedFile;

const PARSE_PY_TMPL: &str = include_str!("../assets/parse_g16_out/parse.py.tmpl");

pub struct ParseG16Out;

const PARAMS: &[RecipeParam] = &[
    RecipeParam { name: "conda_env", ty: RecipeParamType::Str, default: "analysis", help: "conda env that has cclib" },
    RecipeParam { name: "pixi_manifest", ty: RecipeParamType::Path, default: "", help: "empty = no pixi hook" },
];

fn pstr(p: &BTreeMap<String, toml::Value>, k: &str) -> String {
    match p.get(k) {
        Some(toml::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

impl JobTemplate for ParseG16Out {
    fn name(&self) -> &'static str {
        "parse_g16_out"
    }
    fn params(&self) -> &'static [RecipeParam] {
        PARAMS
    }
    fn inputs(&self) -> &'static [&'static str] {
        &["gaussian_out"]
    }
    fn outputs(&self) -> &'static [(&'static str, &'static str)] {
        &[("result_json", "output/result.json")]
    }
    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, crate::recipes::RecipeError> {
        let p = ctx.params;
        let gaussian_out = ctx.inputs.get("gaussian_out").cloned().ok_or_else(|| {
            crate::recipes::RecipeError::WiringMismatch {
                consumer: ctx.job_id.to_string(),
                input: "gaussian_out".into(),
                producer: "<unset>".into(),
                output: "<unset>".into(),
                detail: "instantiate called without resolved input".into(),
            }
        })?;
        let parse_py = PARSE_PY_TMPL.replace("{{inputs.gaussian_out}}", &gaussian_out);
        let bash = base_preamble(&PreambleOpts {
            conda_env: &pstr(p, "conda_env"),
            module_block: "module restore default -f",
            body_block: "python scripts/parse.py",
            pixi_manifest: &pstr(p, "pixi_manifest"),
        });
        let jid = ctx.job_id;
        let mut plan_params: BTreeMap<String, toml::Value> = BTreeMap::new();
        for rp in PARAMS {
            if let Some(v) = p.get(rp.name) {
                plan_params.insert(rp.name.to_string(), v.clone());
            }
        }
        Ok(JobArtifacts {
            program: "python".to_string(),
            body: format!("bash scripts/{jid}.bash\n"),
            time_limit: Some("01:00:00".to_string()),
            plan_params,
            sidecars: vec![
                GeneratedFile {
                    relpath: format!("{jid}/scripts/{jid}.bash").into(),
                    contents: bash,
                    unix_mode: Some(0o755),
                },
                GeneratedFile {
                    relpath: format!("{jid}/scripts/parse.py").into(),
                    contents: parse_py,
                    unix_mode: Some(0o755),
                },
            ],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn instantiate_resolves_wiring_builds_parse_py() {
        let p: BTreeMap<String, toml::Value> = PARAMS
            .iter()
            .map(|rp| (rp.name.to_string(), toml::Value::String(rp.default.into())))
            .collect();
        let mut inp: BTreeMap<&'static str, String> = BTreeMap::new();
        inp.insert("gaussian_out", "../opt/output/main.out".to_string());
        let u = Uuid::nil();
        let ctx = JobCtx {
            job_id: "parse",
            params: &p,
            inputs: &inp,
            uuid: &u,
            created_at: "2026-05-17T00:00:00Z",
        };
        let a = ParseG16Out.instantiate(&ctx).unwrap();
        assert_eq!(a.program, "python");
        assert_eq!(a.body, "bash scripts/parse.bash\n");
        assert_eq!(a.time_limit.as_deref(), Some("01:00:00"));
        let parse = &a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("parse.py"))
            .unwrap()
            .contents;
        assert!(parse.contains(r#"GAUSSIAN_OUT = "../opt/output/main.out""#));
        assert!(parse.contains("import cclib"));
        assert!(parse.contains("result.json"));
        assert!(parse.contains("jm-recipe/1"));
        assert!(parse.contains("TODO(jm recipe): write derived/main.mol2"));
        assert!(!parse.contains("{{"));
        let bash = &a
            .sidecars
            .iter()
            .find(|f| f.relpath.ends_with("parse.bash"))
            .unwrap()
            .contents;
        assert!(bash.contains("module restore default -f"));
        assert!(bash.contains("python scripts/parse.py"));
        assert!(!bash.contains("srun"));
    }

    #[test]
    fn outputs_declares_result_json() {
        assert_eq!(
            ParseG16Out.outputs(),
            &[("result_json", "output/result.json")]
        );
        assert_eq!(ParseG16Out.inputs(), &["gaussian_out"]);
    }
}
```

- [ ] **Step 3: Run → fail (module not declared)**

Run: `cargo test --lib --no-default-features recipes::jobs::parse_g16_out 2>&1 | tail -6`
Expected: COMPILE FAIL — unresolved `parse_g16_out`.

- [ ] **Step 4: Declare module + run**

Set `src/recipes/jobs/mod.rs` to:
```rust
pub mod g16_opt;
pub mod parse_g16_out;
```
Run: `cargo test --lib --no-default-features recipes::jobs::parse_g16_out 2>&1 | tail -10`
Expected: PASS — both tests ok.

- [ ] **Step 5: Commit**

```bash
git add src/recipes/jobs src/recipes/assets/parse_g16_out
git commit -m "feat(recipes): parse_g16_out JobTemplate + cclib parse.py asset"
```

---

## Task 7: `--param` parse + type-check

**Files:**
- Modify: `src/recipes/mod.rs`
- Test: `src/recipes/mod.rs` (inline)

- [ ] **Step 1: Write failing tests** — append to `src/recipes/mod.rs`:

```rust
#[cfg(test)]
mod param_tests {
    use super::*;
    use crate::recipes::RecipeParam;
    use crate::recipes::RecipeParamType::*;
    use std::collections::BTreeMap;

    const P: &[RecipeParam] = &[
        RecipeParam { name: "charge", ty: Int, default: "0", help: "" },
        RecipeParam { name: "route", ty: Str, default: "#p", help: "" },
        RecipeParam { name: "fast", ty: Bool, default: "false", help: "" },
    ];

    #[test]
    fn splits_on_first_delims_value_keeps_equals() {
        let mut by_node: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        parse_param_raw("opt.route=#p opt=b3lyp", &mut by_node).unwrap();
        assert_eq!(by_node["opt"]["route"], "#p opt=b3lyp");
    }

    #[test]
    fn rejects_missing_delims() {
        let mut m = BTreeMap::new();
        assert!(matches!(
            parse_param_raw("noeq", &mut m),
            Err(RecipeError::BadParamSyntax(_))
        ));
        assert!(matches!(
            parse_param_raw("nodot=v", &mut m),
            Err(RecipeError::BadParamSyntax(_))
        ));
    }

    #[test]
    fn typecheck_fills_defaults_and_overrides() {
        let mut ov: BTreeMap<String, String> = BTreeMap::new();
        ov.insert("charge".into(), "3".into());
        let out = typecheck_node("f", "opt", P, &ov).unwrap();
        assert_eq!(out["charge"], toml::Value::Integer(3));
        assert_eq!(out["route"], toml::Value::String("#p".into()));
        assert_eq!(out["fast"], toml::Value::Boolean(false));
    }

    #[test]
    fn typecheck_rejects_unknown_param() {
        let mut ov = BTreeMap::new();
        ov.insert("bogus".into(), "x".into());
        assert!(matches!(
            typecheck_node("f", "opt", P, &ov),
            Err(RecipeError::UnknownParam { .. })
        ));
    }

    #[test]
    fn typecheck_rejects_bad_int() {
        let mut ov = BTreeMap::new();
        ov.insert("charge".into(), "notint".into());
        assert!(matches!(
            typecheck_node("f", "opt", P, &ov),
            Err(RecipeError::TypeMismatch { .. })
        ));
    }
}
```

- [ ] **Step 2: Run → fail**

Run: `cargo test --lib --no-default-features recipes::param_tests 2>&1 | tail -6`
Expected: COMPILE FAIL — `cannot find function \`parse_param_raw\`` / `typecheck_node`.

- [ ] **Step 3: Implement** — add to `src/recipes/mod.rs` (above the `#[cfg(test)]`):

```rust
use std::collections::BTreeMap;

use crate::recipes::job::{RecipeError, RecipeParam, RecipeParamType};

/// Parse one `--param <JobId>.<param>=<value>`. Splits on the FIRST `=`
/// (value may contain `=`) then the key on the FIRST `.`.
pub fn parse_param_raw(
    raw: &str,
    by_node: &mut BTreeMap<String, BTreeMap<String, String>>,
) -> Result<(), RecipeError> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| RecipeError::BadParamSyntax(raw.to_string()))?;
    let (jobid, param) = key
        .split_once('.')
        .ok_or_else(|| RecipeError::BadParamSyntax(raw.to_string()))?;
    if jobid.is_empty() || param.is_empty() {
        return Err(RecipeError::BadParamSyntax(raw.to_string()));
    }
    by_node
        .entry(jobid.to_string())
        .or_default()
        .insert(param.to_string(), value.to_string());
    Ok(())
}

/// Fill `params` defaults, apply string overrides, type-check each.
pub fn typecheck_node(
    flow: &str,
    jobid: &str,
    params: &[RecipeParam],
    overrides: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, toml::Value>, RecipeError> {
    let known: std::collections::BTreeSet<&str> = params.iter().map(|p| p.name).collect();
    for k in overrides.keys() {
        if !known.contains(k.as_str()) {
            return Err(RecipeError::UnknownParam {
                flow: flow.to_string(),
                jobid: jobid.to_string(),
                param: k.clone(),
                known: known.iter().cloned().collect::<Vec<_>>().join(", "),
            });
        }
    }
    let mut out = BTreeMap::new();
    for rp in params {
        let raw = overrides
            .get(rp.name)
            .map(String::as_str)
            .unwrap_or(rp.default);
        out.insert(rp.name.to_string(), coerce(flow, jobid, rp, raw)?);
    }
    Ok(out)
}

fn coerce(
    flow: &str,
    jobid: &str,
    rp: &RecipeParam,
    raw: &str,
) -> Result<toml::Value, RecipeError> {
    let mismatch = || RecipeError::TypeMismatch {
        flow: flow.to_string(),
        jobid: jobid.to_string(),
        param: rp.name.to_string(),
        ty: rp.ty,
        value: raw.to_string(),
    };
    Ok(match rp.ty {
        RecipeParamType::Str | RecipeParamType::Path => toml::Value::String(raw.to_string()),
        RecipeParamType::Int => {
            toml::Value::Integer(raw.parse::<i64>().map_err(|_| mismatch())?)
        }
        RecipeParamType::Float => {
            toml::Value::Float(raw.parse::<f64>().map_err(|_| mismatch())?)
        }
        RecipeParamType::Bool => {
            toml::Value::Boolean(raw.parse::<bool>().map_err(|_| mismatch())?)
        }
    })
}
```

- [ ] **Step 4: Run → pass + commit**

Run: `cargo test --lib --no-default-features recipes::param_tests 2>&1 | tail -10`
Expected: PASS — 5 `param_tests` ok.
```bash
git add src/recipes/mod.rs
git commit -m "feat(recipes): --param parse + per-type coercion"
```

---

## Task 8: `assemble()` + `g16-opt-parse` FlowRecipe

**Files:**
- Modify: `src/recipes/flow.rs`, `src/recipes/mod.rs`, `src/lib.rs`
- Create: `src/recipes/flows/mod.rs`, `src/recipes/flows/g16_opt_parse.rs`
- Test: `src/recipes/flow.rs` (inline)

- [ ] **Step 1: Create `src/recipes/flows/mod.rs`** (only g16_opt_parse now; blank added Task 9 Step 3):

```rust
pub mod g16_opt_parse;
```

- [ ] **Step 2: Create `src/recipes/flows/g16_opt_parse.rs`**

```rust
//! `g16-opt-parse` FlowRecipe: opt (g16_opt) -afterok-> parse (parse_g16_out).

use crate::recipes::FlowRecipe;

pub struct G16OptParse;

impl FlowRecipe for G16OptParse {
    fn name(&self) -> &'static str {
        "g16-opt-parse"
    }
    fn summary(&self) -> &'static str {
        "Gaussian geometry optimization, then cclib validation of the .out"
    }
    fn nodes(&self) -> &'static [(&'static str, &'static str)] {
        &[("opt", "g16_opt"), ("parse", "parse_g16_out")]
    }
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)] {
        &[("opt", "parse", "afterok")]
    }
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)] {
        &[("parse", "gaussian_out", "opt", "gaussian_out")]
    }
}
```

- [ ] **Step 3: Write failing `assemble` tests** — append to `src/recipes/flow.rs` `mod tests`:

```rust
    use crate::plan::ExperimentPlan;
    use crate::recipes::flows::g16_opt_parse::G16OptParse;
    use gaussian_job_shared::entities::workflow::{JobFlow, JobId};
    use std::collections::BTreeMap;
    use std::path::Path;
    use uuid::Uuid;

    fn rel<'a>(files: &'a [crate::recipes::GeneratedFile], p: &str) -> &'a str {
        files
            .iter()
            .find(|f| f.relpath.to_string_lossy() == p)
            .unwrap_or_else(|| panic!("missing {p}"))
            .contents
            .as_str()
    }

    #[test]
    fn assemble_g16_opt_parse_is_doctor_clean() {
        let u = Uuid::now_v7();
        let abs = Path::new("/work").join(u.to_string());
        let files = assemble(
            &G16OptParse,
            &["opt.charge=1".to_string()],
            &BTreeMap::new(),
            &u,
            "2026-05-17T00:00:00Z",
            &abs,
        )
        .unwrap();

        let flow_txt = rel(&files, "flow.toml");
        let flow: JobFlow = toml::from_str(flow_txt).expect("flow.toml parses");
        assert_eq!(flow.uuid, u);
        let ids: std::collections::BTreeSet<String> =
            flow.jobs.keys().map(|j| j.0.clone()).collect();
        assert_eq!(
            ids,
            ["opt", "parse"].iter().map(|s| s.to_string()).collect()
        );
        let parse = flow.jobs.get(&JobId("parse".into())).unwrap();
        assert_eq!(parse.parents.len(), 1);
        assert_eq!(parse.parents[0].from.0, "opt");
        for (_, j) in &flow.jobs {
            assert_eq!(j.spec.config.partition, "REPLACE_ME");
        }
        assert!(flow_txt.contains(&format!(r#"cd \"{}/opt\""#, abs.display()))
            || flow_txt.contains(&format!("{}/opt", abs.display())));
        assert!(flow_txt.contains("bash scripts/opt.bash"));

        let plan_txt = rel(&files, "plan.toml");
        let plan: ExperimentPlan = toml::from_str(plan_txt).expect("plan.toml parses");
        let pkeys: std::collections::BTreeSet<String> =
            plan.jobs.keys().map(|j| j.0.clone()).collect();
        assert_eq!(pkeys, ids, "flow JobId set == plan key set");
        assert_eq!(
            plan.jobs.get(&JobId("opt".into())).unwrap().get("charge"),
            Some(&toml::Value::Integer(1))
        );

        assert!(rel(&files, "parse/scripts/parse.py")
            .contains(r#"GAUSSIAN_OUT = "../opt/output/main.out""#));
        assert!(rel(&files, "opt/scripts/run.py").contains("cwd=scratch"));
        assert!(!rel(&files, "opt/input/main.gjf").contains("%rwf"));
        assert_eq!(
            files
                .iter()
                .find(|f| f.relpath.to_string_lossy() == "opt/scripts/opt.bash")
                .unwrap()
                .unix_mode,
            Some(0o755)
        );
    }

    #[test]
    fn assemble_rejects_unknown_param() {
        let u = Uuid::now_v7();
        let e = assemble(
            &G16OptParse,
            &["opt.bogus=1".to_string()],
            &BTreeMap::new(),
            &u,
            "t",
            &std::path::PathBuf::from("/work/x"),
        )
        .unwrap_err();
        assert!(matches!(e, crate::recipes::RecipeError::UnknownParam { .. }));
    }

    #[test]
    fn assemble_rejects_unknown_node() {
        let u = Uuid::now_v7();
        let e = assemble(
            &G16OptParse,
            &["nope.charge=1".to_string()],
            &BTreeMap::new(),
            &u,
            "t",
            &std::path::PathBuf::from("/work/x"),
        )
        .unwrap_err();
        assert!(matches!(e, crate::recipes::RecipeError::UnknownNode { .. }));
    }
```

- [ ] **Step 4: Implement `assemble`** — add to `src/recipes/flow.rs` (after the trait, before `#[cfg(test)]`):

```rust
use std::collections::BTreeMap;
use std::path::Path;

use uuid::Uuid;

use crate::recipes::job::{GeneratedFile, JobCtx, RecipeError};
use crate::recipes::jobs;
use crate::recipes::{parse_param_raw, typecheck_node};

fn find_job_local(name: &str) -> Option<Box<dyn crate::recipes::JobTemplate>> {
    match name {
        "g16_opt" => Some(Box::new(jobs::g16_opt::G16Opt)),
        "parse_g16_out" => Some(Box::new(jobs::parse_g16_out::ParseG16Out)),
        _ => None,
    }
}

/// Resolve a FlowRecipe into flow.toml + plan.toml text + all sidecars,
/// doctor-clean by construction. `abs_flow_dir` = `<root>/<uuid>`.
pub fn assemble(
    recipe: &dyn FlowRecipe,
    raw_params: &[String],
    tags: &BTreeMap<String, String>,
    uuid: &Uuid,
    created_at: &str,
    abs_flow_dir: &Path,
) -> Result<Vec<GeneratedFile>, RecipeError> {
    let flow_name = recipe.name();
    let node_names: Vec<&str> = recipe.nodes().iter().map(|(j, _)| *j).collect();

    let mut by_node: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for raw in raw_params {
        parse_param_raw(raw, &mut by_node)?;
    }
    for jobid in by_node.keys() {
        if !node_names.contains(&jobid.as_str()) {
            return Err(RecipeError::UnknownNode {
                flow: flow_name.to_string(),
                jobid: jobid.clone(),
                nodes: node_names.join(", "),
            });
        }
    }

    // wiring: consumer input -> relative producer path
    let mut inputs_by_node: BTreeMap<String, BTreeMap<&'static str, String>> = BTreeMap::new();
    for (consumer, input, producer, out_name) in recipe.wiring() {
        let ptmpl_name = recipe
            .nodes()
            .iter()
            .find(|(j, _)| j == producer)
            .map(|(_, t)| *t)
            .ok_or_else(|| RecipeError::WiringMismatch {
                consumer: consumer.to_string(),
                input: input.to_string(),
                producer: producer.to_string(),
                output: out_name.to_string(),
                detail: "producer node not in recipe".into(),
            })?;
        let ptmpl = find_job_local(ptmpl_name).ok_or_else(|| RecipeError::UnknownJobTemplate {
            node: producer.to_string(),
            tmpl: ptmpl_name.to_string(),
        })?;
        let rel = ptmpl
            .outputs()
            .iter()
            .find(|(n, _)| n == out_name)
            .map(|(_, p)| *p)
            .ok_or_else(|| RecipeError::WiringMismatch {
                consumer: consumer.to_string(),
                input: input.to_string(),
                producer: producer.to_string(),
                output: out_name.to_string(),
                detail: "producer has no such output".into(),
            })?;
        inputs_by_node
            .entry(consumer.to_string())
            .or_default()
            .insert(*input, format!("../{producer}/{rel}"));
    }

    let mut files: Vec<GeneratedFile> = Vec::new();
    let mut flow_jobs = String::new();
    let mut plan_jobs = String::new();
    for (jobid, tmpl_name) in recipe.nodes() {
        let tmpl = find_job_local(tmpl_name).ok_or_else(|| RecipeError::UnknownJobTemplate {
            node: jobid.to_string(),
            tmpl: tmpl_name.to_string(),
        })?;
        let empty = BTreeMap::new();
        let overrides = by_node.get(*jobid).unwrap_or(&empty);
        let params = typecheck_node(flow_name, jobid, tmpl.params(), overrides)?;
        let empty_inputs = BTreeMap::new();
        let inputs = inputs_by_node.get(*jobid).unwrap_or(&empty_inputs);
        let ctx = JobCtx {
            job_id: jobid,
            params: &params,
            inputs,
            uuid,
            created_at,
        };
        let art = tmpl.instantiate(&ctx)?;

        let abs_job = abs_flow_dir.join(jobid);
        let body = format!("cd \"{}\" || exit 1\n{}", abs_job.display(), art.body);

        flow_jobs.push_str(&format!(
            "\n[jobs.{jobid}]\nprogram = {}\nbody = {}\n",
            toml::Value::String(art.program.clone()),
            toml::Value::String(body),
        ));
        for (from, to, kind) in recipe.edges() {
            if to == jobid {
                flow_jobs.push_str(&format!(
                    "\n[[jobs.{jobid}.parents]]\nfrom = {}\nkind = {}\n",
                    toml::Value::String(from.to_string()),
                    toml::Value::String(kind.to_string()),
                ));
            }
        }
        flow_jobs.push_str(&format!("\n[jobs.{jobid}.config]\npartition = \"REPLACE_ME\"\n"));
        if let Some(tl) = &art.time_limit {
            flow_jobs.push_str(&format!(
                "time_limit = {}\n",
                toml::Value::String(tl.clone())
            ));
        }

        plan_jobs.push_str(&format!("\n[jobs.{jobid}]\n"));
        for (k, v) in &art.plan_params {
            plan_jobs.push_str(&format!("{k} = {v}\n"));
        }

        files.extend(art.sidecars);
    }

    let mut tag_lines = String::new();
    for (k, v) in tags {
        tag_lines.push_str(&format!("{k} = {}\n", toml::Value::String(v.clone())));
    }
    let flow_toml = format!(
        "# Generated by `jm new {flow_name}` on {created_at}.\n\
         uuid = \"{uuid}\"\ncreated_at = \"{created_at}\"\n\n[tags]\n\
         recipe = \"{flow_name}\"\n{tag_lines}{flow_jobs}"
    );
    let plan_toml = format!(
        "# Generated by `jm new {flow_name}`. Per-JobId render params.\n{plan_jobs}"
    );

    files.push(GeneratedFile {
        relpath: "flow.toml".into(),
        contents: flow_toml,
        unix_mode: None,
    });
    files.push(GeneratedFile {
        relpath: "plan.toml".into(),
        contents: plan_toml,
        unix_mode: None,
    });
    Ok(files)
}
```

- [ ] **Step 5: Wire + run**

In `src/recipes/mod.rs`: add `pub mod flows;` (after `pub mod jobs;`); change `pub use flow::FlowRecipe;` to `pub use flow::{FlowRecipe, assemble};`. Add `assemble` to `src/lib.rs` re-exports.
Run: `cargo test --lib --no-default-features recipes::flow 2>&1 | tail -15`
Expected: PASS — `assemble_g16_opt_parse_is_doctor_clean`, `assemble_rejects_unknown_param`, `assemble_rejects_unknown_node`, `trait_object_dispatch`.

- [ ] **Step 6: Commit**

```bash
git add src/recipes/flow.rs src/recipes/flows src/recipes/mod.rs src/lib.rs
git commit -m "feat(recipes): assemble() + g16-opt-parse FlowRecipe (doctor-clean)"
```

---

## Task 9: `blank` FlowRecipe (byte-identical generator)

**Files:**
- Create: `src/recipes/flows/blank.rs`
- Modify: `src/recipes/flows/mod.rs`
- Test: `src/recipes/flows/blank.rs` (inline)

`blank` is **non-decomposed**: it does NOT use `assemble()`/JobTemplates. It emits text byte-identical to the current `src/bin/jm.rs` `build_flow_template`/`build_plan_template`. (Plan C deletes the jm.rs copies and routes `jm new` / `jm new blank` here; Plan B only adds the generator + locks invariants.)

- [ ] **Step 1: Create `src/recipes/flows/blank.rs`**

```rust
//! `blank` FlowRecipe — the legacy 2-job step1->step2 boilerplate,
//! emitted byte-identically to the pre-recipe `jm new` (spec §8).
//! Non-decomposed: bypasses assemble()/JobTemplate.

use std::collections::BTreeMap;

use crate::recipes::FlowRecipe;

pub fn blank_plan_toml() -> String {
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

pub fn blank_flow_toml(
    uuid: &uuid::Uuid,
    created_at: &str,
    tags: &BTreeMap<String, String>,
) -> String {
    let mut tag_lines = String::new();
    if tags.is_empty() {
        tag_lines.push_str("# free-form key=value tags; populate via `jm new --tag k=v`\n");
    } else {
        for (k, v) in tags {
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

pub struct Blank;

impl FlowRecipe for Blank {
    fn name(&self) -> &'static str {
        "blank"
    }
    fn summary(&self) -> &'static str {
        "legacy 2-job step1->step2 boilerplate (no recipe sidecars)"
    }
    fn nodes(&self) -> &'static [(&'static str, &'static str)] {
        &[]
    }
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)] {
        &[]
    }
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)] {
        &[]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gaussian_job_shared::entities::workflow::JobFlow;
    use std::collections::BTreeMap;

    #[test]
    fn plan_parses_step1_step2() {
        use crate::plan::ExperimentPlan;
        let plan: ExperimentPlan = toml::from_str(&blank_plan_toml()).unwrap();
        let keys: std::collections::BTreeSet<String> =
            plan.jobs.keys().map(|j| j.0.clone()).collect();
        assert_eq!(
            keys,
            ["step1", "step2"].iter().map(|s| s.to_string()).collect()
        );
    }

    #[test]
    fn flow_parses_two_step_dag_replace_me_partition() {
        let uuid = uuid::Uuid::now_v7();
        let s = blank_flow_toml(&uuid, "2026-05-16T00:00:00Z", &BTreeMap::new());
        let flow: JobFlow = toml::from_str(&s).unwrap();
        assert_eq!(flow.uuid, uuid);
        let ids: std::collections::BTreeSet<String> =
            flow.jobs.keys().map(|j| j.0.clone()).collect();
        assert_eq!(
            ids,
            ["step1", "step2"].iter().map(|s| s.to_string()).collect()
        );
        let s2 = flow
            .jobs
            .get(&gaussian_job_shared::entities::workflow::JobId("step2".into()))
            .unwrap();
        assert_eq!(s2.parents.len(), 1);
        assert_eq!(s2.parents[0].from.0, "step1");
        for (_, j) in &flow.jobs {
            assert_eq!(j.spec.config.partition, "REPLACE_ME");
        }
    }

    #[test]
    fn flow_renders_tags_keeps_value_with_equals() {
        let uuid = uuid::Uuid::now_v7();
        let mut tags = BTreeMap::new();
        tags.insert("env".to_string(), "prod".to_string());
        tags.insert("owner".to_string(), "a=b".to_string());
        let s = blank_flow_toml(&uuid, "2026-05-16T00:00:00Z", &tags);
        let flow: JobFlow = toml::from_str(&s).unwrap();
        assert_eq!(flow.tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(flow.tags.get("owner").map(String::as_str), Some("a=b"));
    }

    #[test]
    fn flow_no_tags_emits_placeholder_comment() {
        let s = blank_flow_toml(&uuid::Uuid::now_v7(), "t", &BTreeMap::new());
        assert!(s.contains("# free-form key=value tags; populate via `jm new --tag k=v`"));
    }
}
```

- [ ] **Step 2: Run → fail (module not declared)**

Run: `cargo test --lib --no-default-features recipes::flows::blank 2>&1 | tail -6`
Expected: COMPILE FAIL — `module \`blank\` not found`.

- [ ] **Step 3: Declare module + run**

Set `src/recipes/flows/mod.rs`:
```rust
pub mod blank;
pub mod g16_opt_parse;
```
Run: `cargo test --lib --no-default-features recipes::flows::blank 2>&1 | tail -10`
Expected: PASS — 4 `blank` tests ok.

- [ ] **Step 4: Manual byte-identity sanity check (automated gate is Plan C)**

The strict byte-for-byte regression vs the live `jm new` is **Plan C** (its `cmd_new` rewire keeps the existing `tests/integration_new.rs` green after deleting the jm.rs copies). Here, visually diff this file's `blank_flow_toml`/`blank_plan_toml` text against `src/bin/jm.rs`'s `build_flow_template`/`build_plan_template` (lines ~497-574): same comments, same `partition = "REPLACE_ME"`, same step1/step2 `echo` bodies, same tag handling. The 4 unit tests lock the structural invariants.

- [ ] **Step 5: Commit**

```bash
git add src/recipes/flows
git commit -m "feat(recipes): blank FlowRecipe (byte-identical legacy generator)"
```

---

## Task 10: registries + `--list` / `--describe`

**Files:**
- Modify: `src/recipes/mod.rs`, `src/lib.rs`
- Test: `src/recipes/mod.rs` (inline)

- [ ] **Step 1: Write failing tests** — append to `src/recipes/mod.rs`:

```rust
#[cfg(test)]
mod registry_tests {
    use super::*;

    #[test]
    fn flow_registry_has_blank_and_g16_opt_parse() {
        let names: Vec<&str> = flow_registry().iter().map(|r| r.name()).collect();
        assert!(names.contains(&"blank"));
        assert!(names.contains(&"g16-opt-parse"));
    }

    #[test]
    fn find_flow_known_and_unknown() {
        assert!(find_flow("g16-opt-parse").is_some());
        assert!(find_flow("nope").is_none());
    }

    #[test]
    fn every_recipe_node_resolves_and_wiring_consistent() {
        for r in flow_registry() {
            if r.name() == "blank" {
                continue; // non-decomposed
            }
            for (jid, tmpl) in r.nodes() {
                let jt = find_job(tmpl).unwrap_or_else(|| {
                    panic!("recipe {}: node {jid} -> unknown JobTemplate {tmpl}", r.name())
                });
                let _ = jt.params();
            }
            for (consumer, input, producer, output) in r.wiring() {
                let ctmpl = r.nodes().iter().find(|(j, _)| j == consumer).unwrap().1;
                let ptmpl = r.nodes().iter().find(|(j, _)| j == producer).unwrap().1;
                assert!(find_job(ctmpl).unwrap().inputs().contains(input));
                assert!(
                    find_job(ptmpl)
                        .unwrap()
                        .outputs()
                        .iter()
                        .any(|(n, _)| n == output)
                );
            }
        }
    }

    #[test]
    fn format_list_has_name_and_summary() {
        let s = format_list();
        assert!(s.contains("g16-opt-parse"));
        assert!(s.contains("cclib validation"));
        assert!(s.contains("blank"));
    }

    #[test]
    fn format_describe_lists_nodes_and_typed_params() {
        let s = format_describe("g16-opt-parse").unwrap();
        assert!(s.contains("opt") && s.contains("g16_opt"));
        assert!(s.contains("parse") && s.contains("parse_g16_out"));
        assert!(s.contains("opt.route"));
        assert!(s.contains("opt.charge"));
        assert!(s.contains("Int"));
        assert!(format_describe("nope").is_err());
    }
}
```

- [ ] **Step 2: Run → fail**

Run: `cargo test --lib --no-default-features recipes::registry_tests 2>&1 | tail -6`
Expected: COMPILE FAIL — `cannot find function \`flow_registry\``/`find_flow`/`find_job`/`format_list`/`format_describe`.

- [ ] **Step 3: Implement** — add to `src/recipes/mod.rs` (above `#[cfg(test)]`):

```rust
use crate::recipes::flows::{blank::Blank, g16_opt_parse::G16OptParse};

pub fn flow_registry() -> Vec<Box<dyn FlowRecipe>> {
    vec![Box::new(Blank), Box::new(G16OptParse)]
}

pub fn find_flow(name: &str) -> Option<Box<dyn FlowRecipe>> {
    flow_registry().into_iter().find(|r| r.name() == name)
}

pub fn find_job(name: &str) -> Option<Box<dyn JobTemplate>> {
    match name {
        "g16_opt" => Some(Box::new(jobs::g16_opt::G16Opt)),
        "parse_g16_out" => Some(Box::new(jobs::parse_g16_out::ParseG16Out)),
        _ => None,
    }
}

pub fn format_list() -> String {
    let mut s = String::from("Available flow recipes:\n");
    for r in flow_registry() {
        s.push_str(&format!("  {:<16} {}\n", r.name(), r.summary()));
    }
    s
}

pub fn format_describe(name: &str) -> Result<String, RecipeError> {
    let r = find_flow(name).ok_or_else(|| {
        RecipeError::UnknownRecipe(
            name.to_string(),
            flow_registry()
                .iter()
                .map(|r| r.name())
                .collect::<Vec<_>>()
                .join(", "),
        )
    })?;
    let mut s = format!("recipe {} — {}\nnodes:\n", r.name(), r.summary());
    for (jid, tmpl) in r.nodes() {
        s.push_str(&format!("  {jid} ({tmpl})\n"));
        if let Some(jt) = find_job(tmpl) {
            for p in jt.params() {
                s.push_str(&format!(
                    "    --param {jid}.{}=<{:?}>  (default {:?}) {}\n",
                    p.name, p.ty, p.default, p.help
                ));
            }
        }
    }
    Ok(s)
}
```

- [ ] **Step 4: Run → pass; finalize re-exports**

Run: `cargo test --lib --no-default-features recipes::registry_tests 2>&1 | tail -12`
Expected: PASS — 5 `registry_tests` ok.
Ensure `src/lib.rs`'s `pub use recipes::{...}` includes: `FlowRecipe, JobTemplate, RecipeError, assemble, flow_registry, find_flow, format_list, format_describe`.

- [ ] **Step 5: Commit**

```bash
git add src/recipes/mod.rs src/lib.rs
git commit -m "feat(recipes): registries + --list/--describe + integrity lint"
```

---

## Task 11: full CI gate + push

- [ ] **Step 1: Run the CLAUDE.md CI gate**

Run:
```bash
cargo fmt --check && \
cargo clippy --all-targets --all-features -- -D warnings && \
cargo build --no-default-features 2>&1 | tail -2 && \
cargo test --all-features 2>&1 | tail -25 && \
uv run pytest python/tests -v 2>&1 | tail -8
```
Expected: fmt clean; clippy no warnings; `cargo build --no-default-features` `Finished` (proves the recipe lib has no pyo3 dependency — spec §4/§11); `cargo test --all-features` all `ok` including every `recipes::*` test and **all pre-existing suites unchanged** (`tests/integration_new.rs` still green — jm.rs untouched in Plan B); pytest unchanged (no Python touched; `.tmpl` assets are not collected).

- [ ] **Step 2: If fmt failed, format + amend**

Run: `cargo fmt && git add -u && git commit --amend --no-edit` (only if Step 1 fmt failed).

- [ ] **Step 3: Push + verify exit criteria**

Run: `git branch --show-current && git push 2>&1 | tail -2`
Confirm:
- `cargo build --no-default-features` green ⇒ `src/recipes/` is pyo3-free.
- `assemble(&G16OptParse,…)` ⇒ flow.toml/plan.toml parse as `JobFlow`/`ExperimentPlan`, `{opt,parse}`, `parse` afterok `opt`, both `partition=="REPLACE_ME"`, plan keys == flow ids; R3 absolute `cd` in opt body; wiring `../opt/output/main.out` in `parse/scripts/parse.py`.
- `g16_opt` gjf no `%rwf`; `run.py` reproduces run_g16 (no `gaussian_job_shared`/`cclib` import); `parse.py` reproduces parse_results (cclib → `result.json`).
- `base_preamble()` matches `_base.bash.j2` order, no `#SBATCH`.
- registry-integrity lint green; `blank` invariants locked.
- `git diff --stat <tip-before-PlanB>..HEAD -- src/bin src/render` empty (jm.rs/render untouched).

---

## Self-Review

**1. Spec coverage (rev.6 §4, §4.0, §5.1, §7, §8, §13):**
- §4 module layout / two-layer types / `assemble()` → Tasks 1,2,8. ✓
- §4.0 `base_preamble()` `_base.bash.j2` shell port, fixed conda-reset, site-only params, no `#SBATCH` → Task 3. ✓
- §5.1 R3 (`assemble` prepends absolute `cd`) → Task 8, asserted in `assemble_g16_opt_parse_is_doctor_clean`. ✓
- §7 `g16_opt` (full param set incl. launcher/scratch_root/g16_cmd/input_coordinate; gjf no `%rwf`; run.py reproduction; program "g16") → Task 5. `parse_g16_out` (cclib→result.json; program "python"; params conda_env/pixi_manifest; outputs result_json) → Task 6. `g16-opt-parse` nodes/edges/wiring → Task 8. ✓
- §8 `blank` non-decomposed byte-identical → Task 9. ✓
- §13 self-contained (no Group B/C/D import; run.py stdlib; parse.py cclib only; `# REPLACE_ME` hooks) → Tasks 5,6 asserts. ✓
- §5.5/§5.6 *consumption* (run.py reads `JM_LAUNCHER`/`JM_SCRATCH_ROOT`) ships in the asset (Task 5); the *render-time resolution/export* is Plan C — Plan B only ships the env-reading asset. No gap. ✓
- `--param` parse/typecheck (§3/§9) → Task 7; `--list`/`--describe` formatters (§3) → Task 10. CLI wiring = Plan C. ✓

**2. Placeholder scan:** Every code step is complete and compilable; every command states expected output. "temporary single-module mod.rs / declare in Task N" notes are explicit sequencing with the exact reversing step named — not placeholders. Task 9 Step 4 is explicitly a manual sanity reminder (auto byte-identity gate is Plan C's `tests/integration_new.rs`). ✓

**3. Type consistency:** All type/method/field names match across job.rs (def) ↔ g16_opt.rs/parse_g16_out.rs (impl) ↔ flow.rs (`assemble`) ↔ mod.rs (registries/param): `JobTemplate{name,params,inputs,outputs,instantiate}`, `FlowRecipe{name,summary,nodes,edges,wiring}`, `JobArtifacts{program,body,time_limit,plan_params,sidecars}`, `JobCtx{job_id,params,inputs,uuid,created_at}`, `GeneratedFile{relpath,contents,unix_mode}`, `RecipeError` variants, `PreambleOpts{conda_env,module_block,body_block,pixi_manifest}`, `parse_param_raw`/`typecheck_node`/`flow_registry`/`find_flow`/`find_job`/`format_list`/`format_describe`/`assemble`/`base_preamble`/`parse_xyz`. `find_job_local` (flow.rs, module-private) and `find_job` (mod.rs, pub registry) are intentionally separate 3-line matches with identical bodies so Task 8 compiles before Task 10 exists — documented, not a drift. `instantiate` returns `body` WITHOUT the R3 cd everywhere; `assemble` is the sole place that prepends it (refinement §1). ✓

No blocking issues.

---

## Execution Handoff

Plan B complete and saved to `docs/superpowers/plans/2026-05-17-jm-new-recipes-planB-recipes-core.md`.

Plan C (final sub-plan) covers: `Cmd::New` extension (recipe positional, `--param`, `--list`, `--describe`); `cmd_new` rewrite (route through `recipes::assemble` for FlowRecipes / `blank_flow_toml`+`blank_plan_toml` for `blank`; `input_coordinate` copy + `.xyz` splice via `parse_xyz`; rollback); render-path `JM_LAUNCHER`/`JM_SCRATCH_ROOT` resolution (4-case/3-case, additive `render_batch_bash` sibling, both `submit`/`render_only` call sites); delete the now-duplicated jm.rs `build_*_template`; integration tests (`tests/integration_new_recipes.rs`) + the strict `tests/integration_new.rs` byte-identity gate + python smoke for `run.py`/`parse.py` + docs/CI.

Execution options for Plans A + B (and C once written):

1. **Subagent-Driven (recommended)** — fresh subagent per task, two-stage review between tasks. Plan A Task 1 has a cross-repo (D2) owner PR-merge gate; Plan B is single-repo, library-only.
2. **Inline Execution** — batch execution with checkpoints in this session.

Shall I proceed to write **Plan C** now as well (completing all three), or hold for review of A/B first? And which execution approach?
