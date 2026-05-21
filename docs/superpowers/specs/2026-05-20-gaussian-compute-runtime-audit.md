# `gaussian-compute-runtime` Audit — current swap-in surface snapshot

| Field | Value |
|---|---|
| Date | 2026-05-20 |
| Issue | [#32](https://github.com/kkiyama117/job_manager/issues/32) (blocker for #33–#36) |
| Audited package | `miyake-ken/gaussian-compute-runtime` v0.2.0 (sibling worktree `../gaussian-compute-runtime`) |
| Upstream dep audited | `gaussian-job-shared` (α-D ≥0.2.0, sibling worktree `../gaussian-job-shared`) |
| Status | **Documentation snapshot only**. No code change in this PR. |
| Audience | future v2 spec (PR #31) / `# REPLACE_ME` swap-in comment update / gap-fix PRs (#33–#36) |

## 1. Purpose

Capture the **actual** swap-in interface exposed by today's
`gaussian-compute-runtime` so the v2 design spec and the recipe scaffolds
can be aligned before any code change. Two known drifts triggered this
audit:

1. v2 spec (PR #31, `docs/superpowers/specs/2026-05-20-jm-recipe-v2-design.md`)
   §4.3 / §10 / §13 documents the swap-in form as
   `python -m gaussian_compute_runtime <step> --common "$JM_JOB_DIR/common.toml" --uuid "$JM_FLOW_UUID" --job-id "$JM_JOB_ID"`,
   but the runtime's README and source expose `--config <abs.toml>` with
   no `--uuid` / `--job-id` flags.
2. The user has signaled (2026-05-20) that `gaussian-compute-runtime`'s
   folder structure / job-flow assumptions may be rewritten. This audit
   flags which conventions are **stable contract** vs **subject to
   upcoming rewrite**, so #33–#36 can decide what to align against now
   vs defer.

This document is intentionally a snapshot — it does **not** propose
changes. Each follow-up issue is responsible for proposing the
alignment in its own scope.

## 2. Repository layout (audited tree)

```
../gaussian-compute-runtime/
├── CHANGELOG.md                            ← key (Known issues block!)
├── LICENSE
├── README.md                               ← v0.1.0 era; γ not documented here
├── pyproject.toml                          ← name=gaussian_compute_runtime, ver=0.2.0
├── src/gaussian_compute_runtime/
│   ├── __init__.py                         ← guarded re-export (see §5)
│   ├── __main__.py                         ← argparse dispatcher (parse_known_args)
│   ├── _cli.py                             ← add_config_argument / EXIT_USAGE=2 / EXIT_IO=3
│   ├── run_g16.py                          ← legacy subcommand (broken under D-α v0.2.0)
│   ├── parse_results.py                    ← legacy subcommand (broken under D-α v0.2.0)
│   └── consume/
│       ├── __init__.py                     ← empty
│       ├── parent_results.py               ← γ subcommand (consume-parent-results)
│       └── gjf_render.py                   ← pure-function gjf renderer
└── tests/
    ├── conftest.py
    ├── consume/                            ← γ tests (active, covers parent_results + gjf_render)
    ├── test_cli_helpers.py                 ← pytest.skip(allow_module_level=True)
    ├── test_dispatcher.py                  ← pytest.skip(allow_module_level=True)
    ├── test_parse_results.py               ← pytest.skip(allow_module_level=True)
    ├── test_public_api.py                  ← pytest.skip(allow_module_level=True)
    └── test_run_g16.py                     ← pytest.skip(allow_module_level=True)
```

Runtime deps (`pyproject.toml`):
- `gaussian_job_shared >= 0.2.0`
- `gaussian_job_results >= 0.2.0`

Python: `>=3.12,<3.13`.

## 3. Subcommand dispatcher (`__main__.py`)

Single entry point:

```
python -m gaussian_compute_runtime <subcommand> [subcommand args...]
```

Outer parser uses `parse_known_args`; every flag after `<subcommand>` is
forwarded verbatim to the subcommand's own argparse. The outer parser
owns only the subcommand name. Subcommand table:

| Subcommand | Module | Status (v0.2.0) |
|---|---|---|
| `run-g16` | `gaussian_compute_runtime.run_g16` | **BROKEN** — `ImportError` on `ConfigManager` / `JobPaths` (D-α removed surfaces). Dispatcher catches `ImportError` and emits `error: subcommand 'run-g16' is unavailable (awaits B-α migration)` then `return 2`. |
| `parse-results` | `gaussian_compute_runtime.parse_results` | **BROKEN** — same as above. |
| `consume-parent-results` | `gaussian_compute_runtime.consume.parent_results` | **WORKING** — γ surface shipped in v0.2.0 (2026-05-04). |

Exit-code convention (`_cli.py` + per-subcommand):
- `0` — success.
- `2` — usage / config error (`EXIT_USAGE`). Raised for argparse failures, missing TOML, malformed TOML, strict-schema violations, bad `child_uuid`, etc.
- `3` — IO error (`EXIT_IO`). Raised for `OSError` during writes / copies.
- For `run-g16`, **g16's own non-zero return code is propagated unchanged** (after the `copy_results` finally-block runs).

The dispatcher itself uses `importlib.import_module(...)` (cached) so
`unittest.mock.patch("gaussian_compute_runtime.run_g16.main")` keeps
working post-migration.

## 4. Subcommand details

### 4.1 `run-g16` (legacy, broken under D-α v0.2.0)

`src/gaussian_compute_runtime/run_g16.py` — invoked from inside the
rendered SLURM bash on the compute node.

**Flags** (argparse):

| Flag | Required | Type | Help |
|---|---|---|---|
| `--config` | yes | `pathlib.Path` | Absolute path to the GAUSSIAN job TOML. |
| `--no-srun` | no | flag | Invoke `g16` directly without `srun` (local smoke). |

**Behavior** (when restored):

1. `ConfigManager.from_toml(args.config)` — read TOML. Errors → exit 2.
2. `paths = JobPaths.from_manager(manager)` — derive `target_dir` (input/output) and `temp_dir` (scratch).
3. `prepare_inputs(target_dir, temp_dir)` — stage `input/*` into scratch (see `gaussian_job_shared.fs.prepare`).
4. Build `argv = [launcher?, g16_cmd, f"{task}.gjf", f"{task}.out"]` where `task = manager.env.task_basename`.
5. `subprocess.run(argv, cwd=temp_dir, check=False)` — chdir-in-child only; aux files (`.chk`, scratch files) land under `temp_dir`.
6. `finally: copy_results(temp_dir, target_dir)` — ships results back even on g16 failure.

**Failure modes**:
- `FileNotFoundError` on launcher/g16 → `EXIT_USAGE` (typical: missing `module restore`, `--no-srun` on host without g16).
- `copy_results` `OSError` → `EXIT_IO` *unless* g16 itself returned non-zero (g16 rc has top precedence).

### 4.2 `parse-results` (legacy, broken under D-α v0.2.0)

`src/gaussian_compute_runtime/parse_results.py` — invoked from a
post-batch on the compute node.

**Flags** (argparse mutually-exclusive group on `--config` / `--input`):

| Flag | Required | Type | Help |
|---|---|---|---|
| `--config` | one of {config, input} | `Path` | Resolve log under `PathResolver.target_dir(env.compound_id)`. Default output: `<target_dir>/result.json`. |
| `--input` | one of {config, input} | `Path` | Direct mode: `.out` file or directory containing `main.out`. Default output: next to the source. |
| `--output` | no | `Path` | Override destination JSON path. |
| `--no-write` | no | flag | Print JSON to stdout instead of writing. |
| `--quiet` | no | flag | Suppress `[parse-results]` progress line. |

**Behavior** (when restored): `resolve(...)` → `parse_log` or
`parse_compound` from `gaussian_job_results` → `write_json` (or stdout
on `--no-write`). The output schema is **not** flat; it's the
`GaussianResult` dataclass serialized via `dataclasses.asdict`:

```jsonc
{
  "run_info": {
    "source_path": "...",
    "metadata": { /* cclib data.metadata verbatim, JSON-safe */ },
    "optdone": true,
    "natom": 5,
    "charge": 0,
    "mult": 1,
    "gbasis": [...],          // tuple-of-tuples
    "scannames": null,
    "temperature": null,
    "pressure": null
  }
  // "raw" (ccData) is stripped before encoding.
}
```

See `gaussian-results-py/src/gaussian_job_results/{result,serializer}.py`
(`GaussianResult`, `_to_serializable`).

### 4.3 `consume-parent-results` (γ, working)

`src/gaussian_compute_runtime/consume/parent_results.py` — converts a
parent's `derived/main.mol2` plus the child's `metadata.toml` into the
child's `input/main.gjf`. Single-parent only; multi-parent (TODO-γ2)
and `geom=connectivity` routes (TODO-γ1) are guarded errors.

**Invocation**:

```
python -m gaussian_compute_runtime consume-parent-results --config <common.toml> <child_uuid>
```

**Flags / positionals**:

| Slot | Required | Type | Notes |
|---|---|---|---|
| `--config` | yes | `Path` | Path to `common.toml` (validated via `read_common_toml` — strict schema). |
| `child_uuid` | yes | positional `str` | Must match canonical UUID v7 regex `^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`. |

**Behavior** (paraphrased; see `consume/parent_results.py:main`):

1. `common = read_common_toml(args.config)`.
2. `resolver = PathResolver(common.env)`.
3. `child = read_metadata(resolver.metadata_path(child_uuid))` — strict.
4. Pre-conditions: `child.calc.program == "gaussian"`, exactly one `parent_uuid`, `isinstance(child.params, GaussianParams)`, `"geom=connectivity"` not in route.
5. Read parent's mol2: `resolver.derived_dir(parent_uuid) / "main.mol2"` via `read_mol2`.
6. `render(atoms, params, resource_spec, title=child_uuid, chk_basename=task_basename + ".chk")` → pure-function gjf text.
7. Atomic-write to `resolver.input_dir(child_uuid) / (task_basename + ".gjf")` with idempotent fast path (byte-equal → no-op) + tmp file + `fsync` + `os.replace`.

**Notes**:
- γ does **not** read the parent's `metadata.toml` — only the parent's
  `derived/main.mol2` path is constructed via `PathResolver` (meta §5.2
  C5: "PathResolver is the only place layout literals appear").
- Output `gjf` shape (from `consume/gjf_render.py`):
  ```
  %nprocshared={resource_spec.c}
  %mem={resource_spec.m}
  %chk={task_basename}.chk
  {route}

  {title=child_uuid}

  {charge} {multiplicity}
  {atoms…}

  {extra_input?}
  ```

## 5. Public API stability

`src/gaussian_compute_runtime/__init__.py` re-exports legacy surface
inside a `try/except ImportError` guard:

```python
try:
    from . import parse_results, run_g16
    from .parse_results import ResolvedRun, resolve
    __all__ = ["ResolvedRun", "parse_results", "resolve", "run_g16"]
except ImportError:
    __all__ = []
```

Implication: importing `gaussian_compute_runtime` does **not** raise
even when the legacy modules cannot import. Callers must defensively
check `__all__` or guard their own imports.

The γ surface is not currently re-exported at top-level — callers must
go through `gaussian_compute_runtime.consume.parent_results.main` or
the dispatcher.

## 6. `common.toml` schema (the `--config` document)

Authoritative source: `gaussian_job_shared.dataclasses.common`
(`Slurm`, `SlurmPost`, `ResourceSpec`, `Env`, `GaussianCmd`,
`CommonConfig`) + `gaussian_job_shared.config.common` (reader). Sample
verified against the live γ test fixture
(`../gaussian-compute-runtime/tests/consume/conftest.py:_common_toml_text`):

```toml
[slurm]
partition  = "gr10641a"
job_name   = "GAUSSIAN"
time_limit = "48:00:00"
log_stdout = "/tmp/log.out"
log_stderr = "/tmp/log.err"
mail_user  = ""
mail_types = []

[slurm.resource_spec]   # required, used by gjf render (%nprocshared / %mem)
p = 1
t = 56
c = 56
m = "56G"

# [slurm.post]          # optional partial override (post-step inherits unless field set)

[env]
root          = "<abs path to calc data root>"
tmp_root      = "/tmp/gaussian"
task_basename = "main"

[gaussian_cmd]
command = "g16"
```

| Section | Field | Type | Required | Notes |
|---|---|---|---|---|
| `[slurm]` | `partition` | str | yes | A1's `SlurmJobConfig.partition` contract. |
| | `job_name`, `time_limit`, `log_stdout`, `log_stderr` | str | yes | |
| | `mail_user` | str \| `""` | yes (nullable) | Empty string maps to `None`. |
| | `mail_types` | list[str] | yes | |
| `[slurm.resource_spec]` | `p`, `t`, `c` | int | yes | nodes/tasks/cpus-per-task. |
| | `m` | str | yes | g16 `%mem` literal (`"56G"`). |
| `[slurm.post]` | — | partial override | no | Each field nullable; merged via `Slurm.effective_post()` (resource_spec replaced wholesale, no field-level merge). |
| `[env]` | `root` | Path | yes | Parent of `<uuid_v7>/` calc dirs. |
| | `tmp_root` | Path | yes | Parent of scratch `<uuid_v7>/` dirs. |
| | `task_basename` | str | yes | Drives file naming (`main.gjf`, `main.out`, `main.chk`). |
| `[gaussian_cmd]` | `command` | str | yes | g16 binary name on PATH (`"g16"`). |

Strict schema — unknown keys / missing required keys / type mismatches
raise `StrictSchemaError` subclasses (`UnknownKeyError`,
`MissingRequiredKeyError`, `TypeMismatchError`,
`NonCanonicalNameError`, …).

> Stability flag: schema **changed once already** between D-α v0.1.0 and
> v0.2.0 (split from a single `gaussian_batch.toml` into
> `common.toml` + `experiment.toml`; UUID v7 PathResolver; metadata
> dataclasses). CHANGELOG suggests further drift is possible but no
> v0.3.0 spec exists yet.

## 7. `metadata.toml` schema (per-calc)

Authoritative source:
`gaussian_job_shared.dataclasses.metadata.Metadata` + `CalcBlock` +
`gaussian_job_shared.dataclasses.params.{CalcParams, GaussianParams}`.
Verified against γ test fixture
(`tests/consume/conftest.py:_child_metadata_text`):

```toml
[calc]
uuid          = "0190f7c2-1e3b-7a4c-9d5e-f67890abcdef"   # canonical lowercase UUID v7
program       = "gaussian"                                # registry-validated
calc_type     = "opt"                                     # registry-validated per program
created_at    = 2026-05-04T12:34:56Z                      # **unquoted** TOML offset datetime
parent_uuids  = ["<parent UUID v7>", ...]                  # empty list = root calc

# experiment_id, slurm_jobid, post_jobid are optional (None).

[compounds]
ids = ["ROSDSFDQCJNGOL-UHFFFAOYSA-O"]

[params]                                                   # shape depends on program
route        = "#p opt b3lyp/def2-tzvp"
charge       = 0
multiplicity = 1
extra_input  = ""

[tags]
basis = "def2-tzvp"   # arbitrary Mapping[str, str]
```

> Gotcha: `created_at` MUST be an unquoted TOML offset datetime — α-D's
> `read_metadata` raises `StrictSchemaError` with message "must be an
> offset datetime (got str)" if the value is quoted.

## 8. Folder layout — `PathResolver` (the single source of truth)

`gaussian_job_shared.paths.resolver.PathResolver(env)`:

| Method | Resolved path | Used by runtime in |
|---|---|---|
| `target_dir(uuid)` | `<env.root>/<uuid>/` | run-g16 (src), parse-results, γ (input/derived parents) |
| `temp_dir(uuid)` | `<env.tmp_root>/<uuid>/` | run-g16 (scratch / staging) |
| `metadata_path(uuid)` | `<env.root>/<uuid>/metadata.toml` | γ (child metadata) |
| `status_path(uuid)` | `<env.root>/<uuid>/status` | (not used directly by runtime; A1/D2 status writer) |
| `input_dir(uuid)` | `<env.root>/<uuid>/input/` | γ (write `<task_basename>.gjf`) |
| `output_dir(uuid)` | `<env.root>/<uuid>/output/` | (parse-results writes here in v0.1 era; v0.2 uses `target_dir/result.json` per `resolve()`) |
| `derived_dir(uuid)` | `<env.root>/<uuid>/derived/` | γ (read parent `main.mol2`) |

Layout is **per-calc UUID v7 directory** — there is no flow-level or
job-level directory at the `<env.root>` level.

UUID validation: lowercase canonical, regex
`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
(enforced by `is_canonical_uuid_v7` in
`gaussian_job_shared/_uuid.py`).

## 9. File-name conventions

| Path | Producer | Notes |
|---|---|---|
| `<root>/<uuid>/input/<task_basename>.gjf` | γ (`consume-parent-results`) or external pre-stage | γ writes via atomic + idempotent path. |
| `<root>/<uuid>/output/<task_basename>.out` | g16 | Copied back from scratch by `copy_results`. |
| `<root>/<uuid>/output/<task_basename>.chk` | g16 | Copied back if present. |
| `<root>/<uuid>/output/result.json` | parse-results (`<target_dir>/result.json` per `resolve()`) | `GaussianResult` JSON (see §4.2). |
| `<root>/<uuid>/derived/main.mol2` | external (gaussian-results-py) | γ consumes this. **Filename is hardcoded `main.mol2`**, not `<task_basename>.mol2`. |
| `<root>/<uuid>/metadata.toml` | external (gaussian-experiment-manager) | γ reads this. |
| `<root>/<uuid>/status` | external (job-shared `fs.status`) | Not touched by runtime. |
| `<tmp_root>/<uuid>/...` | run-g16 (scratch) | Stages `input/*` here, runs g16 with `cwd=tmp_dir`. |

> Note: `derived/main.mol2` filename is hardcoded in `consume/parent_results.py:158`
> (`parent_mol2_path = resolver.derived_dir(parent_uuid) / "main.mol2"`).
> It does **not** key off `task_basename`. This is a γ-specific quirk
> worth flagging if `task_basename` ever stops being `"main"`.

## 10. Gap matrix — v1 self-contained scripts vs current runtime

v1 scripts (production today, generated by `jm new g16-opt-parse`):
- `src/recipes/assets/g16_opt/run.py.tmpl`
- `src/recipes/assets/parse_g16_out/parse.py.tmpl`
- `src/recipes/assets/g16_opt/main.gjf.tmpl`
- `src/recipes/assets/_base.bash.j2`

Compared against the runtime audited above:

| Dimension | v1 self-contained | Current `gaussian-compute-runtime` | Verdict |
|---|---|---|---|
| Folder unit | `<flow_dir>/<job_id>/` (kebab-case job_id like `opt`, `parse`) | `<env.root>/<uuid_v7>/` per-calc | **Drift**. Categorically different identifiers. |
| Per-folder subdirs | `input/`, `output/`, `.scratch/<flow_uuid>/<job_id>/` | `input/`, `output/`, `derived/` (scratch lives under `tmp_root` not under target_dir) | Partial overlap. `derived/` is runtime-only; v1 has nothing analogous. v1's scratch is under target_dir (`<job_dir>/.scratch/...`), runtime's is under `tmp_root`. |
| Primary input filename | `main.gjf` (hardcoded `TASK = "main"`) | `<task_basename>.gjf` (default `"main"`) | Match by convention, not by mechanism. |
| Primary output filename | `main.out` | `<task_basename>.out` | Same. |
| `result.json` location | `<job_dir>/output/result.json` | `<target_dir>/result.json` (sibling to `output/`, not inside it — see `resolve()` default) | **Drift**. |
| `result.json` schema | Flat `{schema: "jm-recipe/1", converged, scf_energy, n_atoms, source}` | Nested `{run_info: {source_path, metadata, optdone, natom, charge, mult, gbasis, scannames, temperature, pressure}}` (cclib-derived `GaussianResult`) | **Drift**. v1 = 5 curated fields; runtime = full cclib metadata + structural attrs. |
| Launcher (`srun`) selection | env var `JM_PARAM_LAUNCHER` (single-token, defaults to empty) | hardcoded `["srun"]` unless `--no-srun` flag | **Drift**. |
| `g16` command | env var `JM_PARAM_G16_CMD` (defaults to `g16`) | TOML `[gaussian_cmd].command` | **Drift** (env-driven vs TOML-driven). |
| Scratch root | env var `JM_PARAM_SCRATCH_ROOT` (defaults to `<job_dir>/.scratch`) | TOML `[env].tmp_root` | **Drift**. |
| Stage / copy mechanism | inline `shutil.copytree` (v1 reimplements `prepare_inputs` / `copy_results` in stdlib) | imports `gaussian_job_shared.fs.{prepare_inputs, copy_results}` | v1 deliberately decoupled. |
| `%nprocshared` / `%mem` rewrite | v1 reads `SLURM_CPUS_PER_TASK` / `SLURM_MEM_PER_NODE` and rewrites the gjf in-place at run time | runtime does NOT do this; `%nprocshared` / `%mem` come from `[slurm.resource_spec].c` / `.m` at render time | **Behavioral drift**. v1's runtime rewrite is a job-manager-recipe feature with no runtime counterpart. |
| Metadata / chaining | None. `parse.py` resolves the parent's `.out` via `INPUT_REL` baked at scaffold time. | UUID v7 + `parent_uuids` + `derived/main.mol2`; γ converts parent output → child input. | **Categorical gap**. v1 has no graph; runtime has a 1-step single-parent graph (TODO-γ2 multi-parent). |
| Dependencies | stdlib only for `run.py`; `cclib` only for `parse.py` (no Group B/C/D imports) | `gaussian_job_shared` + `gaussian_job_results` (Group B → C/D fan-out) | v1 deliberately portable; runtime is integrated. |
| CWD assumption | R3' (after PR #28 cherry-pick `ceae2ef`): scripts use scaffold-baked `JOB_DIR = "{abs}"` and run g16 from `cwd=scratch` (subprocess only). Body launches via absolute path (R3' (a) interim). | `run-g16` runs g16 with `cwd=temp_dir`; the runtime process itself does **not** read its own cwd (everything resolved from `--config` TOML). | Both cwd-independent for the inner process; v1 still has flow.toml body launch path (absolute since `1124c7f`). |
| Configuration file | none (env vars + scaffold-time bake) | `--config <abs.toml>` (single TOML) | **Drift** (cf. §11). |

## 11. v2 spec REPLACE_ME example — drift summary

v2 spec PR #31 documents:

```bash
python -m gaussian_compute_runtime <step> \
    --common "$JM_JOB_DIR/common.toml" \
    --uuid   "$JM_FLOW_UUID" \
    --job-id "$JM_JOB_ID"
```

Reality:

| Spec element | Runtime today |
|---|---|
| `<step>` ∈ {?} | `<step>` ∈ {`run-g16`, `parse-results`, `consume-parent-results`}. Hyphenated, not underscore. **Note**: `run-g16` and `parse-results` are currently broken; only `consume-parent-results` runs. |
| `--common <path>` | No such flag on any subcommand. The flag is `--config <path>`. |
| `--uuid <uuid>` | No such flag. `consume-parent-results` takes a **positional** `child_uuid` (must be canonical UUID v7). `run-g16` / `parse-results` don't take any UUID — they derive paths from the TOML's `[env]` block via `PathResolver`. |
| `--job-id <id>` | No such flag on any subcommand. The runtime has no notion of a job-manager-style `JM_JOB_ID`; its addressing unit is a UUID v7 per calc. |
| `$JM_FLOW_UUID` semantics | job-manager flow UUID = `Uuid::new_v4()` (UUID v4) by `FlowRun`; runtime requires UUID **v7**. These are not interchangeable. |
| `$JM_JOB_ID` semantics | job-manager kebab-case job id (`"opt"`, `"parse"`); runtime has no analog. |

This is a **shape drift, not just a name drift**. Aligning the v2 spec
with reality (issue #33) is not a flag-rename — it requires either:

1. Adopting the runtime's CLI shape verbatim: `<step> --config "$JM_JOB_DIR/common.toml" <child_uuid?>`, where `<child_uuid>` is supplied only for `consume-parent-results` and the `common.toml` `[env].root` is set to the per-calc UUID's parent dir; or
2. Extending the runtime to grow a `--job-id` / `--uuid` flag set (out of scope for an audit, but tracked here for #36 / cross-repo coordination); or
3. Treating the spec's example as **aspirational** and documenting the actual swap-in form (most likely (1) wrapped via the spec's per-task `common.toml`).

Recommendation (for #33's discussion, not decided here): option (1)
with explicit note that `run-g16` / `parse-results` are pending B-α
migration before they can be swap-in targets, so the v1 self-contained
scripts remain the production path until then.

## 12. Stability vs in-flux — rewrite risk matrix

| Item | Stability | Evidence |
|---|---|---|
| Dispatcher pattern (`python -m <pkg> <sub>`, `parse_known_args` outer) | **STABLE** | Unchanged since v0.1.0; identical to A2's `gaussian-batch` CLI shape. |
| `--config <Path>` flag name + required-ness | **STABLE (where present)** | `_cli.py:add_config_argument` is shared (private duplication of `gaussian_job_cli/_common.py`); no removal in CHANGELOG. |
| Exit codes (0 / 2 / 3 / propagate g16 rc) | **STABLE** | `_cli.py` constants `EXIT_USAGE=2`, `EXIT_IO=3`; preserved across v0.1 → v0.2. |
| `consume-parent-results` CLI shape (`--config <common.toml> <child_uuid>`) | **STABLE (new)** | Shipped 2026-05-04. Single-parent only; multi-parent guarded with TODO-γ2 error. |
| `run-g16` / `parse-results` CLI shape | **IN FLUX — currently broken** | CHANGELOG v0.2.0 "Known issues": both subcommands fail to import; B-α migration tracked in separate β-tier PR. Final shape post-migration is **not yet documented**. |
| Public top-level re-exports (`from gaussian_compute_runtime import ...`) | **IN FLUX** | Top-level `__init__.py` guards re-exports in `try/except ImportError`; `__all__` is `[]` whenever legacy modules don't import. |
| `common.toml` schema (slurm/env/gaussian_cmd) | **MODERATELY STABLE** | Changed once (v0.1 → v0.2, split from `gaussian_batch.toml`). No v0.3 spec yet. |
| `metadata.toml` schema | **MODERATELY STABLE** | Shipped fresh with v0.2.0 (D-α); registry-driven `program` / `calc_type` validation suggests it's designed for extension. |
| `PathResolver` folder layout (`<root>/<uuid_v7>/{input,output,derived}/`) | **IN FLUX (user-flagged)** | User signal 2026-05-20: "folder structure / job flow may be rewritten". Meta spec lives in `miyake-ken/GAUSSIAN_repo`. PathResolver's layout literals are deliberately localized (C5) — exactly to make rewrite surgical. |
| UUID v7 as folder unit | **POSSIBLY IN FLUX** | Coupled to PathResolver; user-flagged. |
| `derived/main.mol2` hardcoded filename | **STABLE FOR γ, OUT-OF-SCOPE FOR REWRITE** | Lives inside the γ sub-spec which is single-parent only; if multi-parent (TODO-γ2) lands, filename convention may shift. |
| `task_basename` mechanism | **STABLE** | Used uniformly by run-g16 (`<task>.gjf` / `<task>.out`), γ output, and CHK basename. |
| Strict schema (`StrictSchemaError` family) | **STABLE** | Centralized in `gaussian_job_shared.config._toml`; designed for extension. |
| `GaussianResult` JSON output schema | **MODERATELY STABLE** | Tied to cclib's `ccData` shape; v0.2 added structural fields (`scannames`, `temperature`, `pressure`). Likely to grow, unlikely to shrink. |

**Highest rewrite risk** (flag for #33–#36 prioritization):

1. `PathResolver` folder layout — explicit user signal. Anything in
   the v2 spec / recipes that bakes the `<root>/<uuid_v7>/...` shape
   should be parameterized through the runtime API (not hardcoded in
   scaffolds) so the rewrite is surgical.
2. `run-g16` / `parse-results` CLI shape — post-B-α migration may
   introduce new flags or rename `--config` for parity with γ.
3. Cross-cutting: the runtime is **not yet a viable swap-in for the v1
   production scripts** until B-α migration lands. v2 spec REPLACE_ME
   comments should communicate this explicitly.

## 13. Stable-contract summary (one-liner)

> As of 2026-05-20 / v0.2.0: only `consume-parent-results` is a working
> swap-in target. Its contract is `python -m gaussian_compute_runtime
> consume-parent-results --config <common.toml> <child_uuid>`, exits 0
> / 2 / 3, reads `[env].root` + `[env].tmp_root` + `[env].task_basename`,
> resolves all paths via `PathResolver(env)` per UUID v7, and writes
> atomically + idempotently to `<root>/<child_uuid>/input/<task_basename>.gjf`.
> `run-g16` and `parse-results` are awaiting B-α migration and are not
> swap-in candidates today.

## 14. References

Audited source files (paths relative to `<GAUSSIAN_repo_packages>`):

- `gaussian-compute-runtime/src/gaussian_compute_runtime/__main__.py`
- `gaussian-compute-runtime/src/gaussian_compute_runtime/__init__.py`
- `gaussian-compute-runtime/src/gaussian_compute_runtime/_cli.py`
- `gaussian-compute-runtime/src/gaussian_compute_runtime/run_g16.py`
- `gaussian-compute-runtime/src/gaussian_compute_runtime/parse_results.py`
- `gaussian-compute-runtime/src/gaussian_compute_runtime/consume/parent_results.py`
- `gaussian-compute-runtime/src/gaussian_compute_runtime/consume/gjf_render.py`
- `gaussian-compute-runtime/README.md`
- `gaussian-compute-runtime/CHANGELOG.md` (Known issues block, v0.2.0)
- `gaussian-compute-runtime/pyproject.toml`
- `gaussian-compute-runtime/tests/consume/conftest.py` (canonical common.toml + child metadata.toml fixtures)
- `gaussian-job-shared/src/gaussian_job_shared/__init__.py`
- `gaussian-job-shared/src/gaussian_job_shared/dataclasses/common.py`
- `gaussian-job-shared/src/gaussian_job_shared/dataclasses/metadata.py`
- `gaussian-job-shared/src/gaussian_job_shared/paths/resolver.py`
- `gaussian-results-py/src/gaussian_job_results/{result,serializer}.py`

v1 self-contained scripts compared:

- `job-manager/src/recipes/assets/g16_opt/{run.py.tmpl, main.gjf.tmpl}`
- `job-manager/src/recipes/assets/parse_g16_out/parse.py.tmpl`
- `job-manager/src/recipes/assets/_base.bash.j2`

Linked job-manager design docs:

- `docs/superpowers/specs/2026-05-20-jm-recipe-v2-design.md` (PR #31, drift origin §4.3 / §10 / §13)
- `docs/superpowers/specs/2026-05-18-jm-g16-opt-parse-recipe-design.md` (v1 spec)
- `docs/superpowers/plans/2026-05-18-jm-g16-opt-parse-recipe.md` (v1 plan)

## 15. Out of scope

- Any code change in `job-manager`, `gaussian-compute-runtime`, or
  `gaussian-job-shared`. This is a docs-only PR.
- Any decision about which alignment (§11 options 1/2/3) wins. That
  belongs to issue #33.
- The B-α migration of `run-g16` / `parse-results`. That lives in
  `miyake-ken/gaussian-compute-runtime`'s own β-tier PR.
- Re-evaluating the v1 self-contained scripts' design (R3' (a) interim,
  R4 env-var injection in v2). Those are tracked by issue #29 (H1
  revoke) and PR #31 respectively.

## 16. Done when

- This doc lands on `develop` via a small docs PR.
- Issues #33, #34, #35, #36 can be unblocked and start citing §10 (gap
  matrix), §11 (CLI drift), §12 (stability matrix), §13 (stable-contract
  one-liner).

## 17. `result.json` schema divergence — issue #36 resolution

> Added 2026-05-21 as the documented-divergence deliverable for issue #36
> ("verify v1 self-contained outputs match current runtime"). Issue #36's
> "Done when" allows *either* an alignment PR *or* a documented divergence
> statement landing in this audit doc. This section is that statement.

### 17.1 Why a static comparison (no live diff)

Issue #36's task list asks for a live `run.py` vs `python -m
gaussian_compute_runtime run-g16` diff. **That is not runnable today**:
`run-g16` and `parse-results` both raise `ImportError` under D-α v0.2.0
(§4.1 / §4.2 — `ConfigManager` / `JobPaths` removed). So this resolution
is a **source-level** comparison of the two `result.json` shapes, not a
runtime diff.

### 17.2 The two schemas, side by side

**v1 self-contained** (`src/recipes/assets/parse_g16_out/parse.py.tmpl`,
written to `<job_dir>/output/result.json`):

```json
{
  "schema": "jm-recipe/1",
  "converged": true,
  "scf_energy": -76.41980012,
  "n_atoms": 3,
  "source": "/abs/.../opt/output/main.out"
}
```

**Runtime** (`gaussian_job_results.serializer.write_json` →
`GaussianResult` with `raw` stripped, written to
`<target_dir>/result.json`; keys alphabetized by `sort_keys=True`):

```json
{
  "run_info": {
    "charge": 0,
    "gbasis": null,
    "metadata": { "package": "Gaussian", "success": true, "...": "full cclib data.metadata verbatim" },
    "mult": 1,
    "natom": 3,
    "optdone": true,
    "pressure": null,
    "scannames": null,
    "source_path": "/abs/.../main.out",
    "temperature": null
  }
}
```

### 17.3 Field-level divergence

| Concept | v1 `jm-recipe/1` | Runtime `GaussianResult` | Interchangeable? |
|---|---|---|---|
| Versioning | `schema: "jm-recipe/1"` tag | **no version/schema tag at all** | No — runtime consumers can't version-detect. |
| Nesting | flat top-level | nested under `run_info` | No. |
| Convergence | `converged` (bool) | `run_info.optdone` (bool) | Semantically equal, renamed. |
| Atom count | `n_atoms` (int) | `run_info.natom` (int) | Semantically equal, renamed. |
| Source path | `source` (abs) | `run_info.source_path` (str) | Semantically equal, renamed. |
| **Final energy** | `scf_energy` (float) | **absent** | **No — the runtime JSON omits the final energy entirely.** Computed quantities live on the in-memory `raw` ccData, which `serializer._to_serializable` strips before encoding (`payload.pop("raw")`). |
| Extra metadata | none | `metadata` dict + `charge` / `mult` / `gbasis` / `scannames` / `temperature` / `pressure` | Runtime is a superset *except* for energy. |

### 17.4 Verdict + decision

**Not interchangeable.** The most load-bearing field for v1's purpose —
`scf_energy`, the number the `parse → afterok` gate is curated around — is
**not present** in the runtime's `result.json` (it only survives on the
stripped `raw` object). Beyond that, the two differ in versioning,
nesting, field names, and file location (§10 row "`result.json`
location").

**Decision: accept the divergence (issue #36 option 2).** Do **not**
align the v1 scripts to the runtime schema, because:

1. **Broken target.** `parse-results` does not run under D-α v0.2.0; there
   is no stable shape to align to yet.
2. **Moving target.** The user flagged (2026-05-20) an imminent
   folder-structure / job-flow rewrite (§12 highest-risk #1). Issue #36
   explicitly warns against aligning to a moving target.
3. **Energy loss.** Adopting the runtime shape verbatim would *drop*
   `scf_energy` from the on-disk JSON — a regression for v1's curated
   minimal-result contract.
4. **By design.** v1 is intentionally self-contained and does not track
   gem-stack changes; `jm-recipe/1` is job-manager's own curated envelope,
   not a claim of runtime parity.

**Re-evaluation trigger.** Revisit alignment only when *all three* hold:
(a) B-α migration lands and `parse-results` runs again, (b) the
PathResolver folder rewrite settles (§12 #1 cleared), and (c) the runtime
serializer is extended to emit the final energy in JSON (or a documented
mapping from `raw` is published). Until then, the v1 `# REPLACE_ME` hint
in `parse.py.tmpl` records this divergence and points here.
