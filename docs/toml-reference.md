# TOML File Reference

job-manager reads and writes five TOML files. **The authoritative schema
is the Rust serde structs** — run `cargo doc --no-deps --open` for field
docs. This page consolidates them. Every struct uses
`#[serde(deny_unknown_fields)]`, so a misspelled key is a hard parse
error. Validate a tree with **`jm --root <root> doctor`**.

## On-disk layout

```
<root>/
├── common.toml                         # optional, root-level (CommonConfig)
└── <flow_uuid>/
    ├── flow.toml                       # user-authored (JobFlow)
    ├── plan.toml                       # user-authored (ExperimentPlan)
    └── .jm/                            # program-managed (read-only to you)
        ├── flow.effective.toml         # materialized snapshot (JobFlow)
        └── <JobId>/status.toml         # per-job state (JobRun)
```

Exhaustive valid examples: [`examples/full/`](../examples/full/).

| File | Rust type | Source | Authored by |
|---|---|---|---|
| `common.toml` | `CommonConfig` | `gaussian_job_shared::config::common` | user (optional) |
| `flow.toml` | `JobFlow` | `gaussian_job_shared::entities::workflow` | user |
| `plan.toml` | `ExperimentPlan` | `job_manager::plan` | user |
| `.jm/flow.effective.toml` | `JobFlow` | same as flow.toml | program |
| `.jm/<JobId>/status.toml` | `JobRun` | `job_manager::job::run` | program |

## `common.toml` — `CommonConfig`

Optional. SLURM defaults merged into every job's `config` at submit time
(the job's own value wins; `common` fills gaps). `partition` set here is
injected into jobs that omit it.

| Key | Type | Required | Notes |
|---|---|---|---|
| `[slurm_default]` | `SlurmJobConfig` | yes | see [SlurmJobConfig](#slurmjobconfig) |
| `[directories].project_root` | path | yes | absolute; tilde/`$HOME` not expanded |

## `flow.toml` — `JobFlow`

| Key | Type | Required | Notes |
|---|---|---|---|
| `uuid` | UUID v7 string | yes | must equal the directory name |
| `created_at` | RFC3339 UTC | yes | e.g. `2026-05-15T00:00:00Z` |
| `[tags]` | map<string,string> | no | free-form metadata |
| `[jobs.<JobId>]` | `Job` | no | the DAG; key = stable JobId |

`Job` = `JobSpec` (flattened) + `parents`:

| Key | Type | Required | Notes |
|---|---|---|---|
| `program` | string | yes | e.g. `"g16"`, `"echo"` |
| `body` | string | yes | bash script body |
| `[jobs.<id>.config]` | `SlurmJobConfig` | no\* | \*the whole section may be omitted if `common.toml` supplies `partition`; only `partition` is mandatory and it may be inherited |
| `[[jobs.<id>.parents]]` | `JobEdge[]` | no | empty = root node |

`JobEdge`: `from` = a JobId key in this flow (FAIL if dangling);
`kind` ∈ `afterok afterany after afternotok aftercorr afterburstbuffer singleton`.

### SlurmJobConfig

Used by `[jobs.*.config]` and `common.toml [slurm_default]`.

| Key | Type | Required | Notes |
|---|---|---|---|
| `partition` | string | yes | inheritable from common.toml |
| `time_limit` | string | no | `HH:MM:SS`; also `MM`, `MM:SS` (min:sec), `D-H`, `D-H:M`, `D-H:M:S` |
| `job_name` | string | no | |
| `comment` | string | no | |
| `log_stdout` | path | no | `%x`=job_name, `%j`=slurm jobid |
| `log_stderr` | path | no | |
| `mail_user` | string | no | email |
| `mail_types` | string[] | no | TOML array of `BEGIN END FAIL REQUEUE ALL` (UPPERCASE) |
| `resource_spec` | string | no | CPU `p=N:t=N:c=N:m=NG` (each key optional; at least one required) **or** GPU `g=N` — one string, mutually exclusive |
| `array_spec` | string | no | `START-END[:STEP][%MAXCONC]`, comma-joined entries |
| `dependency` | string | no | raw SLURM dep; prefer `parents[]` |

### SLURM value grammars

- **time_limit**: `30` (30 min), `5:30` (5m30s), `12:34:56`, `1-0` (1 day),
  `2-3:30`, `3-12:00:00`. Serialized canonical `HH:MM:SS` (hours may exceed 23).
- **array_spec**: `0-15`, `0-15:4`, `0,6,16-32`, `0-15%4` (max 4 concurrent).
- **resource_spec**: CPU `p=4:t=8:c=8:m=8G` (each of p/t/c/m optional;
  memory suffix `K|M|G|T`, unitless = MiB); GPU `g=1`. CPU and GPU keys
  must not mix; zero counts rejected.
- **dependency**: `afterok:200`, `afterok:200:201`,
  `afterok:200,afterany:201` (AND), `afterok:200?afterany:201` (OR — do
  not mix `,` and `?`), `after:200+5` (`+min` only on `after`),
  `singleton` (no job ids).
- **mail_types**: a TOML array, e.g. `["BEGIN", "END", "FAIL"]` —
  elements are case-sensitive UPPERCASE tokens from
  `BEGIN END FAIL REQUEUE ALL`.
- **partition defaulting**: `flow.toml` may omit `[jobs.*.config].partition`;
  `read_flow` injects it from `common.toml [slurm_default].partition`. If
  neither supplies it, `jm doctor`/`submit` fails with `PartitionMissing`.

## `plan.toml` — `ExperimentPlan`

| Key | Type | Required | Notes |
|---|---|---|---|
| `[jobs.<JobId>]` | map<string, any TOML> | yes | arbitrary per-job render params |

Values may be string/int/float/bool/array/table. Every JobId in
`flow.toml` should have an entry (`jm doctor` WARNs on missing/extra).

## `.jm/flow.effective.toml` — `JobFlow`

Program-written materialized snapshot (Cargo.lock analogue): all
`common.toml` defaults baked in; readable without `common.toml`. Same
schema as `flow.toml`. **Do not edit.**

## `.jm/<JobId>/status.toml` — `JobRun`

Program-written per-job state. **Do not edit.**

| Key | Type | Required | Notes |
|---|---|---|---|
| `lifecycle` | enum | yes | `queued running success failed skipped` |
| `updated_at` | RFC3339 UTC | yes | |
| `slurm_jobid` | u64 | no | omitted until submitted |
| `note` | string | no | |
| `[slurm_status]` | `JobStatus` | no | `state` (UPPERCASE SLURM token, e.g. `PENDING`, `RUNNING`, `COMPLETED`, `OUT_OF_MEMORY`) + `reason` (PascalCase, e.g. `None`, `Priority`, `Dependency`; unknown reason strings are preserved verbatim — forward-compat) |

`lifecycle`: `Success|Failed|Skipped` are terminal (never overwritten by
`tick`). Pending is the *absence* of `status.toml` (no enum value).

## `jm new <recipe>` — generated scaffold layout

`jm new g16-opt-parse` writes the following files under `<root>/<uuid>/` in addition to
`flow.toml` and `plan.toml`:

- `<job>/scripts/<job>.bash` — thin launcher (base preamble + `python scripts/run.py` or
  `python scripts/parse.py`); chmod 0755.
- `opt/scripts/run.py` — pure-stdlib g16 orchestrator: `prepare_inputs` → `subprocess.run`
  with `cwd=scratch` → `finally` copy-back to `output/`; exit code mirrors g16 rc.
- `parse/scripts/parse.py` — cclib parser writing `output/result.json`
  (`{"schema":"jm-recipe/1", "converged": bool, "scf_energy": float, "n_atoms": int, ...}`).
- `opt/input/main.gjf` — Gaussian input template with charge/multiplicity/geometry filled in.

**`JM_PARAM_*` recipe params.** `plan.toml [jobs.opt]` contains `launcher`, `scratch_root`,
and `g16_cmd`. `jm render` exports these as `JM_PARAM_LAUNCHER`, `JM_PARAM_SCRATCH_ROOT`,
and `JM_PARAM_G16_CMD` in `batch.bash`. The scripts read them at runtime to control the
launch command, scratch directory, and Gaussian binary — edit `plan.toml` and re-run `jm render`
to update without re-scaffolding.

**`# REPLACE_ME` sentinel.** Lines marked `# REPLACE_ME` in the generated scripts are
swap-in points for site-specific tooling (e.g. `python -m gaussian_compute_runtime`). The
scaffolded scripts are self-contained without that tooling.

**v1 caveats.**

- `JM_PARAM_LAUNCHER` is passed as a single command token (e.g. `srun`). A multi-word value
  such as `srun --ntasks=1` is not shell-split and will cause a launch failure (non-silent —
  `run.py` exits non-zero). Use a single launcher command name in v1.
- `opt.input_coordinate` auto-converts only `.xyz` files in v1. A non-`.xyz` coordinate file
  is copied into `opt/input/` but the gjf geometry block is left as an explicit `REPLACE_ME`
  placeholder to fill manually.
- `extra_input` content is appended after the geometry block in `main.gjf`; a trailing
  newline is always added by the scaffold to satisfy Gaussian's blank-line terminator rule.
