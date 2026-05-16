# API reference

The `job_manager` surface, grouped by capability. For the canonical
Rust signatures see the inline rustdoc (`cargo doc --no-deps --open`).
For Python signatures see the generated stubs at
`python/job_manager/_job_manager_core/__init__.pyi`.

For *how the pieces fit together* (module map, on-disk layout, data
flow, lifecycle authority split, Pyclass Single Owner rule) see
[`architecture.md`](./architecture.md).

## Orchestration (SP-3)

| Surface | Rust | Python |
|---|---|---|
| `FlowRun` aggregate (flow_uuid + JobFlow + ExperimentPlan + Option<CommonConfig>) | `flow::FlowRun` | `job_manager.FlowRun` |
| Load `FlowRun` from `.jm/flow.effective.toml` (snapshot-driven; no `common.toml` needed) | `FlowRun::load_effective` | (used by `jm tick` / `jm show`) |
| Topological order + cycle detection | `FlowRun::topological_order()` | (used by `submit_flow`) |
| Render → submit → write `status.toml` | `runner::FlowRunner::{submit, tick, render_only}` | `await job_manager.submit_flow(root, uuid, dry_run)` |
| SLURM submit abstraction (3 impls) | `slurm::executor::{Executor, SbatchExecutor, DryRunExecutor, MockExecutor}` | (internal) |
| SLURM query abstraction (3 impls) | `slurm::querier::{Querier, SlurmQuerier, InMemoryQuerier, MockQuerier}` | (internal) |
| State transition (pure) | `runner::transition::{decide_transition, Decision, TickResult}` | (internal) |
| Bash render (env-export style) | `render::render_batch_bash` | `job_manager.render_batch_bash` |

## Persistence

| Surface | Rust | Python |
|---|---|---|
| Path composition | `persistence::PathResolver` | `job_manager.PathResolver` |
| `flow.toml` I/O (partition default injected from `common.toml`) | `persistence::flow::{read_flow, write_flow}` | `read_flow` / `write_flow` |
| `.jm/flow.effective.toml` I/O (materialized snapshot; no `common.toml` needed for reads) | `persistence::flow::{read_flow_effective, write_flow_effective}` | `read_flow_effective` / (write is Rust-only) |
| `plan.toml` I/O | `persistence::plan::{read_plan, write_plan}` | `read_plan` / `write_plan` |
| `common.toml` I/O | `persistence::common::{read_common, write_common}` | `read_common` / `write_common` |
| `status.toml` I/O (under `.jm/<JobId>/`) | `persistence::job_run::{read_job_run, write_job_run}` | `read_job_run` / `write_job_run` |
| `JobRun` data type | `job::{JobRun, Lifecycle}` | `JobRun` / `Lifecycle` |
| `ExperimentPlan` data type | `plan::ExperimentPlan` | `ExperimentPlan` |
| `CommonConfig` (defaults merge) | `persistence::{merge_with_defaults, synth_empty_common}` | (internal) |

## Search / discovery

| Surface | Rust | Python |
|---|---|---|
| Parallel walk over `<root>/<uuid>/flow.toml` | `walk::walk_flows` (Stream) | `await job_manager.walk_flows(root)` |
| Post-walk filter | `search::{SearchFilter, matches}` | `SearchFilter` |
| Per-Job facade | `view::CalcView` | `CalcView` |

`SearchFilter.status` (Rust): `BTreeSet<DisplayLifecycle>` (was `Option<Lifecycle>`).
`SearchFilter.status` (Python): `list[str]` — accepts short codes or long names, case-insensitive. Valid tokens: `pd`/`pending`, `q`/`queued`, `r`/`running`, `ok`/`success`, `f`/`failed`, `sk`/`skipped`.

## Listing

These are Rust-only (not exposed to Python).

Read-only cross-flow projection and formatting for `jm ls`. No SLURM contact — call `jm tick` first to reconcile state. All symbols are re-exported from `lib.rs`.

| Surface | Rust | Purpose |
|---|---|---|
| Walk + read all job statuses | `listing::collect(root, common, filter) → Vec<CollectedFlow>` | Async; missing/unreadable `status.toml` → Pending |
| Project to per-job rows | `listing::job_rows(collected, filter, limit)` | Filter + flatten; newest-flow-first × topo job order |
| Project to per-flow rows | `listing::flow_rows(collected, filter, limit)` | Aggregated status per flow |
| Flows where any job passes filter | `listing::matched_flows(collected, filter, limit)` | Used by `ls tree` and `ls flows` |
| Table formatter — jobs | `listing::format_jobs_table(rows, no_header)` | Aligned columns: FLOW JOB ST SLURM_ID PROGRAM UPDATED CREATED |
| Table formatter — flows | `listing::format_flows_table(rows, no_header)` | Aligned columns: FLOW TOTAL DONE STATUS CREATED |
| JSON formatter — jobs | `listing::format_jobs_json(rows)` | Pretty JSON array; full UUID, long status names |
| JSON formatter — flows | `listing::format_flows_json(rows)` | Pretty JSON array; full UUID |
| Tree formatter | `listing::format_tree(flows)` | Forest of flow→job trees; topological order; parent-edge annotations |
| Rolled-up flow status | `listing::aggregate_flow_status(jobs)` | Priority: FAILED > RUNNING > QUEUED > DONE > PARTIAL > PENDING |
| Display-time lifecycle | `listing::DisplayLifecycle` | 6 values including Pending (no `status.toml`) |

`DisplayLifecycle` code / long name mapping:

| Code | Long name |
|---|---|
| `PD` | `pending` |
| `Q` | `queued` |
| `R` | `running` |
| `OK` | `success` |
| `F` | `failed` |
| `SK` | `skipped` |

## Helpers (SP-2)

| Surface | Rust | Python |
|---|---|---|
| `JobId` build | `jobid::build_job_id(step_id, axis_combo)` | `build_job_id` |
| `JobId` parse | `jobid::parse_job_id(s)` → `JobIdParts` | `parse_job_id` (returns dict) |
| Step / job id validation | `jobid::{validate_step_id, validate_job_id}` | `validate_*` |

## Test-only surface (intentionally public)

`MockExecutor` and `InMemoryQuerier` are re-exported from `lib.rs` so
downstream crates can write deterministic tests without a live SLURM
cluster. See
[`development.md`](./development.md#slurm-facing-tests) for the
canonical pattern.

## File schemas

> Full field-by-field TOML format: [toml-reference.md](toml-reference.md).
> Exhaustive valid examples: [`examples/full/`](../examples/full/).
> Validate a tree: `jm --root <root> doctor`.

| File | Schema | `deny_unknown_fields` |
|---|---|---|
| `flow.toml` | D2 `JobFlow` (jobs + edges; `partition` per job may be omitted and inherited from `common.toml`) | upstream |
| `.jm/flow.effective.toml` | D2 `JobFlow` after defaulting is resolved (Cargo.lock analogue; safe to read without `common.toml`) | upstream |
| `plan.toml` | `ExperimentPlan { jobs: BTreeMap<JobId, Table> }` | yes |
| `common.toml` | D2 `CommonConfig` (SLURM resource defaults) | yes |
| `.jm/<JobId>/status.toml` | `JobRun { lifecycle, updated_at, slurm_jobid?, note?, slurm_status? }` | yes |

See [`architecture.md`](./architecture.md#statustoml-schema) for the
full `.status.toml` example and the lifecycle authority split.
