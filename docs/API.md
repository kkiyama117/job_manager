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
| Topological order + cycle detection | `FlowRun::topological_order()` | (used by `submit_flow`) |
| Render → submit → write `.status.toml` | `runner::FlowRunner::{submit, tick, render_only}` | `await job_manager.submit_flow(root, uuid, dry_run)` |
| SLURM submit abstraction (3 impls) | `slurm::executor::{Executor, SbatchExecutor, DryRunExecutor, MockExecutor}` | (internal) |
| SLURM query abstraction (3 impls) | `slurm::querier::{Querier, SlurmQuerier, InMemoryQuerier, MockQuerier}` | (internal) |
| State transition (pure) | `runner::transition::{decide_transition, Decision, TickResult}` | (internal) |
| Bash render (env-export style) | `render::render_batch_bash` | `job_manager.render_batch_bash` |

## Persistence

| Surface | Rust | Python |
|---|---|---|
| Path composition | `persistence::PathResolver` | `job_manager.PathResolver` |
| `flow.toml` I/O | `persistence::flow::{read_flow, write_flow}` | `read_flow` / `write_flow` |
| `plan.toml` I/O | `persistence::plan::{read_plan, write_plan}` | `read_plan` / `write_plan` |
| `common.toml` I/O | `persistence::common::{read_common, write_common}` | `read_common` / `write_common` |
| `.status.toml` I/O | `persistence::job_run::{read_job_run, write_job_run}` | `read_job_run` / `write_job_run` |
| `JobRun` data type | `job::{JobRun, Lifecycle}` | `JobRun` / `Lifecycle` |
| `ExperimentPlan` data type | `plan::ExperimentPlan` | `ExperimentPlan` |
| `CommonConfig` (defaults merge) | `persistence::merge_with_defaults` | (internal) |

## Search / discovery

| Surface | Rust | Python |
|---|---|---|
| Parallel walk over `<root>/<uuid>/flow.toml` | `walk::walk_flows` (Stream) | `await job_manager.walk_flows(root)` |
| Post-walk filter | `search::{SearchFilter, matches}` | `SearchFilter` |
| Per-Job facade | `view::CalcView` | `CalcView` |

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

| File | Schema | `deny_unknown_fields` |
|---|---|---|
| `flow.toml` | D2 `JobFlow` (jobs + edges) | upstream |
| `plan.toml` | `ExperimentPlan { jobs: BTreeMap<JobId, Table> }` | yes |
| `common.toml` | D2 `CommonConfig` (SLURM resource defaults) | yes |
| `.status.toml` | `JobRun { lifecycle, updated_at, slurm_jobid?, note?, slurm_status? }` | yes |

See [`architecture.md`](./architecture.md#statustoml-schema) for the
full `.status.toml` example and the lifecycle authority split.
