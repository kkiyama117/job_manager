# job-manager

SLURM job orchestration library — Rust core + PyO3 Python bindings + `jm` CLI.

Sits between two upstream crates:

- **D2** (`gaussian-job-shared`): `JobFlow` DAG / `JobId` / `Program` newtypes / `CommonConfig`
- **A1** (`slurm-async-runner`): `SbatchCmd` / `SbatchManager` / `SlurmManager` / `JobStatus` / `SlurmDependency`

Implements file-based persistence, render → submit orchestration, and
SLURM state reconciliation (tick) on top of those primitives. Cf.
Airflow / Prefect vocabulary: `FlowRun` ≈ DAG Run, `JobRun` ≈
TaskInstance, `Lifecycle` ≈ TaskState.

See `docs/superpowers/specs/2026-05-13-job-manager-sp3-rearch-design.md`
for the current (SP-3 v2) design rationale.

## Capabilities

### Orchestration (SP-3)

| Surface | Rust | Python |
|---|---|---|
| `FlowRun` aggregate (flow_uuid + JobFlow + ExperimentPlan + Option<CommonConfig>) | `flow::FlowRun` | `job_manager.FlowRun` |
| Topological order + cycle detection | `FlowRun::topological_order()` | (used by `submit_flow`) |
| Render → submit → write `.status.toml` | `runner::FlowRunner::{submit, tick, render_only}` | `await job_manager.submit_flow(root, uuid, dry_run)` |
| SLURM submit abstraction (3 impls) | `slurm::executor::{Executor, SbatchExecutor, DryRunExecutor, MockExecutor}` | (internal) |
| SLURM query abstraction (3 impls) | `slurm::querier::{Querier, SlurmQuerier, InMemoryQuerier, MockQuerier}` | (internal) |
| State transition (pure) | `runner::transition::{decide_transition, Decision, TickResult}` | (internal) |
| Bash render (env-export style) | `render::render_batch_bash` | `job_manager.render_batch_bash` |

### Persistence

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

### Search / discovery

| Surface | Rust | Python |
|---|---|---|
| Parallel walk over `<root>/<uuid>/flow.toml` | `walk::walk_flows` (Stream) | `await job_manager.walk_flows(root)` |
| Post-walk filter | `search::{SearchFilter, matches}` | `SearchFilter` |
| Per-Job facade | `view::CalcView` | `CalcView` |

### Helpers (SP-2)

| Surface | Rust | Python |
|---|---|---|
| `JobId` build | `jobid::build_job_id(step_id, axis_combo)` | `build_job_id` |
| `JobId` parse | `jobid::parse_job_id(s)` → `JobIdParts` | `parse_job_id` (returns dict) |
| Step / job id validation | `jobid::{validate_step_id, validate_job_id}` | `validate_*` |

## Lifecycle state machine (5 values)

```
(no .status.toml)          ← implicit "Pending"
        │
        │ FlowRunner::submit() → sbatch succeeded
        ▼
   ┌─────────┐  tick: SLURM RUNNING   ┌──────────┐
   │ Queued  │ ─────────────────────► │ Running  │
   └────┬────┘                        └────┬─────┘
        │ tick: parent Failed/Skipped      │ tick: SLURM終了
        ▼                                  ▼
   ┌─────────┐                  ┌──────────┬──────────┐
   │ Skipped │                  │ Success  │  Failed  │
   └─────────┘ (terminal)       └──────────┴──────────┘ (terminal)
```

`Lifecycle::is_terminal()` → `Success | Failed | Skipped`. `Skipped` is
Airflow's `upstream_failed` analogue: emitted by `decide_transition` when
any parent is `Failed`/`Skipped`, carrying the actual culprit `JobId` in
`Decision::SkipDueToParent { parent }`.

## CLI (`jm`)

The `jm` binary is built alongside the library (`cargo build`).

```bash
# 1. (dry-run) render batch.bash only — no sbatch call
jm --root /work run <flow_uuid>

# 2. submit to SLURM (or DryRunExecutor + InMemoryQuerier when --dry-run)
jm --root /work submit <flow_uuid>
jm --root /work submit <flow_uuid> --dry-run

# 3. poll SLURM and apply transitions to .status.toml
jm --root /work tick <flow_uuid>

# 4. inspect flow + per-job status
jm --root /work show <flow_uuid>

# 5. cross-flow search
jm search /work --program g16
```

`--root` accepts an explicit path or falls back to `JM_ROOT`. Paths are
canonicalized (resolves `..` and symlinks). `<flow_uuid>` may be a bare
UUID string or an absolute path whose last component is the UUID.

## Python async API

```python
import asyncio
import job_manager

async def go(root: str, uuid: str):
    # submit returns dict[JobId, slurm_jobid] — empty when dry_run=True
    jobids = await job_manager.submit_flow(root, uuid, dry_run=False)
    print(jobids)

asyncio.run(go("/work", "01997cdc-..."))
```

`submit_flow` resolves to a coroutine bound to the *running* event loop
at call time, so always invoke it from inside `asyncio.run(...)` or an
existing coroutine. Same constraint applies to `walk_flows`.

## Development

Read [`docs/development.md`](./docs/development.md) — setup, build,
test, lint, stub regeneration, common pitfalls.

## Further reading

See [`docs/`](./docs/README.md) for:
- [`architecture.md`](./docs/architecture.md) — module map, on-disk
  layout, data flow, lifecycle model, Pyclass Single Owner rule.
- [`references/orchestration-systems.md`](./docs/references/orchestration-systems.md) —
  Airflow / Prefect vocabulary alignment that informs the SP-3 design.
- `docs/superpowers/specs/` and `docs/superpowers/plans/` — design spec
  and TDD implementation plan for each phase (SP-1, SP-2, SP-3 v1, SP-3 v2).

## Out of scope

- experiment DSL / sweep expansion / parent resolution (user writes
  this in Python — itertools / f-string / direct `JobEdge` construction)
- webhook trigger / long-running worker daemon
- per-flow `common.toml` (only root-level `common.toml` supported)
- TUI / interactive UI on top of `jm`
