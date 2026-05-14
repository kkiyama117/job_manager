# job-manager

SLURM job orchestration library — Rust core + PyO3 Python bindings + `jm` CLI.

Sits between two upstream crates:

- **D2** (`gaussian-job-shared`): `JobFlow` DAG / `JobId` / `Program` newtypes / `CommonConfig`
- **A1** (`slurm-async-runner`): `SbatchCmd` / `SbatchManager` / `SlurmManager` / `JobStatus` / `SlurmDependency`

Implements file-based persistence, render → submit orchestration, and
SLURM state reconciliation (tick) on top of those primitives. Cf.
Airflow / Prefect vocabulary: `FlowRun` ≈ DAG Run, `JobRun` ≈
TaskInstance, `Lifecycle` ≈ TaskState.

## Capabilities

job-manager is plumbing. You bring three on-disk artifacts under a
working root, then drive `jm` (or the Python async API) against them.

### On-disk layout you prepare

```
<root>/                          ← --root / JM_ROOT
├── common.toml                  ← (optional) SLURM resource defaults shared by all flows
└── <flow_uuid>/
    ├── flow.toml                ← D2 JobFlow DAG (jobs + edges)
    └── plan.toml                ← ExperimentPlan — per-JobId render params
```

After `jm submit`, the same tree fills with per-job folders:

```
<root>/<flow_uuid>/<JobId>/
├── batch.bash                   ← rendered sbatch script (chmod 0600)
├── slurm-<id>.out / .err        ← SLURM stdout/stderr
└── .status.toml                 ← lifecycle + slurm_jobid + JobStatus
```

`PathResolver` is the single source of truth for these paths; status
files are dot-prefixed so they don't collide with SLURM outputs or
user files. Per-flow `common.toml` is **not** supported — there is one
`common.toml` per root.

### 1. Author inputs in Python

`ExperimentPlan` is a flat `{ job_id → params }` table. Compose
`JobId`s with `build_job_id(step_id, axis_combo)` and expand sweeps
with plain `itertools`:

```python
from itertools import product
from job_manager import ExperimentPlan, PathResolver, build_job_id, write_plan

compounds = ["benzene", "toluene", "p-xylene"]
methods = [{"name": "b3lyp", "route": "B3LYP"}, {"name": "m062x", "route": "M06-2X"}]

params: dict[str, dict] = {}
for (i, c), (j, m) in product(enumerate(compounds), enumerate(methods)):
    opt_id  = build_job_id("opt",  [("compound", i), ("method", j)])
    freq_id = build_job_id("freq", [("compound", i), ("method", j)])
    params[opt_id]  = {"route": f"# {m['route']}/6-31G* opt",  "compound": c, "nproc": 16}
    params[freq_id] = {"route": f"# {m['route']}/6-31G* freq", "compound": c, "nproc": 16}

plan = ExperimentPlan(params)        # 12 jobs

resolver = PathResolver("/work")
uuid = "01997cdc-0000-7000-8000-000000000000"
write_plan(resolver.plan_toml(uuid), plan)
# write_flow(resolver.flow_toml(uuid), flow_toml_text)   # D2 JobFlow (build via D2's API)
```

`build_job_id` also validates: `validate_step_id` / `validate_job_id`
reject reserved names (`flow`, `plan`, `experiment`, `derived`,
`status`), unsafe characters (`/`, `=`, whitespace), and path
traversal. The library refuses to render or submit if a `JobId` would
escape its parent directory.

The DAG itself (`flow.toml`) is plain D2 `JobFlow` — build it with
D2's Rust/Python API. Sweep expansion, parent resolution, and any DSL
on top of this live in the **caller**; this library deliberately stops
at the data layer.

### 2. Drive a flow from the shell

```bash
# render batch.bash only — no sbatch call, useful for review
jm --root /work render <flow_uuid>

# submit all jobs in topological order (writes .status.toml as it goes)
jm --root /work submit <flow_uuid>
jm --root /work submit <flow_uuid> --dry-run     # rehearse: DryRunExecutor + InMemoryQuerier

# query SLURM and reconcile every .status.toml under the flow
jm --root /work tick <flow_uuid>

# inspect the flow + per-job lifecycle
jm --root /work show <flow_uuid>

# cross-flow search across <root>/*/flow.toml
jm --root /work search --program g16
```

`--root <path>` is required for every subcommand (including
`search`). `JM_ROOT=<path>` works as a fallback. Paths are
canonicalized at entry (`..` and symlinks resolved). `<flow_uuid>` is
a bare UUID string or an absolute path whose last component is the UUID.

### 3. Reconcile on a timer

`tick` is idempotent and safe to schedule. A minimal cron entry:

```cron
*/5 * * * * jm --root /work tick <flow_uuid>
```

`decide_transition` is pure and is the **only** writer of
`Success`/`Failed`. Terminal states (`Success | Failed | Skipped`) are
never overwritten. `Skipped` propagates from a `Failed` or `Skipped`
parent on an `afterok` edge and carries the actual culprit `JobId` so
you can render an accurate cause chain.

### 4. Drive from Python instead

```python
import asyncio
import job_manager

async def run(root: str, uuid: str):
    # submit returns dict[JobId, slurm_jobid] — empty when dry_run=True
    jobids = await job_manager.submit_flow(root, uuid, dry_run=False)
    return jobids

asyncio.run(run("/work", "01997cdc-..."))
```

> ⚠ `submit_flow` / `walk_flows` bind to the **running** event loop at
> *call time*, not at await time. Always invoke from inside an
> existing coroutine — `asyncio.run(job_manager.submit_flow(...))`
> fails with "no running event loop".

### 5. Discover across flows

`walk_flows(root)` walks `<root>/*/flow.toml` in parallel
(`JOB_MANAGER_PARALLELISM` env var, default 32). One malformed
`flow.toml` surfaces as an error entry without aborting the rest.

`SearchFilter` is a post-walk predicate — filter by `program` /
`tags` / `status` / `flow_uuid_prefix` / `created_after` /
`created_before` / `slurm_jobid` / `job_id`.

```python
import asyncio, job_manager

async def find_g16_failures():
    flows = await job_manager.walk_flows("/work")
    f = job_manager.SearchFilter(
        program="g16",
        status=job_manager.Lifecycle.Failed,
    )
    # filtering happens in the caller — see search::matches in Rust
    return flows, f
```

`CalcView(resolver, flow_uuid, job_id)` is the per-Job facade — it
exposes `job_dir`, `status_path`, `status()`, and `files()` (filtering
out dot-prefixed entries like `.status.toml`).

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
Airflow's `upstream_failed` analogue: emitted by `decide_transition`
when any parent is `Failed`/`Skipped`, carrying the actual culprit
`JobId` in `Decision::SkipDueToParent { parent }`.

## Development

Read [`docs/development.md`](./docs/development.md) — setup, build,
test, lint, stub regeneration, common pitfalls.

## Further reading

See [`docs/`](./docs/README.md) for:
- [`API.md`](./docs/API.md) — full Rust / Python surface and file schemas
- [`architecture.md`](./docs/architecture.md) — module map, on-disk
  layout, data flow, lifecycle model, Pyclass Single Owner rule
- [`references/orchestration-systems.md`](./docs/references/orchestration-systems.md) —
  Airflow / Prefect vocabulary alignment that informs the SP-3 design.

## Out of scope

- experiment DSL / sweep expansion / parent resolution (caller writes
  this in Python — itertools / f-string / direct `JobEdge` construction)
- webhook trigger / long-running worker daemon
- per-flow `common.toml` (only root-level `common.toml` supported)
- TUI / interactive UI on top of `jm`
