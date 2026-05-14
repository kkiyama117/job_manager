# Architecture

`job_manager` is a Rust orchestration library + PyO3 Python bindings +
`jm` CLI for SLURM jobs. It sits between two upstream crates and exposes
its capabilities to Rust, Python, and the shell through a single PyO3
extension and a single binary.

For the current design rationale see
`docs/superpowers/specs/2026-05-13-job-manager-sp3-rearch-design.md`
(SP-3 v2). The Airflow / Prefect vocabulary alignment is in
[`references/orchestration-systems.md`](./references/orchestration-systems.md).

## Position in the stack

```
                ┌───────────────────────────────────────┐
                │  jm CLI / Python / downstream callers │
                └─────────────────┬─────────────────────┘
                                  │
        ┌─────────────────────────▼─────────────────────────┐
        │                 job_manager                       │
        │  ┌─────────────────────────────────────────────┐  │
        │  │  FlowRun (aggregate)                        │  │
        │  │   flow_uuid, JobFlow, ExperimentPlan,       │  │
        │  │   Option<CommonConfig>, topological_order   │  │
        │  └────────────────────┬────────────────────────┘  │
        │                       │                           │
        │  ┌────────────────────▼────────────────────────┐  │
        │  │  FlowRunner — submit / tick / render_only   │  │
        │  └────────┬───────────────────┬────────────────┘  │
        │           │                   │                   │
        │  ┌────────▼──────┐    ┌───────▼──────┐            │
        │  │ Executor      │    │ Querier      │            │
        │  │ (sbatch)      │    │ (sacct)      │            │
        │  └────────┬──────┘    └───────┬──────┘            │
        │           │ wraps A1          │ wraps A1          │
        └───────────┼───────────────────┼───────────────────┘
                    ▼                   ▼
        ┌─────────────────┐    ┌────────────────────────┐
        │ gaussian_job_   │    │ slurm_async_runner     │
        │ shared (D2)     │    │ (A1)                   │
        │  JobFlow / Job  │    │  SbatchManager         │
        │  JobId / Program│    │  SlurmManager          │
        │  CommonConfig   │    │  JobStatus / JobState  │
        │  JobEdge        │    │  SlurmDependency       │
        └─────────────────┘    └────────────────────────┘
```

The two upstream crates own their pyclass definitions. `job_manager`
consumes their Rust types only (`default-features = false`) — see
**Pyclass Single Owner rule** below.

## Module map (Rust)

```
src/
├── lib.rs                  # Public re-exports
├── error.rs                # JobManagerError (thiserror), SchemaParseError
├── concurrency.rs          # Atomic write helpers (used by persistence + render)
├── jobid.rs                # build_job_id / parse_job_id / validate_*  (SP-2)
│
├── flow/                   # Flow aggregate
│   ├── mod.rs
│   ├── run.rs              #   FlowRun struct (flow_uuid + JobFlow + ExperimentPlan + Option<CommonConfig>)
│   └── topology.rs         #   Kahn's algorithm + cycle detection
│
├── job/                    # Per-job state
│   ├── mod.rs
│   ├── lifecycle.rs        #   Lifecycle enum (5 values) + is_terminal()
│   └── run.rs              #   JobRun struct (.status.toml payload)
│
├── persistence/            # All file I/O
│   ├── mod.rs              #   Re-exports + merge_with_defaults
│   ├── path.rs             #   PathResolver (root → flow_dir → batch_bash / status_file / *.toml)
│   ├── flow.rs             #   read_flow / write_flow (atomic, PID-suffixed tmp)
│   ├── plan.rs             #   read_plan / write_plan (atomic, PID-suffixed tmp)
│   ├── common.rs           #   read_common / write_common (atomic, PID-suffixed tmp)
│   └── job_run.rs          #   read_job_run / write_job_run (atomic, PID-suffixed tmp)
│
├── plan/                   # ExperimentPlan struct (no I/O — moved to persistence/plan.rs)
│   └── mod.rs
│
├── render/                 # batch.bash rendering
│   └── mod.rs              #   render_batch_bash + sanitize_var_name + quote_for_bash
│
├── slurm/                  # All A1 contact surface
│   ├── mod.rs
│   ├── executor.rs         #   Executor trait + Sbatch/DryRun/Mock impls
│   ├── querier.rs          #   Querier trait + Slurm/InMemory/Mock impls
│   └── dependency.rs       #   JobEdge[] + submitted → SlurmDependency
│
├── runner/                 # Orchestration
│   ├── mod.rs
│   ├── flow.rs             #   FlowRunner struct — submit / tick / render_only
│   └── transition.rs       #   decide_transition (pure) + Decision + TickResult
│
├── search.rs               # SearchFilter + matches()
├── view.rs                 # CalcView<'a> — per-Job facade
├── walk.rs                 # walk_flows — async stream over <root>/*
│
├── bin/
│   ├── jm.rs               # CLI binary (clap, 5 subcommands)
│   └── stub_gen.rs         # pyo3-stub-gen entry — generates .pyi
│
└── py_export/              # PyO3 surface (cfg-gated, `pyo3` feature)
    ├── mod.rs              #  - pymodule init via sys.modules
    ├── flow.rs             #  - PyFlowRun (frozen, __repr__)
    ├── job.rs              #  - PyJobRun (frozen, __repr__) + PyLifecycle
    ├── path.rs             #  - PyPathResolver
    ├── plan.rs             #  - PyExperimentPlan
    ├── search.rs           #  - PySearchFilter
    ├── view.rs             #  - PyCalcView
    ├── walk.rs             #  - walk_flows pyfunction (async)
    ├── runner.rs           #  - submit_flow pyfunction (async)
    ├── render.rs           #  - render_batch_bash pyfunction
    ├── persistence.rs      #  - read_common / write_common / read_flow / write_flow
    ├── jobid.rs            #  - build_job_id / parse_job_id / validate_*
    ├── transition.rs       #  - (internal helpers only)
    └── error.rs            #  - JobManagerError → PyErr mapping
```

Each module has a single responsibility. The split between
`job/run.rs` (data type) and `persistence/job_run.rs` (filesystem)
mirrors `flow/run.rs` vs `persistence/flow.rs`: the domain model is
free of I/O imports.

## On-disk layout

`PathResolver` is the single source of truth for path composition:

```
<root>/                          <- PathResolver.root()
├── common.toml                  <- PathResolver.common_toml()    (optional)
└── <flow_uuid>/                 <- PathResolver.flow_dir(&uuid)
    ├── flow.toml                <- PathResolver.flow_toml(&uuid) (D2 JobFlow)
    ├── plan.toml                <- PathResolver.plan_toml(&uuid) (ExperimentPlan)
    └── <JobId>/                 <- per-Job folder
        ├── batch.bash           <- PathResolver.batch_bash(&uuid, &jid)
        ├── input.gjf            <- user / grammar layer (out of scope)
        ├── slurm-<id>.out       <- SLURM stdout
        ├── slurm-<id>.err       <- SLURM stderr
        └── .status.toml         <- PathResolver.status_file(&uuid, &jid)
```

`common.toml` lives at the **root** level (one per root, shared across
all flows). Per-flow common.toml is not supported.

Status is **not** stored inside `JobFlow` so the D2 schema stays
unchanged. The dot-prefix on `.status.toml` keeps it from colliding with
SLURM outputs (`slurm-*.out`) and user files. `CalcView::files()` filters
dot-prefixed entries.

All TOML writes go through an atomic-rename helper with a
**PID-suffixed tmp file** (`<name>.<pid>.tmp`) so two processes can
write the same path in parallel without trampling each other's
intermediate state. `batch.bash` additionally `chmod 0600` on Unix
before the rename so another process cannot race-read it between write
and `sbatch`.

## Public surface

Re-exported from `lib.rs`:

| Symbol | Kind | Module |
|---|---|---|
| `FlowRun` | struct | `flow` |
| `JobRun` / `Lifecycle` | struct / enum | `job` |
| `FlowRunner` / `Decision` / `TickResult` / `decide_transition` | runner | `runner` |
| `Executor` / `SbatchExecutor` / `DryRunExecutor` / `MockExecutor` | trait + impls | `slurm::executor` |
| `Querier` / `SlurmQuerier` / `InMemoryQuerier` | trait + impls | `slurm` |
| `PathResolver` / `merge_with_defaults` | struct / fn | `persistence` |
| `read_flow` / `write_flow` / `read_plan` / `write_plan` / `read_common` / `write_common` / `read_job_run` / `write_job_run` | fn | `persistence::*` |
| `ExperimentPlan` | struct | `plan` |
| `render_batch_bash` | fn | `render` |
| `walk_flows` | fn → `Stream<Item=Result<JobFlow>>` | `walk` |
| `SearchFilter` / `matches` | struct / fn | `search` |
| `CalcView` | struct | `view` |
| `JobIdParts` / `build_job_id` / `parse_job_id` / `validate_step_id` / `validate_job_id` | fn / struct | `jobid` |
| `JobManagerError` / `SchemaParseError` | enum | `error` |

`py_export/` mirrors most of this in Python under
`job_manager._job_manager_core`, re-exported from
`python/job_manager/__init__.py`.

## Data flow

### `jm submit <uuid>` (or `submit_flow(...)`)

```
CLI / Python  ──► resolve_root → PathResolver::new(&canonical_root)
                                          │
                                          ▼
                  FlowRun::read(&resolver, uuid)
                    ├─ persistence::flow::read_flow      → JobFlow
                    ├─ persistence::plan::read_plan      → ExperimentPlan
                    └─ persistence::common::read_common  → Option<CommonConfig>
                                          │
                                          ▼
               (executor, querier) pair:
                  dry_run = true   → DryRunExecutor + InMemoryQuerier
                  dry_run = false  → SbatchExecutor + SlurmQuerier
                                          │
                                          ▼
                  FlowRunner::new(executor, querier, &resolver)
                                          │
                                          ▼
                  FlowRunner::submit(&flow_run, dry_run)
                    │
                    │  preseed `submitted` from any pre-existing
                    │  .status.toml (defensive — supports re-runs
                    │  and future skip logic)
                    │
                    │  for jid in topological_order():
                    │     params  = fr.params_of(jid)
                    │     parts   = parse_job_id(jid)
                    │     script  = render_batch_bash(...)
                    │     write batch.bash atomically (chmod 0600 unix)
                    │     if dry_run: continue
                    │     cfg     = fr.effective_config(jid)   // merge_with_defaults
                    │     deps    = slurm::dependency::build(parents, &submitted, jid)
                    │     cmd     = SbatchCmd::from(cfg, deps)
                    │     jobid   = executor.submit(cmd).await
                    │     submitted.insert(jid, jobid)
                    │     write .status.toml { lifecycle=Queued, slurm_jobid=jobid }
                    ▼
                  BTreeMap<JobId, u64>
```

The synchronous TOML I/O runs inside `tokio::task::spawn_blocking` so
the tokio runtime is never stalled.

### `jm tick <uuid>`

```
FlowRunner::tick(&flow_run)
  │  read all .status.toml under <uuid>/
  │  collect non-terminal slurm_jobid into jobids_to_query
  │  states = querier.query(&jobids_to_query).await
  │
  │  for jid in topological_order():
  │     run = current[jid]              (skip if missing / terminal)
  │     parents = parents_of(jid) → [(JobId, Lifecycle)]
  │     decision = decide_transition(run.lifecycle, states.get(jid), &parents)
  │     match decision:
  │        NoChange       → record & continue
  │        Transition{to,..}              → write new JobRun
  │        SkipDueToParent{parent}        → write Lifecycle::Skipped
  │     update local cache so later jobs see the new lifecycle
  ▼
TickResult { transitions: BTreeMap<JobId, Decision> }
```

`decide_transition` is pure. It uses parent lifecycles to detect
`SkipDueToParent`, which carries the **actual culprit JobId** so the
caller can render an accurate cause chain.

### `walk_flows` + `SearchFilter` (cross-flow discovery)

```
caller ─► walk_flows(root)               ┐
         │   stream<Result<JobFlow>>     │ buffer_unordered(N)
         │   parallel read_flow per dir  │ via spawn_blocking
         ▼                                ┘
       .filter(matches(.., &SearchFilter))
         │
         ▼
       caller consumes JobFlow
```

`walk_flows` is async-stream over candidates `<root>/<uuid>/flow.toml`.
Parallelism (default 32, override via `JOB_MANAGER_PARALLELISM`) is
bounded by `buffer_unordered` so a directory with 10k flows does not
exhaust file descriptors. Errors per entry surface as `Err` stream
items rather than aborting the stream — one malformed `flow.toml` does
not hide the rest.

## Lifecycle model (5 values)

```
(no .status.toml)                          = Pending (implicit)
        │
        │ FlowRunner::submit() — sbatch returned a jobid
        ▼
   ┌─────────┐    tick: SLURM RUNNING       ┌──────────┐
   │ Queued  │ ───────────────────────────► │ Running  │
   └────┬────┘                              └────┬─────┘
        │                                        │ tick: SLURM 終了
        │ tick: parent Failed/Skipped            ▼
        ▼                              ┌─────────┬──────────┐
   ┌─────────┐                         │ Success │  Failed  │
   │ Skipped │                         └─────────┴──────────┘
   └─────────┘  (terminal)               (terminal)
```

Authority split:
- `decide_transition` is the sole writer of `Success` and `Failed` (it
  promotes `Running → Success` when SLURM reports `Completed`,
  `Running → Failed` when SLURM reports `Failed/Timeout/OOM/...`).
- `SkipDueToParent` emits `Lifecycle::Skipped` and carries the parent
  `JobId` for diagnostics. It triggers when any parent is in
  `Failed | Skipped` and the dependency was `afterok`.
- Terminal states (`Success | Failed | Skipped`) are never overwritten
  by `tick`.

The raw SLURM `(state, reason)` pair (`slurm_status: JobStatus`) is
persisted alongside the 5-state lifecycle so a UI can render scheduler
details like `OUT_OF_MEMORY/OutOfMemory` when explaining a failure.

## `.status.toml` schema

```toml
lifecycle = "queued"          # snake_case: queued | running | success | failed | skipped
updated_at = "2026-05-13T12:34:56Z"
slurm_jobid = 12345
note = "..."                  # optional

[slurm_status]                # optional, A1 JobStatus shape
state = "PENDING"
reason = ""                   # optional A1 JobReason
```

`#[serde(deny_unknown_fields)]` is applied on `JobRun`, `ExperimentPlan`,
and `CommonConfig` so typos surface as parse errors.

## Pyclass Single Owner rule

Both `gaussian_job_shared` and `slurm_async_runner` own pyclass impls
behind their own `pyo3` features. If `job_manager`'s cdylib also pulled
those features in, the linker would emit a second copy of each pyclass
type — same `__module__` string, distinct Python type object — and
`isinstance` checks across crates would silently fail.

We enforce single ownership in `Cargo.toml`:

```toml
gaussian_job_shared = { path = "../gaussian-job-shared2", default-features = false }
slurm_async_runner  = { path = "../slurm-async-runner2",  default-features = false }
```

Plus `[patch."https://github.com/kkiyama117/slurm-async-runner.git"]`
redirects D2's git-sourced SAR to the same local path so cargo treats
`JobStatus`/`DependencyType` as one type.

## Async + GIL bridging

- Rust async: pure `tokio` + `futures` + `async-stream`. Sync TOML I/O
  inside `FlowRunner::submit` / `tick` runs on `spawn_blocking`.
- Python async: `pyo3-async-runtimes::tokio::future_into_py` wraps each
  pyfunction. The runtime is the tokio multi-thread runtime.
- The Python facade binds to the *running* event loop at call time, so
  callers must invoke from inside `asyncio.run(...)` or an existing
  coroutine — see `python/tests/test_python_api.py` for the pattern.

`PyFlowRun` and `PyJobRun` are declared `#[pyclass(frozen)]` (read-only
in Python; mutation only happens in Rust via `FlowRunner::submit` /
`tick`) and both implement `__repr__`.

## CLI (`jm`)

The `jm` binary (`src/bin/jm.rs`) is built alongside the library and
exposes 5 subcommands wired to `FlowRunner` via clap:

| Subcommand | Action | Executor / Querier pair |
|---|---|---|
| `run <uuid>` | render batch.bash only | `DryRunExecutor + InMemoryQuerier` |
| `submit <uuid> [--dry-run]` | render + sbatch + write `.status.toml` | `--dry-run` → `DryRun + InMemory`; else `SbatchExecutor + SlurmQuerier` |
| `tick <uuid>` | query SLURM and apply transitions | `DryRunExecutor + SlurmQuerier` (executor unused) |
| `show <uuid>` | read flow + per-job `.status.toml` | (none; pure reads) |
| `search [--program X]` | walk all flows under `--root`, filter | (none) |

`--root <path>` or `JM_ROOT=<path>` is required for every subcommand
including `search`. Paths are canonicalized at entry.

## Testing surface

```
src/**/*.rs                       — unit tests in #[cfg(test)] modules
tests/integration_sp3.rs          — end-to-end FlowRunner exercise via MockExecutor
tests/integration_walk.rs         — 100 flows enumerated under 1s
python/tests/test_python_api.py   — Python async smoke tests
python/tests/test_*.py            — Python wrappers (plan, jobid, ...)
```

`InMemoryQuerier` and `MockExecutor` are part of the **public** API
(`pub use slurm::querier::InMemoryQuerier`, `pub use
slurm::executor::MockExecutor` in `lib.rs`) deliberately, so downstream
crates can write deterministic tests without a live SLURM cluster.

`MockExecutor` records every submitted `SbatchCmd` (poison-recovery
`Mutex` so a panicked test still surfaces the recorded calls). The
test suite of 100+ tests exercises submit, tick, render, search, and
all transition rules.

## Deferred to future work

Not implemented here (see GitHub issue #13 for the deferred review
followups from PR #12):

- jm `search` UX (positional vs global `--root`)
- `FlowRunner` split (`FlowSubmitter` / `FlowTicker` / `FlowRenderer`)
- TOML read size limit (DoS hardening)
- `JobState` enum exhaustiveness with respect to A1 evolution
- experiment DSL / sweep expansion / parent resolution
- TUI / interactive UI on top of `jm`
