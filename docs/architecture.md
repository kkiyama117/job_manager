# Architecture

`job_manager` is the **data layer (SP-1)** of a 3-stage rework of
`gaussian-experiment-manager`. It sits between two upstream crates and
exposes the same operations to Rust and Python through a single PyO3
extension.

For the full design rationale and the SP-2/SP-3 scope split, see
`docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`.

## Position in the stack

```
                ┌────────────────────────┐
                │  CLI / future SP-2/3   │
                └──────────┬─────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │  job_manager   (this crate, SP-1)   │
        │   - PathResolver / flow_io          │
        │   - StatusEntry / status::io        │
        │   - walk_flows / SearchFilter       │
        │   - SlurmFacade / tick_many         │
        │   - CalcView (per-Job facade)       │
        └──────────┬───────────────┬──────────┘
                   │               │
    ┌──────────────▼──┐         ┌──▼────────────────────┐
    │ gaussian_job_   │         │ slurm_async_runner    │
    │ shared (D2)     │         │ (A1)                  │
    │  JobFlow / Job  │         │  SlurmManager         │
    │  JobId          │         │  JobStatus / JobState │
    │                 │         │  JobReason            │
    └─────────────────┘         └───────────────────────┘
```

The two upstream crates own their pyclass definitions. `job_manager`
consumes their Rust types only (`default-features = false`) — see
**Pyclass Single Owner rule** below.

## Module map (Rust)

```
src/
├── lib.rs              # public API re-exports
├── error.rs            # JobManagerError (thiserror)
├── path.rs             # PathResolver — pure path composition
├── flow_io.rs          # read_flow / write_flow (atomic-rename TOML)
├── status/
│   ├── mod.rs          # PerJobStatus, StatusEntry
│   └── io.rs           # read_status / write_status
├── walk.rs             # walk_flows — async stream over <root>/*
├── filter.rs           # SearchFilter + matches()
├── slurm_facade.rs     # SlurmFacade trait + A1SlurmFacade + InMemorySlurmFacade
├── tick.rs             # decide_transition (pure) + tick_many (orchestrator)
├── view.rs             # CalcView<'a> — per-Job facade
├── py_export/          # PyO3 surface (cfg-gated, `pyo3` feature)
│   ├── mod.rs          #  - pymodule init via sys.modules
│   ├── path.rs         #  - PyPathResolver  (wraps Arc<PathResolver>)
│   ├── status.rs       #  - PyPerJobStatus
│   ├── filter.rs       #  - PySearchFilter
│   ├── view.rs         #  - PyCalcView
│   ├── walk.rs         #  - walk_flows pyfunction (async)
│   ├── tick.rs         #  - tick_many pyfunction (async)
│   └── error.rs        #  - JobManagerError -> PyErr mapping
└── bin/stub_gen.rs     # pyo3-stub-gen entry — generates .pyi
```

Each module has a single responsibility. The split between
`status/mod.rs` (data type) and `status/io.rs` (filesystem) keeps the
domain model free of I/O imports, mirroring the same split between
`flow_io.rs` and `gaussian_job_shared`'s `JobFlow` type.

## On-disk layout

`PathResolver` is the single source of truth for path composition:

```
<root>/                      <- PathResolver.root
└── <flow_uuid>/             <- PathResolver.flow_dir(&flow.uuid)
    ├── flow.toml            <- JobFlow TOML (D2 schema)
    └── <JobId>/             <- per-Job folder (D2 convention)
        ├── input.gjf        <- user / grammar layer (SP-2)
        ├── slurm-<id>.out   <- SLURM stdout
        ├── slurm-<id>.err   <- SLURM stderr
        └── .status.toml     <- job_manager status (this crate)
```

Status is **not** stored inside `JobFlow` so the D2 schema stays
unchanged. The dot-prefix on `.status.toml` keeps it from colliding with
SLURM outputs (`slurm-*.out`) and user files. `CalcView::files()` filters
dot-prefixed entries.

## Public surface

Re-exported from `lib.rs`:

| Symbol | Kind | Purpose |
|---|---|---|
| `PathResolver` | struct | path composition |
| `read_flow` / `write_flow` | fn | JobFlow TOML I/O (atomic) |
| `PerJobStatus` / `StatusEntry` | enum / struct | per-Job lifecycle |
| `walk_flows` | fn → `Stream<Item=Result<JobFlow>>` | parallel filesystem walk |
| `SearchFilter` / `matches` | struct / fn | post-walk filter |
| `SlurmFacade` (`A1SlurmFacade`, `InMemorySlurmFacade`) | trait | SLURM query abstraction |
| `decide_transition` / `tick_many` | fn | SLURM ↔ local reconciliation |
| `TickResult` / `Decision` | struct | tick output |
| `CalcView` | struct | per-Job paths + status getter |
| `JobManagerError` / `SchemaParseError` | enum | errors |

`py_export/` mirrors the same surface in Python under
`job_manager._job_manager_core`, re-exported from
`python/job_manager/__init__.py`.

## Data flow

### Walk & filter

```
caller ─► walk_flows(root)               ┐
         │   stream<JobFlow>             │ buffer_unordered(32)
         │   parallel read_flow per dir  │ via spawn_blocking
         ▼                                ┘
       .filter(matches(.., &SearchFilter))
         │
         ▼
       caller consumes JobFlow
```

`walk_flows` is async-stream over candidates `<root>/<uuid>/flow.toml`.
Blocking TOML reads run on `spawn_blocking`; parallelism (default 32,
override via `JOB_MANAGER_PARALLELISM`) is bounded by `buffer_unordered`
so a directory with 10k flows does not exhaust file descriptors.

Errors per entry surface as `Err` stream items rather than aborting the
stream — one malformed `flow.toml` does not hide the rest.

### Tick (SLURM reconciliation)

```
caller ─► tick_many(targets, facade, resolver)
   │      where targets: &[(Uuid, JobId, slurm_jobid)]
   │
   │   1. facade.query_states_batch(jobids)  ─►  HashMap<u64, JobStatus>
   │   2. for each target:
   │       prev = read_status(<status path>)
   │       Decision = decide_transition(prev.lifecycle, slurm_status)
   │       if changed: write_status(StatusEntry { lifecycle: new, slurm_status, .. })
   │
   ▼
 Vec<TickResult>
```

`decide_transition` is a pure function — its full 5-rule invariant set
(no overwrite of `Done`, no overwrite of terminal local, etc.) is
covered by the rstest matrix in `src/tick.rs:266`. `tick_many` only
adds the orchestration: batch SLURM query, read/write per target,
collect results.

The raw SLURM `(state, reason)` pair (`slurm_status: JobStatus`) is
persisted alongside the 4-state lifecycle so the UI can render
scheduler details like `OUT_OF_MEMORY/OutOfMemory` when explaining a
failure.

### Per-Job facade

```
caller ─► CalcView::new(&flow, job_id, &resolver)?
            │ validates job_id ∈ flow.jobs
            ▼
          { job(), status(), job_dir(), status_path(), files() }
```

Lifetime-tied: `CalcView<'a>` borrows the `JobFlow` and `PathResolver`,
so the type system guarantees the flow outlives the view.

## Lifecycle model

`PerJobStatus` is a 4-state aggregated view, decoupled from SLURM's
~20-state enum:

```
        ┌─────────┐    SLURM Running/Completing/Resizing/...
        │ Queued  │ ─────────────► ┌──────────┐
        └────┬────┘                │ Running  │ ─┐
             │                     └────┬─────┘  │
             │ SLURM Pending/             │       │
             │ Configuring/...            │       │
             ▼                            ▼       │
         (no-op)                  ┌───────────────┴──┐
                                  │     Failed       │ ◄── SLURM Failed/OutOfMemory/...
                                  └──────────────────┘     except SLURM Completed
                                          │
                                  ┌───────▼────────┐
                                  │      Done      │ ◄── written ONLY by post.bash,
                                  └────────────────┘     never by tick
```

Authority split:
- **post.bash** is the sole authority for `Done`. `tick_many` never
  promotes `Running → Done` even when SLURM reports `Completed` — it
  emits a warning note instead, because completion is not the same as
  successful post-processing.
- `Failed` is terminal but `tick_many` can write it from any non-terminal
  prev state when SLURM is `is_failed_terminal()`.
- Terminal states (`Done`, `Failed`) are never overwritten by `tick`.

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

- Rust async: pure `tokio` + `futures` + `async-stream`.
- Python async: `pyo3-async-runtimes::tokio::future_into_py` wraps each
  pyfunction. The runtime is the tokio multi-thread runtime; blocking
  TOML I/O runs on `spawn_blocking`.
- The Python facade binds to the *running* event loop at call time, so
  callers must invoke from inside `asyncio.run(...)` or an existing
  coroutine — see `python/tests/test_python_api.py` for the pattern.

## Testing surface

```
src/**/*.rs                       — unit tests in #[cfg(test)] modules
tests/integration_walk.rs         — 100 flows enumerated under 1s
tests/integration_tick.rs         — 3-target tick via InMemorySlurmFacade
python/tests/test_python_api.py   — Python smoke tests
```

The `InMemorySlurmFacade` is a `pub` part of the library deliberately: it
lets downstream crates write deterministic tests without taking a
fixture dependency on a live SLURM cluster.

## Deferred to SP-2 / SP-3

Not implemented here:

- `experiment.toml` parsing, sweep expansion, parent resolution
- `submit_chain` equivalent (sbatch submission)
- CLI commands (`run` / `submit` / `show` / `tick` / `kill` / ...)
- Full `JobFlow` pyclass interop (SP-1 returns dicts via `walk_flows`)

These layers consume this crate's API; they do not modify it.
