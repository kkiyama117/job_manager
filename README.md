# job-manager

SLURM job data management library — Rust core + PyO3 Python bindings.

Implements the **data layer (SP-1)** of a 3-stage rework of
`gaussian-experiment-manager` on top of `gaussian-job-shared2` (D2,
`JobFlow` DAG) and `slurm-async-runner2` (A1, SLURM submission). See
`docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md` for the
design rationale.

## Capabilities (SP-1)

| Surface | Rust | Python |
|---|---|---|
| `PathResolver` | `path::PathResolver` | `job_manager.PathResolver` |
| `JobFlow` I/O | `flow_io::{read_flow, write_flow}` | (return as dict via `walk_flows`) |
| Per-Job status | `status::{PerJobStatus, StatusEntry}` | `job_manager.PerJobStatus` |
| Parallel walk | `walk::walk_flows` (Stream) | `await job_manager.walk_flows(root)` |
| Filter | `filter::{SearchFilter, matches}` | `job_manager.SearchFilter` |
| SLURM tick | `tick::tick_many` + `SlurmFacade` | `await job_manager.tick_many(...)` |
| Per-Job facade | `view::CalcView` | `job_manager.CalcView` |

## SP-2 (plan + jobid helpers) capabilities

- `ExperimentPlan` — per-job params sidecar (SP-3 が bash render で使う)
- `read_plan(path)` / `write_plan(path, plan)` — `plan.toml` atomic rename I/O
- `build_job_id(step_id, axis_combo)` — JobId 文字列を組み立てる
- `parse_job_id(s)` — JobId を `{source_step_id, axis_combo}` に分解
- `validate_step_id(s)` / `validate_job_id(s)` — 命名規約検証
- `PathResolver.plan_toml(&uuid)` / `.experiment_toml(&uuid)` — path 解決

**experiment.toml DSL は SP-2 に含まない。** sweep / placeholder / parent 解決はユーザーが Python (itertools / f-string / `JobEdge` 直接構築) で書く。spec §1.1 にサンプルあり。

## Development

Read [`development.md`](./docs/development.md) — setup, build, test, lint, stub regeneration, common pitfalls.

## Further reading

See [`docs/`](./docs/README.md) for:
- [`architecture.md`](./docs/architecture.md) — module map, on-disk
  layout, data flow, lifecycle model.
- `docs/superpowers/specs/` and `docs/superpowers/plans/` — design spec
  and TDD implementation plan for each phase.

## Out of scope (deferred to SP-2 / SP-3)

- `experiment.toml` parsing, sweep expansion, parent resolution
- sbatch submission (`submit_chain` equivalent)
- CLI commands (`run` / `submit` / `show` / `tick` / ...)
- Full `JobFlow` pyclass interop (bridge module)
