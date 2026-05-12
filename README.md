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

## Layout

- `src/` — Rust crate.
- `python/job_manager/` — Python facade re-exporting the compiled
  `_job_manager_core` extension.
- `tests/` — Rust integration tests.
- `python/tests/` — Python integration tests.

## Development

```bash
uv sync
uv run maturin develop                    # build extension in-place
cargo test --all-features                  # Rust tests
uv run pytest python/tests -v              # Python tests
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
```

## Stub regeneration

```bash
cargo run --bin stub_gen && uv run ruff format python/
```

## Out of scope (deferred to SP-2 / SP-3)

- `experiment.toml` parsing, sweep expansion, parent resolution
- sbatch submission (`submit_chain` equivalent)
- CLI commands (`run` / `submit` / `show` / `tick` / ...)
- Full `JobFlow` pyclass interop (bridge module)
