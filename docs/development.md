# Development

Setup, build, test, and lint instructions for working on `job_manager`.

For *what* the code does, see [architecture.md](./architecture.md).
For the SP-1 design rationale, see
`docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`.

## Toolchain

| Tool | Version | Notes |
|---|---|---|
| Rust | **nightly** (pinned by `rust-toolchain.toml`) | edition 2024 |
| Python | **>= 3.12** | abi3-py312 wheel |
| uv | latest | drives Python env + maturin |
| maturin | `>=1.13,<2.0` | dev dependency under `[dependency-groups]` |

`rustup` picks up `nightly` automatically from `rust-toolchain.toml`.
No global toolchain selection needed.

## Repository layout

Upstream crates live as **siblings** of this repo on disk:

```
GAUSSIAN_repo_packages/
├── gaussian-job-shared2/   # D2 — JobFlow, Job, JobId
├── slurm-async-runner2/    # A1 — SlurmManager, JobStatus
└── job-manager/            # this repo
    ├── Cargo.toml          # has path = "../gaussian-job-shared2" etc.
    ├── src/                # Rust crate
    ├── python/             # Python facade + tests + .pyi
    ├── tests/              # Rust integration tests
    └── docs/               # this directory
```

If the sibling layout is missing, `cargo build` fails on the path
dependency resolution. Clone all three before building.

## First-time setup

```bash
# 1. Install Python env + maturin
uv sync

# 2. Build the Rust extension into the Python env (editable install)
uv run maturin develop
```

`maturin develop` rebuilds in-place whenever Rust sources change. Re-run
after editing `src/`. The compiled artifact lands at
`python/job_manager/_job_manager_core/_job_manager_core.<abi>.so` and is
auto-imported by `python/job_manager/__init__.py`.

## Build

| Command | Purpose |
|---|---|
| `cargo build` | Rust-only build (default features: `pyo3`, `stub_gen`) |
| `cargo build --no-default-features` | Bare core, no PyO3 — what downstream crates see |
| `uv run maturin develop` | Build cdylib + install into Python env |
| `uv run maturin build --release` | Build a release wheel |

The default Cargo features enable PyO3 because the in-tree
`bin/stub_gen` requires it. `cargo check --no-default-features` is the
fastest way to verify the pure-Rust API still compiles.

## Test

```bash
cargo test --all-features                     # Rust (lib + integration)
cargo test --lib --all-features               # Rust unit only
cargo test --test integration_walk            # one integration suite
uv run pytest python/tests -v                 # Python smoke tests
uv run pytest python/tests -k tick -v         # one test
```

Run both before pushing:

```bash
cargo test --all-features && uv run pytest python/tests -v
```

### Test layout

- `src/**/*.rs` — unit tests inside `#[cfg(test)] mod tests`.
  Co-located with the code under test. Currently 44 passing.
- `tests/integration_walk.rs` — 100 `flow.toml` files under a tempdir,
  must complete in under 1s.
- `tests/integration_tick.rs` — 3-target `tick_many` via
  `InMemorySlurmFacade`.
- `python/tests/test_python_api.py` — Python-side smoke tests.

### Adding tests

Follow the test-first / TDD pattern documented under
`docs/superpowers/plans/2026-05-12-job-manager-sp1.md` (RED → GREEN →
REFACTOR). The plan's task templates show the exact shape expected for
new modules.

Rust unit tests live next to the code they cover:

```rust
// src/foo.rs
pub fn foo() -> bool { true }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_true() {
        assert!(foo());
    }
}
```

Parameterized tests use `rstest` (already a dev-dependency). See
`src/tick.rs:266` for the canonical `#[rstest] #[case(...)]` pattern.

### SLURM-facing tests

Do **not** require a live SLURM. Use `InMemorySlurmFacade`:

```rust
use job_manager::{InMemorySlurmFacade, tick_many};
use slurm_async_runner::{JobState, JobStatus};
use std::collections::HashMap;

let mut responses = HashMap::new();
responses.insert(99u64, JobStatus::new(JobState::Running));
let facade = InMemorySlurmFacade::new(responses);
let results = tick_many(&targets, &facade, &resolver).await;
```

The mock is intentionally part of the public API (`pub use
slurm_facade::InMemorySlurmFacade` in `lib.rs`) so downstream crates can use
it too.

### Coverage

```bash
cargo install cargo-llvm-cov   # one-time
cargo llvm-cov --all-features  # summary
cargo llvm-cov --html          # browsable report under target/llvm-cov/html/
cargo llvm-cov --fail-under-lines 80
```

Current data-layer line coverage: **85.57%**.

## Format & lint

```bash
cargo fmt --check                                         # Rust format check
cargo fmt                                                  # apply
cargo clippy --all-targets --all-features -- -D warnings   # treat lints as errors
uv run ruff format python/                                 # Python format
uv run ruff check python/                                  # Python lint
```

`rustfmt.toml` pins `edition = "2024"` so editor-driven rustfmt does not
reformat to edition 2015. `cargo fmt` honors it automatically.

The CI gate is:

```bash
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo test --all-features \
  && uv run pytest python/tests -v
```

Run this before pushing to keep PRs green.


## Stub generation (.pyi)

The `python/job_manager/_job_manager_core/__init__.pyi` is **generated**
— do not edit by hand. Regenerate after touching `py_export/`:

```bash
cargo run --bin stub_gen && uv run ruff format python/
```

`stub_gen` links against the dylib via the `stub_gen` feature. It needs
`libpython3.so` on `LD_LIBRARY_PATH`; under `uv sync`'d environments
this is wired up automatically.

> **Don't** enable `pyo3/auto-initialize` in `Cargo.toml`. It conflicts
> with maturin's statically-linked Python and produces the
> "auto-initialize feature is enabled, but your Python installation only
> supports embedding the Python interpreter statically" build error.
> The `Cargo.toml` already documents this in a NOTE comment — keep it.

## Environment variables

| Variable | Default | Effect |
|---|---|---|
| `JOB_MANAGER_PARALLELISM` | `32` | `buffer_unordered` width inside `walk_flows`. Lower to constrain filesystem load on large `<root>` directories. |

## Common pitfalls

- **Type mismatch between `JobStatus` from D2 vs A1.** Resolved by the
  `[patch."https://github.com/kkiyama117/slurm-async-runner.git"]`
  block in `Cargo.toml`. If you ever see `expected JobStatus, found
  JobStatus` from cargo, that block has been removed or paths drifted —
  see [architecture.md](./architecture.md#pyclass-single-owner-rule).
- **`asyncio.run(walk_flows(...))` fails with "no running event loop".**
  `pyo3-async-runtimes` binds the future to the loop at *call time*,
  not at await time. Wrap in an inner coroutine:
  ```python
  async def run(root):
      return await job_manager.walk_flows(root)
  asyncio.run(run(root))
  ```
- **`isinstance` returns `False` across crate boundaries.** A new
  pyclass was probably added to a sibling cdylib without disabling its
  `pyo3` feature here. See the **Pyclass Single Owner rule** in
  [architecture.md](./architecture.md#pyclass-single-owner-rule).
- **`stub_gen` segfaults or fails to link.** A duplicate
  `#[gen_stub_pyfunction]` on both the outer pymodule export *and* the
  inner free function will fail. Register the stub on exactly one
  layer.

## Workflow

For full feature work the project follows the superpowers planning loop:

1. Brainstorm → `docs/superpowers/specs/YYYY-MM-DD-*-design.md`
2. Plan → `docs/superpowers/plans/YYYY-MM-DD-*.md`
3. Subagent-driven implementation per plan task
4. Two-stage review per task (spec compliance, then code quality)
5. Final code review across the whole branch
6. PR against the parent docs/plan branch

The SP-1 design + plan are in `docs/superpowers/`. Future SP-2 / SP-3
work follows the same shape.

## Commit & PR

- Commit messages use Conventional Commits (`feat:`, `fix:`, `refactor:`,
  `test:`, `chore:`, `docs:`).
- Per-task commits during implementation, not one mega-commit per
  feature.
- PRs target the closest parent branch (impl → plan branch → main),
  not main directly.
