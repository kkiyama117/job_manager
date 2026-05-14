# Development

Setup, build, test, and lint instructions for working on `job_manager`.

For *what* the code does, see [architecture.md](./architecture.md).
For the current design rationale (SP-3 v2), see
`docs/superpowers/specs/2026-05-13-job-manager-sp3-rearch-design.md`.

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

```
job-manager/
├── Cargo.toml          # D2/A1 pulled from GitHub via `git = "..."`
├── src/                # Rust crate
├── python/             # Python facade + tests + .pyi
├── tests/              # Rust integration tests
└── docs/               # this directory
```

Upstream crates are fetched directly from GitHub at build time:

- D2 (`gaussian_job_shared`) — `https://github.com/kkiyama117/gaussian_job_shared.git`
- A1 (`slurm_async_runner`)  — `https://github.com/kkiyama117/slurm-async-runner.git`

Both consumed with `default-features = false` to respect the
[Pyclass Single Owner rule](./architecture.md#pyclass-single-owner-rule).
No sibling-repo checkout is required.

To work against a local checkout of D2 or A1 during development, override
the resolver in your global Cargo config (`~/.cargo/config.toml`) rather
than editing this repo's `Cargo.toml`:

```toml
[patch."https://github.com/kkiyama117/gaussian_job_shared.git"]
gaussian_job_shared = { path = "/path/to/your/gaussian-job-shared2" }

[patch."https://github.com/kkiyama117/slurm-async-runner.git"]
slurm_async_runner  = { path = "/path/to/your/slurm-async-runner2" }
```

Keep the override out of git — it's a per-machine concern.

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
| `cargo build` | Rust-only build (default features: `pyo3`, `stub_gen`). Produces both the library and the `jm` binary. |
| `cargo build --no-default-features` | Bare core, no PyO3 — what downstream crates see |
| `cargo build --release` | Optimized library + `jm` binary at `target/release/jm` |
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
  Co-located with the code under test.
- `tests/integration_walk.rs` — 100 `flow.toml` files under a tempdir,
  must complete in under 1s.
- `tests/integration_sp3.rs` — end-to-end `FlowRunner` exercise via
  `MockExecutor` + `InMemoryQuerier`.
- `python/tests/test_python_api.py` — Python-side async smoke tests
  (`submit_flow`, `walk_flows`).
- `python/tests/test_plan.py`, `test_jobid.py`, ... — per-module
  Python wrapper tests.

### Adding tests

Follow the test-first / TDD pattern documented under the per-sprint
plans in `docs/superpowers/plans/` (RED → GREEN → REFACTOR). The plan's
task templates show the exact shape expected for new modules.

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
`src/runner/transition.rs` for the canonical `#[rstest] #[case(...)]`
pattern covering the lifecycle transition matrix.

### SLURM-facing tests

Do **not** require a live SLURM. Use `InMemoryQuerier` for the query
side and `MockExecutor` for the submit side:

```rust
use job_manager::{
    FlowRunner, InMemoryQuerier, MockExecutor, PathResolver,
};
use slurm_async_runner::{JobState, JobStatus};
use std::collections::HashMap;

let mut responses = HashMap::new();
responses.insert(99u64, JobStatus::new(JobState::Running));
let querier = InMemoryQuerier::new(responses);

let executor = MockExecutor::with_recordings(vec![99]);
let resolver = PathResolver::new(tempdir.path());
let runner = FlowRunner::new(Box::new(executor), Box::new(querier), &resolver);
let result = runner.tick(&flow_run).await?;
```

Both mocks are intentionally part of the public API (`pub use
slurm::querier::InMemoryQuerier`, `pub use slurm::executor::MockExecutor`
in `lib.rs`) so downstream crates can use them too. `MockExecutor`
records every submitted `SbatchCmd` and recovers from a poisoned `Mutex`
so a panicked test still surfaces the recorded calls.

### Coverage

```bash
cargo install cargo-llvm-cov   # one-time
cargo llvm-cov --all-features  # summary
cargo llvm-cov --html          # browsable report under target/llvm-cov/html/
cargo llvm-cov --fail-under-lines 80
```

The project ships above the 80% gate; current numbers drift with each
change, so re-run the command above instead of trusting a checked-in
figure.

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

- **Type mismatch between `JobStatus` from D2 vs A1.** D2 references
  `slurm-async-runner` with the same git URL (no `rev`) that this crate
  uses, so the resolver unifies them onto a single source entry. If you
  ever see `expected JobStatus, found JobStatus` from cargo, someone
  has pinned a specific `rev` on one side without patching the other —
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
  pyclass was probably added to an upstream cdylib without disabling
  its `pyo3` feature here. See the **Pyclass Single Owner rule** in
  [architecture.md](./architecture.md#pyclass-single-owner-rule).
- **`stub_gen` segfaults or fails to link.** A duplicate
  `#[gen_stub_pyfunction]` on both the outer pymodule export *and* the
  inner free function will fail. Register the stub on exactly one
  layer.

## Running the `jm` CLI locally

```bash
# Build the binary (debug). `--no-default-features` strips the pyo3
# stack so `jm` is not dynamically linked against libpython3.13 — see
# the deployment note below.
cargo build --bin jm --no-default-features
./target/debug/jm --root /work render <flow_uuid>
./target/debug/jm --root /work submit <flow_uuid> --dry-run
./target/debug/jm --root /work tick   <flow_uuid>
./target/debug/jm --root /work show   <flow_uuid>
./target/debug/jm --root /work search --program g16
```

`--root <path>` or `JM_ROOT=<path>` is required for every subcommand.
`<flow_uuid>` is a bare UUID string or an absolute path whose last
component is the UUID.

### Deploying `jm` on a SLURM node without libpython

The default Cargo features (`pyo3 + stub_gen`) are needed for the
`stub_gen` binary and the Python extension build, but they also wire
`libpython3.13.so.1.0` into the `jm` binary's dynamic-link table. On
login / compute nodes that don't ship CPython, the result is:

```
./jm: error while loading shared libraries: libpython3.13.so.1.0:
cannot open shared object file: No such file or directory
```

Two options:

1. Build with `--no-default-features` and ship the resulting binary —
   `jm` itself never calls into Python, so this is the right answer
   for production:
   ```bash
   cargo build --release --bin jm --no-default-features
   scp target/release/jm <login-node>:~/bin/
   ```
2. (Discouraged) install CPython 3.13 on the target node so the
   abi3-linked `jm` can find `libpython3.13.so.1.0` at runtime.

## Workflow

For full feature work the project follows the superpowers planning loop:

1. Brainstorm → `docs/superpowers/specs/YYYY-MM-DD-*-design.md`
2. Plan → `docs/superpowers/plans/YYYY-MM-DD-*.md`
3. Subagent-driven implementation per plan task
4. Two-stage review per task (spec compliance, then code quality)
5. Final code review across the whole branch
6. PR against the parent docs/plan branch

SP-1, SP-2, and SP-3 (v1 + v2) all follow this shape. The active spec
is `2026-05-13-job-manager-sp3-rearch-design.md` (v2).

## Commit & PR

- Commit messages use Conventional Commits (`feat:`, `fix:`, `refactor:`,
  `test:`, `chore:`, `docs:`).
- Per-task commits during implementation, not one mega-commit per
  feature.
- PRs target the closest parent branch (impl → plan branch → main),
  not main directly.
