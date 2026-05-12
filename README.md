# job-manager

PyO3 + maturin scaffold for the `job_manager` Python package.

## Layout

- `src/` — Rust crate (`job_manager`) exposing the `_job_manager_core`
  extension module under the `pyo3` cargo feature.
- `src/bin/stub_gen.rs` — pyo3-stub-gen entry point. Builds when the
  `stub_gen` feature is enabled (on by default).
- `python/job_manager/` — pure-Python wrapper that imports the compiled
  `_job_manager_core` module.

## Development

```bash
uv sync
uv run maturin develop
uv run pytest
```

## Stub generation

```bash
cargo run --bin stub_gen
```
