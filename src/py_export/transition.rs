//! Python wrapper for `runner::transition` (`Decision`, `TickResult`).
//!
//! Currently no public Python API — `FlowRunner::tick` is exposed via the
//! `runner` submodule (`submit_flow`). The `Decision` / `TickResult` types
//! remain Rust-internal until a Python-facing tick API is added.
