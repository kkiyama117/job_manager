# job-manager

[![CI](https://github.com/kkiyama117/job_manager/actions/workflows/CI.yml/badge.svg)](https://github.com/kkiyama117/job_manager/actions/workflows/CI.yml)
[![Stub Check](https://github.com/kkiyama117/job_manager/actions/workflows/stub-check.yml/badge.svg)](https://github.com/kkiyama117/job_manager/actions/workflows/stub-check.yml)
[![Rust](https://img.shields.io/badge/rust-nightly%20%7C%20edition%202024-orange.svg)](./rust-toolchain.toml)
[![Python](https://img.shields.io/badge/python-%E2%89%A53.12-blue.svg)](./pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)

**SLURM job orchestration library — Rust core + PyO3 Python bindings + a `jm` CLI.**
Renders user-authored DAGs into `sbatch` scripts, submits them in topological order, and
reconciles SLURM state into a per-flow file tree so reruns and `cron`-driven ticks are idempotent.
Vocabulary mirrors Airflow / Prefect: `FlowRun` ≈ DAG Run, `JobRun` ≈ TaskInstance, `Lifecycle` ≈ TaskState.

This crate is the orchestration layer on top of two upstream libraries:

- **D2** — [`gaussian_job_shared`](https://github.com/kkiyama117/gaussian_job_shared): `JobFlow` DAG / `JobId` / `Program` newtypes / `CommonConfig`
- **A1** — [`slurm_async_runner`](https://github.com/kkiyama117/slurm-async-runner): `SbatchCmd` / `SbatchManager` / `SlurmManager` / `JobStatus` / `SlurmDependency`

---

## Table of contents

- [Demo](#demo)
- [Install](#install)
- [Quick start](#quick-start)
- [On-disk layout](#on-disk-layout)
- [`common.toml` defaulting](#commontoml-defaulting)
- [Driving flows](#driving-flows)
  - [From the shell (`jm`)](#1-from-the-shell-jm)
  - [From Python (async)](#2-from-python-async)
  - [Cron-driven reconciliation](#3-cron-driven-reconciliation)
  - [Cross-flow discovery](#4-cross-flow-discovery)
- [Lifecycle state machine](#lifecycle-state-machine-5-values)
- [API surface](#api-surface)
- [Examples](#examples)
- [Development](#development)
- [Out of scope](#out-of-scope)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Demo

> A real screencast lives at `docs/assets/jm-demo.gif` (TBD). Until then, here is an
> illustrative sketch of a 3-job flow run — stylized for readability; the real
> `jm render` prints a terser `rendered 3 jobs in <uuid>` line:

```text
$ jm --root /work render 01997cdc-0000-7000-8000-000000000000
✔ flow.effective.toml         (resolved 3 jobs against common.toml)
✔ <opt>/batch.bash            (chmod 0600)
✔ <freq>/batch.bash           (chmod 0600)
✔ <single_point>/batch.bash   (chmod 0600)

$ jm --root /work submit 01997cdc-0000-7000-8000-000000000000
→ opt           sbatch ... 482910   (Queued)
→ freq          deferred (afterok:482910)
→ single_point  deferred (afterok:482911)

$ jm --root /work tick 01997cdc-0000-7000-8000-000000000000
opt           Running        slurm=482910
freq          Queued         slurm=482911
single_point  Pending

$ jm --root /work show 01997cdc-0000-7000-8000-000000000000
opt           Success        slurm=482910
freq          Success        slurm=482911
single_point  Skipped        reason=upstream_failed(freq)   ← (only when freq dies)
```

---

## Install

`job-manager` is published as **source** — there is no PyPI / crates.io release yet. Build locally:

### 1. Prerequisites (fresh machine)

| Tool | Why | Install |
|---|---|---|
| [`rustup`](https://rustup.rs) | Rust **nightly** is required (edition 2024). `rust-toolchain.toml` selects it automatically — no `rustup default` needed. | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| [`uv`](https://github.com/astral-sh/uv) | Drives the Python env (`>= 3.12`) and pulls `maturin` as a dev dependency. | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| **SLURM** (`sbatch` / `sacct`) | Only needed to *actually* submit jobs. Tests use `MockExecutor` + `InMemoryQuerier`, so it is **not** required for development. | Cluster-provided. |

You do **not** need a sibling checkout of D2 / A1 — both crates are fetched from GitHub at build time.

### 2. Build

```bash
git clone https://github.com/kkiyama117/job_manager.git
cd job_manager

# Python env + maturin
uv sync

# Build the cdylib and install editable into the venv
uv run maturin develop
```

Re-run `uv run maturin develop` whenever you edit `src/`.

### 3. Build the `jm` CLI

```bash
# IMPORTANT on SLURM nodes that do not ship CPython:
# default features pull in pyo3+abi3-py312, which makes `jm` dynamically
# link libpython3.13. `jm` itself never calls Python, so build with
# --no-default-features for deployment.
cargo build --bin jm --no-default-features --release
install -m 0755 target/release/jm /usr/local/bin/jm    # or ~/.local/bin/jm
```

### 4. Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `JM_ROOT` | — | Fallback for `jm --root <path>`. Required (one or the other) on **every** subcommand including `search`. |
| `JOB_MANAGER_PARALLELISM` | `32` | `buffer_unordered` width inside `walk_flows`. Lower it to constrain FD load on huge `<root>` directories. |

---

## Quick start

```bash
# 1. Lay out one flow under a working root
mkdir -p /work/01997cdc-0000-7000-8000-000000000000
$EDITOR /work/01997cdc-0000-7000-8000-000000000000/flow.toml   # D2 JobFlow DAG
$EDITOR /work/01997cdc-0000-7000-8000-000000000000/plan.toml   # per-JobId render params
$EDITOR /work/common.toml                                      # (optional) shared SLURM defaults

# 2. Render batch.bash + .jm/flow.effective.toml snapshot
jm --root /work render 01997cdc-0000-7000-8000-000000000000

# 3. Submit (writes .jm/<JobId>/status.toml as it goes)
jm --root /work submit 01997cdc-0000-7000-8000-000000000000

# 4. Reconcile every status.toml against sacct
jm --root /work tick 01997cdc-0000-7000-8000-000000000000

# 5. Inspect lifecycle
jm --root /work show 01997cdc-0000-7000-8000-000000000000
```

A fully worked walkthrough — author plan with `itertools` sweep, render, submit (mocked), tick — lives under [`examples/`](./examples/).

---

## On-disk layout

You author two files per flow (plus an optional root-level `common.toml`); `jm` materializes everything else into a sibling `.jm/` subtree.

```
<root>/                              ← --root / JM_ROOT
├── common.toml                      ← (optional) SLURM resource defaults shared by all flows
└── <flow_uuid>/
    ├── flow.toml                    ← D2 JobFlow DAG (jobs + edges) — user input
    ├── plan.toml                    ← ExperimentPlan — per-JobId render params — user input
    └── .jm/                         ← program-managed (safe to .gitignore per-flow)
        ├── flow.effective.toml      ← materialized snapshot (Cargo.lock analogue)
        └── <JobId>/
            ├── batch.bash           ← rendered sbatch script (chmod 0600)
            ├── slurm-<id>.out/.err  ← SLURM stdout/stderr
            └── status.toml          ← lifecycle + slurm_jobid + JobStatus
```

`PathResolver` is the single source of truth for these paths. Add `.jm/` to your per-flow `.gitignore` to cleanly separate committed inputs from program output.

> Per-flow `common.toml` is **not** supported — there is exactly one `common.toml` per root.

---

## `common.toml` defaulting

`flow.toml` may omit `[jobs.*.config] partition`; the value flows in from `common.toml [slurm_default] partition` at `read_flow` time. If both are missing the read fails with a job-pointed `PartitionMissing` error; a non-string `partition` value fails with `PartitionWrongType` naming the offending TOML type.

This mirrors Airflow's `default_args` inheritance and Prefect's Work Pool `base_job_template` — see [`docs/architecture.md`](./docs/architecture.md#commontoml-as-pool-template-airflow--prefect-mapping).

### `.jm/flow.effective.toml` — materialized snapshot

`jm render` and `jm submit` write the snapshot with every default resolved. `jm tick` and `jm show` read it (via `FlowRun::load_effective`) and **do not need `common.toml` at runtime**. Use `jm render --effective-only <uuid>` to regenerate just the snapshot without re-rendering every `batch.bash`.

---

## Driving flows

### 1. From the shell (`jm`)

```bash
# scaffold flow.toml + plan.toml under a fresh uuid
jm --root /work new
jm --root /work new --tag program=g16 --tag basis=6-31g    # attach key=value tags; --print-path prints the dir instead of the uuid

# render every batch.bash + write .jm/flow.effective.toml snapshot
jm --root /work render <flow_uuid>

# regenerate ONLY .jm/flow.effective.toml without touching batch.bash
jm --root /work render <flow_uuid> --effective-only

# submit all jobs in topological order
jm --root /work submit <flow_uuid>
jm --root /work submit <flow_uuid> --dry-run     # rehearse: DryRunExecutor + InMemoryQuerier

# query SLURM and reconcile every status.toml under the flow (snapshot-driven)
jm --root /work tick <flow_uuid>

# inspect the flow + per-job lifecycle (snapshot-driven)
jm --root /work show <flow_uuid>

# cross-flow search across <root>/*/flow.toml
jm --root /work search --program g16

# validate TOML + structure
jm --root /work doctor [<flow_uuid>]
```

`--root <path>` is required for every subcommand (including `search`); `JM_ROOT=<path>` works as a fallback. Paths are canonicalized at entry (`..` and symlinks resolved). `<flow_uuid>` is a bare UUID string or an absolute path whose last component is the UUID.

### 2. From Python (async)

Authoring inputs:

```python
from itertools import product
from job_manager import ExperimentPlan, PathResolver, build_job_id, write_plan

compounds = ["benzene", "toluene", "p-xylene"]
methods   = [{"name": "b3lyp", "route": "B3LYP"}, {"name": "m062x", "route": "M06-2X"}]

params: dict[str, dict] = {}
for (i, c), (j, m) in product(enumerate(compounds), enumerate(methods)):
    opt_id  = build_job_id("opt",  [("compound", i), ("method", j)])
    freq_id = build_job_id("freq", [("compound", i), ("method", j)])
    params[opt_id]  = {"route": f"# {m['route']}/6-31G* opt",  "compound": c, "nproc": 16}
    params[freq_id] = {"route": f"# {m['route']}/6-31G* freq", "compound": c, "nproc": 16}

plan = ExperimentPlan(params)   # 12 jobs

resolver = PathResolver("/work")
uuid = "01997cdc-0000-7000-8000-000000000000"
write_plan(resolver.plan_toml(uuid), plan)
# write_flow(resolver.flow_toml(uuid), flow_toml_text)   # D2 JobFlow — build via D2's API
```

`build_job_id` validates: `validate_step_id` / `validate_job_id` reject reserved names (`flow`, `plan`, `experiment`, `derived`, `status`), unsafe characters (`/`, `=`, whitespace), and path traversal. The library refuses to render or submit if a `JobId` would escape its parent directory.

Driving a submission:

```python
import asyncio
import job_manager

async def run(root: str, uuid: str):
    # submit returns dict[JobId, slurm_jobid] — empty when dry_run=True
    jobids = await job_manager.submit_flow(root, uuid, dry_run=False)
    return jobids

asyncio.run(run("/work", "01997cdc-..."))
```

Snapshot-driven reads (after `jm render` / `jm submit` has materialized `.jm/flow.effective.toml`) don't need `common.toml`:

```python
from job_manager import PathResolver, read_flow_effective

resolver = PathResolver("/work")
flow = read_flow_effective(resolver.flow_effective_toml("01997cdc-..."))
```

> ⚠ **Event-loop gotcha:** `pyo3-async-runtimes` binds the future to the **running** event
> loop at *call time*, not at await time. So this fails with `no running event loop`:
>
> ```python
> asyncio.run(job_manager.submit_flow(root, uuid))   # WRONG
> ```
>
> Wrap in an inner coroutine:
>
> ```python
> async def go(root, uuid):
>     return await job_manager.submit_flow(root, uuid)
> asyncio.run(go(root, uuid))
> ```
>
> Same applies to `walk_flows`.

### 3. Cron-driven reconciliation

`tick` is idempotent and safe to schedule. Minimal cron:

```cron
*/5 * * * * jm --root /work tick <flow_uuid>
```

`decide_transition` (in `runner/transition.rs`) is **pure** and is the only writer of `Success` / `Failed`. Terminal states (`Success | Failed | Skipped`) are never overwritten. `Skipped` propagates from a `Failed` or `Skipped` parent on an `afterok` edge and carries the actual culprit `JobId` so you can render an accurate cause chain.

### 4. Cross-flow discovery

`walk_flows(root)` walks `<root>/*/flow.toml` in parallel (`JOB_MANAGER_PARALLELISM`, default 32). One malformed `flow.toml` surfaces as an error entry without aborting the rest.

`SearchFilter` is a post-walk predicate — filter by `program` / `tags` / `status` / `flow_uuid_prefix` / `created_after` / `created_before` / `slurm_jobid` / `job_id`.

```python
import asyncio, job_manager

async def find_g16_failures():
    flows = await job_manager.walk_flows("/work")
    f = job_manager.SearchFilter(
        program="g16",
        status=job_manager.Lifecycle.Failed,
    )
    return flows, f
```

`CalcView(resolver, flow_uuid, job_id)` is the per-Job facade — `job_dir`, `status_path`, `status()`, and `files()` (filtering out dot-prefixed entries like `.status.toml`).

---

## Lifecycle state machine (5 values)

```
(no .status.toml)          ← implicit "Pending"
        │
        │ FlowRunner::submit() → sbatch succeeded
        ▼
   ┌─────────┐  tick: SLURM RUNNING   ┌──────────┐
   │ Queued  │ ─────────────────────► │ Running  │
   └────┬────┘                        └────┬─────┘
        │ tick: parent Failed/Skipped      │ tick: SLURM terminal
        ▼                                  ▼
   ┌─────────┐                  ┌──────────┬──────────┐
   │ Skipped │                  │ Success  │  Failed  │
   └─────────┘ (terminal)       └──────────┴──────────┘ (terminal)
```

`Lifecycle::is_terminal()` → `Success | Failed | Skipped`. `Skipped` is Airflow's `upstream_failed` analogue: emitted by `decide_transition` when any parent is `Failed`/`Skipped`, carrying the culprit `JobId` in `Decision::SkipDueToParent { parent }`.

---

## API surface

The full surface — every Rust function and PyO3 export, plus the TOML file schemas — is documented in [`docs/API.md`](./docs/API.md). Highlights:

| Layer | Key types / functions | Purpose |
|---|---|---|
| **Orchestration** | `FlowRunner`, `FlowRun`, `JobRun`, `Lifecycle` | Render → submit → tick pipeline; per-job state machine |
| **Executors** | `SbatchExecutor`, `DryRunExecutor`, `MockExecutor` | Pluggable `sbatch` impls (`MockExecutor` is **public API** for downstream SLURM-free tests) |
| **Queriers** | `SlurmQuerier`, `InMemoryQuerier`, `MockQuerier` | Pluggable `sacct` impls |
| **Persistence** | `PathResolver`, `read_flow`, `read_flow_effective`, `write_plan`, `write_status` | Single source of truth for on-disk paths; atomic-rename TOML I/O |
| **Plan helpers** | `build_job_id`, `validate_step_id`, `validate_job_id`, `ExperimentPlan` | Compose `JobId`s, reject reserved names / unsafe characters / path traversal |
| **Search** | `walk_flows`, `SearchFilter`, `CalcView` | Cross-flow discovery, per-job facade |
| **Transition (pure)** | `decide_transition`, `Decision` | The only writer of `Success` / `Failed` / `Skipped` |

Python types are also exposed via a generated stub (`python/job_manager/_job_manager_core/*.pyi`). Regenerate after editing `py_export/`:

```bash
cargo run --bin stub_gen && uv run ruff format python/
```

---

## Examples

Working flows live under [`examples/`](./examples/):

- [`examples/simple/`](./examples/simple/) — two-job linear flow (opt → freq) authored by hand.
- [`examples/sweep/`](./examples/sweep/) — Python-authored sweep (compound × method) with `itertools`, plus a failure-path variant (`inputs-fail` / `outputs-fail`) showing `Skipped` propagation.

Each example's `README.md` walks through render → submit (dry-run) → tick → show against a local `<root>`.

---

## Development

Setup, build, test, lint, stub regeneration, common pitfalls: see [`docs/development.md`](./docs/development.md).

CI gate to run before pushing:

```bash
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo test --all-features \
  && uv run pytest python/tests -v
```

For deeper architectural background:

- [`docs/architecture.md`](./docs/architecture.md) — module map, on-disk layout, data flow, lifecycle model, **Pyclass Single Owner rule**.
- [`docs/references/orchestration-systems.md`](./docs/references/orchestration-systems.md) — Airflow / Prefect vocabulary alignment.
- [`docs/toml-reference.md`](./docs/toml-reference.md) — every TOML file's format, field by field

---

## Out of scope

Not part of this crate — don't add them here:

- Experiment DSL / sweep expansion / parent resolution (the caller composes `JobEdge` in Python — `itertools` / f-strings / direct construction).
- Webhook trigger / long-running worker daemon.
- Per-flow `common.toml` (root-level only).
- TUI / interactive UI on top of `jm`.

---

## Contributing

Issues and PRs are welcome. Before opening a PR:

1. Read [`docs/development.md`](./docs/development.md) and [`CLAUDE.md`](./CLAUDE.md) (architecture / conventions).
2. Install the pre-commit hook: `pre-commit install`. It runs `ruff`, `clippy --fix`, `rustfmt`, and `stub_gen` — when the stub hook aborts a commit because `.pyi` drifted, `git add python/job_manager/_job_manager_core/*.pyi` and retry.
3. Follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `refactor:`, `test:`, `chore:`, `docs:`). One issue per commit.
4. Stack PRs on the closest parent branch (impl → plan branch → main), not main directly.
5. Run the CI gate locally (above) — green tests, no clippy warnings, no fmt diff.

The project follows a superpowers planning loop: design spec → plan → subagent-driven implementation → two-stage review → final review. Active specs live under [`docs/superpowers/specs/`](./docs/superpowers/).

---

## License

Released under the [MIT License](./LICENSE) — © 2026 kkiyama117. You are
free to use, modify, and redistribute the source, subject to the
conditions in the license file.

---

## Contact

- **Author:** kkiyama117 — k.kiyama117@gmail.com
- **Issues / PRs:** https://github.com/kkiyama117/job_manager/issues
- **Upstream crates:** [gaussian_job_shared](https://github.com/kkiyama117/gaussian_job_shared) (D2) · [slurm_async_runner](https://github.com/kkiyama117/slurm-async-runner) (A1)
