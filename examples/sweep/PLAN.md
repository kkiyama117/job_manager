# examples/sweep — design plan

Reviewed before implementation. After the example ships, this file
stays as the audit trail.

## Goal

A second example that goes beyond `examples/simple`'s linear 2-job
flow. Demonstrates four things `simple/` does not exercise:

| Capability | How shown |
|---|---|
| Sweep expansion (`build_job_id` with axis_combo) | 3 × `opt__compound=N` / `freq__compound=N` |
| Fan-out + parallel branches | 1 root → 3 opt → 3 freq |
| `plan.toml` params surfacing into the body | body reads `JM_PARAM_COMPOUND_NAME` / `JM_PARAM_ROUTE` |
| `Skipped` lifecycle propagation | variant where `opt__compound=1` exits 1 → `freq__compound=1` becomes Skipped, other branches still Succeed |
| Python authoring of inputs | `author.py` regenerates inputs from a config dict |

## DAG

```
                              prep
                  ┌─────────────┼─────────────┐
                  │             │             │
                  ▼             ▼             ▼
        opt__compound=0  opt__compound=1  opt__compound=2
                  │             │             │
                  ▼             ▼             ▼
        freq__compound=0 freq__compound=1 freq__compound=2
```

- 7 nodes, 6 edges (3 fan-out from `prep`, 3 linear `opt→freq`)
- Every edge is `afterok`
- Sweep axis: `compound ∈ {0=benzene, 1=toluene, 2=p-xylene}`
- JobId rules from `validate_job_id`: `prep`, `opt__compound=0`,
  `freq__compound=0`, etc. — all pass

## File layout

```
examples/sweep/
├── PLAN.md                                            ← this file (kept post-ship as audit trail)
├── README.md                                          ← DAG diagram, run instructions, lifecycle expectations
├── author.py                                          ← Python regenerator for inputs/ (success variant only)
│
├── inputs/                                            ← success variant
│   ├── common.toml
│   └── 0199999a-0000-7000-8000-000000000000/
│       ├── flow.toml                                  ← 7 jobs, 6 edges
│       └── plan.toml                                  ← 7 entries, per-compound params
│
├── inputs-fail/                                       ← failure variant (separate root for clean docs)
│   ├── common.toml                                    ← identical to inputs/common.toml
│   └── 0199999a-0000-7000-8000-000000000001/          ← different uuid
│       ├── flow.toml                                  ← only difference: opt__compound=1's body has `exit 1`
│       └── plan.toml                                  ← identical content modulo uuid-tied references
│
├── outputs/                                           ← success snapshot (we commit dry-run batch.bash; user fills .status.toml + slurm-*.out from a real run)
│   └── 0199999a-0000-7000-8000-000000000000/
│       ├── prep/batch.bash
│       ├── opt__compound=0/batch.bash
│       ├── opt__compound=1/batch.bash
│       ├── opt__compound=2/batch.bash
│       ├── freq__compound=0/batch.bash
│       ├── freq__compound=1/batch.bash
│       └── freq__compound=2/batch.bash
│
└── outputs-fail/                                      ← failure snapshot (same caveat)
    └── 0199999a-0000-7000-8000-000000000001/
        ├── prep/batch.bash
        ├── opt__compound={0,1,2}/batch.bash
        └── freq__compound={0,1,2}/batch.bash
```

Two separate roots (rather than one root with two flows) keep
"success path" and "failure path" cleanly separable in docs and in
the `--root` flag the user passes.

## Body shape (per job)

```bash
echo "[$JM_JOB_ID] flow=$JM_FLOW_UUID compound=$JM_PARAM_COMPOUND_NAME route=$JM_PARAM_ROUTE"
sleep 2
```

For the failure variant's `opt__compound=1` only:

```bash
echo "[$JM_JOB_ID] simulated failure: $JM_PARAM_COMPOUND_NAME"
exit 1
```

Why `exit 1` in the body (not a real workload failure): cluster-agnostic.
A real workload would need real software to crash and is brittle to
SLURM versions.

## plan.toml params

| JobId | compound_name | route |
|---|---|---|
| `prep` | (empty) | `bootstrap` |
| `opt__compound=0` | `benzene` | `B3LYP/6-31G* opt` |
| `opt__compound=1` | `toluene` | `B3LYP/6-31G* opt` |
| `opt__compound=2` | `p-xylene` | `B3LYP/6-31G* opt` |
| `freq__compound=0` | `benzene` | `B3LYP/6-31G* freq` |
| `freq__compound=1` | `toluene` | `B3LYP/6-31G* freq` |
| `freq__compound=2` | `p-xylene` | `B3LYP/6-31G* freq` |

(The route strings are demo text — `body` is still `echo`, not g16.)

## common.toml

Same shape as `examples/simple/inputs/common.toml`: two `REPLACE_ME`
sentinels (`partition`, `project_root`), `time_limit = "00:10:00"`,
`job_name = "jm-sweep"`. log_stdout/log_stderr template stays
commented-out so SLURM captures land next to batch.bash when the
operator enables them.

## author.py

```python
# Regenerate examples/sweep/inputs/ from a config dict. Run from the
# repo root after `uv sync && uv run maturin develop`:
#
#   uv run python examples/sweep/author.py
#
# Idempotent — overwrites flow.toml and plan.toml under the committed
# uuid. Common.toml is NOT regenerated (it has cluster-specific values
# you don't want clobbered).

import job_manager
from job_manager import ExperimentPlan, PathResolver, build_job_id, write_plan
```

Constraints:
- Only depends on `job_manager` (already installed via `maturin
  develop`). No `tomli_w` / `toml` python package.
- `write_flow` takes a TOML string — script hand-builds that string
  via f-string templating. Acceptable because flow.toml schema is
  small (`uuid`, `created_at`, `[jobs.X]` + `[jobs.X.config]` +
  optional `[[jobs.X.parents]]`).
- Verifies the output by reading back via `read_flow` / `read_plan`
  and asserting jobcount + ID set match.

The `inputs-fail/` variant is **not** generated by author.py — it's a
hand-edited copy of `inputs/` with one body changed and one uuid
bumped. Keeping it manual avoids complicating author.py with branch
logic for a single-line variant.

## Expected `.status.toml` outcomes

### inputs/ (success variant)

All 7 jobs reach `lifecycle = "success"`:

```toml
lifecycle = "success"
slurm_jobid = ...
[slurm_status]
state = "COMPLETED"
```

### inputs-fail/ (failure variant)

| JobId | lifecycle | Notes |
|---|---|---|
| `prep` | `success` | runs unconditionally |
| `opt__compound=0` | `success` | independent branch |
| `opt__compound=1` | `failed` | body `exit 1` |
| `opt__compound=2` | `success` | independent branch |
| `freq__compound=0` | `success` | parent succeeded |
| `freq__compound=1` | `skipped` | `decide_transition` emits `SkipDueToParent { parent: "opt__compound=1" }`. **The culprit JobId stays in the in-memory `Decision`, not on disk** — `.status.toml` shows only `lifecycle = "skipped"` plus whatever `note` was already there (typically empty). `slurm-<jobid>.out` is also unlikely to exist because SLURM's own `afterok` cancels the child server-side before it ever runs |
| `freq__compound=2` | `success` | parent succeeded |

This demonstrates partial completion — failure of one branch does
not cascade to sibling branches.

## Implementation order

1. Hand-author `examples/sweep/inputs/{common.toml, <uuid>/flow.toml, <uuid>/plan.toml}`.
2. `./target/debug/jm --root examples/sweep/inputs submit <uuid> --dry-run` → verify parse + capture 7 batch.bash. Move them to `examples/sweep/outputs/`.
3. Copy `inputs/ → inputs-fail/`, change uuid + opt__compound=1 body, re-dry-run, move batch.bash to `outputs-fail/`.
4. Write `author.py`, run it, diff against the committed inputs/ — script must reproduce them.
5. Write `README.md`.
6. Update `.gitignore` if needed (probably no change — already covers `slurm-*.out`).
7. Commit + push.
8. (Out of scope for this commit, separate user task): user runs both variants in their cluster, captures `.status.toml` + `slurm-*.out` into `outputs/` and `outputs-fail/`, commits a follow-up.

## Risks / open questions

- **Q1 (RESOLVED).** `decide_transition` returns
  `Decision::SkipDueToParent { parent: JobId }` carrying the culprit
  (`src/runner/transition.rs:38-44`). `FlowRunner::tick`
  (`src/runner/flow.rs:316,322-330`) maps that decision to
  `Lifecycle::Skipped` and writes a fresh `JobRun`, but **does not
  populate `note`** — the field is `run.note.clone()`, preserving
  whatever was there before (`None`/empty by default). So on disk the
  Skipped child's `.status.toml` shows nothing about the culprit. The
  parent JobId is only visible to in-process callers of `tick()` who
  inspect the returned `TickResult.transitions`.

  Implication for the example: README must NOT claim the
  `.status.toml` `note` field tells you which parent caused the skip.
  If we want that on disk we'd need a code change (separate work).

- **Q2 (RESOLVED).** `decide_transition` short-circuits on parent
  lifecycle BEFORE consulting the SLURM query
  (`src/runner/transition.rs:36-44`), and `tick` walks jobs in
  topological order, so the child sees its parent's freshly-updated
  `Failed` lifecycle inside the same `tick` pass and emits
  `SkipDueToParent` immediately. SLURM's own `afterok` cancels the
  child server-side once the parent exits non-zero, typically before
  any compute node allocates the child — so **`slurm-<jobid>.out`
  usually does not exist for a Skipped child**. The
  `outputs-fail/<uuid>/freq__compound=1/` snapshot will therefore have
  `batch.bash` (rendered at submit) and `.status.toml`
  (lifecycle=`skipped`), but no `slurm-*.out` companion. README will
  document this.

- **Q3.** Uuid spelling: `0199999a-0000-7000-8000-00000000000{0,1}`
  (one hex digit higher than simple's `01999999`). Both clearly
  synthetic.

## Non-goals

- 2+ axes (`compound × method`) — single axis keeps the example
  readable; multi-axis is a future `examples/sweep-2d/` if needed.
- Tags + cross-flow search (`SearchFilter`, `walk_flows`) — those need
  a `--root` with multiple flows; defer to a third example.
- D2 Python API for JobFlow construction — using `job_manager` alone
  keeps the example's Python deps minimal.
- Per-job overriding of `time_limit` / `resource_spec` — the simple
  example already covers per-job `[jobs.X.config]`; here we keep all
  per-job configs identical to focus attention on the DAG shape.
