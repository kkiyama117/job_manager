# examples/sweep — sweep + fan-out + Skipped propagation

A second example that goes beyond [`examples/simple`](../simple/)'s
linear 2-job flow. Demonstrates four things `simple/` does not:

| Capability | How shown |
|---|---|
| Sweep expansion (`build_job_id` with axis_combo) | `opt__compound={0,1,2}` and `freq__compound={0,1,2}` |
| Fan-out + parallel branches | one `prep` → 3 `opt` → 3 `freq` |
| `plan.toml` params surfacing into the body | body reads `JM_PARAM_COMPOUND_NAME` / `JM_PARAM_ROUTE` |
| `Skipped` lifecycle propagation | `inputs-fail/` variant: `opt__compound=1` exits 1 → `freq__compound=1` becomes `Skipped`; other branches still succeed |
| Python authoring of inputs | [`author.py`](./author.py) regenerates `inputs/` byte-for-byte from a config dict |

## DAG

```
                              prep
                  ┌─────────────┼─────────────┐
                  ▼             ▼             ▼
        opt__compound=0  opt__compound=1  opt__compound=2
                  │             │             │
                  ▼             ▼             ▼
        freq__compound=0 freq__compound=1 freq__compound=2
```

- 7 nodes, 6 `afterok` edges
- Sweep axis: `compound ∈ {0=benzene, 1=toluene, 2=p-xylene}`
- All bodies are `echo` + `sleep 2` — cluster-agnostic, no Gaussian needed

## Layout

```
examples/sweep/
├── README.md                                       ← this file
├── PLAN.md                                         ← design audit trail
├── author.py                                       ← regenerates inputs/ byte-for-byte
│
├── inputs/                                         ← success variant (you author / commit)
│   ├── common.toml                                 ← root-level SLURM defaults (REPLACE_ME)
│   └── 0199999a-0000-7000-8000-000000000000/
│       ├── flow.toml                               ← 7 jobs, 6 afterok edges
│       └── plan.toml                               ← per-JobId compound_name + route
│
├── inputs-fail/                                    ← failure variant — single-line edit of inputs/
│   ├── common.toml                                 ← identical to inputs/common.toml
│   └── 0199999a-0000-7000-8000-000000000001/      ← different uuid (...001 vs ...000)
│       ├── flow.toml                               ← `opt__compound=1`'s body is `exit 1`
│       └── plan.toml                               ← identical to inputs/.../plan.toml modulo uuid-tied refs
│
├── outputs/                                        ← success variant snapshot (batch.bash only — fill in status.toml + slurm-*.out from a real run)
│   └── 0199999a-0000-7000-8000-000000000000/.jm/<JobId>/batch.bash   × 7
│
└── outputs-fail/                                   ← failure variant snapshot
    └── 0199999a-0000-7000-8000-000000000001/.jm/<JobId>/batch.bash   × 7
```

`.jm/` is the program-managed subdir job-manager writes under each
`<flow_uuid>/`. After F2 (PR #17) the per-job artifacts (`batch.bash`,
`status.toml`, `slurm-*.out/err`) live under `.jm/<JobId>/` rather
than directly under `<flow_uuid>/<JobId>/`.

Two separate roots (rather than one root with two flows) keep
"success path" and "failure path" cleanly separable — `--root` points
at one variant at a time.

## What's committed in `outputs/` and `outputs-fail/`

Only the rendered `batch.bash` files (one per JobId, 7 each). They
come from `jm submit --dry-run` against the matching `inputs/` /
`inputs-fail/` tree. The renderer is deterministic, so a dry-run on
any host produces byte-identical scripts.

`status.toml` and `slurm-*.out` are **not** committed yet — those
need a real cluster run. See "Run on real SLURM" below; the user
typically commits a snapshot of both as a follow-up after running.

## Regenerate inputs/ via `author.py`

[`author.py`](./author.py) regenerates `inputs/<uuid>/{flow.toml,
plan.toml}` from a config dict (`COMPOUNDS`, `ROUTE_OPT`,
`ROUTE_FREQ`). Output is **byte-identical** to the committed files —
edit the config, re-run the script, commit.

```bash
uv sync
uv run maturin develop          # one-time
uv run python examples/sweep/author.py
# wrote examples/sweep/inputs/0199999a-0000-7000-8000-000000000000/flow.toml
# wrote examples/sweep/inputs/0199999a-0000-7000-8000-000000000000/plan.toml
```

`common.toml` is **not** regenerated (it carries cluster-specific
sentinels you don't want clobbered). `inputs-fail/` is **not**
regenerated either — it's a one-line hand edit of `inputs/`, kept
manual so the script stays branch-free.

The script writes via `Path.write_text` (not `write_flow` /
`write_plan`) so that header comments and the hand-aligned
formatting survive — Rust's TOML serializer would strip both. It
still runs `read_flow` / `read_plan` round-trip to catch any TOML
syntax error.

## Regenerate `outputs/` from `inputs/`

After editing anything under `inputs/` or `inputs-fail/` — or after a
renderer change in the crate — the committed snapshot under
`outputs/` / `outputs-fail/` must be regenerated so the two stay in
lock-step. Use the script:

```bash
bash examples/sweep/regenerate_outputs.sh
git diff --stat examples/sweep/outputs examples/sweep/outputs-fail
```

What the script does (read it — it's ~30 lines of bash):

1. `cd`s to the git repo root (so it works from anywhere)
2. Builds `jm` with `--no-default-features` if `./target/debug/jm` is
   missing
3. Wipes `outputs/<UUID>/.jm/` and `outputs-fail/<UUID>/.jm/`
4. **Stages** `common.toml` + `flow.toml` + `plan.toml` *into*
   `outputs/<UUID>/`, runs `jm render` against `--root
   examples/sweep/outputs`, then deletes the stage — leaving only
   `.jm/` behind. This avoids rendering INTO `inputs/`, which is what
   produced the c5d6efc "fucking mistake" commit (auto-generated
   artifacts leaking next to user-authored TOMLs). `inputs/<UUID>/`
   carries a per-flow `.gitignore` containing `.jm/` to keep any
   accidental in-tree render from staining git status either.
5. Asserts `outputs/<UUID>/.jm/` actually exists post-render (loud
   failure if `jm` exit=0 but wrote nothing visible — catches stale
   binaries and `--root` canonicalization surprises on unfamiliar
   filesystems)

The script is **idempotent**: with no `inputs/` change, `git diff` is
empty. Any diff means something real changed — review before staging.

`status.toml` and `slurm-*.out/err` are not produced (real SLURM
needed). Commit those separately after an actual cluster run.

A rendered `opt__compound=0/batch.bash` looks like:

```bash
#!/bin/bash
# Generated by job_manager SP-3. Do not edit; regenerated on every `jm run`.

# --- job-manager runtime context ---
export JM_FLOW_UUID='0199999a-0000-7000-8000-000000000000'
export JM_JOB_ID='opt__compound=0'
export JM_AXIS_COMPOUND='0'

# --- plan.toml params ---
export JM_PARAM_COMPOUND_NAME='benzene'
export JM_PARAM_ROUTE='B3LYP/6-31G* opt'

# --- user body (JobSpec.body) ---
echo "[$JM_JOB_ID] flow=$JM_FLOW_UUID compound=$JM_PARAM_COMPOUND_NAME route=$JM_PARAM_ROUTE"
sleep 2
```

`JM_AXIS_COMPOUND` is auto-derived from the JobId by
`render_batch_bash`; `prep` (no `__compound=N` suffix) does not get
an axis export. `JM_PARAM_*` come from `plan.toml`.

## Run on real SLURM

Same procedure as `examples/simple`. After F2 (PR #17) `flow.toml` no
longer carries per-job `partition` — the value flows in from
`common.toml [slurm_default].partition` at `read_flow` time via
TOML preparse. The only `REPLACE_ME` sentinels you need to rewrite
are in `common.toml` (the `partition` field and `project_root`).
See [`examples/simple/README.md`](../simple/README.md#run-on-real-slurm)
for the full step-by-step (the only differences here are: `UUID` is
`0199999a-...000` for the success variant or `...001` for the
failure variant; everything else, including the one-line sed, is
identical).

The sed steps adapt directly:

```bash
PART=<your_partition>          # e.g. regular / debug / gr10641a / ...
ROOT=/path/to/your/scratch/jm-sweep-demo

# Stage one variant at a time:
UUID=0199999a-0000-7000-8000-000000000000      # success variant
# UUID=0199999a-0000-7000-8000-000000000001    # failure variant — point ROOT at examples/sweep/inputs-fail instead
mkdir -p "$ROOT/$UUID"
cp examples/sweep/inputs/common.toml          "$ROOT/common.toml"
cp examples/sweep/inputs/$UUID/flow.toml      "$ROOT/$UUID/flow.toml"
cp examples/sweep/inputs/$UUID/plan.toml      "$ROOT/$UUID/plan.toml"

sed -i "s|^partition[[:space:]]*=.*|partition = \"$PART\"|" \
    "$ROOT/common.toml"
sed -i "s|^project_root[[:space:]]*=.*|project_root = \"$ROOT/scratch\"|" \
    "$ROOT/common.toml"

! grep -rn REPLACE_ME "$ROOT" || { echo "ERROR: REPLACE_ME left over"; exit 1; }

./target/debug/jm --root "$ROOT" submit "$UUID"
./target/debug/jm --root "$ROOT" tick "$UUID"        # poll until terminal
./target/debug/jm --root "$ROOT" show "$UUID"
```

## Expected lifecycles

### Success variant (`inputs/`)

All 7 jobs reach `lifecycle = "success"`:

```toml
lifecycle   = "success"
slurm_jobid = ...

[slurm_status]
state = "COMPLETED"
```

### Failure variant (`inputs-fail/`)

The `exit 1` in `opt__compound=1` triggers SLURM's `afterok`
short-circuit on the matching `freq__compound=1`; the other two
branches finish normally:

| JobId | lifecycle | Notes |
|---|---|---|
| `prep` | `success` | unconditional root |
| `opt__compound=0` | `success` | independent branch |
| `opt__compound=1` | `failed` | body `exit 1` |
| `opt__compound=2` | `success` | independent branch |
| `freq__compound=0` | `success` | parent succeeded |
| `freq__compound=1` | `skipped` | upstream failure, see notes below |
| `freq__compound=2` | `success` | parent succeeded |

#### Two non-obvious things about `Skipped`

**`status.toml` does not record which parent caused the skip.**
`decide_transition` (`src/runner/transition.rs`) emits
`Decision::SkipDueToParent { parent: JobId }`, but `FlowRunner::tick`
maps that to `Lifecycle::Skipped` and writes a fresh `JobRun`
whose `note` field preserves whatever was there before (typically
empty). The culprit JobId is only visible to in-process callers of
`tick()` who inspect the returned `TickResult.transitions`. If you
need the culprit persisted on disk, that's a code change — not a
config knob on this example.

**`slurm-<jobid>.out` typically does not exist for a Skipped
child.** `tick` walks jobs in topological order and
`decide_transition` short-circuits on parent lifecycle before
consulting the SLURM query, so the Skipped child's `Lifecycle`
flips inside the same `tick` pass that recorded the parent's
`Failed`. At the same time, SLURM's own `afterok` cancels the child
server-side once the parent exits non-zero — usually before any
compute node allocates the child — so there's no stdout file to
capture. `outputs-fail/<uuid>/freq__compound=1/` will have
`batch.bash` (rendered at submit time) and `status.toml`
(`lifecycle = "skipped"`), but no `slurm-*.out`.

## Common errors

| Symptom | Cause | Fix |
|---|---|---|
| `sbatch: error: invalid partition specified: REPLACE_ME` | sed step skipped, or only `common.toml` rewritten — every per-job `partition` overrides it when non-empty | re-run both `sed` commands against `$ROOT/common.toml` and `$ROOT/$UUID/flow.toml`; sanity-check with `! grep -rn REPLACE_ME "$ROOT"` |
| `error while loading shared libraries: libpython3.13.so.1.0` | built `jm` with default features | rebuild with `--no-default-features` |
| `Error: ... missing field 'partition'` | edited `common.toml` and deleted the line | `partition` is required by `SlurmJobConfig`; restore it |
| `freq__compound=1` shows `lifecycle = "failed"` instead of `"skipped"` | `tick` had not seen the parent's `Failed` yet when the child was checked | run `tick` again — the next pass will pick up the parent transition and emit `Skipped` |

See [`examples/simple/README.md`](../simple/README.md#common-errors) for
more.

## Related

- [`docs/architecture.md`](../../docs/architecture.md) — lifecycle authority split, `decide_transition`, `tick` ordering
- [`PLAN.md`](./PLAN.md) — design audit trail for this example
- [`examples/simple/`](../simple/) — minimal 2-step prerequisite reading
