# examples/simple — minimal 2-step flow

A self-contained smoke example: a 2-job DAG (`opt` → `freq`, `afterok`)
with `echo` bodies. Runnable on any SLURM cluster, no Gaussian / no
real workload required — `freq` only fires if `opt` exits 0, which is
enough to exercise the full **render → submit → tick → terminal** path
end-to-end.

## Layout

```
examples/simple/
├── README.md                                           ← this file
├── inputs/                                             ← what you author / commit (pristine, REPLACE_ME sentinels)
│   ├── common.toml                                     ← root-level SLURM defaults
│   └── 01999999-0000-7000-8000-000000000000/
│       ├── flow.toml                                   ← JobFlow DAG (2 jobs + 1 edge)
│       └── plan.toml                                   ← per-JobId render params
└── outputs/                                            ← what `jm` + SLURM produced on a real run
    └── 01999999-0000-7000-8000-000000000000/
        ├── opt/
        │   ├── batch.bash                              ← rendered by `jm submit`
        │   ├── .status.toml                            ← lifecycle = "success"
        │   └── slurm-7543027.out                       ← SLURM stdout capture
        └── freq/
            ├── batch.bash
            ├── .status.toml                            ← lifecycle = "success"
            └── slurm-7543028.out
```

> `inputs/` and `outputs/` are split here for clarity — in production
> they share the same `<root>` directory. `jm` writes `batch.bash`,
> `.status.toml`, and (via SLURM) `slurm-<id>.out` / `.err` into
> `<root>/<flow_uuid>/<JobId>/`.

## What's committed in `outputs/`

A complete end-to-end snapshot from a real run on a KUDPC partition.
Both jobs reached `lifecycle = "success"` / SLURM `state = "COMPLETED"`. The
`batch.bash` files match what `jm submit --dry-run` produces
byte-for-byte against the same inputs (the renderer is
deterministic), so you can reproduce the dry-run version locally
without SLURM and they should be identical modulo nothing.

The committed snapshot is here as a reference for what a clean
`submit → tick → terminal` cycle looks like, including the
`[slurm_status]` table inside `.status.toml`. Don't expect the
`slurm_jobid` numbers to match your run.

## Reproduce the dry-run locally

No SLURM needed. From the repo root:

```bash
# 1. Build the CLI without pyo3 (one-time)
#    The default feature set links `jm` against libpython3.13 for the
#    sibling `stub_gen` binary. `jm` itself doesn't call Python, so
#    `--no-default-features` produces a self-contained binary that runs
#    on hosts (login / compute nodes) without libpython installed.
cargo build --bin jm --no-default-features

# 2. Run a dry-run against the committed inputs/
./target/debug/jm \
    --root examples/simple/inputs \
    submit 01999999-0000-7000-8000-000000000000 --dry-run
# → "submitted 0 jobs"   (DryRunExecutor returns an empty result map)

# 3. Compare what dropped into inputs/ against what's in outputs/
diff -r \
    <(find examples/simple/inputs/01999999-0000-7000-8000-000000000000 -name batch.bash | sort | xargs cat) \
    <(find examples/simple/outputs/01999999-0000-7000-8000-000000000000 -name batch.bash | sort | xargs cat)
```

`--dry-run` writes `batch.bash` into the **inputs** tree (because
`PathResolver` keys everything off `--root`). After verifying, move
the rendered scripts to `outputs/` or clean them up:

```bash
find examples/simple/inputs -name batch.bash -delete
find examples/simple/inputs -type d -empty -delete
```

## Run on real SLURM

The committed inputs ship with `REPLACE_ME` sentinels — they exist so
`sbatch` fails fast with `invalid partition specified: REPLACE_ME` if
you forget this step, rather than silently submitting under a
partition that happens to be named the same as the placeholder.
**You must override all three before `jm submit` will reach SLURM:**

| File | Key | Set to |
|---|---|---|
| `inputs/common.toml` | `[slurm_default].partition` | a real partition on your cluster (run `sinfo -s` on a login node to list) |
| `inputs/common.toml` | `[directories].project_root` | an absolute path you can write to (scratch / lustre) |
| `inputs/<uuid>/flow.toml` | `[jobs.opt.config].partition` and `[jobs.freq.config].partition` | same partition as above |

The per-job partition in `flow.toml` exists so individual jobs in a
DAG can override the cluster-wide default — `merge_with_defaults`
takes the per-job value over `common.toml` whenever it's non-empty.
For this example we just want all three pointing at the same
partition, so the sed step below rewrites every `partition = "REPLACE_ME"`
line uniformly.

(`echo` + `sleep 2` itself works on any cluster — no Gaussian, no
special modules. The sentinels are about cluster-side config, not
the workload.)

You may also want to point logs somewhere persistent:

```toml
[slurm_default]
log_stdout = "/path/to/logs/%x.%j.out"
log_stderr = "/path/to/logs/%x.%j.err"
```

### Step-by-step

```bash
# 0. Build the CLI without pyo3 so it doesn't need libpython3.13 at
#    runtime — login nodes and most compute nodes don't ship a CPython
#    shared library. If you forget `--no-default-features`, you'll see:
#       error while loading shared libraries: libpython3.13.so.1.0: cannot open shared object file
cargo build --release --bin jm --no-default-features
JM=./target/release/jm

# Pick a working root — anywhere your cluster can sbatch from
ROOT=/path/to/your/scratch/jm-simple-demo
UUID=01999999-0000-7000-8000-000000000000

# 1. Stage the inputs under <ROOT>/  — copy first, then edit the copy
#    so the committed example stays clean.
mkdir -p "$ROOT/$UUID"
cp examples/simple/inputs/common.toml          "$ROOT/common.toml"
cp examples/simple/inputs/$UUID/flow.toml      "$ROOT/$UUID/flow.toml"
cp examples/simple/inputs/$UUID/plan.toml      "$ROOT/$UUID/plan.toml"

# 2. Replace the REPLACE_ME sentinels with values for your cluster.
#    Get a partition name from `sinfo -s` on the login node.
PART=<your_partition>          # e.g. regular / debug / gr10641a / ...
sed -i "s|^partition[[:space:]]*=.*|partition = \"$PART\"|" \
    "$ROOT/common.toml" "$ROOT/$UUID/flow.toml"
sed -i "s|^project_root[[:space:]]*=.*|project_root = \"$ROOT/scratch\"|" \
    "$ROOT/common.toml"

# sanity-check: REPLACE_ME must not appear anywhere under $ROOT
! grep -rn REPLACE_ME "$ROOT" || { echo "ERROR: REPLACE_ME left over"; exit 1; }

# 3. (Optional) Render only — sanity-check the generated batch.bash
"$JM" --root "$ROOT" render "$UUID"
cat "$ROOT/$UUID/opt/batch.bash"

# 4. Submit for real — calls sbatch, writes .status.toml (lifecycle=Queued)
"$JM" --root "$ROOT" submit "$UUID"
# → prints { "freq": <slurm_jobid>, "opt": <slurm_jobid> }

# 5. Tick — query SLURM and transition lifecycles. Run in a loop or via cron.
"$JM" --root "$ROOT" tick "$UUID"

# 6. Inspect
"$JM" --root "$ROOT" show "$UUID"
```

### Common errors

| Symptom | Cause | Fix |
|---|---|---|
| `sbatch: error: invalid partition specified: REPLACE_ME` | step 2 was skipped, or only `common.toml` was rewritten — `flow.toml`'s per-job `partition` overrides it when non-empty | re-run the two `sed` commands in step 2 against both `$ROOT/common.toml` and `$ROOT/$UUID/flow.toml`, then `! grep -rn REPLACE_ME "$ROOT"` |
| `error while loading shared libraries: libpython3.13.so.1.0` | built `jm` with default features | rebuild with `--no-default-features` (step 0) |
| `Error: ... missing field 'partition'` | edited `common.toml` and deleted the line | `partition` is required by `SlurmJobConfig`; restore it |
| `slurm-<id>.out` lands in the directory you ran `cargo run` / `jm submit` from, not under the job dir | `log_stdout` / `log_stderr` unset in `common.toml` | uncomment the `log_stdout` / `log_stderr` template lines in `common.toml` and point them under your `project_root` |

`tick` is idempotent and only mutates non-terminal `.status.toml`
entries. A minimal cron entry:

```cron
*/1 * * * * /path/to/jm --root /path/to/your/scratch/jm-simple-demo tick 01999999-0000-7000-8000-000000000000
```

Both jobs should reach `Success` within ~10 seconds of SLURM picking
them up (echo + 2-second sleep).

### Update the committed `outputs/` (if you want to refresh the snapshot)

The committed `outputs/` already carries one real-run snapshot — you
only need to refresh it if you change the inputs (DAG shape, body,
plan params). The renderer is deterministic, so a different cluster
will produce identical `batch.bash` files; only `slurm_jobid`s in
`.status.toml` and the SLURM stdout content will differ.

```bash
# Refresh batch.bash (deterministic — should diff cleanly except for
# the generator header timestamp).
cp "$ROOT/$UUID/opt/batch.bash"      examples/simple/outputs/$UUID/opt/batch.bash
cp "$ROOT/$UUID/freq/batch.bash"     examples/simple/outputs/$UUID/freq/batch.bash

# Refresh .status.toml — `slurm_jobid` will be your run's, `slurm_status`
# state should still read "COMPLETED" on a clean run.
cp "$ROOT/$UUID/opt/.status.toml"    examples/simple/outputs/$UUID/opt/.status.toml
cp "$ROOT/$UUID/freq/.status.toml"   examples/simple/outputs/$UUID/freq/.status.toml

# Refresh SLURM stdout/stderr (if `log_stdout`/`log_stderr` are set in
# common.toml — otherwise they land in the directory you ran `jm
# submit` from). Delete the old captures first so we don't accumulate
# job ids from past runs.
rm -f examples/simple/outputs/$UUID/opt/slurm-*.{out,err}
rm -f examples/simple/outputs/$UUID/freq/slurm-*.{out,err}
cp "$ROOT/$UUID"/opt/slurm-*.{out,err}    examples/simple/outputs/$UUID/opt/  2>/dev/null || true
cp "$ROOT/$UUID"/freq/slurm-*.{out,err}   examples/simple/outputs/$UUID/freq/ 2>/dev/null || true

# Scrub absolute log paths / mail addresses / user names from the
# captured files before committing — see §Hygiene below.
```

### Hygiene before committing real outputs

- Replace your username / lustre path in `slurm-<id>.out` headers with
  a placeholder.
- Don't commit `mail_user` values if you set them in `common.toml`
  before the run.
- Real `slurm_jobid` values are fine to commit (they're meaningless
  outside the cluster). The `slurm_status.state` / `slurm_status.reason`
  pair on a clean run will be `COMPLETED` / empty.

## Expected `.status.toml` shape (for reference)

After a successful end-to-end run, each `outputs/<uuid>/<jid>/.status.toml`
will look approximately like:

```toml
lifecycle   = "success"
updated_at  = "2026-05-15T10:00:42Z"
slurm_jobid = 12345678

[slurm_status]
state = "COMPLETED"
```

A `Skipped` propagation (if `opt` had exited non-zero) would instead show:

```toml
lifecycle  = "skipped"
updated_at = "2026-05-15T10:00:30Z"
note       = "upstream_failed: opt"
```

See [`docs/architecture.md`](../../docs/architecture.md#statustoml-schema)
for the full schema and lifecycle authority split.
