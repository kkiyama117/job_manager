#!/usr/bin/env bash
# Regenerate examples/sweep/outputs/ and examples/sweep/outputs-fail/
# snapshots from the matching inputs/ trees.
#
# Why this script exists:
#   `outputs/<uuid>/.jm/` is a committed snapshot of what `jm render`
#   produces for the example. After any edit to flow.toml / plan.toml /
#   common.toml under inputs/, the snapshot under outputs/ must be
#   regenerated so the two stay in lock-step. Reviewers (and CI) check
#   `git diff` against a fresh run.
#
# Where this runs:
#   Anywhere — the script cd's to the git repo root first, so it works
#   from dev clones, from the SLURM login node where you authored
#   inputs/, or from CI. `--root` is given a path *relative to repo
#   root* so the `std::fs::canonicalize` jm runs at startup
#   (src/bin/jm.rs:resolve_root) lands on the right path regardless of
#   symlink layout.
#
# Idempotence:
#   With no inputs/ change, `git diff` after running is empty.
#   Any diff means either flow.toml changed, the renderer changed, or
#   the SbatchCmd serializer reordered keys — investigate before
#   committing.
#
# Outputs:
#   examples/sweep/outputs/<UUID_OK>/.jm/
#       flow.effective.toml
#       <JobId>/batch.bash   × 7
#   examples/sweep/outputs-fail/<UUID_FAIL>/.jm/
#       flow.effective.toml
#       <JobId>/batch.bash   × 7
#
# `status.toml` and `slurm-*.out/err` are NOT generated — those require
# a real SLURM run. Commit them separately if you've actually executed
# the example on a cluster.

set -euo pipefail

UUID_OK=0199999a-0000-7000-8000-000000000000
UUID_FAIL=0199999a-0000-7000-8000-000000000001

REPO_ROOT=$(cd "$(git rev-parse --show-toplevel)" && pwd -P)
cd "$REPO_ROOT"

JM=./target/debug/jm
if [[ ! -x "$JM" ]]; then
    echo "Building jm (no-default-features → no libpython linkage)…"
    cargo build --bin jm --no-default-features
fi

# Step 0: wipe any pre-existing snapshot under outputs/. The committed
# snapshot is overwritten in-place by the render below — `git diff` then
# tells you whether anything actually changed.
rm -rf "examples/sweep/outputs/$UUID_OK/.jm" \
       "examples/sweep/outputs-fail/$UUID_FAIL/.jm"

# `jm` writes everything under <--root>/<uuid>/.jm/, and `--root` must
# contain `common.toml` + `<uuid>/flow.toml` + `<uuid>/plan.toml`. We
# don't want to render INTO inputs/ (that would leak `.jm/` artifacts
# next to user-authored TOMLs — exactly the c5d6efc "fucking mistake"
# this script was written to prevent). Instead we stage the three input
# files inside outputs/, render there, then sweep the stage away — the
# only thing left behind is `.jm/`.
render_into() {
    local IN=$1 OUT=$2 UUID=$3
    mkdir -p "$OUT/$UUID"
    cp "$IN/common.toml"      "$OUT/common.toml"
    cp "$IN/$UUID/flow.toml"  "$OUT/$UUID/flow.toml"
    cp "$IN/$UUID/plan.toml"  "$OUT/$UUID/plan.toml"

    "$JM" --root "$OUT" render "$UUID"

    # Loud failure if jm exit=0 but wrote nothing visible at the
    # expected path. Catches binary/source skew (stale debug build,
    # wrong --root after canonicalize on an unfamiliar filesystem, …).
    if [[ ! -d "$OUT/$UUID/.jm" ]]; then
        echo "FATAL: jm reported success but $OUT/$UUID/.jm does not exist."
        echo "Listing $OUT/$UUID/ to help diagnose:"
        ls -la "$OUT/$UUID/"
        exit 1
    fi

    rm "$OUT/common.toml" \
       "$OUT/$UUID/flow.toml" \
       "$OUT/$UUID/plan.toml"
}

render_into examples/sweep/inputs       examples/sweep/outputs       "$UUID_OK"
render_into examples/sweep/inputs-fail  examples/sweep/outputs-fail  "$UUID_FAIL"

echo
echo "Regenerated:"
find "examples/sweep/outputs/$UUID_OK/.jm" \
     "examples/sweep/outputs-fail/$UUID_FAIL/.jm" \
     -type f | sort

echo
echo "Sanity check:"
echo "    git diff --stat examples/sweep/outputs examples/sweep/outputs-fail"
echo
echo "Expect 0 changed lines if nothing in inputs/ changed since the last"
echo "committed snapshot. Any diff is a real regression — investigate"
echo "before staging."
