#!/usr/bin/env bash
  set -euo pipefail

  UUID_OK=0199999a-0000-7000-8000-000000000000
  UUID_FAIL=0199999a-0000-7000-8000-000000000001

  cd "$(git rev-parse --show-toplevel)"
  JM=./target/debug/jm
  cargo build --bin jm --no-default-features

  # 既存 outputs/.jm を捨てる
  rm -rf examples/sweep/outputs/$UUID_OK/.jm \
         examples/sweep/outputs-fail/$UUID_FAIL/.jm

  render_into() {
      # $1: input root (inputs / inputs-fail)
      # $2: output root (outputs / outputs-fail)
      # $3: uuid
      local IN=$1 OUT=$2 UUID=$3
      # 1. outputs/ に flow/plan/common を stage
      mkdir -p "$OUT/$UUID"
      cp "$IN/common.toml"        "$OUT/common.toml"
      cp "$IN/$UUID/flow.toml"    "$OUT/$UUID/flow.toml"
      cp "$IN/$UUID/plan.toml"    "$OUT/$UUID/plan.toml"
      # 2. jm を outputs/ に向けて render → .jm/ が outputs/$UUID/.jm/ に直接書かれる
      "$JM" --root "$OUT" render "$UUID"
      # 3. .jm/ が実在するか確認 (前回 silent fail を踏んだので明示チェック)
      test -d "$OUT/$UUID/.jm" \
          || { echo "FATAL: $OUT/$UUID/.jm not created (jm exit=0 だが書かれていない)"; \
               ls -la "$OUT/$UUID/"; exit 1; }
      # 4. stage したファイルだけ片付け (.jm/ は残す)
      rm  "$OUT/common.toml"
      rm  "$OUT/$UUID/flow.toml" "$OUT/$UUID/plan.toml"
  }

  render_into examples/sweep/inputs       examples/sweep/outputs       "$UUID_OK"
  render_into examples/sweep/inputs-fail  examples/sweep/outputs-fail  "$UUID_FAIL"

  # 既存 a487ef7 snapshot と diff
  git diff --stat examples/sweep/outputs examples/sweep/outputs-fail
  git status --short examples/sweep/outputs examples/sweep/outputs-fail

  git add examples/sweep/outputs examples/sweep/outputs-fail
  git commit -m "feat(examples/sweep): regenerate outputs incl. flow.effective.toml"


