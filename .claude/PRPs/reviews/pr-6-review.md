# PR Review: #6 — refactor(sp1): adopt D2 v4 (drop JobFlow.work_dir references)

**Reviewed:** 2026-05-13
**Author:** kkiyama117
**Branch:** `refactor/sp1-drop-work-dir` → `develop`
**Decision:** APPROVE with comments

## Summary

PR #7 (`feat/sp2-impl`) が既に PR #6 ブランチに merge 済みのため、本 PR は SP-1 work_dir refactor + SP-2 plan/jobid feat の 15 commits 累積 (24 files, +956/-23) を含む。すべての validation はパスし、CRITICAL/HIGH の blocker はないが、Python 公開境界での optional な入力検証ガードを後追いで追加する余地がある (フォローアップ issue 推奨)。

## PR #7 ステータス

**MERGED** (`feat/sp2-impl` → `refactor/sp1-drop-work-dir`)。SP-2 の機能は本 PR (#6) で `develop` に取り込まれる構造になっている。

## Findings

### CRITICAL
None.

### HIGH
None — rust-reviewer の H-1 (`build_job_id` 未検証) は spec の「optional helper」設計と整合的なため MEDIUM に降格。

### MEDIUM

**M-1: `PyExperimentPlan::new` が job_id key を未検証で受け入れる**
- File: `src/py_export/plan.rs:32-40`
- 内容: Python dict のキー `jid_str` を `JobId::from(jid_str)` で生 String 化。`validate_job_id` を通らないため、`"../../../etc/target"` や予約名 (`"flow"` 等) を含む任意文字列が `ExperimentPlan.jobs` に挿入できる。
- 影響: 現状の TOML key シリアライズには直接の injection はないが、SP-3 で job_id をパス構築に再利用すると path traversal 起点になりうる。
- Fix: 挿入前に `crate::jobid::validate_job_id(&jid_str).map_err(PyErr::from)?;` を 1 行追加。

**M-2: `PyPathResolver::status_file` が `job_id` を未検証で path join に使う**
- File: `src/py_export/path.rs:53-60`
- 内容: `JobId::from(job_id)` でラップするだけ。内部 `PathResolver::status_file` は `flow_dir(uuid).join(&job_id.0)` を行うため、`..` を含む job_id で上位ディレクトリへの参照が構築可能。
- Fix: 関数冒頭で `crate::jobid::validate_job_id(job_id).map_err(PyErr::from)?;`。
- 注: これは SP-1 時点から存在する問題で、PR #6 で新規導入したものではないが、SP-2 で `validate_job_id` が利用可能になった以上、Python 公開境界で活用すべき。

**M-3: `build_job_id` が `source_step_id` / axis 名を未検証**
- File: `src/jobid.rs:45-58` / `src/py_export/jobid.rs`
- 内容: `build_job_id("../evil", &[("ax", 0)])` → `"../evil__ax=0"` を生成。`parse_job_id` で reject されるため round-trip は破綻するが、Python ユーザーがエラーなく不正文字列を得られる。
- 設計判断: モジュール docstring (`src/jobid.rs:8-9`) は「Python authoring で『規約に従った JobId 文字列』を作る helper」と書いており、infallible API は spec 通り。
- 推奨: Python 公開境界 (`py_export/jobid.rs`) では fallible にして `validate_step_id` を呼ぶのが Python の慣習に合う。

**M-4: `write_plan` が `create_dir_all` を呼ばない (write_flow との非対称)**
- File: `src/plan/io.rs:19`
- 内容: `write_flow` は親ディレクトリを自動作成するが `write_plan` はしない。`integration_plan.rs:167` で呼び側が手動で作成している。
- Fix 案: doc comment に「path の親ディレクトリは事前に作成されている前提」を明記、または `write_flow` と同じく `create_dir_all` を呼ぶ。

**M-5: `validate_job_id` が reserved step_id を拒否することのテスト不足**
- File: `src/jobid.rs` tests
- 内容: 動作は正しい (parse_job_id 経由で validate_step_id が呼ばれる) が、`validate_job_id("flow")` 等を明示的に検証するテストがない。
- Fix: テスト 1 件追加。

### LOW

**L-1: `parse_job_id` の `.expect()` に SAFETY/INVARIANT コメントがない** (`src/jobid.rs:72`)
- `str::split` の挙動上 panic は起きないが、production コードで `expect` を使う場合は理由を明記すべき。

**L-2: `parse_job_id` の axis 名が予約語チェックを受けない** (`src/jobid.rs:86-92`)
- `"opt__flow=0"` のような axis 名 `flow` が通過する。攻撃経路はないが将来の命名衝突の余地あり。

**L-3: `write_plan` の I/O 失敗時に `.toml.tmp` を削除しない** (`src/plan/io.rs`)
- `flow_io.rs` も同じパターン (既存設計の踏襲)。シングルユーザー環境では実害限定的。

**L-4: `ExperimentPlan` / `read_plan` に `#[must_use]` がない** (`src/plan/mod.rs:9`)

## Validation Results

| Check | Result |
|---|---|
| cargo fmt --check | PASS |
| cargo clippy --all-features --all-targets -- -D warnings | PASS |
| cargo test --all-features | PASS (73 tests) |
| uv run maturin develop --uv | PASS |
| uv run pytest python/tests -v | PASS (17 tests) |

## Files Reviewed (24)

Source (Rust):
- src/error.rs (Modified)
- src/jobid.rs (Added)
- src/lib.rs (Modified)
- src/path.rs (Modified)
- src/plan/mod.rs (Added)
- src/plan/io.rs (Added)
- src/filter.rs / src/flow_io.rs / src/status/mod.rs / src/view.rs / src/walk.rs (Modified — fixture cleanup)

py_export:
- src/py_export/error.rs (Modified)
- src/py_export/jobid.rs (Added)
- src/py_export/mod.rs (Modified)
- src/py_export/path.rs (Modified)
- src/py_export/plan.rs (Added)

Python:
- python/job_manager/__init__.py (Modified)
- python/job_manager/_job_manager_core/__init__.pyi (Modified)
- python/tests/test_jobid.py (Added)
- python/tests/test_plan.py (Added)

Tests:
- tests/integration_plan.rs (Added)
- tests/integration_walk.rs (Modified)

Docs:
- README.md (Modified)
- docs/architecture.md (Modified)

## 観点別サマリー

| 観点 | 結論 |
|---|---|
| JobId round-trip | parse → build は一貫。build → parse 方向は M-3 で破綻可能 |
| atomic rename I/O | 正しいパターン。L-3 の cleanup と TOCTOU (O_EXCL なし) は既存設計の踏襲 |
| deny_unknown_fields | ExperimentPlan に付与済み + テストあり |
| BTreeMap 選択 | TOML key ordering の決定性担保 |
| エラー設計 | 4 variant とも context 十分 |
| D2 single owner | default-features = false 維持、newtype 再定義なし |
| A1 SLURM 構造 | 変更ゼロ (UNTOUCHABLE 制約クリア) |
