# `jm ls` — cross-flow status listing — design

**Date:** 2026-05-16
**Status:** Draft (awaiting user review)
**Reference:** `src/bin/jm.rs` (既存 CLI / `cmd_search` / `cmd_show` / `parse_target` / `resolve_root` / `parse_tag`), `src/search.rs` (`SearchFilter` + 純粋 `matches()`), `src/walk.rs` (`walk_flows` 並列ストリーム), `src/view.rs` (`CalcView`), `src/flow/topology.rs` (`topological_order`), `src/py_export/search.rs` (`PySearchFilter`), `docs/superpowers/specs/2026-05-15-common-env-defaulting-design.md` (`.jm/` レイアウト・common 注入の前提)

---

## 1. 問題設定

`<root>` 配下に多数の flow が溜まると、「いま全体で何が走っているか / どれが失敗したか」を一望する手段が無い。現状の CLI は:

- `jm search --program <P>` — 全 flow 横断だが **`--program` フィルタ1つだけ**。出力は `uuid\tcreated_at` のみで状態を示さない。さらに **既存の純粋述語 `search::matches()` を使わず**、その場限りの `flow.jobs.values().any(...)` 判定をしている（`SearchFilter` の8フィールドのインフラが CLI から未配線）。
- `jm show <flow_uuid>` — 単一 flow の per-job lifecycle をフラットに列挙するのみ。横断不可。親子依存（`afterok` エッジ）は見えない。

一方 `src/search.rs` には既に `SearchFilter`（program / tags / status / flow_uuid_prefix / created_after / created_before / slurm_jobid / job_id）と、テスト済みの純粋述語 `matches()` が存在し、Rust / Python 双方にエクスポート済み。**機能は揃っているのに CLI が使っていない**のが本質的な欠落。

`jm ls` で「全 flow 横断のステータス一覧 + リッチなフィルタ」を提供し、既存 `SearchFilter`/`matches()`/`walk_flows`/`topological_order` を正しく配線する。

参考にした既存 orchestrator の慣習（調査済み）:

- **Airflow** — 列スキーマが変わるビューはフラグでなくサブコマンドで分離（`dags list-runs` vs `tasks states-for-dag-run`）。`--state` 反復、`-o table|json|...`、新しい順、全 DAG 横断は `~` センチネル1コードパス。
- **Prefect** — `flow-run ls`（`ls` 動詞）。状態フィルタ反復・OR、`-o json`、相対時刻表示、既定 limit。
- **SLURM `squeue`/`sacct`** — `jm` ユーザーが最も慣れた語彙。`-s/--state` はカンマ区切り・短名/長名・大小無視。`--noheader`、`--json`。先頭 ID 列、短い状態コード。

## 2. ゴール / 非ゴール

### Goals

1. `jm ls` を **入れ子サブコマンド**として追加し、3 ビューを提供する:
   - `jm ls jobs` — 全 flow 横断、**ジョブ単位 1 行**のフラット表。
   - `jm ls flows` — **flow 単位 1 行**の集約俯瞰。
   - `jm ls tree` — flow→job 階層（`afterok` 依存）+ 状態をツリー表示。引数なし=全 flow の森、`flow_uuid` 指定=単一 flow。
2. 3 ビュー共通の**リッチフィルタ**を、既存 `SearchFilter` を CLI へ配線して提供する。
3. `--status` を **SLURM 風カンマ区切り複数指定**（短名/長名・大小無視・OR）にする。これに伴い共有型 `SearchFilter.status` を `Option<Lifecycle>` → `BTreeSet<DisplayLifecycle>` へ拡張する（破壊的変更・§7）。
4. 出力は **整列表（既定・ヘッダ付き）/ `--json`（機械可読）/ `--no-header`** をサポート（`jobs`/`flows`）。`tree` は常にツリー形式固定。
5. フィルタ・集約・整形の中核ロジックを**純粋・テスト可能な lib コード**（`src/listing.rs` 新設 + `src/search.rs` 拡張）に置き、CLI（binary crate）は薄いラッパに保つ（プロジェクトの `transition.rs`/`search.rs` 規約に一致）。
6. 既存 `jm search` を**削除**し `jm ls jobs` に統合（後者が `SearchFilter` をフル配線した上位互換）。`jm show`（単一ジョブ詳細 / `CalcView::files` のファイル一覧用途）は**維持**。
7. 全コマンドは **オンディスク読み取り専用**。ライブ SLURM 呼び出し・`tick` を行わない（`jm show`/旧 `jm search` と CLAUDE.md「テストでライブ SLURM 不要」に一致）。

### Non-goals

- SLURM 完全互換の出力エンジン（`--format`/`-O` 任意列選択、`--parsable2`/`--delimiter`）。YAGNI。`--json` で機械可読要件は充足。将来必要なら別 spec。
- `--limit` 以外の高度なソート（`--order-by` 任意フィールド / `--offset` ページング）。既定は新しい順固定。`--limit N` のみ任意提供（§5.4）。
- ライブ SLURM 照合（`tick` 相当）。一覧はディスク状態のスナップショット。最新化は既存 `jm tick` の責務。
- `tree` でのフロー内ジョブの部分描画（フィルタ非一致ジョブの非表示）。DAG 整合のため、表示対象 flow はジョブ全描画（§6）。
- 対話的 UI / TUI / カラー必須化（端末非 TTY でも壊れない素のテキスト）。
- per-flow `common.toml`（CLAUDE.md "Out of scope" 準拠・root-level のみ）。

## 3. CLI 形

```
jm --root <path> ls jobs  [FILTERS] [--json] [--no-header]
jm --root <path> ls flows [FILTERS] [--json] [--no-header]
jm --root <path> ls tree  [FLOW_UUID] [FILTERS]

FILTERS（3 ビュー共通）:
  --program <NAME>            JobSpec.program 完全一致
  --tag <KEY=VALUE>           繰り返し可。全 tag 一致（AND）。parse_tag 再利用
  --status <CSV>              例: running,failed / R,F。短名/長名・大小無視・OR
  --flow <UUID_PREFIX>        flow uuid 前方一致（大小無視・既存 matches() 仕様）
  --created-after <RFC3339>   flow.created_at >= 値
  --created-before <RFC3339>  flow.created_at <= 値
  --slurm-jobid <N>           JobRun.slurm_jobid 一致
  --job <JOB_ID>              JobId 完全一致
  --limit <N>                 先頭 N 件（新しい順）。既定=無制限

--root / JM_ROOT は既存どおり全サブコマンドで必須（resolve_root 再利用）。
```

clap モデル:

```rust
enum Cmd {
    // ... Render / Submit / Show / Tick / Doctor / New（不変）
    // Search を削除
    Ls {
        #[command(subcommand)]
        view: LsView,
    },
}

enum LsView {
    Jobs  { #[command(flatten)] filter: FilterArgs, #[command(flatten)] fmt: FmtArgs },
    Flows { #[command(flatten)] filter: FilterArgs, #[command(flatten)] fmt: FmtArgs },
    Tree  { target: Option<String>, #[command(flatten)] filter: FilterArgs },
}

struct FilterArgs { program, tag (Vec<String>), status (Option<String>),
                    flow, created_after, created_before, slurm_jobid, job, limit }
struct FmtArgs    { json: bool, no_header: bool }
```

`--json` と `--no-header` 併用時は `--no-header` を無視（JSON にヘッダ概念なし）。
`jm ls --help` で jobs/flows/tree を一括発見できる（入れ子サブコマンドの利点）。

## 4. Lifecycle ↔ 短縮コード

`Lifecycle` は **SLURM の state そのものではない**（orchestrator 固有のライフサイクル）。SLURM コードを偽装すると誤解を生むため、独自の短縮コードを定義し、`--status` パーサは**短縮コードと長名の双方を大小無視で受理**、表示は短縮コードを使う。

| Lifecycle | status.toml | code | 長名 |
|---|---|---|---|
| Pending | 不在 | `PD` | `pending` |
| Queued | あり | `Q` | `queued` |
| Running | あり | `R` | `running` |
| Success | あり | `OK` | `success` |
| Failed | あり | `F` | `failed` |
| Skipped | あり | `SK` | `skipped` |

- `Pending` は実 enum 値ではなく「`.jm/<JobId>/status.toml` が存在しない」状態（`jm show` の `<pending>` と同義）。`SearchFilter.status` に `Pending` を含めるため、列挙用の表示型 `DisplayLifecycle { Pending, Real(Lifecycle) }` を `src/listing.rs` に置き、`--status pending` を「status.toml 不在のジョブ」にマップする。
- パース失敗（未知のコード/長名）は clap レベルではなく `SearchFilter` 構築時に明示エラー（例: `unknown status "xyz" (expected one of pd,q,r,ok,f,sk / pending,queued,running,success,failed,skipped)`）。
- コードの綴り（`Q`/`OK`/`SK`）は変更容易。実装着手時に最終確定（§11）。

## 5. 行モデルと出力

### 5.1 `jm ls jobs`

全 flow 横断で `(flow, job_id)` ごとに 1 行。

列: `FLOW  JOB  ST  SLURM_ID  PROGRAM  UPDATED  CREATED`

- `FLOW` = flow uuid 先頭 8 文字（`jm show`/Airflow 流の短縮。`--json` では完全 uuid）。
- `JOB` = `JobId`。
- `ST` = §4 短縮コード（status.toml 不在=`PD`）。
- `SLURM_ID` = `JobRun.slurm_jobid`（無ければ `-`）。
- `PROGRAM` = `JobSpec.program`。
- `UPDATED` = `JobRun.updated_at`（status.toml 不在=`-`）RFC3339。
- `CREATED` = `flow.created_at` RFC3339。

行抽出 = 「`flow` の各 `job` を `matches(flow, job_id, job, status_opt, &filter)` で判定」（既存純粋述語をそのまま使用）。

### 5.2 `jm ls flows`

flow ごとに 1 行。**「いずれかの job がフィルタ一致」した flow のみ**表示（`tree` 森と同一の包含規則）。

列: `FLOW  TOTAL  DONE  STATUS  CREATED`

- `TOTAL` = flow の job 数。
- `DONE` = `Lifecycle::Success` の job 数。
- `STATUS` = 集約規則（上から最初に当たったもの、優先順）:
  1. いずれか `Failed` → `FAILED`
  2. いずれか `Running` → `RUNNING`
  3. いずれか `Queued` → `QUEUED`
  4. job が 1 つ以上 & 全て `Success` → `DONE`
  5. いずれか `Skipped` かつ残りが terminal（Success/Skipped。Failed は規則 1 で除外済） → `PARTIAL`
  6. それ以外（status.toml 不在の Pending を含む） → `PENDING`
  - job 0 件の flow は `PENDING`（`TOTAL=0 DONE=0`）。
- `CREATED` = `flow.created_at`。

集約は純粋関数 `aggregate_flow_status(&[DisplayLifecycle]) -> FlowStatus`（`src/listing.rs`）。優先順は rstest マトリクスで網羅。

### 5.3 出力モード

- **既定**: 等幅整列表。各列幅は出力対象行の最大幅で算出（`squeue` 風）。先頭にヘッダ行。
- `--no-header`: ヘッダ行のみ抑制（`awk '{print $1}'` 連携）。
- `--json`: serde で行オブジェクトの JSON 配列を 1 回出力。`FLOW` は完全 uuid、`ST` は長名（`success` 等、機械処理向け）、時刻は RFC3339。`jobs`/`flows` それぞれ専用の `#[derive(Serialize)]` 行型。`--no-header` は無視。
- 整形関数は純粋（`Vec<Row> -> String`）。I/O と分離しテスト可能。

### 5.4 並び順 / limit

- 既定 = `flow.created_at` **降順**（新しい順。Airflow/Prefect 慣習）。同 flow 内の job は `topological_order` 順（依存順）、サイクル時はキー順フォールバック（`jm doctor` がサイクルを別途検出するため、ここでは落とさず描画）。
- `--limit N` = ソート後の先頭 N 件（`jobs` は行単位、`flows`/`tree` は flow 単位）。既定=無制限（ローカル FS。現 `jm search` の全件ストリームと同等。`tests/integration_walk.rs` が 100 flow < 1s を担保）。

## 6. `jm ls tree`

- **森（引数なし）**: フィルタ一致した flow（包含規則は §5.2 と同一）を、新しい順にツリーの森として出力。
  ```
  01997cdc  (3 jobs · 2 OK / 1 F)
  ├─ pre   OK  slurm=120350
  ├─ main  F   slurm=120351  (afterok pre)
  └─ post  SK            (afterok main)

  0199abcd  (2 jobs · 1 OK / 1 R)
  ├─ step1 OK  slurm=120345
  └─ step2 R   slurm=120346  (afterok step1)
  ```
- **単一 flow（`FLOW_UUID` 指定）**: その flow のみ。`parse_target`（uuid 文字列 / 絶対パス末尾 uuid の両対応）を再利用。
- ジョブ並び = `topological_order`。親 → 子へ字下げ。各エッジに `(afterok <parent>)`（`DependencyType` をそのまま表示）。子の状態は §4 短縮コード。
- **フィルタは「どの flow を表示するか」のみを選択**。表示対象 flow は DAG 整合のため全ジョブ描画（非一致ジョブも表示）。この挙動を `--help` と docs に明記。
- `tree` に `--json`/`--no-header` は無し（常にツリー固定。Goals 4）。

## 7. 共有型の変更（破壊的・pre-1.0 で許容）

`SearchFilter.status: Option<Lifecycle>` → `status: BTreeSet<DisplayLifecycle>`（空集合=フィルタなし）。

- `DisplayLifecycle`（`src/listing.rs`）= `Pending | Real(Lifecycle)`。`pending` を第一級に扱うため `Lifecycle` を直接使わずラップ。
- `search::matches()` の status 節を「`f.status.is_empty()` なら通過、非空なら job の `DisplayLifecycle`（status.toml 不在=`Pending`）が集合に含まれるか」に変更。`matches()` のシグネチャ（`status: Option<&JobRun>`）は不変 — 内部で `DisplayLifecycle` に正規化。
- `py_export/search.rs` `PySearchFilter.status: Option<PyLifecycle>` → `Vec<String>`（短名/長名受理、Python 側は素の文字列リストが扱いやすい）。`#[new]` シグネチャ更新。
- 影響: `src/search.rs` 既存テスト（`status_filter_requires_status_entry` 等）、`py_export` テスト、`python/tests`、`.pyi`（`cargo run --bin stub_gen` で再生成、手編集禁止）。CLAUDE.md の pyo3-stub-gen 規約に従う。
- コミットで「`SearchFilter` public API 破壊的変更」を明記（Conventional Commits の本文）。`docs/API.md` の `SearchFilter` 記述を更新。

## 8. モジュール配置 / アーキテクチャ

```
src/search.rs   （拡張）  SearchFilter.status: BTreeSet<DisplayLifecycle>
                          matches() の status 節を集合判定に変更
src/listing.rs  （新設・lib.rs から re-export）
                          - DisplayLifecycle{Pending,Real} + 短縮コード⇔長名 parse/Display
                          - JobRow / FlowRow（serde Serialize）射影（純粋）
                          - aggregate_flow_status()（純粋）
                          - format_jobs_table / format_flows_table / format_*_json
                            / format_tree（純粋: 入力 Vec -> String）
                          - async collect(root, common, &filter) -> 行データ
                            （walk_flows + 並列 read_job_run を spawn_blocking、
                             walk.rs パターン踏襲。tokio ランタイムを塞がない）
src/bin/jm.rs   （変更）  Cmd::Search/cmd_search 削除
                          Cmd::Ls + LsView + FilterArgs/FmtArgs
                          各 cmd_ls_* は: フラグ→SearchFilter 構築→listing 呼出→print のみ
                          parse_tag / parse_target / resolve_root を再利用
src/lib.rs      （変更）  pub use search::{SearchFilter, matches};（型変更を反映）
                          pub use listing::{...};
```

データフロー（読み取り専用）:

```
jm ls <view> [filters]
  → resolve_root → read_common（無ければ synth_empty_common、cmd_search と同様）
  → SearchFilter 構築（--status を DisplayLifecycle 集合へパース）
  → listing::collect(root, Arc<common>, &filter)
        walk_flows(root, common)               # 並列・既存
          各 flow: topological_order で job 順
          各 job: read_job_run(status_file)    # spawn_blocking 並列、不在=Pending
          matches(flow, jid, job, status_opt, &filter) で AND/OR 判定
  → listing::format_*（純粋・String）
  → stdout
```

malformed TOML は `walk_flows` が `Err` をストリーム要素として返す（既存仕様）。`jm ls` は当該 flow を `stderr` に warn 出力してスキップし、残りを継続（一覧の堅牢性。`jm doctor` が厳密検証の責務）。

## 9. テスト戦略（TDD・80%+）

- `src/search.rs`: `--status` 集合化の rstest マトリクス（空=全通過 / 単一 / 複数 OR / Pending を含む / status.toml 不在の扱い）。既存テストの移行。
- `src/listing.rs` 単体:
  - `DisplayLifecycle` 短縮コード/長名の parse・Display 往復、未知値エラー（rstest）。
  - `aggregate_flow_status()` の優先順マトリクス（6 規則 × 代表組合せ、0 件 flow 含む）。
  - `format_jobs_table`/`format_flows_table` の整列・`--no-header`・空入力。
  - `format_*_json` の serde 形状（完全 uuid・長名 ST・RFC3339）。
  - `format_tree` の森/単一・字下げ・`afterok` 注記・サイクル時キー順フォールバック。
- `tests/integration_listing.rs`（新設）: `tests/integration_walk.rs`/`examples/full` のフィクスチャ流儀を流用。on-disk のみ（ライブ SLURM 不要 / `MockExecutor` 不要 — 一覧は SLURM を呼ばない）。flow×job を複数生成し各 view × 代表フィルタ × `--json` 形状 × `--limit` × 新しい順 を検証。
- `python/tests`: `PySearchFilter.status` の新シグネチャ（文字列リスト）の smoke。
- CI ゲート（CLAUDE.md）: `cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test --all-features && uv run pytest python/tests -v` を通す。`.pyi` ドリフトは `cargo run --bin stub_gen && uv run ruff format python/` で解消し `git add`。

## 10. ドキュメント / 互換性影響

- `README.md` / `docs/API.md` / `docs/architecture.md` / `docs/development.md` の `jm search` 記述を `jm ls jobs` に差し替え、`jm ls flows`/`jm ls tree` を追記。アーキ図に「読み取り専用一覧」経路を追記。
- `docs/API.md` の `SearchFilter`（Rust/Python）を新シグネチャに更新。
- `.pyi` 再生成（手編集禁止・stub_gen）。
- `jm search` 廃止は破壊的 CLI 変更。コミット本文 /（あれば）CHANGELOG に明記。`jm ls jobs` が機能上位互換であることを移行注記。
- `examples/full` を変更する場合は `tests/doctor_examples.rs` のドリフトガードに注意（本機能は読み取りのみで examples 改変は不要の見込み）。

## 11. 未解決 / 実装時確定事項

- 短縮コードの最終綴り（`Q`/`OK`/`SK`）— §4 表で提案、実装着手時に確定。
- `--json` の正確なフィールド名（`flow`/`job`/`status`/`slurm_jobid`/`program`/`updated_at`/`created_at` を想定）— 実装時に serde スキーマ確定。
- `format_tree` のツリー罫線（ASCII `|-`/`` `-`` か Unicode `├─/└─`）— 既定 Unicode、非 UTF-8 端末考慮は将来。
