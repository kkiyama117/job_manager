# common.toml defaulting + `.jm/` program-managed subfolder — design

**Date:** 2026-05-15
**Status:** Draft (awaiting user review)
**Predecessors (thought):**
  - `docs/superpowers/thought/2026-05-15-flow-toml-partition-defaulting.md` (F2 推奨)
  - `docs/superpowers/thought/2026-05-15-common-env-injection-orchestrator-lessons.md` (Airflow/Prefect 補強)
**Reference:** `docs/references/orchestration-systems.md`

---

## 1. 問題設定

`examples/simple` 等のセットアップで、ユーザーは少なくとも以下 2 か所で `partition`
（と関連の SLURM 値）を書く必要がある:

- `<root>/common.toml` の `[slurm_default] partition`
- `<root>/<flow_uuid>/flow.toml` の `[jobs.*.config] partition`

A1 の `SlurmJobConfig::partition` は **型として必須** (`pub type JobPartition = String;`
の単純 alias、`Option` ではなく `#[serde(default)]` も無い)。
A1 の "partition is required" 契約を破らず、ユーザーの flow.toml では
省略可能にしたい。

加えて現状は `<flow_uuid>/<JobId>/` 配下に user-authored ファイル
(flow.toml/plan.toml) と program-managed ファイル
(batch.bash/.status.toml/slurm-*.out/err) が混在しており、視覚的・ git 上の境界が
曖昧。

## 2. ゴール / 非ゴール

### Goals

1. **`flow.toml` の `partition` を省略可能** にする（`[jobs.*.config]` テーブル丸ごとも省略可）。common.toml の値が default として注入される。
2. **`.flow.effective.toml`** という materialized snapshot を新規導入。submit / render 時に program が書き出し、tick / show は snapshot を直接読む（common 不要）。
3. **`<flow_uuid>/.jm/`** 配下に全 program-managed ファイルを集約。user input
   (flow.toml, plan.toml) と program output (snapshot, batch.bash, status, slurm logs) を
   ディレクトリレベルで分離。
4. A1 / D2 の crate を変更しない（"partition is required" 契約を尊重）。
5. Public API として `JobFlow` (D2) を変えない。
6. `examples/simple` の inputs を新形式に移行、 `examples/sweep/PLAN.md` のレイアウト記述を更新。
7. `docs/architecture.md` に「common.toml ≈ Prefect Pool template / Airflow default_args」
   の対応関係を 1 段落で追記。

### Non-goals

- partition 以外のフィールド (time_limit 等) の追加デフォルト機構（既存
  `merge_with_defaults` の per-field merge を継続するだけ、設計拡張は別 spec）
- `REPLACE_ME` 等 sentinel 値の検知・拒否（別 spec）
- 複数 `common.toml` (per-pool) への拡張
- Round-trip 対称性 (`write_flow` で common 由来値を省略して書く等)
- 既存 `<flow_uuid>/` レイアウトからの自動 migration（破壊 OK、user が再 render する想定）

## 3. アーキテクチャ

### 3.1 ファイルレイアウト

```
<root>/
├── common.toml                              # user input (slurm defaults + directories)
└── <flow_uuid>/
    ├── flow.toml                            # user input (partial spec, partition 省略可)
    ├── plan.toml                            # user input (ExperimentPlan)
    └── .jm/                                 # ★ program-managed (hidden, gitignore 対象)
        ├── flow.effective.toml              # ★ 新規 materialized full spec
        └── <JobId>/
            ├── batch.bash                   # SBATCH script
            ├── status.toml                  # 旧 .status.toml (`.jm/` 内なので頭の . 不要)
            ├── slurm-*.out                  # SLURM stdout
            └── slurm-*.err                  # SLURM stderr
```

### 3.2 設計原則

- **One-way materialization**: `flow.toml` (partial input) → `.flow.effective.toml`
  (full snapshot)。逆方向は無い。round-trip 問題は存在しない。
- **Snapshot 自己完結**: `.jm/` 配下だけで tick / show が動く。common 非依存。
- **user/program 分離**: flow_dir 直下は user-editable のみ、`.jm/` は program-managed のみ。
- **git 連携**: `.jm/` を ignore したい場合、flow_dir ごとに `<flow_uuid>/.gitignore`（`.jm/` の 1 行）を配置する。repo 直の `.gitignore` で `.jm/` を一括 ignore すると `examples/*/outputs/` の commit と衝突するため避ける。詳細は §10。

### 3.3 命名対応 (Airflow / Prefect)

| job-manager | Airflow | Prefect |
|---|---|---|
| `common.toml [slurm_default]` | DAG `default_args` | Work Pool `base_job_template` + variables |
| `flow.toml [jobs.*.config]` | Operator kwargs (partial) | Deployment variables (per-task override) |
| `read_flow(path, &common)` | `apply_defaults` + DAG load | template render |
| `.flow.effective.toml` | (Airflow は materialize 後保存しない) | Deployment spec |

## 4. コンポーネント

### 4.1 Persistence layer (`src/persistence/`)

| 関数 | シグネチャ | 役割 |
|---|---|---|
| `read_flow` (modified) | `(path: &Path, common: &CommonConfig) -> Result<JobFlow>` | flow.toml を読み materialize |
| `read_flow_effective` (new) | `(path: &Path) -> Result<JobFlow>` | snapshot をそのまま deserialize |
| `write_flow_effective` (new) | `(path: &Path, flow: &JobFlow) -> Result<()>` | snapshot を atomic-rename で書く |
| `write_flow` (kept) | `(path: &Path, flow: &JobFlow) -> Result<()>` | 既存。test fixture / Python TOML 文字列書き出し用途、変更なし |
| `merge_with_defaults` (modified) | `(common, override_: &SlurmJobConfig) -> SlurmJobConfig` | preparse 段で partition が必ず materialize されている前提に立ち、partition は触らず `override_.partition` をそのまま使う。残りの Option フィールド（time_limit / log_stdout / log_stderr / resource_spec 等）のみ common から fallback。旧 `partition.is_empty()` 特殊分岐は廃止 |

#### 4.1.1 `read_flow` 内部処理

```
1. toml::from_str(file) → toml::Value
2. inject_partition_defaults(&mut v, &common.slurm_default.partition):
     - jobs.<id>.config テーブル無ければ作る
     - partition キー無ければ inject (common から)
     - common.partition が None なら PartitionMissing { job } を返す
3. toml::Value.try_into::<JobFlow>() → A1 SlurmJobConfig が deserialize される
4. merge_with_defaults を各 job に適用、Optional フィールドも resolve
```

### 4.2 Path resolver (`src/persistence/path.rs`)

| メソッド | 旧 | 新 |
|---|---|---|
| `flow_toml(uuid)` | `<flow_uuid>/flow.toml` | 変更なし |
| `plan_toml(uuid)` | `<flow_uuid>/plan.toml` | 変更なし |
| `common_toml()` | `<root>/common.toml` | 変更なし |
| `flow_effective_toml(uuid)` | — | `<flow_uuid>/.jm/flow.effective.toml` (new) |
| `job_dir(uuid, jobid)` | `<flow_uuid>/<JobId>/` | `<flow_uuid>/.jm/<JobId>/` |
| `batch_script(uuid, jobid)` | `<flow_uuid>/<JobId>/batch.bash` | `<flow_uuid>/.jm/<JobId>/batch.bash` |
| `status_file(uuid, jobid)` | `<flow_uuid>/<JobId>/.status.toml` | `<flow_uuid>/.jm/<JobId>/status.toml` |
| `slurm_stdout_template(...)` | `<JobId>/slurm-%j.out` | `.jm/<JobId>/slurm-%j.out` |
| `slurm_stderr_template(...)` | `<JobId>/slurm-%j.err` | `.jm/<JobId>/slurm-%j.err` |

### 4.3 Runner integration (`src/runner/flow.rs`)

| 関数 | 変更内容 |
|---|---|
| `FlowRunner::submit(&fr, dry_run)` | render loop 前に `write_flow_effective(&path, &fr.flow)` を 1 回呼ぶ。残りは不変 |
| `FlowRunner::render_only(&fr)` | 同上 |
| `FlowRunner::tick(&fr)` | `fr.flow` の組み立て元を `read_flow_effective` に変更（common 依存切る） |

### 4.4 FlowRun loader (`src/flow/run.rs`)

- `FlowRun::load_from_disk(uuid, &common)` (既存 + common 引数追加): submit / render 経路用、`read_flow(flow.toml, &common)`
- `FlowRun::load_effective(uuid)` (new): tick / show 経路用、`read_flow_effective(.jm/flow.effective.toml)`、snapshot 不在で `SnapshotMissing { uuid, hint }`

### 4.5 CLI (`src/bin/jm.rs`)

| サブコマンド | 変更 |
|---|---|
| `jm submit <uuid>` | 既存挙動 + 暗黙の snapshot 書き出し |
| `jm render <uuid>` | 既存挙動 + 暗黙の snapshot 書き出し |
| `jm render <uuid> --effective-only` (new flag) | snapshot のみ更新、batch.bash には触れない |
| `jm tick <uuid>` | snapshot を読む。無ければエラー with hint |
| `jm show <uuid>` | 同上 |

### 4.6 SLURM 出力ディレクティブ

`batch.bash` 生成時の `#SBATCH --output` / `--error` テンプレートが `.jm/<JobId>/slurm-%j.out` を指すよう更新。`common.toml` の `log_stdout` / `log_stderr` テンプレートはユーザー上書き可能、デフォルトが `.jm/` 配下を指す。

### 4.7 PyO3 公開 (`src/py_export/`)

| Python 関数 | シグネチャ | 内部 |
|---|---|---|
| `read_flow(path) -> str` | 既存 | path → root 推定 → `read_common` → `inner_read_flow(path, &common)` |
| `write_flow(path, toml_str) -> None` | 既存 | 変更なし |
| `read_flow_effective(path) -> str` (new) | path 1 引数 | snapshot を読んで TOML 文字列を返す |

Python 公開 `read_flow` の "path から root 推定" ロジック:
`path.parent().parent()` を試し、そこに `common.toml` があれば使用、無ければ `RootInferenceFailed { path }`。

## 5. データフロー

### 5.1 `jm submit <uuid>`

```
1. CLI parse + PathResolver init
2. read_common(<root>/common.toml) → CommonConfig
3. FlowRun::load_from_disk(uuid, &common):
     a. read_plan(<flow_uuid>/plan.toml) → ExperimentPlan
     b. read_flow(<flow_uuid>/flow.toml, &common) → JobFlow (materialized)
4. FlowRunner::submit(&fr, dry_run):
     a. write_flow_effective(<flow_uuid>/.jm/flow.effective.toml, &fr.flow)
     b. topological_order → for each JobId:
          - render batch.bash into <flow_uuid>/.jm/<JobId>/batch.bash
          - if !dry_run: executor.submit(...) → write status.toml
```

### 5.2 `jm render <uuid>` (`--effective-only` フラグも)

```
1〜3 は submit と同じ
4. FlowRunner::render_only(&fr):
     a. write_flow_effective(...)
     b. topological_order → render batch.bash only (executor 呼ばない)

`--effective-only` 時:
4'. write_flow_effective(...) のみ、batch.bash 不変
```

### 5.3 `jm tick <uuid>`

```
1. CLI parse + PathResolver init
2. FlowRun::load_effective(uuid):
     a. read_plan(<flow_uuid>/plan.toml) → ExperimentPlan
     b. read_flow_effective(<flow_uuid>/.jm/flow.effective.toml) → JobFlow
        - snapshot 無し → Err(SnapshotMissing { uuid, hint })
3. FlowRunner::tick(&fr):
     a. for each JobId: read status.toml + querier.query + decide_transition
     b. write status.toml の更新
```

tick は `read_common` を一切呼ばない。`.jm/` 配下だけ別ホストに持っていっても動く。

### 5.4 `jm show <uuid>`

tick とほぼ同じ。snapshot を読み、各 job の status.toml を集めて表示。

### 5.5 Stale シナリオ (case 1: snapshot 優先)

```
t0: user writes flow.toml
t1: jm submit  → .jm/flow.effective.toml 焼き付け、sbatch 発射
t2: user edits flow.toml
t3: jm tick    → snapshot を読む (編集は無視)
t4: user wants edit to take effect:
       jm render <uuid> --effective-only  (snapshot 再生成)
       既に sbatch 済みのジョブには影響しない (SLURM 側の挙動)
```

stale 検知 (mtime 比較等) は **初期スコープ外**。

### 5.6 PyO3 経路

```python
flow_str = job_manager.read_flow("/path/to/flow.toml")
    # 内部で <root> 推定 → common.toml load → preparse → return TOML str

eff_str = job_manager.read_flow_effective("/path/to/.jm/flow.effective.toml")
    # 内部で snapshot を読んで return TOML str
```

## 6. エラーハンドリング

### 6.1 新規エラー variant

```rust
#[derive(thiserror::Error, Debug)]
pub enum JobManagerError {
    // 既存 ...

    #[error("partition is required but missing: job={job} has no partition and common.toml [slurm_default] has no partition either")]
    PartitionMissing { job: JobId },

    #[error("effective snapshot missing at {path}: run `jm render <uuid>` first to materialize")]
    SnapshotMissing { path: PathBuf, uuid: String },

    #[error("cannot infer root from flow.toml path {path}: expected <root>/<flow_uuid>/flow.toml layout")]
    RootInferenceFailed { path: PathBuf },
}
```

### 6.2 エラーケース対応

| シナリオ | 検知箇所 | 処理 |
|---|---|---|
| flow.toml に partition 無し ＆ common.toml に partition 無し | `read_flow` 内 preparse | `PartitionMissing { job }` |
| common.toml 不在 | `read_common` 呼び出し | 既存 IoError 伝搬 |
| `.flow.effective.toml` 不在 | `read_flow_effective` 前 | `SnapshotMissing { path, uuid }` |
| snapshot parse 失敗 | `toml::from_str` | 既存 ParseError 伝搬 |
| `.jm/` ディレクトリ作成失敗 | `fs::create_dir_all` | 既存 IoError 伝搬 |
| PyO3 `read_flow(path)` で `path.parent().parent()` が None | py_export shim | `RootInferenceFailed { path }` |
| flow.toml の partition が `"REPLACE_ME"` 等 sentinel | **検知しない (スコープ外)** | sbatch がエラーを出すのを利用 |

### 6.3 preparse の核ロジック

```rust
fn inject_partition_defaults(
    v: &mut toml::Value,
    common_partition: Option<&str>,
) -> Result<(), JobManagerError> {
    let jobs = v.get_mut("jobs")
        .and_then(|j| j.as_table_mut())
        .ok_or(JobManagerError::ParseError(...))?;

    for (job_id_str, job) in jobs.iter_mut() {
        let job_t = job.as_table_mut().ok_or(...)?;
        let cfg = job_t
            .entry("config")
            .or_insert_with(|| toml::Value::Table(Default::default()));
        let cfg_t = cfg.as_table_mut().ok_or(...)?;

        if !cfg_t.contains_key("partition") {
            match common_partition {
                Some(p) => cfg_t.insert(
                    "partition".into(),
                    toml::Value::String(p.to_string()),
                ),
                None => return Err(JobManagerError::PartitionMissing {
                    job: JobId(job_id_str.clone()),
                }),
            };
        }
    }
    Ok(())
}
```

ポイント:
- `common_partition: Option<&str>` で受ける（common.toml の partition も省略可能なケースに備える）
- `[jobs.*.config]` テーブル自体が無いケースを `or_insert_with` で対応
- `partition = ""` は inject 対象外。空文字は後段 sbatch が弾く。`is_empty()` 特殊扱いは廃止

## 7. テスト戦略

### 7.1 ユニットテスト

`src/persistence/flow.rs`:
- `read_flow_with_partition_in_flow`
- `read_flow_with_partition_from_common`
- `read_flow_with_config_section_missing`
- `read_flow_partition_missing_both` → `PartitionMissing`
- `read_flow_effective_roundtrip`
- `read_flow_effective_missing_file` → `SnapshotMissing`
- `read_flow_effective_parse_error`
- `inject_partition_idempotent`
- `inject_handles_multiple_jobs`

`src/persistence/common.rs`:
- `merge_with_defaults_option_based`
- `merge_preserves_explicit_empty_string`

`src/persistence/path.rs`:
- `flow_effective_toml_path_under_jm_dir`
- `batch_script_path_under_jm_dir`
- `status_file_path_under_jm_dir`

`src/runner/flow.rs`:
- `submit_writes_effective_snapshot` (MockExecutor + InMemoryQuerier)
- `render_only_writes_effective_snapshot`
- `render_effective_only_flag_skips_batch`
- `tick_reads_effective_snapshot`
- `tick_fails_without_snapshot` → `SnapshotMissing`

### 7.2 統合テスト (`tests/`)

- `tests/integration_sp3.rs` 修正: submit → snapshot 生成確認 → tick が snapshot 経由で動作
- `tests/integration_walk.rs` 修正: 各 flow で `read_flow(path, &common)` 動作
- `tests/integration_effective_isolation.rs` (新規): submit 後に flow.toml を編集 / `.jm/` を別パスへ cp / common.toml 無しで tick 動作

### 7.3 Python smoke テスト

- `python/tests/test_read_flow_effective.py` (新規): roundtrip via subprocess `jm render`
- 既存 walk test 等を `.jm/` レイアウト前提に修正

### 7.4 例の更新

`examples/simple/inputs/`:
- `<uuid>/flow.toml` から `[jobs.*.config] partition` 行を削除
- common.toml 不変

`examples/simple/outputs/`:
- 古いレイアウト削除、`<uuid>/.jm/<JobId>/batch.bash` と `<uuid>/.jm/flow.effective.toml` を新規生成

`examples/sweep/PLAN.md`:
- File layout セクションを `.jm/` 配下に更新
- partition 重複の話を「F2 で省略可能」と修正
- 実装本体は別 PR

### 7.5 ドキュメント更新

`CLAUDE.md`:
- `<root>/<flow_uuid>/{flow,plan}.toml + <root>/<flow_uuid>/<JobId>/{batch.bash, .status.toml, slurm-*.out/err}` の記述を `.jm/` 配下に更新
- `flow.toml は read-only な user input、program-managed は .jm/ 配下` のポリシーを 1 文追加

`docs/architecture.md`:
- common.toml ≈ Prefect Pool template / Airflow default_args の対応関係を 1 段落追記
- `.flow.effective.toml` の役割 (Cargo.lock パターン) を 1 段落追記

`docs/development.md`:
- `.jm/` レイアウト前提に test 実行例を更新
- `.gitignore` に `.jm/` 追加の指針を追記

### 7.6 CI ゲート

CLAUDE.md の "CI gate" は変更なし:
```bash
cargo fmt --check \
  && cargo clippy --all-targets --all-features -- -D warnings \
  && cargo test --all-features \
  && uv run pytest python/tests -v
```

カバレッジ目標 80% 維持。`cargo llvm-cov --fail-under-lines 80`。

## 8. 破壊的変更 (Breaking changes)

- **PathResolver API**: `job_dir`, `batch_script`, `status_file`, `slurm_*_template` の戻り値がすべて `.jm/` 配下に変わる
- **`read_flow` signature**: `(path)` → `(path, &CommonConfig)`
- **既存 `<flow_uuid>/` レイアウト**: 古い形式の flow_dir は `jm tick` / `show` が `SnapshotMissing` を返す。`jm render <uuid>` で再生成すれば新形式に移行可能
- **既存テスト fixture**: 多くが `partition` を空文字や placeholder で持っていたら修正必要
- **Python API**: `read_flow_effective` が新規追加 (`.pyi` 再生成、`__init__.py` の re-export 追加)

migration は提供しない (user が `jm render` で再生成する想定)。

## 9. 実装順序

1. `JobManagerError` に variant 追加 (`PartitionMissing`, `SnapshotMissing`, `RootInferenceFailed`)
2. `PathResolver` を `.jm/` レイアウトに移行 + ユニットテスト
3. `inject_partition_defaults` 実装 + ユニットテスト
4. `read_flow` を `(path, &common)` に変更 + 既存呼び出し側を common 渡しに更新
5. `merge_with_defaults` を Option ベースに書き換え + テスト更新
6. `read_flow_effective` / `write_flow_effective` 実装 + ユニットテスト
7. `FlowRun::load_effective` 追加
8. `FlowRunner::submit` / `render_only` に snapshot 書き出しを組み込む
9. `FlowRunner::tick` を snapshot 読み込みに切替え
10. CLI `jm render --effective-only` フラグ追加
11. SLURM `--output` / `--error` テンプレートを `.jm/` 配下に更新
12. PyO3 `read_flow` shim を root 推定ロジック付きに更新、`read_flow_effective` 公開
13. `__init__.py` / `.pyi` に `read_flow_effective` 追加 (stub_gen 経由)
14. `examples/simple` を新形式に再生成
15. `examples/sweep/PLAN.md` 更新
16. `CLAUDE.md` / `docs/architecture.md` / `docs/development.md` 更新
17. `.gitignore` に `.jm/` 追加方針を documentation で示す（リポ直の .gitignore は examples の outputs/ を ignore したくないので、案内のみ）
18. 全テスト + Python smoke + CI gate 通過確認

## 10. オープン項目 / 要確認

- **PyO3 root 推定**: `path.parent().parent()` で良いか、それとも `--root` 相当を Python 側でも引数として受けるか。今回は前者で進めるが、Python から非標準レイアウトを扱いたい場合は別途検討。
- **stale 検知**: 将来 `mtime(flow.toml) > mtime(.flow.effective.toml)` で warning を出す機能の余地。本 spec ではスコープ外。
- **`.gitignore`**: `examples/simple/outputs/` を commit する運用なので、リポ直の `.gitignore` で `.jm/` を ignore できない。`examples/sweep/.gitignore` を flow_dir 単位で配置する案が無難（運用ドキュメントで示す）。

## 11. 参考

- 前 thought: `docs/superpowers/thought/2026-05-15-flow-toml-partition-defaulting.md`
- 補強 thought: `docs/superpowers/thought/2026-05-15-common-env-injection-orchestrator-lessons.md`
- 比較資料: `docs/references/orchestration-systems.md`
- A1 `SlurmJobConfig`: `slurm_async_runner` rev `a734a06`,
  `src/entities/slurm/sbatch_options.rs:170-217`
- D2 `JobSpec`: `gaussian_job_shared` rev `00c645e`,
  `src/entities/workflow/job.rs:125`
- 現行 `merge_with_defaults`: `src/persistence/common.rs:27-67`
- 現行 `read_flow` / `write_flow`: `src/persistence/flow.rs`
- examples: `examples/simple/inputs/{common.toml,01999999-.../flow.toml}`
- sweep plan: `examples/sweep/PLAN.md`
