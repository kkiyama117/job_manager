# job-manager SP-1 (データ層) 設計

- **Date**: 2026-05-12
- **Status**: Draft (brainstorming 完了、レビュー待ち)
- **Targets**: `crate::*` (Rust) + `job_manager._job_manager_core.*` (Python)
- **Subproject**: SP-1 of 3 — データ層 (`JobFlow` 永続化、走査、フィルタ、SLURM tick、CalcView)
- **References**:
  - Python リファレンス: `../../../gaussian-experiment-manager/` (E layer, 旧 D1 ベース)
  - 上流 (A1): `../../../slurm-async-runner2/` (`SlurmJobConfig` / `JobStatus` / `SbatchManager`)
  - 上流 (D2): `../../../gaussian-job-shared2/` (`JobFlow` / `Job` / `JobSpec` / `JobEdge`)
  - D2 設計: `../../../gaussian-job-shared2/docs/superpowers/specs/2026-05-08-slurm-job-flow-structs-design.md`

---

## 1. 背景

SLURM 計算ジョブ群のデータ管理基盤を Rust + PyO3 で構築する。Python 実装 (`gaussian-experiment-manager`) を仕様リファレンスとして「データ管理」面のみを抽出し、新 D2 スキーマ (`JobFlow`) 前提で書き直す。

### 1.1 Python 実装 (リファレンス) の課題

`gaussian-experiment-manager` のコード読みで顕在化した問題:

1. **走査のシリアル I/O** — `walk_metadata` は `<env.root>/*/metadata.toml` を 1 件ずつ tomllib parse。永続インデックス無し。
2. **SLURM 状態問い合わせの `asyncio.run` per call** — `_SlurmFacade.query_states_batch` がイベントループを毎回新規作成。並列合成不能。
3. **`metadata.toml` と `status.toml` の 2 ファイル分離** — 書き込みアトミシティが弱い (どちらか片方だけ書き換わる窓がある)。
4. **process-local mutable globals** — `uuid7()` が `_LAST_TS_MS` / `_COUNTER` をモジュールスコープで保持、スレッドセーフでない。
5. **`submit_chain` のリカバリ性** — metadata 書き込み → sbatch の順。中間失敗で dangling metadata が残る (E 自体は scope 外だが、データ層の status 管理に影響)。
6. **`resolve_parents` の O(P × C) ネストループ** — コード内に TODO コメントあり。
7. **Gaussian 専用スキーマ** — `Compounds` / `params_cls` レジストリが組み込まれており、program agnostic にしにくい。

D2 (`gaussian-job-shared2`) はすでに **program-agnostic な `JobFlow` DAG モデル**にリファクタ済みで、旧 D1 (`Metadata` / `Compounds` / `Status`) は存在しない。よって SP-1 は D2 を直接消費する形で再設計する。

### 1.2 SP-1 のスコープ

| 含める | 含めない (SP-2 / SP-3) |
|---|---|
| `JobFlow` の TOML I/O (atomic write) | `experiment.toml` パーサ (grammar) |
| `<work_dir>` 配下の `JobFlow` 走査 (並列) | sweep / parent resolution |
| `SearchFilter` (program / tags / status / 時刻範囲 / jobid) | sbatch 投入 (`submit_chain` 相当) |
| `tick`: SLURM 状態問い合わせ + ローカル status 更新 | CLI コマンド (`run`/`submit`/`show`/...) |
| `CalcView` (Job 単位の paths / metadata / status getter) | β-adapter / `gaussian_batch_cli` 連携 |
| status の atomic 書き込み | log_paths 解決 (SLURM `%j`/`%x` 展開) |

### 1.3 サブプロジェクト位置付け

```
SP-1 (データ層, 本spec)   ←── SP-2 (grammar)   ←── SP-3 (submit + CLI)
   │
   └── 完了後の判断ポイント: SQLite インデックス導入 (今は未採用)
```

---

## 2. 採用アプローチ: **Approach A — Pure-Rust データ層 + 薄い PyO3**

### 2.1 比較した 3 案

| 比較項目 | A (採用) | B (SQLite 永続インデックス) | C (Python 経由) |
|---|---|---|---|
| Rust 側責務 | I/O + 検索 + フィルタ + tick | A に加え `<root>/.index.sqlite` の lazy 構築 | TOML serde のみ |
| 検索計算量 | O(N) ファイル並列読み | O(log N) (インデックスあり) | 旧 Python 相当 |
| 初期実装コスト | 中 | 高 (スキーマ + リコンサイル) | 低 |
| ファイル/インデックス整合性責任 | 不要 (FS が SoT) | 要 (インデックス更新リトライ) | 不要 |
| 将来のスケール | 〜1000 JobFlows まで実用 | 1000+ JobFlows でも O(log N) | 数百で破綻 |
| ユーザー要件「remake」適合性 | ✅ 高速・型安全 | ✅ さらに高速 | ❌ Python 版相当 |

**判断:**
- C は再実装の動機を満たさない。
- B は SQLite スキーマ管理コスト・FS とのリコンサイル責任が SP-1 の本筋でない。
- A で並列 I/O により実用範囲をカバーし、SP-2/SP-3 完了後に B を別 spec として追加可能 (FS が SoT のままインデックスを後付けできる構造)。

### 2.2 案 A の設計判断

- **Filesystem is single source of truth.** SQLite/インメモリインデックスは持たない。
- **並列読みは tokio + `futures::stream::buffer_unordered`** で実現。デフォルト並列度は CPU 並列度 (`tokio::runtime::Handle::current().metrics().num_workers()`) を採用予定だが、ベンチ後に調整可能な定数とする。
- **`flow.toml` に runtime status を埋め込まない** (D2 を変更しない方針)。各 Job の status は別ファイル `status/<job_id>.toml` に分離。理由: D2 は別パッケージ、SP-1 でスキーマ変更 PR を出すのは依存サイクル管理上重い。
- **SLURM 問い合わせは A1 (`slurm-async-runner`) を Rust 側で直接呼ぶ。** `pyo3-async-runtimes` 経由で Python から async 公開。`asyncio.run` per call は完全排除。

---

## 3. ディレクトリレイアウト (永続データ)

```
<work_dir>/
├── <flow_uuid_A>/
│   ├── flow.toml                  # JobFlow (D2 スキーマ、atomic write)
│   ├── status/                    # per-Job runtime status
│   │   ├── g16.toml               # PerJobStatusEntry for JobId="g16"
│   │   └── post.toml
│   ├── jobs/                      # per-Job 作業ディレクトリ (SP-3 で書き込む側、SP-1 は読み only)
│   │   ├── g16/
│   │   │   ├── input.gjf          # SP-3 担当
│   │   │   ├── batch_g16.bash     # SP-3 担当
│   │   │   └── slurm-<jobid>.out  # SLURM 直書き
│   │   └── post/
│   └── derived/                   # 解析結果 (将来; SP-1 は触れない)
├── <flow_uuid_B>/
│   └── ...
```

**判断: `flow.toml` 単一ファイルに JobFlow 全体を持つ。** 旧 D1 の per-uuid `metadata.toml` 分散ではなく、JobFlow 単位で 1 ファイル。理由: 1 つの実験 = 1 つの JobFlow という新スキーマの単位を踏襲、TOML パース回数 = #experiments で済む。

---

## 4. Rust モジュール構成

```
job_manager/
├── Cargo.toml                     # gaussian_job_shared, slurm_async_runner を path 依存追加
├── src/
│   ├── lib.rs                     # re-exports
│   ├── error.rs                   # 既存 (拡張)
│   ├── path.rs                    # PathResolver
│   ├── flow_io.rs                 # read_flow / write_flow (atomic rename)
│   ├── status/
│   │   ├── mod.rs                 # PerJobStatus enum + StatusEntry
│   │   └── io.rs                  # read_status / write_status (atomic)
│   ├── walk.rs                    # walk_flows (Stream)
│   ├── filter.rs                  # SearchFilter + matches()
│   ├── tick.rs                    # tick_many + transition decision
│   ├── view.rs                    # CalcView (per-Job facade)
│   ├── slurm_facade.rs            # trait + A1 adapter (mockable)
│   ├── bin/stub_gen.rs            # 既存
│   └── py_export/
│       ├── mod.rs                 # 既存 (sub-module wiring を追加)
│       ├── error.rs               # 既存
│       ├── path.rs                # PathResolver pyclass
│       ├── filter.rs              # SearchFilter pyclass
│       ├── walk.rs                # walk_flows pyfunction (async)
│       ├── tick.rs                # tick_many pyfunction (async)
│       └── view.rs                # CalcView pyclass
```

### 4.1 主要型のシグネチャ

```rust
// path.rs
pub struct PathResolver { root: PathBuf }
impl PathResolver {
    pub fn new(root: PathBuf) -> Self;
    pub fn flow_dir(&self, flow_uuid: &Uuid) -> PathBuf;
    pub fn flow_toml(&self, flow_uuid: &Uuid) -> PathBuf;
    pub fn status_dir(&self, flow_uuid: &Uuid) -> PathBuf;
    pub fn status_file(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf;
    pub fn job_dir(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf;
}

// status/mod.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PerJobStatus { Queued, Running, Done, Failed }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusEntry {
    pub status: PerJobStatus,
    pub updated_at: DateTime<Utc>,
    pub slurm_jobid: Option<u64>,
    pub note: Option<String>,
}

// flow_io.rs
pub fn read_flow(path: &Path) -> Result<JobFlow, JobManagerError>;
pub fn write_flow(path: &Path, flow: &JobFlow) -> Result<(), JobManagerError>; // atomic rename

// walk.rs — async stream
pub fn walk_flows(root: &Path)
    -> impl Stream<Item = Result<JobFlow, JobManagerError>>;

// filter.rs
#[derive(Debug, Clone, Default)]
pub struct SearchFilter {
    pub program: Option<Program>,
    pub tags: BTreeMap<String, String>,
    pub status: Option<PerJobStatus>,
    pub flow_uuid_prefix: Option<String>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub slurm_jobid: Option<u64>,
    pub job_id: Option<JobId>,
}

pub fn matches(flow: &JobFlow, job_id: &JobId, status: Option<&StatusEntry>, f: &SearchFilter) -> bool;

// slurm_facade.rs — trait for mockability
//
// 型注: A1 の `SlurmManager::query_job_states_batch` の戻り型 `HashMap<u64, JobStatus>`
// をそのまま透過する。`JobStatus` は `(state: JobState, reason: JobReason)` のペア。
// tick.rs の `decide_transition` は `JobState` 単体を見るだけなので、呼び出し側で
// `.state` を抽出して渡す。
#[async_trait::async_trait]
pub trait SlurmFacade: Send + Sync {
    async fn query_states_batch(&self, jobids: &[u64])
        -> Result<HashMap<u64, JobStatus>, JobManagerError>;
}

// concrete impl — A1 の低レベル SlurmManager を保持
// (SbatchManager は spawn/cancel 中心、状態クエリは SlurmManager の仕事)
pub struct A1SlurmFacade { manager: Arc<SlurmManager> }

// tick.rs
#[derive(Debug, Clone)]
pub struct TickResult {
    pub flow_uuid: Uuid,
    pub job_id: JobId,
    pub previous: Option<PerJobStatus>,
    pub new: Option<PerJobStatus>,
    pub slurm_state: Option<JobState>,
    pub queried_jobid: Option<u64>,
    pub note: String,
}

pub async fn tick_many(
    targets: &[(Uuid, JobId, u64)],
    slurm: &dyn SlurmFacade,
    resolver: &PathResolver,
) -> Vec<TickResult>;

// view.rs
pub struct CalcView<'a> {
    pub flow: &'a JobFlow,
    pub job_id: JobId,
    resolver: &'a PathResolver,
}
impl<'a> CalcView<'a> {
    pub fn job(&self) -> &'a Job;
    pub fn status(&self) -> Result<StatusEntry, JobManagerError>;
    pub fn input_files(&self) -> Result<Vec<PathBuf>, JobManagerError>;
    pub fn output_files(&self) -> Result<Vec<PathBuf>, JobManagerError>;
    pub fn job_dir(&self) -> PathBuf;
}
```

### 4.2 エラー型

```rust
#[derive(Debug, thiserror::Error)]
pub enum JobManagerError {
    #[error("io error at {path}: {source}")]
    Io { path: PathBuf, #[source] source: std::io::Error },

    #[error("toml parse error at {path}: {source}")]
    TomlParse { path: PathBuf, #[source] source: toml::de::Error },

    #[error("toml serialize error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("flow uuid {0} not found under {1}")]
    FlowNotFound(Uuid, PathBuf),

    #[error("job id {0} not found in flow {1}")]
    JobNotFound(JobId, Uuid),

    #[error("status file missing for {flow}/{job}")]
    StatusNotFound { flow: Uuid, job: JobId },

    #[error("slurm facade error: {0}")]
    Slurm(String),
}
```

---

## 5. Status 状態遷移ロジック (tick)

Python 版 `_decide_transition` を踏襲しつつ簡素化。invariants:

1. **`done` を tick が書くことはない。** `Done` は SP-3 で post-script が書く想定 (ステータス書き込みの唯一の権威)。
2. **terminal 状態は上書きしない。** prev ∈ {Done, Failed} の時は遷移しない。
3. **SLURM 側の失敗 (FAILED / CANCELLED / TIMEOUT / NODE_FAIL / OUT_OF_MEMORY / PREEMPTED / BOOT_FAIL / DEADLINE) で local 非 terminal なら Failed に遷移。**
4. **SLURM UNKNOWN (jobid expire) かつ local 非 terminal は warning + no-op** (orphan)。
5. **SLURM COMPLETED かつ local Queued/Running は no-op + warning** (post.bash がまだ done を書いていない過渡状態の可能性)。

擬似コード:

```rust
fn decide_transition(prev: Option<PerJobStatus>, slurm: Option<JobState>)
    -> (Option<PerJobStatus>, &'static str)
{
    use PerJobStatus::*;
    let prev = prev;
    match slurm {
        None => (prev, "no slurm_jobid"),
        Some(JobState::Pending) => match prev {
            Some(Running) => (Some(Queued), "warning: regressed running→queued"),
            _ => (Some(Queued), "synced: pending"),
        },
        Some(JobState::Running) | Some(JobState::Completing) | Some(JobState::Suspended) => {
            match prev {
                Some(Done) | Some(Failed) => (prev, "warning: SLURM running but local terminal"),
                _ => (Some(Running), "promoted to running"),
            }
        },
        Some(JobState::Completed) => match prev {
            Some(Done) => (Some(Done), "unchanged"),
            Some(Failed) => (Some(Failed), "warning: SLURM completed but local failed"),
            _ => (prev, "warning: SLURM completed but post.bash didn't write done"),
        },
        Some(s) if s.is_failed_terminal() => match prev {
            Some(Done) => (Some(Done), "warning: SLURM failed but local done"),
            _ => (Some(Failed), "synced: failed-terminal"),
        },
        Some(JobState::Unknown) => match prev {
            Some(Done) | Some(Failed) => (prev, "unchanged: jobid expired"),
            _ => (prev, "orphan: manual investigation needed"),
        },
        _ => (prev, "unhandled slurm state"),
    }
}
```

`JobState::is_failed_terminal()` ヘルパは A1 側で持っていない場合、本 crate で extension trait として定義する。

---

## 6. 並列化ポリシー

| 操作 | 並列化 | デフォルト並列度 |
|---|---|---|
| `walk_flows` ファイル読み | `buffer_unordered(N)` | `min(64, num_workers * 4)` |
| `tick_many` SLURM 問い合わせ | バッチ化 1 回 (A1 既存 API) | 1 (バルク) |
| `tick_many` status 書き込み | `buffer_unordered(N)` | `min(32, num_workers * 2)` |

具体的な数値は SP-1 完了時の自前ベンチで決定。`std::env::var("JOB_MANAGER_PARALLELISM")` で override 可能にする。

---

## 7. Python API (PyO3)

```python
from job_manager import (
    PathResolver,
    SearchFilter,
    PerJobStatus,
    StatusEntry,
    walk_flows,        # async generator (pyo3-async-runtimes)
    search,            # async, takes (root, filter) -> list[(JobFlow, JobId, StatusEntry|None)]
    tick_many,         # async, takes targets list -> list[TickResult]
    CalcView,
)
from gaussian_job_shared import JobFlow, Job, JobSpec, JobId      # D2 から re-export

resolver = PathResolver("/path/to/work_dir")

# 1. JobFlow 列挙
async for flow in walk_flows("/path/to/work_dir"):
    print(flow.uuid, flow.tags)

# 2. 検索
flt = SearchFilter(program="g16", status=PerJobStatus.QUEUED)
hits = await search("/path/to/work_dir", flt)
for flow, job_id, status_entry in hits:
    print(flow.uuid, job_id, status_entry)

# 3. tick
import asyncio
targets = [(flow.uuid, JobId("g16"), 123456)]  # (flow_uuid, job_id, slurm_jobid)
results = await tick_many(resolver, targets)

# 4. CalcView (per-Job facade)
view = CalcView(resolver, flow, JobId("g16"))
print(view.status())
print(view.input_files())
```

**設計判断:**
- Python 側で `JobFlow` 等の型は **`gaussian_job_shared` から re-import** するだけ (Pyclass Single Owner 規約に従う)。`job_manager` は自前で `JobFlow` pyclass を持たない。
- `walk_flows` / `search` / `tick_many` は async (pyo3-async-runtimes)。Python 側は `await` または `asyncio.run()`。

---

## 8. テスト計画

### 8.1 Unit tests (Rust 側, `#[cfg(test)]`)

- `path.rs`: パス組み立てが UUID / JobId で正しく
- `flow_io.rs`: TOML round-trip、atomic rename (renameat への置換確認は integration)
- `status/io.rs`: round-trip、欠損時の `StatusNotFound`
- `filter.rs`: 各フィルタ単体 AND の挙動 (rstest で組み合わせパラメタライズ)
- `tick.rs::decide_transition`: SLURM × prev の全組み合わせを rstest で網羅
- `walk.rs`: tempdir に N 件並べて完全列挙確認

### 8.2 Integration tests (`tests/`)

- 100 個の `JobFlow` を temp dir に書き、`walk_flows` で完全列挙される
- `tick_many` を mock SlurmFacade で実行、status ファイル更新が確認できる
- 並列 write race 確認 (atomic rename の検証)

### 8.3 Python tests (`python/tests/`)

- pyo3 経由で `walk_flows` を `asyncio.run` で実行
- D2 (`gaussian_job_shared`) からの `JobFlow` 直接渡しが通る
- `CalcView` の各 getter が `PathResolver` 経由で正しいパスを返す

### 8.4 カバレッジ目標

`cargo llvm-cov --fail-under-lines 80` で 80%+。Python 側は pytest で stub + 統合カバー。

---

## 9. リスクと未決事項

| 項目 | リスク | 対応 |
|---|---|---|
| D2 の path 依存 | `gaussian-job-shared2` ディレクトリ名変更 (`2` サフィックス) | `Cargo.toml` で local path 指定、いずれ git 依存に切り替え |
| A1 の `JobState` enum の網羅性 | A1 が未対応の SLURM state | `_ => (prev, "unhandled")` で安全側 fallthrough |
| status を別ファイルにする選択 | 書き込み race (`flow.toml` 書き換えと同時) | SP-1 では `flow.toml` は読み only、status はタイムスタンプ + atomic rename で衝突回避 |
| `walk_flows` の並列度 | ファイルシステムによってはディスク IO bound | env var override + ベンチ後調整 |
| pyo3-async-runtimes 統合 | A1 と同じ tokio ランタイム共有要 | A1 と同様の `[features]` 設定を踏襲 (slurm-async-runner2 を参照) |

---

## 10. 完了基準

- [ ] `cargo build --all-features` 成功
- [ ] `cargo test --lib` 成功 (カバレッジ 80%+)
- [ ] `cargo clippy -- -D warnings` 成功
- [ ] `cargo fmt --check` 成功
- [ ] `uv run maturin develop` 成功
- [ ] `uv run pytest python/tests` 成功
- [ ] `cargo run --bin stub_gen` で `.pyi` 再生成、`ruff format` クリーン
- [ ] `walk_flows` を 100 件のテンプディレクトリで実行し sub-second 完了
- [ ] tick の mock テストで全 SLURM state を網羅

## 11. 次工程

SP-1 完了後:
- **SP-2 (grammar)**: experiment.toml → `JobFlow` 変換、sweep 展開、parent 解決
- **SP-3 (submit + CLI)**: A1 `SbatchManager` 経由の `submit_chain` 相当、CLI ラッパ

SP-1 設計が承認されたら writing-plans skill で実装計画書に変換します。
