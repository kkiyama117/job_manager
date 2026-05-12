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

D2 (`gaussian-job-shared2`) の `JobFlow.work_dir` docstring に従う:

> Working directory: `<work_dir>/<JobId>/` is each Job's folder.
> TaskManager creates these and writes the rendered `.bash` etc.

つまり **`flow.work_dir` の直下に `<JobId>/` サブディレクトリ**が並ぶ。`jobs/` 中間レイヤは入れない。

```
<root>/                            # PathResolver.root (job-manager の検索ルート)
├── <flow_uuid_A>/                 # = JobFlow.work_dir (規約: root / flow.uuid.to_string())
│   ├── flow.toml                  # JobFlow (D2 スキーマ、atomic write)
│   ├── g16/                       # = flow.work_dir / "g16" (D2 規約)
│   │   ├── .status.toml           # per-Job runtime status (SP-1 atomic write)
│   │   ├── input.gjf              # SP-3 担当
│   │   ├── batch_g16.bash         # SP-3 担当
│   │   └── slurm-<jobid>.out      # SLURM 直書き
│   ├── post/
│   │   ├── .status.toml
│   │   └── ...
│   └── derived/                   # 解析結果 (将来; SP-1 は触れない、JobId="derived" との衝突は禁止規約)
├── <flow_uuid_B>/
│   └── ...
```

### 設計判断 (Layout)

1. **`flow.work_dir == <root>/<flow.uuid>/`** を job-manager 側の規約とする。PathResolver の `flow_dir(uuid)` がこの不変条件を保つ。これによりファイルシステム位置と D2 の `flow.work_dir` 値が常に一致する。
2. **`flow.toml` 単一ファイルに JobFlow 全体を持つ。** 旧 D1 の per-uuid `metadata.toml` 分散ではなく、JobFlow 単位で 1 ファイル。TOML パース回数 = #JobFlows で済む。
3. **status は Job 直下の隠しファイル `<flow.work_dir>/<JobId>/.status.toml`。** 別ディレクトリの `status/` レイヤを設けると `JobId="status"` との衝突可能性があるため、Job dir 内側にネストしてスコープを限定する。
4. **`derived/` は予約名。** ユーザーが `JobId="derived"` を選ぶと衝突するため、grammar 層 (SP-2) で予約名チェックする (本 spec の範囲外、TODO リスト化)。

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

#### 上流からの import

```rust
use gaussian_job_shared::entities::workflow::{
    JobFlow, Job, JobSpec, JobEdge, JobId, Program, CalcType,
};
use slurm_async_runner::{
    JobState, JobStatus, JobReason,            // re-export at crate root
    SlurmManager, SlurmCmd,                    // batch query manager
};
```

#### 型定義

```rust
// path.rs
//
// 規約: 各 JobFlow のディレクトリ = root / <flow.uuid>。
// そのまま JobFlow.work_dir に書き戻されるので、ファイル位置と
// JobFlow 内の `work_dir` 値は常に一致する (PathResolver 経由で保つ)。
pub struct PathResolver { root: PathBuf }
impl PathResolver {
    pub fn new(root: PathBuf) -> Self;
    pub fn root(&self) -> &Path;
    pub fn flow_dir(&self, flow_uuid: &Uuid) -> PathBuf;        // = root / uuid.to_string()
    pub fn flow_toml(&self, flow_uuid: &Uuid) -> PathBuf;       // = flow_dir / "flow.toml"
    pub fn job_dir(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf;
                                                                 // = flow_dir / job_id.to_string()
    pub fn status_file(&self, flow_uuid: &Uuid, job_id: &JobId) -> PathBuf;
                                                                 // = job_dir / ".status.toml"
}

// status/mod.rs
//
// PerJobStatus は user-visible な 4 状態へ集約。SLURM の生 (state, reason) は
// 同じ StatusEntry に `slurm_status: Option<JobStatus>` として保持し、UI / debug
// から取り出せるようにする (失敗理由の表示等)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PerJobStatus { Queued, Running, Done, Failed }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusEntry {
    pub lifecycle: PerJobStatus,
    pub updated_at: DateTime<Utc>,
    pub slurm_jobid: Option<u64>,
    /// A1 から取得した (state, reason)。tick で書き換えられる。
    /// `JobStatus` 自体は A1 の `slurm_async_runner::entities::slurm::status::JobStatus`。
    pub slurm_status: Option<JobStatus>,
    pub note: Option<String>,
}

// flow_io.rs
//
// JobFlow 自体は D2 が所有 (pyclass single owner)。job-manager は serde I/O のみ。
pub fn read_flow(path: &Path) -> Result<JobFlow, JobManagerError>;
pub fn write_flow(path: &Path, flow: &JobFlow) -> Result<(), JobManagerError>; // atomic rename

// walk.rs — async stream
pub fn walk_flows(root: &Path)
    -> impl Stream<Item = Result<JobFlow, JobManagerError>>;

// filter.rs
//
// `tags` は D2 の `JobFlow.tags: BTreeMap<String,String>` に揃える。
// `status` フィルタは集約された PerJobStatus、`slurm_state` フィルタは
// (SP-1 では不採用、SP-2 以降で追加余地) — リッチ JobStatus 全体での絞り込みは
// 必要性が出てから拡張する。
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
// 設計上のメモ:
// - A1 の `SlurmManager::query_job_states_batch(&[u64]) -> anyhow::Result<HashMap<u64, JobStatus>>`
//   を adapter で包んで `JobManagerError::Slurm(String)` に正規化する。
// - tick.rs の `decide_transition` は `JobState` 単体で十分なので、呼び側で
//   `JobStatus.state` を取り出して渡す。`JobStatus.reason` は `StatusEntry.slurm_status`
//   に保存され、UI/debug で表示される。
// - `SlurmManager` は `Clone + Default` で軽量なので `Arc` は必須ではないが、
//   Python から複数の facade を構築するシナリオで参照を共有しやすいよう保持する。
#[async_trait::async_trait]
pub trait SlurmFacade: Send + Sync {
    async fn query_states_batch(&self, jobids: &[u64])
        -> Result<HashMap<u64, JobStatus>, JobManagerError>;
}

pub struct A1SlurmFacade { manager: Arc<SlurmManager> }

// tick.rs
#[derive(Debug, Clone)]
pub struct TickResult {
    pub flow_uuid: Uuid,
    pub job_id: JobId,
    pub previous: Option<PerJobStatus>,
    pub new: Option<PerJobStatus>,
    /// SLURM から取得した最新 (state, reason)。`note` 生成にも使う。
    pub slurm_status: Option<JobStatus>,
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
    pub fn job(&self) -> &'a Job;                                       // BTreeMap lookup
    pub fn status(&self) -> Result<StatusEntry, JobManagerError>;        // .status.toml read
    pub fn input_files(&self) -> Result<Vec<PathBuf>, JobManagerError>;  // job_dir 内 *.gjf
    pub fn output_files(&self) -> Result<Vec<PathBuf>, JobManagerError>; // job_dir 内 *.log / *.out
    pub fn job_dir(&self) -> PathBuf;                                    // PathResolver 経由
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

Python 版 `_decide_transition` を踏襲しつつ、A1 が公開しているヘルパで分類を簡素化する。

### 5.1 不変条件 (invariants)

1. **`Done` を tick が書くことはない。** `Done` は SP-3 で post-script が書く想定 (ステータス書き込みの唯一の権威)。
2. **terminal 状態は上書きしない。** prev ∈ {Done, Failed} の時、tick は status を遷移させない (warning note のみ更新)。
3. **SLURM 側の失敗 terminal (= `is_terminal() && !Completed`) で local 非 terminal なら Failed に遷移。**
4. **SLURM `Completed` かつ local Queued/Running は no-op + warning** (post.bash がまだ done を書いていない過渡状態の可能性)。
5. **SLURM `Unknown` (jobid expire) かつ local 非 terminal は warning + no-op** (orphan)。

### 5.2 A1 のヘルパを使う

A1 `JobState` には以下のヘルパが定義されている (`slurm-async-runner2/src/entities/slurm/status.rs`):

- `is_running()` → `Running` のみ true。
- `is_terminal()` → `Completed`, `BootFail`, `Cancelled`, `Deadline`, `Failed`, `NodeFail`, `OutOfMemory`, `Preempted`, `Revoked`, `SpecialExit`, `Timeout`。

job-manager 側で 1 つだけ拡張 trait を追加する:

```rust
pub trait JobStateExt {
    /// terminal かつ Completed ではない (= 失敗終端) の判定。
    fn is_failed_terminal(&self) -> bool;
}

impl JobStateExt for JobState {
    fn is_failed_terminal(&self) -> bool {
        self.is_terminal() && !matches!(self, JobState::Completed)
    }
}
```

### 5.3 遷移擬似コード

```rust
fn decide_transition(
    prev: Option<PerJobStatus>,
    slurm: Option<JobStatus>,                                   // 生の (state, reason) を受け取る
) -> (Option<PerJobStatus>, String) {
    use PerJobStatus::*;
    let Some(status) = slurm else {
        return (prev, "no slurm_jobid".to_string());
    };
    let state = status.state;
    let reason = status.reason.as_str();                        // log message 用

    // ---- terminal failure ----
    if state.is_failed_terminal() {
        return match prev {
            Some(Done) => (Some(Done), format!("warning: SLURM {state} but local done")),
            _ => (Some(Failed), format!("synced: failed-terminal {state} ({reason})")),
        };
    }

    // ---- terminal success (Completed) ----
    if matches!(state, JobState::Completed) {
        return match prev {
            Some(Done) => (Some(Done), "unchanged".to_string()),
            Some(Failed) => (Some(Failed), "warning: SLURM completed but local failed".to_string()),
            _ => (prev, "warning: SLURM completed but post.bash didn't write done".to_string()),
        };
    }

    // ---- alive (Running / Completing / Resizing / Signaling / StageOut / Suspended) ----
    // A1 の is_running() は Running のみだが、scheduler 視点で "compute を使っている/開始可能"
    // をまとめたい。Running 同等にまとめる variant を明示列挙する。
    let is_alive = matches!(
        state,
        JobState::Running
            | JobState::Completing
            | JobState::Resizing
            | JobState::Signaling
            | JobState::StageOut
            | JobState::Suspended
    );
    if is_alive {
        return match prev {
            Some(Done) | Some(Failed) => (prev, format!("warning: SLURM {state} but local terminal")),
            _ => (Some(Running), format!("promoted to running ({state})")),
        };
    }

    // ---- queued / configuring / requeued / hold (= まだ実行されていない) ----
    let is_pending = matches!(
        state,
        JobState::Pending
            | JobState::Configuring
            | JobState::Requeued
            | JobState::RequeueFed
            | JobState::RequeueHold
            | JobState::ResvDelHold
            | JobState::Stopped
    );
    if is_pending {
        return match prev {
            Some(Running) => (Some(Queued), format!("warning: regressed running→queued ({state})")),
            _ => (Some(Queued), format!("synced: pending ({state}, {reason})")),
        };
    }

    // ---- Unknown (jobid expire / forward-compat) ----
    match prev {
        Some(Done) | Some(Failed) => (prev, "unchanged: jobid expired".to_string()),
        _ => (prev, "orphan: manual investigation needed".to_string()),
    }
}
```

この実装は A1 の `JobState` 24 variant + `Unknown` を全て if/match で網羅する (cargo clippy `non_exhaustive_omitted_patterns` で確認可能)。

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
| D2 が SAR を git URL で参照 | path-dep を素朴に書くと、D2 の SAR (git) と job-manager の SAR (path) が **別 crate と認識される** → 型不一致 (`DependencyType`, `JobStatus` 等が `expected X, found X` で衝突) | job-manager の `Cargo.toml` に `[patch."https://github.com/kkiyama117/slurm-async-runner.git"]` で `slurm_async_runner = { path = "../slurm-async-runner2" }` を上書き |
| ディレクトリ名 `*-2` サフィックス | パッケージ昇格時にディレクトリ rename される可能性 | `Cargo.toml` の path を 1 箇所に集約 (ワークスペース化は本 spec 範囲外) |
| A1 の `JobState` enum の前向き互換 | A1 が将来 variant を追加した場合 | `decide_transition` は明示分類後の fallthrough を `Unknown` に倒す。clippy `non_exhaustive_omitted_patterns` を CI で有効化 |
| status を Job dir 内側に置く選択 | `JobId="derived"` などの予約名と衝突可能性 | SP-2 (grammar) で予約名 (`derived`, `flow.toml`) をブロック。`.status.toml` は dot 接頭辞で SLURM 出力 (`slurm-*.out` 等) と区別 |
| `walk_flows` の並列度 | ファイルシステムによってはディスク IO bound | `JOB_MANAGER_PARALLELISM` env で override + ベンチ後調整 |
| pyo3-async-runtimes 統合 | A1 と同じ tokio ランタイム共有要 | A1 と同様の `[features]` 設定を踏襲 (slurm-async-runner2 を参照) |
| D2 を経由した pyclass 利用 | D2 が pyclass を所有 (single-owner ルール)。`from gaussian_job_shared import JobFlow` は D2 の wheel 経由でのみ動く | SP-1 の Python テストでは D2 wheel を `pip install -e ../gaussian-job-shared2` でロード前提 (pyproject に dev-dep として記載) |

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
