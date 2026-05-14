# job-manager SP-3 re-architecture 設計 v2

- **Date**: 2026-05-13
- **Status**: Draft (brainstorming 出力、レビュー待ち)
- **Supersedes**: `docs/superpowers/specs/2026-05-13-job-manager-sp3-design.md` (v1, PR #10)
- **Targets**: job-manager `src/` 全面 re-org + 新型 `FlowRun` / `JobRun` / `Lifecycle` / `Executor` / `Querier` / `FlowRunner` + CLI `jm`
- **References**:
  - 上流 (D2): `../../../gaussian-job-shared2/` (newtype 不可侵、struct shape は restructurable)
  - 上流 (A1): `../../../slurm-async-runner2/` (struct/trait/function **完全不可侵**)
  - Orchestration 参考: `docs/references/orchestration-systems.md`
  - 旧 SP-1 設計: `docs/superpowers/specs/2026-05-12-job-manager-sp1-design.md`
  - 旧 SP-2 設計: `docs/superpowers/specs/2026-05-12-job-manager-sp2-design.md`
  - 旧 SP-3 v1: `docs/superpowers/specs/2026-05-13-job-manager-sp3-design.md`

---

## 1. 背景と目的

SP-3 v1 (PR #10) は `submit_chain` を単体関数で起こす方針だった。本 v2 では `docs/references/orchestration-systems.md` で整理した Airflow / Prefect の知見を踏まえて **job-manager の内部 flow を再構築**する。具体的には:

1. **`Executor` trait** を導入して sbatch submit を抽象化 (`SbatchExecutor` / `DryRunExecutor` / `MockExecutor`)。テスト性と `--dry-run` を自然に解決
2. **`FlowRun` aggregate** を導入して `flow_uuid` 単位の値を統一的に扱う (Airflow DAG Run / Prefect Flow Run 相当)
3. **`Lifecycle` state machine** を Airflow に倣って 4 値 → 5 値に拡張 (`Skipped` 追加)
4. **既存 SP-1/SP-2 モジュールも再編** — `slurm_facade` → `slurm::querier`、`StatusEntry` → `JobRun`、`PerJobStatus` → `Lifecycle`、`flow_io`/`plan/io`/`status/io` を `persistence/` に集約

**不可侵制約:**
- **A1 (`slurm-async-runner`)**: 変更ゼロ。`SbatchCmd` / `SbatchManager` / `SbatchJobHandle` / `SlurmManager` / `JobStatus` / `DependencyType` / `SlurmDependency` / `ResourceSpec` をそのまま使う
- **D2 (`gaussian-job-shared`)**: newtype (`JobId`, `Program` 等) 不可侵。struct shape (`CommonConfig`, `JobFlow`, `JobEdge`) は restructurable だが、本 SP-3 では Phase 0 PR で `CommonConfig` + `DirectoryConfig` に serde derives 追加 + `#[serde(deny_unknown_fields)]` のみ

### 1.1 想定ユーザーフロー (v1 と同じ)

```bash
# 1. flow 構築 (Python authoring)
python build_my_experiment.py

# 2. (dry-run) render batch.bash
jm run /work/<flow_uuid>

# 3. submit to SLURM
jm submit /work/<flow_uuid>

# 4. poll status
jm tick /work/<flow_uuid>

# 5. inspect
jm show /work/<flow_uuid>

# 6. cross-flow search
jm search /work --program g16 --status failed
```

### 1.2 スコープ

| 含める | 含めない |
|---|---|
| 新規 `Executor` / `Querier` trait + 各 3-2 impls | 並列 sbatch (順次のみ、依存先 jobid 未確定のため) |
| `FlowRun` / `FlowRunner` / `Lifecycle` / `JobRun` | webhook trigger / 常駐 worker daemon |
| 既存 SP-1/SP-2 の rename + 新モジュール構成への移行 | grammar DSL (SP-2 で削除済み) |
| `.status.toml` schema 書き換え (snake_case + `success`/`skipped` 値) | `.status.toml` migration (SP-3 はリリース前なので migration 不要) |
| `common.toml` の root-level 配置 + serde 読み書き | per-flow common.toml |
| CLI `jm`: `run` / `submit` / `show` / `tick` / `search` | TUI / ncurses UI |
| Python pyfunctions の新型対応 | Python 側からの CLI 起動 |

---

## 2. アーキテクチャ概観

### 2.1 概念図 (Airflow / Prefect 語彙アライン)

```
┌──────────────────────────────────────────────────────────────────┐
│  TOML files (file-based persistence — Airflow metadata DB 代替)  │
│   <root>/common.toml                                             │
│   <root>/<flow_uuid>/{flow.toml, plan.toml, <jid>/{.status.toml, │
│                                                    batch.bash}}  │
└──────────────────────────────────────────────────────────────────┘
                    ▲                              ▲
                    │ read / write                  │ read / write
                    │                              │
       ┌────────────┴────────────┐    ┌────────────┴────────────┐
       │  FlowRun (aggregate)    │    │  JobRun (per-job state) │
       │   flow_uuid, JobFlow,   │    │   Lifecycle, slurm_jobid│
       │   ExperimentPlan,       │    │   slurm_status, ...     │
       │   Option<CommonConfig>  │    │  (= 旧 StatusEntry)     │
       └────────────┬────────────┘    └────────────┬────────────┘
                    │                              │
                    ▼                              ▼
       ┌──────────────────────────────────────────────────┐
       │  FlowRunner  (= 旧 submit_chain の置換)          │
       │   - Box<dyn Executor>                            │
       │   - Box<dyn Querier>                             │
       │   - &PathResolver                                │
       │   methods: submit(), tick(), render_only()       │
       └─────────┬─────────────────────────────┬──────────┘
                 │                             │
                 ▼ submit                      ▼ query
       ┌────────────────────┐     ┌────────────────────┐
       │ Executor (trait)   │     │ Querier (trait)    │
       │ ─ SbatchExecutor   │     │ ─ SlurmQuerier     │
       │ ─ DryRunExecutor   │     │ ─ InMemoryQuerier  │
       │ ─ MockExecutor     │     │ ─ MockQuerier      │
       │      ▲             │     │      ▲             │
       └──────┼─────────────┘     └──────┼─────────────┘
              │ A1 SbatchManager          │ A1 SlurmManager
              │ ::spawn().await           │ ::query_job_states_batch
              ▼                           ▼
       ┌──────────────────────────────────────────────────┐
       │  slurm-async-runner (A1) — UNTOUCHABLE           │
       │  SbatchCmd / SbatchManager / SbatchJobHandle /   │
       │  SlurmManager / JobStatus / DependencyType /     │
       │  SlurmDependency / ResourceSpec ...              │
       └──────────────────────────────────────────────────┘
```

**設計の核:**

- **`FlowRun`** = `flow_uuid` ディレクトリに対応する読み取り専用 aggregate。CLI / Python API / 内部実装すべてが持ち回す
- **`JobRun`** = `.status.toml` の persistence 単位 (Airflow TaskInstance / Prefect Task Run 相当)
- **`Executor` trait** (submit) と **`Querier` trait** (sacct query) で SLURM 接触面を 2 方向に分離
- **`FlowRunner`** が submit / tick / render_only の 3 method を持つ唯一のオーケストレータ

### 2.2 モジュール構成 (新)

```
src/
├── lib.rs                # MODIFY: re-export 大幅変更
├── error.rs              # MODIFY: 新 variants 追加
├── concurrency.rs        # KEEP: atomic write helpers (汎用)
├── jobid.rs              # KEEP: JobId / build_job_id / parse_job_id
│
├── flow/                 # NEW: Flow aggregate
│   ├── mod.rs
│   ├── run.rs            #   FlowRun struct + methods
│   └── topology.rs       #   Kahn's algorithm + cycle detection
│
├── job/                  # NEW: per-job state (旧 status/ を re-org)
│   ├── mod.rs
│   ├── lifecycle.rs      #   Lifecycle enum (5 値) + transition rules
│   └── run.rs            #   JobRun struct (= 旧 StatusEntry)
│
├── slurm/                # NEW: A1 接触面を全部集約
│   ├── mod.rs
│   ├── executor.rs       #   Executor trait + SbatchExecutor/DryRunExecutor/MockExecutor
│   ├── querier.rs        #   Querier trait + SlurmQuerier/InMemoryQuerier/MockQuerier
│   └── dependency.rs     #   JobEdge[] + submitted → SlurmDependency
│
├── persistence/          # NEW: 全ての file I/O を集約
│   ├── mod.rs
│   ├── path.rs           #   PathResolver (= 旧 path.rs, common_toml/batch_bash getter 追加)
│   ├── flow.rs           #   read_flow / write_flow (= 旧 flow_io.rs)
│   ├── plan.rs           #   read_plan / write_plan (= 旧 plan/io.rs)
│   ├── common.rs         #   NEW: read_common / write_common
│   └── job_run.rs        #   read_job_run / write_job_run (= 旧 status/io.rs)
│
├── plan/                 # MODIFY: ExperimentPlan struct のみ残す
│   └── mod.rs            #   io は persistence/plan.rs に移動済
│
├── render/               # NEW: bash render
│   └── mod.rs            #   render_batch_bash + sanitize_var_name + quote_for_bash
│
├── runner/               # NEW: オーケストレーション
│   ├── mod.rs
│   ├── flow.rs           #   FlowRunner struct + submit/tick/render_only methods
│   └── transition.rs     #   decide_transition (新 lifecycle 対応、旧 tick.rs から移植)
│
├── walk.rs               # MODIFY: walk_flows は (uuid, path) を yields のまま
├── search.rs             # MOVED: filter.rs を rename、SearchFilter は新 Lifecycle 対応
├── view.rs               # MODIFY: CalcView を簡素化または FlowRun に吸収 (Phase A で判断)
│
├── bin/
│   └── jm.rs             # NEW: clap CLI + 5 subcommands
│
└── py_export/            # MODIFY: 新構造に合わせて全面再編
    ├── mod.rs
    ├── flow.rs           #   FlowRun, write_flow/read_flow
    ├── job.rs            #   JobRun, Lifecycle, read_job_run
    ├── runner.rs         #   submit/tick async pyfunctions
    ├── render.rs         #   render_batch_bash pyfunction
    ├── persistence.rs    #   read_common/write_common + path helpers
    └── jobid.rs          #   既存
```

### 2.3 既存 SP-1/SP-2 API への影響 (rename / move 一覧)

| 旧 (SP-1/SP-2) | 新 (SP-3 後) | 理由 |
|---|---|---|
| `PerJobStatus` (enum 4 値) | `Lifecycle` (enum 5 値) | Airflow state machine 揃え、`Skipped` 追加 |
| `StatusEntry` | `JobRun` | Airflow TaskInstance 用語、`.status.toml` の意味論的名称 |
| `status::io::{read_status, write_status}` | `persistence::job_run::{read_job_run, write_job_run}` | 永続化を集約 |
| `slurm_facade::SlurmFacade` | `slurm::querier::Querier` | Facade は曖昧、役割 (query) を名前に |
| `slurm_facade::{A1SlurmFacade, InMemorySlurmFacade}` | `slurm::querier::{SlurmQuerier, InMemoryQuerier}` | 同上 + A1 `SlurmManager` wrap という事実を名前に反映 |
| `flow_io::{read_flow, write_flow}` | `persistence::flow::{read_flow, write_flow}` | module 集約 |
| `plan::io::{read_plan, write_plan}` | `persistence::plan::{read_plan, write_plan}` | 同上 |
| `path::PathResolver` | `persistence::path::PathResolver` | 同上 + `common_toml()` / `batch_bash()` getter 追加 |
| `filter::SearchFilter` | `search::SearchFilter` | filter は実装語、search が役割語 |
| `tick::{Decision, TickResult, decide_transition, tick_many}` | `runner::transition::{Decision, TickResult, decide_transition}` + `FlowRunner::tick()` | tick ロジックを Runner に統合、transition だけ独立関数 |
| `view::CalcView` | (Phase A で再評価: 廃止 or `flow::FlowRun::view()` 統合) | "Calc" は古い G16 名残 |

**互換性方針:**
- 旧名の `pub use ... as` deprecate-alias は**作らない**。一気に置換し、テスト・Python・import をすべて新名に書き換える
- 影響: Rust テスト ~50 箇所、Python テスト ~30 箇所、`lib.rs` の re-export ~20 シンボル
- A1 は変更ゼロ、D2 は Phase 0 PR (serde derives 追加) のみ

---

## 3. Core types & state machine

### 3.1 `Lifecycle` enum

```rust
// src/job/lifecycle.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    /// sbatch 成功 → slurm_jobid 取得済み、SLURM queue 入りした
    Queued,
    /// SLURM RUNNING を query で検知
    Running,
    /// SLURM COMPLETED (exit 0) を query で検知
    Success,
    /// SLURM FAILED / TIMEOUT / OOM / NODE_FAIL / CANCELLED 等を検知
    Failed,
    /// 親 JobRun が Failed/Skipped で SLURM 自身が submission を起こさなかった
    /// (afterok dependency が永久に満たされない = DependencyNeverSatisfied)
    /// Airflow の `upstream_failed` 相当
    Skipped,
}

impl Lifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(self, Lifecycle::Success | Lifecycle::Failed | Lifecycle::Skipped)
    }
}
```

**Pending は enum value にしない** — `.status.toml` ファイル不在 = 暗黙 Pending。`view` 層で `<pending>` と表示。

**遷移ルール:**

```
   ┌──────────────┐
   │ (file unset) │  = Pending (暗黙)
   └──────┬───────┘
          │ FlowRunner.submit() で sbatch 成功
          ▼
   ┌──────────────┐
   │   Queued     │──┬─────── tick: SLURM RUNNING ────┐
   └──────┬───────┘  │                                  ▼
          │          │                          ┌──────────────┐
          │          │                          │   Running    │
          │          │                          └──────┬───────┘
          │          │                                 │ tick: SLURM 終了
          │          ▼                                 ▼
          │  parent Failed/Skipped              ┌──────────┬──────────┐
          │  → SkipDueToParent                  │ Success  │ Failed   │
          ▼                                     └──────────┴──────────┘
   ┌──────────────┐
   │   Skipped    │ (terminal)
   └──────────────┘
```

不正遷移は `JobManagerError::IllegalTransition { job, from, to }` で reject。

### 3.2 `JobRun` struct (旧 `StatusEntry`)

```rust
// src/job/run.rs

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRun {
    pub lifecycle: Lifecycle,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub slurm_jobid: Option<u64>,
    pub slurm_status: Option<slurm_async_runner::JobStatus>,
    pub note: Option<String>,
}
```

`.status.toml` の新 schema:

```toml
lifecycle = "queued"
updated_at = "2026-05-13T12:34:56Z"
slurm_jobid = 12345

[slurm_status]
state = "PENDING"
```

旧 schema (SP-1 `PerJobStatus`) との差:
- `lifecycle` 値が PascalCase → snake_case
- `"Done"` → `"success"` (Airflow 語彙)
- 新規 `"skipped"` 値

### 3.3 `FlowRun` aggregate

```rust
// src/flow/run.rs

pub struct FlowRun {
    pub flow_uuid: uuid::Uuid,
    pub flow: gaussian_job_shared::entities::workflow::JobFlow,
    pub plan: crate::plan::ExperimentPlan,
    pub common: Option<gaussian_job_shared::config::common::CommonConfig>,
}

impl FlowRun {
    /// 全 TOML を path から読み出して構築
    pub fn read(
        resolver: &PathResolver,
        flow_uuid: uuid::Uuid,
    ) -> Result<Self, JobManagerError>;

    /// Kahn's algorithm でトポロジカル順、cycle 検出
    pub fn topological_order(&self) -> Result<Vec<JobId>, JobManagerError>;

    /// merge_with_defaults を内部で呼んで effective config を返す
    pub fn effective_config(
        &self,
        jid: &JobId,
    ) -> Result<slurm_async_runner::entities::slurm::SlurmJobConfig, JobManagerError>;

    /// JobEdge[] の parent 側を返す
    pub fn parents_of(&self, jid: &JobId) -> &[JobEdge];

    /// 1 ジョブの param-bound 値 (plan.toml の jobs[jid])
    pub fn params_of(
        &self,
        jid: &JobId,
    ) -> Result<&BTreeMap<String, toml::Value>, JobManagerError>;
}
```

### 3.4 `Executor` trait

```rust
// src/slurm/executor.rs

#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError>;
}

/// A1 SbatchManager 直結 (production)
pub struct SbatchExecutor;

#[async_trait::async_trait]
impl Executor for SbatchExecutor {
    async fn submit(&self, cmd: SbatchCmd) -> Result<u64, JobManagerError> {
        let manager = SbatchManager::new(cmd);
        let handle = manager.spawn().await
            .map_err(|e| JobManagerError::SubmitFailed { source: anyhow::anyhow!(e) })?;
        handle.jobid().ok_or_else(|| JobManagerError::SubmitFailed {
            source: anyhow::anyhow!("sbatch returned no jobid"),
        })
    }
}

/// `jm submit --dry-run` 用、決定的な fake jobid を返す
pub struct DryRunExecutor;

/// テスト用、事前録音した jobid を返す
pub struct MockExecutor {
    pub recordings: Vec<u64>,
    pub calls: std::sync::Mutex<Vec<SbatchCmd>>,
}
```

### 3.5 `Querier` trait (旧 `SlurmFacade` rename)

```rust
// src/slurm/querier.rs

#[async_trait::async_trait]
pub trait Querier: Send + Sync {
    async fn query(&self, jobids: &[u64])
        -> Result<HashMap<u64, JobStatus>, JobManagerError>;
}

pub struct SlurmQuerier { manager: Arc<SlurmManager> }
pub struct InMemoryQuerier { pub responses: HashMap<u64, JobStatus> }
pub struct MockQuerier { pub script: Vec<HashMap<u64, JobStatus>>, pub calls: Mutex<usize> }
```

**命名対称性:** `SbatchExecutor` は A1 `SbatchManager.spawn()` (sbatch) を wrap、`SlurmQuerier` は A1 `SlurmManager.query_job_states_batch()` (sacct) を wrap。それぞれ A1 の対応 manager 名を接頭辞に取る。

ロジックは現 `SlurmFacade` をそのまま (rename のみ)。

### 3.6 `FlowRunner` (submit_chain の置換)

```rust
// src/runner/flow.rs

pub struct FlowRunner<'a> {
    pub executor: Box<dyn Executor>,
    pub querier: Box<dyn Querier>,
    pub resolver: &'a PathResolver,
}

impl<'a> FlowRunner<'a> {
    pub fn new(
        executor: Box<dyn Executor>,
        querier: Box<dyn Querier>,
        resolver: &'a PathResolver,
    ) -> Self;

    /// flow_run の全 job を render + submit (旧 submit_chain)
    /// dry_run = render のみ、Executor は呼ばない
    pub async fn submit(
        &self,
        flow_run: &FlowRun,
        dry_run: bool,
    ) -> Result<BTreeMap<JobId, u64>, JobManagerError>;

    /// flow_run の全 job の .status.toml を読み、SLURM query して transition、書き戻す
    pub async fn tick(
        &self,
        flow_run: &FlowRun,
    ) -> Result<TickResult, JobManagerError>;

    /// batch.bash のみ生成 (jm run)
    pub fn render_only(&self, flow_run: &FlowRun) -> Result<(), JobManagerError>;
}
```

private ヘルパー:
- `build_sbatch_cmd(effective_config, script_path, sbatch_bin) -> SbatchCmd`
- `submit_one(jid, cmd) -> u64` (executor.submit + write JobRun)

### 3.7 `Decision` / `TickResult`

```rust
// src/runner/transition.rs

pub enum Decision {
    NoChange,
    Transition { from: Lifecycle, to: Lifecycle, slurm_status: Option<JobStatus> },
    /// parent が Failed/Skipped で skip するべき
    SkipDueToParent { parent: JobId },
}

pub struct TickResult {
    pub transitions: BTreeMap<JobId, Decision>,
}

pub fn decide_transition(
    current: Lifecycle,
    query_result: Option<&JobStatus>,
    parent_lifecycles: &[Lifecycle],
) -> Decision;
```

`parent_lifecycles` 引数は新規 (現 SP-1 `decide_transition` には無い)、Skipped 判定に利用。

### 3.8 `common.toml` (v1 と同じ)

```toml
[slurm_default]
partition = "long"
# その他 Option<T> フィールドは省略

[directories]
project_root = "/work"
```

D2 `gaussian_job_shared::config::common::CommonConfig` に Phase 0 PR で `serde::Serialize/Deserialize` + `#[serde(deny_unknown_fields)]` を追加してそのまま import。job-manager 側にラッパは作らない (newtype 不可侵原則と無関係、struct shape は D2 内で restructurable)。

### 3.9 bash render (v1 と同じ env-export 方式)

```bash
#!/bin/bash
# Generated by job_manager SP-3. Do not edit; regenerated on every `jm run`.

# --- job-manager runtime context ---
export JM_FLOW_UUID='01997cdc-...'
export JM_JOB_ID='opt__compound=0__method=0'
export JM_AXIS_COMPOUND='0'
export JM_AXIS_METHOD='0'

# --- plan.toml params ---
export JM_PARAM_ROUTE='# B3LYP/6-31G* opt'
export JM_PARAM_COMPOUND='benzene'

# --- user body (JobSpec.body) ---
<JobSpec.body verbatim>
```

POSIX single-quote escape (`'\''` for internal `'`)。`#SBATCH` directives は batch.bash に書かず `SbatchCmd` CLI 引数で渡す。

---

## 4. Data flow

### 4.1 `jm submit /work/<uuid>`

```
CLI parses: root = "/work", uuid = "<uuid>"
   │
   ▼
PathResolver::new("/work")
   │
   ▼
FlowRun::read(&resolver, uuid)
   ├─→ persistence::flow::read_flow      → JobFlow
   ├─→ persistence::plan::read_plan      → ExperimentPlan
   └─→ persistence::common::read_common  → Option<CommonConfig>
   │
   ▼
let runner = FlowRunner::new(
    Box::new(SbatchExecutor),          // --dry-run なら DryRunExecutor
    Box::new(SlurmQuerier::new(...)),  // submit では使わない (lifetime 一致のため construct のみ)
    &resolver,
);
   │
   ▼
runner.submit(&flow_run, dry_run = false)
   │
   ▼ (for each jid in flow_run.topological_order())
   │
   ├── effective_config = flow_run.effective_config(&jid)
   ├── parts            = jobid::parse_job_id(&jid.0)
   ├── params           = flow_run.params_of(&jid)
   ├── body             = render::render_batch_bash(&flow_uuid, &jid, parts, params, &job.spec.body)
   ├── batch_path       = resolver.batch_bash(&flow_uuid, &jid)
   ├── persistence::atomic_write(&batch_path, body)
   ├── dep              = slurm::dependency::build(&job.parents, &submitted, &jid)?
   ├── cmd              = build_sbatch_cmd(&effective_config, &batch_path, sbatch_bin)
   │                       (cmd.dependency = dep)
   ├── slurm_jobid      = executor.submit(cmd).await?         ◄── A1 SbatchManager 呼ぶ
   ├── persistence::job_run::write_job_run(
   │       &resolver.status_file(&flow_uuid, &jid),
   │       &JobRun {
   │           lifecycle: Lifecycle::Queued,
   │           updated_at: Utc::now(),
   │           slurm_jobid: Some(slurm_jobid),
   │           slurm_status: None,
   │           note: None,
   │       })
   └── submitted.insert(jid, slurm_jobid)
   │
   ▼
return BTreeMap<JobId, u64>
```

### 4.2 `jm tick /work/<uuid>`

```
FlowRun::read(...)  (submit と同じ前段)
   │
   ▼
runner.tick(&flow_run)
   │
   ▼ Step 1: 各 jid の現 JobRun を読む
   │   all_runs: BTreeMap<JobId, JobRun> = ...read all .status.toml...
   │
   ▼ Step 2: 既に terminal (Success/Failed/Skipped) のものは除外
   │   pending: Vec<(JobId, JobRun)>
   │
   ▼ Step 3: jobid だけ集めて querier.query
   │   let jobids: Vec<u64> = pending.iter().filter_map(...slurm_jobid).collect();
   │   let statuses: HashMap<u64, JobStatus> = querier.query(&jobids).await?
   │
   ▼ Step 4: 各 jid で decide_transition
   │   for (jid, run) in &pending:
   │       parent_lifecycles =
   │           collect_parent_lifecycles(&flow_run, &jid, &all_runs)
   │       decision = decide_transition(
   │           run.lifecycle,
   │           statuses.get(&run.slurm_jobid?),
   │           &parent_lifecycles,
   │       )
   │       match decision:
   │           NoChange         → skip
   │           Transition       → write new JobRun
   │           SkipDueToParent  → write JobRun { lifecycle: Skipped, ... }
   │
   ▼
TickResult { transitions: BTreeMap<JobId, Decision> }
```

### 4.3 `jm run` / `jm show` / `jm search`

- **`jm run`**: `FlowRun::read` → `runner.render_only(&flow_run)`。batch.bash のみ生成
- **`jm show`**: `FlowRun::read` → 全 jid に対し `persistence::job_run::read_job_run` → tabular 出力 (`<pending>` / `queued (slurm_jobid=N)` / ...)
- **`jm search`**: `walk_flows(root)` → 各 flow_uuid に対し `SearchFilter::matches` → matched flow を yields

---

## 5. Error handling

`src/error.rs` の `JobManagerError` に追加 (旧 variants 維持):

```rust
#[error("dependency cycle detected in flow {flow}")]
DependencyCycle { flow: uuid::Uuid },

#[error("missing plan entry for job {job} in flow {flow}")]
MissingPlanEntry { flow: uuid::Uuid, job: JobId },

#[error("sbatch submission failed: {source}")]
SubmitFailed { #[source] source: anyhow::Error },

#[error("bash render failed for job {job}: {reason}")]
RenderError { job: JobId, reason: String },

#[error("illegal lifecycle transition for {job}: {from:?} -> {to:?}")]
IllegalTransition { job: JobId, from: Lifecycle, to: Lifecycle },

#[error("common.toml not found at {path} and no default available")]
CommonConfigMissing { path: PathBuf },
```

既存 `Slurm(String)` / `Io { path, source }` / `Toml { path, source }` は維持。

---

## 6. Testing strategy

| layer | test 種別 | 主な内容 |
|---|---|---|
| `job/lifecycle.rs` | unit | enum serde (snake_case), 不正遷移 → `IllegalTransition` |
| `job/run.rs` | unit | JobRun TOML round-trip (旧 SP-1 test と置換) |
| `flow/run.rs` | unit | topological_order (cycle 検出含む), effective_config (with/without common), parents_of |
| `flow/topology.rs` | unit | Kahn's algorithm の edge cases |
| `slurm/executor.rs` | unit | `MockExecutor` の recordings 検証, `DryRunExecutor` deterministic 確認 |
| `slurm/querier.rs` | unit | `InMemoryQuerier` (現 SP-1 test rename) |
| `slurm/dependency.rs` | unit | JobEdge + submitted → SlurmDependency (afterok/afternotok/afterany 各 case) |
| `render/mod.rs` | golden | 固定 axis_combo + params → 期待 batch.bash 文字列 (single-quote escape 含む) |
| `runner/transition.rs` | unit | decide_transition 全マトリックス (Queued/Running × SLURM PENDING/RUNNING/COMPLETED/FAILED × parent Success/Failed/Skipped) |
| `runner/flow.rs` | integration | 12-job sample で submit (MockExecutor) → 正しい順序 + 依存 + .status.toml |
| `runner/flow.rs` | integration | dry_run → batch.bash 生成済 + executor.submit 呼ばれない (call count = 0) |
| `runner/flow.rs` | integration | tick (InMemoryQuerier) → 正しい transition、parent failed → child Skipped |
| `bin/jm.rs` | smoke (`tests/cli_smoke.rs`) | `jm run` / `jm submit --dry-run` / `jm show` / `jm tick` / `jm search` 各 1 ケース |
| Python | pytest | FlowRun 構築, write_flow/read_flow, render, runner.submit (dry_run) |

カバレッジ目標: `cargo llvm-cov --fail-under-lines 80`。

---

## 7. Phasing

```
Phase 0 (D2 PR): CommonConfig + DirectoryConfig に serde derives + #[serde(deny_unknown_fields)]
   ↓
Phase A (rename + move): 既存 SP-1/SP-2 を新モジュール構成に re-org
   ├─ status/ → job/ + persistence/job_run.rs に分割
   ├─ slurm_facade.rs → slurm/querier.rs
   ├─ flow_io.rs → persistence/flow.rs
   ├─ plan/io.rs → persistence/plan.rs
   ├─ path.rs → persistence/path.rs
   ├─ filter.rs → search.rs
   ├─ tick.rs → runner/transition.rs (decide_transition, Decision, TickResult)
   ├─ PerJobStatus → Lifecycle (5 値、snake_case)
   ├─ StatusEntry → JobRun (TOML schema 書き換え)
   ├─ view.rs::CalcView を再評価 (廃止 or FlowRun に吸収)
   └─ Rust テスト + Python テスト + lib.rs re-export 全て更新
   ↓
Phase B (common): persistence::common (read/write) + FlowRun::read で common 統合 + merge_with_defaults
   ↓
Phase C (render): render/mod.rs + sanitize_var_name + quote_for_bash + PathResolver::batch_bash
   ↓
Phase D (slurm submit infra): Executor trait + 3 impls + slurm/dependency.rs
   ↓
Phase E (runner): FlowRunner + submit/tick/render_only + decide_transition の Lifecycle 対応
   ↓
Phase F (CLI): bin/jm.rs (clap v4 + tokio) + 5 subcommands + cli_smoke tests
   ↓
Phase G (Python): py_export 再編 (旧 pyfunction 名を新名に置換、互換 alias なし)
```

**Phase 並列性:** A → B → C → D → E → F → G は依存順。各 Phase 内は独立タスクに分割可能 (writing-plans で展開する)。
**Phase A は唯一の破壊的再編 phase**。Phase B 以降は新規追加が主。

---

## 8. 既存 API への破壊範囲 (まとめ)

- **Rust public API (lib.rs re-exports)**: ~20 名のシンボル rename / 削除
- **Python public API**: 旧 `read_status` → `read_job_run`、`StatusEntry` → `JobRun`、`PerJobStatus` → `Lifecycle` 等
- **`.status.toml` schema**: PascalCase → snake_case + `"Done"` → `"success"` + `"skipped"` 追加 (既存ファイル非互換、SP-3 リリース前なので migration 不要)
- **`flow.toml` / `plan.toml`**: 変更なし
- **CI**: `cargo test --all-features`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, `pytest python/tests` 全 PASS が完了条件
- **A1 (slurm-async-runner)**: 変更ゼロ (絶対不可侵)
- **D2 (gaussian-job-shared)**: Phase 0 PR (serde derives 追加) のみ、newtype 不可侵

---

## 9. リスクと未決事項

| 項目 | リスク | 対応 |
|---|---|---|
| Phase A の破壊範囲 | 50+ テスト・30+ pytest・20+ re-export を一気に書き換え、commit が肥大化 | Phase A 内を sub-tasks (status→job, slurm_facade→querier, flow_io→persistence/flow, ...) に分け one-commit-per-rename |
| Lifecycle 5 値 vs A1 JobState | A1 `JobState` (SLURM PENDING/RUNNING/COMPLETED/FAILED 等) と意味論が混同される可能性 | `Lifecycle` は job-manager のドメイン状態、A1 `JobState` は SLURM 生状態。`JobRun.slurm_status: Option<JobStatus>` で両者を分離保持 |
| `Skipped` 判定の依存 | `decide_transition` が `parent_lifecycles` を要求 → tick で全 jid の状態を先読みする必要 | `tick()` 実装で all_runs を先に読んで in-memory map にする (1 flow あたり最大数百 jid なのでメモリ問題なし) |
| `view.rs::CalcView` 廃止判断 | 廃止すべきか維持すべきかが Phase A 時点で未確定 | Phase A 着手時に grep で usage 確認、未使用なら削除、使用ありなら FlowRun に method 統合 |
| D2 PR merge 順 | Phase 0 (D2 serde derives) が先に merge されないと job-manager がビルド不能 | PR 順序: D2 PR → job-manager re-arch PR。CI で path 依存 (`../gaussian-job-shared2`) を解決 |
| 並列 sbatch | 依存解決に SLURM jobid が必要 → 順次のみ | SP-3 v2 でも順次。将来「ルートだけ並列」等を検討 |
| bash injection | plan params に shell metachar が混入 | single-quote + `'\''` エスケープのみで防御 (POSIX) |
| CLI の root 解決優先順 | `--root` arg / `JM_ROOT` env / `common.directories.project_root` の優先順 | `--root` arg > `JM_ROOT` env > `common.directories.project_root` > error |
| `partition: String` の merge | A1 `SlurmJobConfig.partition` が `String` (Option ではない)、空文字と未指定が区別不能 | A1 不可侵のため `is_empty()` fallback。本格解消は A1 改修待ち (本 SP-3 スコープ外) |
| post.bash の扱い | SP-1 で「Done は post.bash の専権」だったが SP-3 で render するか? | しない。`JobSpec.body` 自体が post.bash 相当を含む/含めないかはユーザー責務 |

---

## 10. Python API (v1 と同等、新型対応)

```python
from job_manager import (
    # SP-3 v2 新型
    FlowRun, JobRun, Lifecycle,
    FlowRunner, SbatchExecutor, DryRunExecutor,
    SlurmQuerier, InMemoryQuerier,
    # SP-3 v2 関数
    render_batch_bash,
    # SP-3 v2 persistence
    read_flow, write_flow, read_plan, write_plan,
    read_common, write_common, read_job_run, write_job_run,
    PathResolver,
    # SP-1/SP-2 既存
    ExperimentPlan,
    build_job_id, parse_job_id, validate_step_id,
)
from gaussian_job_shared import CommonConfig, DirectoryConfig

resolver = PathResolver("/work")
common = CommonConfig(
    slurm_default={"partition": "long"},
    directories=DirectoryConfig(project_root="/work"),
)
write_common(resolver.common_toml(), common)

# submit (async)
import asyncio
flow_run = FlowRun.read(resolver, uuid)
runner = FlowRunner(SbatchExecutor(), SlurmQuerier(...), resolver)
jobids = asyncio.run(runner.submit(flow_run, dry_run=False))
print(jobids)
```

旧 `read_status`, `write_status`, `StatusEntry`, `PerJobStatus`, `SlurmFacade`, `A1SlurmFacade`, `InMemorySlurmFacade` は **削除** (互換 alias なし)。

---

## 11. 完了基準

- [ ] **Phase 0 D2 PR merged** (`CommonConfig` + `DirectoryConfig` に serde derives + `#[serde(deny_unknown_fields)]`)
- [ ] **Phase A** 完了: 既存 SP-1/SP-2 が新モジュール構成に re-org、全テスト pass、旧 API シンボル削除
- [ ] **Phase B-G** 完了: 新型 (FlowRun/JobRun/Lifecycle/Executor/Querier/FlowRunner) + render + CLI + Python API すべて実装
- [ ] `cargo build --all-features` 成功 + `jm` binary 生成
- [ ] `cargo test --all-features` 成功 (新規 + 移植テスト 計 80+)
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` クリーン
- [ ] `cargo fmt --check` クリーン
- [ ] `uv run maturin develop --uv` 成功
- [ ] `uv run pytest python/tests` 全 PASS
- [ ] `jm run` / `jm submit --dry-run` / `jm tick` / `jm show` / `jm search` が tempdir で動作
- [ ] **SP-3 v2 PR で A1 への変更ゼロ** (SLURM 不可侵)
- [ ] **D2 への変更は Phase 0 PR のみ** (serde derives 追加だけ、newtype 不可侵)
- [ ] 各 Phase で one-issue-per-commit (Conventional Commits)

---

## 12. 次工程

承認後:
1. **`writing-plans` skill で plan v2 を生成** (`docs/superpowers/plans/2026-05-13-job-manager-sp3-rearch.md`)
2. 既存 PR #10 の plan v1 を supersede (PR description 更新で v2 spec へ link、commit を積み増し)
3. branch `feat/sp3-submit-and-cli` で Phase 0 D2 PR → Phase A→B→C→D→E→F→G を one-issue-per-commit で実装
4. 全 Phase 完了で PR を `develop` に
