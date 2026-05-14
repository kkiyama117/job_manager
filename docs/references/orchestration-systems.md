# Orchestration Systems Reference — Airflow & Prefect

> **Status:** Reference / research notes (一時資料)
> **Date:** 2026-05-13
> **Purpose:** job-manager (SLURM 向け軽量オーケストレータ) の設計判断の参考として、Airflow と Prefect の主要構造・データフロー・状態モデルを整理する。完全再実装は目的ではなく、**どこを真似し、どこを捨てるか**の意思決定材料。

---

## 0. なぜこれを読むか — job-manager との対応

job-manager は SLURM 上での「DAG ベース ジョブチェーン送信 + 状態追跡」が中核。
これは Airflow / Prefect が解いてきた問題のごく小さなサブセットなので、設計用語・状態モデル・トリガー意味論は彼らに揃えると外部利用者にとって学習コストが低くなる。

| job-manager 用語 (SP-1〜SP-3) | Airflow 対応 | Prefect 対応 |
|---|---|---|
| `JobFlow` (flow.toml) | DAG | Flow |
| `JobSpec` / `Job` (axis 展開後の 1 ジョブ) | Task / TaskInstance | Task / Task Run |
| `JobEdge` (parent → child + DependencyType) | TaskFlow `>>` + trigger_rule | `wait_for` + state check |
| `ExperimentPlan` (展開済みパラメータ) | DAG Run の rendered template | Flow Run の bound parameters |
| `PerJobStatus` (Queued/Running/Done/Failed) | TaskInstance State | Task Run State |
| `.status.toml` (file-based persistence) | metadata DB (Postgres等) | Prefect API / DB |
| `submit_chain` (順次 sbatch + 依存付き) | Scheduler + Executor | Worker + Engine |
| CLI `jm submit` | `airflow dags trigger` | `prefect deployment run` |

---

## 1. Apache Airflow 2.x / 3.x

### 1.1 アーキテクチャの構成要素

Airflow は **役割の分離が強い** 多コンポーネント構成:

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ DAG Files    │────▶│ DAG Processor│────▶│ Metadata DB  │
│ (Python)     │     │ (parse/serialize)  │ (Postgres等) │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │
                            ┌─────────────────────┼─────────────────────┐
                            ▼                     ▼                     ▼
                    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
                    │ Scheduler    │     │ Webserver    │     │ Triggerer    │
                    │ + Executor   │     │ (UI/REST API)│     │ (deferred IO)│
                    └──────┬───────┘     └──────────────┘     └──────────────┘
                           │
                           ▼
                    ┌──────────────────────────────────────┐
                    │ Workers: Local / Celery / K8s        │
                    │ (ここで Task が actually 動く)        │
                    └──────────────────────────────────────┘
```

- **DAG Processor:** `dags/` ディレクトリを定期スキャン、DAG オブジェクトを serialize して metadata DB に保存。実行から完全に分離。
- **Scheduler:** metadata DB を見て「次に走らせるべき TaskInstance」を決め、Executor に渡す。Executor は scheduler プロセス内に同居 (Airflow 2.x 以降の重要ポイント: Executor は独立コンポーネントではない)。
- **Executor:** SequentialExecutor / LocalExecutor / CeleryExecutor / KubernetesExecutor。**ここに SLURM 実装を差し込むのがアナロジー的に正しい設計位置**。
- **Triggerer:** asyncio で動く軽量イベントループ。`deferred` 状態のタスク (外部条件待ち) を効率よくポーリング。**job-manager の `--watch` 相当**。
- **Metadata DB:** 全ての真実が入っている。DAG run、TaskInstance、Xcom (タスク間データ受け渡し)、変数、コネクション。

### 1.2 DAG / Task のデータモデル

- **DAG (Directed Acyclic Graph):** Python の関数 + デコレータ (`@dag`, `@task`) または `DAG()` クラスで宣言。タスク間の依存は `>>` 演算子。
- **Task:** Operator のインスタンス。BashOperator / PythonOperator / KubernetesPodOperator / 任意のカスタム。
- **TaskInstance:** DAG Run × Task の組。**実行ごとに 1 レコード**。状態は metadata DB に正規化保存。
- **Rendered template:** タスク投入時に Jinja で `{{ ds }}` `{{ params }}` 等を解決し、`rendered_task_instance_fields` テーブルに保存。**job-manager の "param-set rendered batch.bash" に相当**。

### 1.3 State Machine

TaskInstance の状態遷移 (主要部分のみ):

```
none ──▶ scheduled ──▶ queued ──▶ running ──┬──▶ success
                                            ├──▶ failed (→ up_for_retry → queued ループ)
                                            ├──▶ skipped
                                            └──▶ deferred ──▶ scheduled (Triggerer 経由)
```

- **`up_for_retry`** がリトライ専用の中間状態として独立しているのは設計上重要。**job-manager の `Failed` を直接 retry に流すか、別状態を挟むかは要検討**。
- **`upstream_failed`** / **`skipped`** は親の結果に応じて伝播。これが次の trigger_rule の前提。

### 1.4 Trigger Rules (依存意味論)

これが **job-manager の `DependencyType` (afterok/afternotok/afterany/aftercorr) と直接対応する概念**。Airflow の trigger_rule:

| trigger_rule | 意味 | SLURM `--dependency=` 相当 |
|---|---|---|
| `all_success` (default) | 全親が success のとき走る | `afterok` |
| `all_failed` | 全親が failed のとき走る | `afternotok` (各親) |
| `all_done` | 全親が終わったら走る (結果問わず) | `afterany` |
| `one_success` | 親のいずれかが success | (SLURM 直接対応なし、要シェル判定) |
| `one_failed` | 親のいずれかが failed | (同上) |
| `none_failed` | failed が無い (success/skipped) | `afterok` + skip 許容 |
| `none_skipped` | skipped が無い | (要判定) |
| `always` | 無条件 | (依存なし) |

**学び:** SLURM の `--dependency` 文法だけだと表現できないルール (one_success 等) があるため、job-manager は **trigger_rule という抽象を一段噛ませて、SLURM dependency に落とせない場合は wrapper script で判定**、という戦略が筋。今は `DependencyType` で SLURM 直対応分のみ実装する判断で OK。

### 1.5 スケジュール & Backfill

- **schedule:** cron / timedelta / Dataset / `@daily` 等。
- **catchup:** `catchup=True` の場合、start_date から現在までの未実行 DAG Run を全部生成。これが **backfill** の自動版。
- **manual backfill:** `airflow dags backfill -s START -e END my_dag`。実験再実行に便利。
- **job-manager への示唆:** スケジュールは要らない (HPC は人間トリガー)。Backfill 相当は **「過去の flow.toml + plan.toml を引き直して再 submit」** という運用で、CLI として `jm resubmit <flow_uuid>` を将来追加する余地。

### 1.6 Retries / SLA

- Task 単位で `retries=N`, `retry_delay=timedelta`, `retry_exponential_backoff=True`, `max_retry_delay` を指定。
- `sla=timedelta(hours=1)` で SLA miss を別途記録 (失敗ではない)。
- **job-manager:** retries はまだ未実装。SLURM 自身に `--requeue` があるので二重実装注意。

### 1.7 XCom (タスク間データ受け渡し)

- 小さな値 (jobid 等) を `xcom_push` / `xcom_pull` で受け渡す。デフォルトは metadata DB。
- **job-manager 対応:** `.status.toml` の `slurm_jobid` を子ジョブの `--dependency=` 用に利用する我々のフローは、まさに XCom のミニ版。

---

## 2. Prefect 3

### 2.1 アーキテクチャの構成要素

Prefect 3 は **Agent モデルを廃止し Worker モデルに移行**した世代。Airflow よりも構成が単純で hybrid 寄り:

```
┌────────────────────┐                ┌──────────────────────┐
│ Flow Code (Python) │                │ Prefect API + DB     │
│ @flow / @task      │ ─── register ──▶│ (Cloud or self-host) │
└────────────────────┘                └──────────┬───────────┘
                                                  │
                            ┌─────────────────────┼─────────────────────┐
                            ▼                     ▼                     ▼
                  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
                  │ Pull Work Pool   │  │ Push Work Pool   │  │ Managed Work Pool│
                  │ + Worker (poll)  │  │ (serverless)     │  │ (Prefect運用)    │
                  └──────────────────┘  └──────────────────┘  └──────────────────┘
                            │
                            ▼
                  ┌──────────────────────────────────────┐
                  │ Execution Env (Docker, ECS, K8s, …)  │
                  └──────────────────────────────────────┘
```

- **Flow:** `@flow` デコレータ付き Python 関数 = ワークフローのエントリポイント。
- **Task:** `@task` デコレータ付き関数。Flow 内で呼ばれて Task Run を生成。**Airflow と違って明示的 DAG 宣言は不要**で、関数呼び出し順から依存を推論。
- **Deployment:** Flow を「いつ」「どこで」走らせるかの設定 (schedule + work pool + parameters)。Flow と Deployment は 1:N。
- **Work Pool:** Job をキューイングする論理プール。`type` (process / docker / kubernetes / ecs / ...) で実行環境を分類。
- **Worker:** Work pool を poll する長期プロセス。マッチする type の pool だけ poll できる。
- **Push Work Pool:** Worker 不要。Prefect が直接 serverless 基盤に submit。

### 2.2 Flow / Task のデータモデル

- **Flow Run / Task Run:** Airflow の DAG Run / TaskInstance と対応。
- **State はファーストクラスオブジェクト:** `Pending / Running / Completed / Failed / Crashed / Cancelled / Paused / Retrying / Scheduled / Late` 等。**state を返り値として明示返却可能** (`return Failed(message="...")` 等)、これが Airflow との大きな差。
- **動的タスク生成:** Flow 内で `for` ループで task を呼べばその数だけ Task Run が生まれる。**Airflow の TaskFlow API より動的性が高い**。
- **Parameters:** Flow 引数として渡す。Deployment 単位で default を上書き可。**job-manager の `ExperimentPlan` ≒ Flow Run の bound parameters**。

### 2.3 State 駆動の設計

Prefect の特徴は「state は単なるフラグではなくオブジェクト」:

```python
from prefect import flow, task
from prefect.states import Failed, Completed

@task(retries=3, retry_delay_seconds=60)
def step():
    if some_check_fails():
        return Failed(message="condition X not met")
    return Completed(data=result)
```

- state ごとに hook (`on_completion`, `on_failure`, `on_crashed`, ...) を登録可能。
- **state を file-based に持つ job-manager の `StatusEntry` は、Prefect の state object のミニ版と捉えると整理しやすい**。

### 2.4 Trigger / 依存

- 依存は Python 関数呼び出しの順序から自動推論 (futures を取って次に渡せばエッジが張られる)。
- 明示的 `wait_for=[other_task]` も可能。
- 親 task が failed なら下流は自動的に `Skipped` (Airflow の `upstream_failed` 相当)。
- **conditional flow:** Python の `if` でそのまま分岐可能。**Airflow の `BranchPythonOperator` より素直**。

### 2.5 Retries

- `@task(retries=N, retry_delay_seconds=...)`、Flow 単位でも同じ。
- **Flow retry は全 task を再実行**、**Task retry は失敗 task のみ再実行**。意味論が明確に分かれている。

### 2.6 イベント駆動

- `Automations` で「state X になったら Y を実行」が宣言的に書ける。
- Webhook トリガー: 外部から HTTP で flow run を起こせる。
- **job-manager:** SLURM の cluster event を webhook 化して受けると、`--watch` ループを省略できる可能性 (将来検討)。

---

## 3. 設計判断テーブル — job-manager にどう取り込むか

| 概念 | Airflow | Prefect | job-manager 採用方針 (推奨) |
|---|---|---|---|
| **DAG 宣言形式** | Python decorator / class | Python decorator + 関数呼び出し順 | **TOML** (実験者は Python 書かない) — 既決定 |
| **状態モデル** | TaskInstance State (列挙) | State object (オブジェクト) | 列挙 + Optional fields (現状の `StatusEntry` 設計を維持) |
| **永続化** | 中央 DB (Postgres等) | 中央 API + DB | **flow_dir 配下 file-based**, 中央DB なし — 既決定 |
| **trigger_rule** | 8種のルール (`all_success` 等) | state チェック + Python | **`DependencyType` で SLURM 対応分のみ** (afterok/afternotok/afterany/aftercorr) |
| **Backfill** | `dags backfill` CLI | manual run + parameter | **plan.toml 再生成 + `jm resubmit` (将来)** |
| **Retries** | Task 単位 `retries=N` | Task 単位、Flow 単位両方 | **SP-3 では未実装**, 将来 `flow.toml` で per-job 指定 |
| **Scheduler/Worker 分離** | 強い (scheduler + executor + workers) | やや弱い (worker = 実行+poll) | **不要** — CLI `jm submit` が同期的に sbatch を発射するだけ |
| **Triggerer (deferred)** | asyncio で外部条件待ち | 同等の polling | **`jm tick` / `--watch` で簡素な poll** |
| **Webhook トリガー** | Airflow REST API | Automations / webhooks | **将来検討** (現状は手動 CLI で十分) |
| **タスク間データ受け渡し** | XCom | future の返り値 | **`.status.toml` の slurm_jobid のみ** (これで十分) |
| **動的タスク生成** | TaskFlow + dynamic mapping | for ループで自然に | **flow.toml の axis 展開**で既に実現済み |
| **UI / 観測性** | Webserver (UI + REST) | Cloud / OSS UI | **CLI `jm show`** で文字ベース、UI は将来 |

---

## 4. job-manager 設計に活かす「言葉と境界」

### 4.1 採用すべき語彙

外部利用者が Airflow / Prefect に親しんでいる前提で、以下の語は揃えると説明コストが下がる:

- **Flow** (job-manager: `JobFlow`) — Airflow の DAG と等価
- **Run / FlowRun** — flow_uuid の各実行回。job-manager は今のところ flow_uuid 単位で 1run = 1 ディレクトリ。
- **Task / Job** — 我々は `Job` を使用 (SLURM 文化に寄せている)。
- **State / Lifecycle** — `PerJobStatus` は Airflow の TaskInstance state にほぼ対応。
- **Trigger rule** — `DependencyType` を「trigger rule」と呼び換えるかは検討余地。今は SLURM ネイティブ語の方が明快。
- **Backfill** — 過去の plan を再 submit する操作。

### 4.2 採用しないもの (YAGNI)

- 中央 DB / Webserver / Triggerer プロセス — file-based で十分。
- 動的タスク生成 API — axis 展開で必要十分。
- Sensors / Deferred operators — `jm tick` の単純 polling で代替。
- XCom — `.status.toml` で代替済み。

### 4.3 将来の余地 (今は採らない)

- **Webhook trigger:** SLURM の `EpilogSlurmctld` を webhook 化して `jm tick` 不要にする。
- **per-job retries:** `flow.toml` で `retries = 3` 等。
- **conditional branching:** trigger_rule を SLURM ネイティブ外まで拡張 (wrapper script 経由)。
- **Backfill CLI:** `jm resubmit <flow_uuid> --from <job_id>` 等。

---

## 5. 参考リンク

### Airflow

- [Architecture Overview — Airflow 3.2.1](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Scheduler — Airflow 3.2.1](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html)
- [apache/airflow GitHub](https://github.com/apache/airflow)
- [Apache Airflow Architecture (Towards Data Science)](https://towardsdatascience.com/apache-airflow-architecture-496b9cb28288/)

### Prefect

- [Work pools — Prefect 3](https://docs.prefect.io/v3/concepts/work-pools)
- [Workers — Prefect 3](https://docs-3.prefect.io/v3/deploy/infrastructure-concepts/workers)
- [How to Migrate from Airflow](https://docs.prefect.io/v3/how-to-guides/migrate/airflow)
- [Workers and Work Pools (DeepWiki)](https://deepwiki.com/PrefectHQ/prefect/4.3-deployment-cli-and-workers)

### 比較記事

- [Prefect vs Airflow (Prefect 公式)](https://www.prefect.io/compare/airflow)
- [Airflow vs Prefect 2024 (Orchestra)](https://www.getorchestra.io/guides/airflow-vs-prefect-key-differences-2024)
- [Airflow vs Prefect vs Dagster (Branch Boston)](https://branchboston.com/apache-airflow-vs-prefect-vs-dagster-modern-data-orchestration-compared/)
- [Workflow Orchestration 2025 (Procycons)](https://procycons.com/en/blogs/workflow-orchestration-platforms-comparison-2025/)

---

## 6. TL;DR

- **Airflow** は重い中央集権アーキテクチャ + 豊富な trigger_rule + backfill。**学ぶべきは語彙と state machine の正規化、捨てるのはコンポーネント分離**。
- **Prefect** は state ファーストクラス + Python-native + worker pool。**学ぶべきは state object の意味論と「最小コンポーネントで成立する」設計姿勢、捨てるのは中央 API**。
- **job-manager は両者のごく小さなサブセット**で、Airflow の `trigger_rule` 語彙と Prefect の state-as-object 思想だけ部分採用するのが筋。残りは SLURM + file-based で代替する現方針が正しい。
