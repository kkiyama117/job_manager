# common env を job の必須パラメータに反映する — オーケストレータ事例からの逆引き

**Date:** 2026-05-15
**Status:** thought / exploratory（spec でも plan でもない）
**Scope:** `common.toml` の値（特に `partition` 等 SLURM 必須パラメータ）を、`flow.toml` の各 job に「重複記述させずに」流し込む方式の比較・選定
**Predecessor:** `2026-05-15-flow-toml-partition-defaulting.md`（F2: 内部 Partial + ファクトリ関数を推奨）
**Reference:** `docs/references/orchestration-systems.md`（Airflow / Prefect 設計対応表）

これは設計議論メモであり、決定事項でも実装プランでもない。

---

## 1. 問題の再設定

前回 thought で扱った「flow.toml と common.toml の `partition` 二重記述問題」を、
**もう少し広い問い** に置き直す:

> A1 の `SlurmJobConfig` のように **型として必須** のフィールドを、
> ユーザーには毎回 flow.toml に書かせたくない。共通値は `common.toml`
> 1か所に置き、各 job 仕様には足りないものだけを書かせたい。
> どこで、いつ、どう merge するのがクリーンか。

これは job-manager 固有の課題ではない。**Airflow / Prefect が既に解いている問題**。
語彙と境界線をそろえると設計判断がブレない。

---

## 2. 既存オーケストレータがどう解いているか

### 2.1 Airflow — `default_args` パターン

DAG 構築時に dict を渡し、各 Operator が自分の `__init__` でそれを継承する:

```python
default_args = {
    "owner": "alice",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "queue": "long",        # ← partition に相当する SLURM 概念
}

dag = DAG("my_dag", default_args=default_args, ...)

t1 = BashOperator(task_id="t1", bash_command="...", dag=dag)
                                                    # ↑ default_args 継承
t2 = BashOperator(task_id="t2", bash_command="...",
                  retries=5, dag=dag)               # ← override
```

- スコープ: **DAG レベル**（全タスクの親）
- 継承: `apply_defaults` デコレータが Operator の `__init__` 引数に merge
- override: Operator 個別引数が dict より強い
- 永続化: DAG オブジェクト自体に dict が乗り、TaskInstance は materialize 後の値で記録

### 2.2 Prefect — `base_job_template` + `variables` パターン

Work Pool が **Jinja テンプレート + 変数デフォルト** を保持し、
Deployment / Run 時に variable を上書き:

```json
{
  "job_configuration": {
    "image":    "{{ image }}",
    "command":  "{{ command }}",
    "queue":    "{{ queue }}"
  },
  "variables": {
    "properties": {
      "image": { "default": "prefecthq/prefect:2-latest" },
      "queue": { "default": "long" }
    }
  }
}
```

- スコープ: **Work Pool レベル**（実行基盤を共有する論理プール）
- 継承: テンプレート + variable default を `render` して JobSpec を生成
- override: Deployment-level / Run-level variable で個別上書き
- 永続化: テンプレートと variables は別レコード。**round-trip で「default 由来か明示指定か」が残る**

### 2.3 Prefect — `@task(retries=3)` デコレータパターン

タスク定義側にデフォルト値を埋める方式。Flow 全体には effect しないが、
**特定タスクの "ベース" を関数定義に閉じ込める** やり方:

```python
@task(retries=3, retry_delay_seconds=60)
def step():
    ...
```

Flow 単位で再上書きしたければ runner 側で `step.with_options(retries=5)` する。

---

## 3. 3例から抽出される共通原則

| 原則 | Airflow | Prefect (pool) | Prefect (task deco) |
|---|---|---|---|
| **デフォルトの居場所はタスクの一段上のスコープ** | DAG | Work Pool | （タスク定義自体に baked-in） |
| **タスク仕様は user-write 時に partial で OK** | ○ | ○ | ○ |
| **materialize は parse とは別ステップ** | DAG load 時 | render 時 | 呼び出し時 |
| **欠損は "missing key" で表現（empty-string sentinel ではない）** | ○ | ○ (JSON null) | ○ |
| **override は per-field shallow merge** | ○ | ○ | ○ |
| **default 由来かの区別を保存** | ✕ (失う) | ○ (template/variables 別) | ✕ |

最も重要なのは下から2つ目: **欠損は missing key**。
現行の `merge_with_defaults` の `partition.is_empty()` 分岐は、Airflow も
Prefect も採っていない。両者とも「キーが無い／JSON null」と「空文字」を
意図的に区別する。

---

## 4. job-manager への対応関係

`docs/references/orchestration-systems.md` の対応表に **「共通値の継承」スロット** を
追加するなら:

| 概念 | Airflow | Prefect | job-manager 現状 | job-manager 推奨方向 |
|---|---|---|---|---|
| **タスク仕様の partial 表現** | Operator kwargs が optional | template variable が optional | 全フィールド必須（A1 contract） | **内部 Partial 型** で flow.toml 側だけ optional に |
| **デフォルト保持場所** | DAG `default_args` | Work Pool `base_job_template` + variables | `common.toml [slurm_default]` ✔ | そのまま継続 |
| **merge 実行点** | DAG load 時に Operator instance に焼き込み | render 時に JobSpec 生成 | `merge_with_defaults`（読み取り後、submit 直前） | **read 時に factory が merge**（F2） |
| **欠損表現** | `**kwargs` に無いキー | JSON null / missing | 空文字 sentinel | **`Option<JobPartition>::None`**（F2） |
| **override 粒度** | per-kwarg | per-variable | per-field | per-field（変わらず） |
| **round-trip 保持** | しない | する | テストでしか write しない | **flow.toml は read-only という運用ポリシー** で割り切る |

### 4.1 命名上の発見

- `common.toml [slurm_default]` は実質 **Prefect Work Pool の `base_job_template`**。
- `[directories]` は env（実行環境設定）相当。これは Airflow の `default_args` には無い、
  プールレベル設定。Prefect 寄り。
- `flow.toml` の各 `[jobs.<id>.config]` は **Prefect deployment variables の per-run override** 相当。

つまり **概念的には Prefect の "Pool template + per-run variables" モデルに最も近い**。
F2 ファクトリ案は、このモデルの最小実装と言い換えられる。

### 4.2 partition は本来 "Pool" 概念

SLURM の partition は queue name でありリソースクラス。Prefect の Work Pool に概念対応する。
**partition ごとに default を持つ多重 Pool 化** までやれば究極形だが、現状 `common.toml`
は1ファイルのみ・1 partition 前提なので **「常に1個の Pool」モデル** と捉えて十分。
複数 partition 運用が必要になったら **複数 `common.toml` を name 付きで持ち、flow.toml
側に `pool = "long"` 等で参照** という拡張余地を残せる。今は不要。

---

## 5. 前回 thought（F2 推奨）との整合性

前回の F2（`pub(super) PartialSlurmJobConfig` + `read_flow_factory(path, &CommonConfig)`）は、
Airflow の `apply_defaults` ＆ Prefect の `render` に対応する **materialize ステップを read
時に置く** という設計判断と完全に一致する。

| F2 の構成要素 | Airflow 対応 | Prefect 対応 |
|---|---|---|
| `PartialSlurmJobConfig`（全 Option） | Operator の `__init__` kwargs (`Optional[T]` 群) | variables（default 持ち） |
| `read_flow_factory(path, common)` | `apply_defaults` ＋ DAG load | `render` (template + variables → JobSpec) |
| `JobFlow<SlurmJobConfig>` 出力 | TaskInstance with concrete kwargs | rendered JobSpec |
| `JobPartition::None` でフォールバック | kwarg in `default_args` が活きる | variable default が活きる |

→ **F2 推奨方向に変更なし**。本 thought は F2 を「業界標準モデルの最小実装である」と
**外部根拠で補強** する位置づけ。

---

## 6. オーケストレータを見て新規に出てくる選択肢（参考まで）

### 6.1 テンプレート変数置換（Prefect 寄り）

flow.toml に Jinja-like 変数を書き、common.toml の値を変数解決する案:

```toml
# flow.toml
[jobs.opt.config]
partition = "{{ common.slurm_default.partition }}"
time_limit = "01:00:00"
```

- Pros: どの値が common 由来か flow.toml 上に明示される。round-trip 自然。
- Cons:
  - **テンプレートエンジン依存追加**（`tera` 等）
  - flow.toml が "純 TOML" でなくなり、外部ツール（IDE 補完・lint）が壊れる
  - YAGNI: 「省略 → common 由来」というシンプルセマンティクスで十分

→ **採用しない**。注釈レベルで欲しければ将来 `jm show --explain` で diff を出す方が筋。

### 6.2 partition ごとの Pool 化（Prefect Work Pool 完全模倣）

複数の `common-<name>.toml` を持ち、flow.toml に `pool = "long"` 等を書く拡張:

- Pros: 大学クラスタで `short` / `long` / `gpu` を切替えるユーザーには自然
- Cons: 現状ユーザー（自分）は単一 partition 運用。spec が膨らむ。
- 判断: **`common.toml` を1個に固定** という CLAUDE.md の既決定方針を維持。
  必要になったらこの方向に拡張。F2 を入れても拡張余地は塞がない。

### 6.3 boilerplate generator（前回 B 案 / Airflow CLI 寄り）

`airflow connections add`, `prefect work-pool create` 相当の `jm new-flow` を追加:

- F2 を入れて flow.toml が短くなれば（partition 等を省略可能）、**`cp -r examples/simple/inputs`
  で十分** なケースが大半。
- B 案は F2 と独立。後でも入れられる。今は不要。

### 6.4 sentinel 値（`REPLACE_ME`）の検出責務

Airflow も Prefect も「テンプレートに残った未解決の placeholder」を submit 前に検出する仕組みを
持つ（Airflow は連結時にエラー、Prefect は schema validation）。
job-manager は現状 `REPLACE_ME` をそのまま `sbatch` に渡してしまう。

→ F2 とは独立の論点だが、**A 系案の共通必須要件** として「submit 直前に
`is_sentinel(partition)` チェック + 明示エラー」をセットで設計すべき。これは前回 thought
§7-5 にも書いた。

---

## 7. 推奨の確定形

前回 thought の **F2 を採用** で確定。本 thought の追加貢献は以下:

1. **設計判断が業界標準と一致していることを確認**:
   F2 は Prefect の `base_job_template` + variables、Airflow の `default_args` と同型。
   独自設計ではない。

2. **語彙の整流**:
   - `common.toml` = Pool template + defaults
   - `flow.toml [jobs.*.config]` = per-task override
   - `PartialSlurmJobConfig` = "renderable" partial spec
   - `read_flow_factory` = render / apply_defaults 相当

   `docs/architecture.md` か `docs/development.md` でこの対応を1段落入れると、
   Airflow / Prefect 経験者が読んだ時に F2 の動機が即座に伝わる。

3. **「default 由来かの保存」を捨てる根拠**:
   Airflow 自体が default_args 由来の値を materialize 後に区別しない（Prefect だけが保存する）。
   job-manager は HPC 用途・file-based・round-trip 不要なので Airflow 寄りで十分。
   F2 が round-trip を失う件は「flow.toml は read-only 入力ファイル」という運用ポリシーで決着可能、
   かつ業界標準の半分（Airflow）が同じ妥協をしている。

4. **拡張余地の確認**:
   - 6.1 テンプレート変数: 不採用、`jm show --explain` で代替可能
   - 6.2 複数 Pool: 拡張余地を塞がない（`common.toml` を name 付き複数ファイル化 → `pool` フィールド追加で対応可能）
   - 6.3 generator: 直交、後付け可
   - 6.4 sentinel 検出: F2 とセットで必須

---

## 8. 進めるなら（前回 §7 の更新版）

前回 thought §7 の 7 ステップに以下を追加:

8. `docs/architecture.md` に「`common.toml` is the Pool template (Prefect 対応) / `default_args`
   (Airflow 対応)、per-job `config` is per-task override」という対応表を1段落追記
9. `is_sentinel(partition)` チェックを `FlowRunner::submit` 直前に追加（`REPLACE_ME` を弾く）

それ以外の 1〜7 は前回のままで OK。

---

## 9. 未確定 / 要検証

前回 §8 の 3 件はそのまま残る:

- `JobPartition` のシリアル表現
- D2 (`gaussian_job_shared`) 側 `JobSpec.config` の型固定 / 型パラメータ化
- `write_flow` の現用ユースケース全件監査

加えて本 thought で出た:

- **`common.toml` 複数化への将来拡張余地**を、F2 実装時にコメント1行で明示しておくか
  （`PartialSlurmJobConfig::merge` 関数の docstring に "pool name resolution は将来追加可能"
  と書く程度で十分）
- **`jm show --explain`** を将来追加するなら、`SlurmJobConfig` の各フィールドが
  common 由来か flow 由来かを **`PartialSlurmJobConfig` を merge 前に保持** することで
  実現できる（F2 とは独立の小機能、UX 向上案）

---

## 10. 参照

- 前 thought: `docs/superpowers/thought/2026-05-15-flow-toml-partition-defaulting.md`
- 比較表の元ネタ: `docs/references/orchestration-systems.md`（特に §1.2 DAG/Task data model、
  §2.1 Prefect Work Pool アーキ、§3 設計判断テーブル）
- A1 `SlurmJobConfig`: `slurm_async_runner` rev `a734a06`,
  `src/entities/slurm/sbatch_options.rs:170-217`
- 現行 merge: `src/persistence/common.rs:27-67` (`merge_with_defaults`)
- examples: `examples/simple/inputs/{common.toml,01999999-.../flow.toml}`
- Airflow `apply_defaults` 解説: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#default-arguments>
- Prefect `base_job_template`: <https://docs.prefect.io/v3/concepts/work-pools#base-job-template>
