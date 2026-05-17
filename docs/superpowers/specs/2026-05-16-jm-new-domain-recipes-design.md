# `jm new <flow-recipe>` — 二層レシピ(Job 層 / Flow 層)— design (rev.5)

**Date:** 2026-05-17
**Status:** Draft (rev.5 — `_base.bash.j2` 共有プリアンブル完全移植 + launcher を D2 `CommonConfig` 拡張で partition と同列の **read 時解決**(scaffold 焼付けではない)+ KUDPC srun 必須反映。rev.4 からの差分は §4.0/§5.5/§6/§7/§10/§11/§13。awaiting user review)
**Reference:**
- 既存エコシステム(整合対象):
  - `miyake-ken/gaussian-experiment-manager`("collapsed" δ/E)・`miyake-ken/GAUSSIAN_repo/examples`:`experiment.toml [step.params]`(`route`/`charge`/`multiplicity`/`extra_input`)、main→afterok→post 2-batch、`.gjf` 形式、`<env.root>/<uuid>/{input,output,derived}/`、`task_basename`(既定 `main`)、InChIKey compound、`[slurm]`/`[slurm.post]`
  - `miyake-ken/gaussian-batch`(β/A2: `gaussian_batch_generator` + `gaussian_batch_cli`)。テンプレは **Jinja 継承 4 枚**:`_base.bash.j2`(共有プリアンブル + `{% block modules %}`/`{% block body %}`)← `gaussian_g16.bash.j2`/`gaussian_post.bash.j2` の 2 leaf + `main.gjf.j2`。実コード評価は §13。**job-manager は依存しない**が、`_base.bash.j2` の共有プリアンブル構造を Rust に**完全移植**する(§4.0)
  - **京大 KUDPC バッチ規約**(`https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch`):「逐次・MPI を問わず、ジョブスクリプトのプログラム実行箇所に**必ず `srun`**」。gem は `srun` を python ラッパ(`gaussian_compute_runtime run-g16`)内部に隠蔽。job-manager レシピは自己完結(python ラッパ無し)ゆえ**生成 body の exec 行を自前で `srun` 包む**(§5.5/§7)
- 二層分離の先行例:nf-core(modules/subworkflows)/ Snakemake(rule/workflow)/ atomate(Firework/Workflow)/ gem(step program / step 連鎖)
- `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`(既存 `jm new` sentinel 哲学)
- `docs/superpowers/specs/2026-05-15-common-env-defaulting-design.md`(partition の read 時 default 注入機構。本 spec の launcher はこれと**同タイミング**)
- `src/bin/jm.rs`(`Cmd::New` / `cmd_new` / `build_flow_template` / `build_plan_template` / `atomic_write_str`)
- `src/persistence/path.rs`(**PathResolver 真実**:`flow_dir=<root>/<uuid>/`、`batch.bash=<root>/<uuid>/.jm/<JobId>/batch.bash`、`.jm` は `<uuid>` 直下で `<JobId>` は `.jm` の下。user-authored は `.jm` の外・一段上)
- `src/persistence/common.rs`(`read_common`/`merge_with_defaults`/`synth_empty_common`。`CommonConfig{slurm_default:SlurmJobConfig(A1), directories:DirectoryConfig(D2)}`。`launcher` は D2 `CommonConfig` 新 field)
- `src/render/mod.rs`(`render_batch_bash(flow_uuid,jid,parts,params,body)` — 公開 API + Python エクスポート。`JM_FLOW_UUID/JM_JOB_ID/JM_AXIS_*/JM_PARAM_*` を `params` から焼く。**シグネチャ不変方針**)
- `src/runner/flow.rs:148-269`(submit/render 経路。`fr.common`(materialized `CommonConfig`)を保持し、job ごとに `effective_config`(partition 解決済)/`params_of` を取り `render_batch_bash` 呼出 → **launcher read 時解決の seam はここ**)
- 上流 A1 `slurm-async-runner2/src/sbatch/cmd.rs`(`build_argv` は script を絶対パスで sbatch に渡す。sbatch は spool コピー実行 → 実行中 `$0`/`pwd` は元位置でない。job cwd = `SLURM_SUBMIT_DIR` = `jm submit` を叩いた場所)
- CLAUDE.md(Out of scope / PyO3 境界 / `.jm/` レイアウト / `--no-default-features` / **`### Upstream modification policy`** = A1 不変・D2 必要時可)

---

## 1. 問題設定

`jm new`(別 spec)は静的 2-job 雛形のみ。ユーザ要求は「g16 構造最適化 → afterok → 結果検証」のドメインチェーン scaffold。エコシステム精査の結果、確立実装は **cclib による .out パース検証**(gem `gaussian-parse-results`。ただし β/A2 では γ-pending 未実装 — §13)。

加えて 2 つの実運用要件が rev.4 に欠けていた:

- **(A) 共通フローのプリアンブル**:gem の `_base.bash.j2` は全 job 共通の重い環境セットアップ(`set -euo pipefail`・継承 conda スタックの全消去・module init・`module restore <profile> -f`・`conda activate <env>`・任意 pixi hook・末尾 `echo JOB DONE; exit 0`)を Jinja 継承で共有し、leaf は `modules`/`body` ブロックのみ差分。rev.4 の薄い body はこのプリアンブルを丸ごと欠いていた。
- **(B) srun 必須**:KUDPC は exec 箇所に必ず `srun`。gem は python ラッパに隠蔽するが job-manager レシピは自己完結ゆえ自前で包む必要。さらに「クラスタ固有値は一元定義」を `partition` と同じフローに揃える必要(ユーザ指示)。

テンプレートは **再利用可能な二層**:

- **Job 層(`JobTemplate`)** — 1 バッチ = 1 ジョブの自己完結部品(例 `g16_opt`, `parse_g16_out`)。
- **Flow 層(`FlowRecipe`)** — JobTemplate を DAG に合成(例 `g16-opt-parse`)。`jm new` が叩く scaffold 単位。

レシピはコードではなく **編集可能で自己完結な job-manager flow 一式**を生成。gem の**ドメイン意味論と `_base.bash.j2` プリアンブル構造**を写すが**スキーマは job-manager の `flow.toml`/`plan.toml`**。gaussian-batch には依存しない(自己完結。§13)。

## 2. ゴール / 非ゴール

### Goals

1. `jm new` に位置引数 `<flow-recipe>` を追加。`jm new`(無引数)= 組込 `blank`(既存 2-job 雛形、**後方互換**)。`jm new g16-opt-parse --param opt.charge=1`。`--param opt.input_coordinate=<path>` で分子座標ファイルを scaffold 時取り込み(§7)。
2. **二層レジストリ**:`JobTemplate` / `FlowRecipe`。FlowRecipe のみ scaffold 可能(JobTemplate は内部合成単位)。
3. 出力ツリーは構築時 `jm doctor`-clean を保証(flow JobId 集合 == plan `[jobs.*]` キー集合、uuid == ディレクトリ名、親エッジ整合)。
4. domain JobTemplate は flow.toml/plan.toml 寄与に加え、**自分の JobId 名前空間下にサイドカー**を出す:`<JobId>/scripts/<JobId>.bash`(`_base.bash.j2` 完全移植プリアンブル + modules/body ブロック、編集可能)、`<JobId>/input/main.gjf`、`parse` は `<JobId>/scripts/parse_results.py`。配置・命名は gem 準拠。
5. JobTemplate ごとに型付きパラメータ。CLI は **`--param <JobId>.<param>=<value>`**。`jm new --list` / `<flow-recipe> --describe`。
6. v1:JobTemplate `g16_opt` / `parse_g16_out`、FlowRecipe `blank` / `g16-opt-parse`。「テンプレ多数」要求は §4.0 共有プリアンブル + 継承モデルで満たし、leaf は v1 で増やさない(YAGNI、ユーザ判断)。
7. 書き込みは中途半端を残さない(既存 `jm new` rollback 規約踏襲)。
8. **path 解決は core 変更ゼロの R3**(scaffold 時に flow.toml `body` へ絶対 job dir を焼く。§5)。**launcher のみ R3 焼付けではなく partition と同タイミングの read 時解決**(§5.5)。`render_batch_bash` 公開シグネチャ/PyO3/`.pyi`/cwd 契約は不変。
9. **D2 `CommonConfig` に `launcher: Option<String>` を追加**(`#[serde(default)]`)。CLAUDE.md `### Upstream modification policy`(A1 不変・D2 必要時可)に則る調整変更。A1 `SlurmJobConfig` は不変。

### Non-goals

- `common.toml` の**自動生成**(v1。per-job `[jobs.*.config]` は `partition="REPLACE_ME"` sentinel 据置)。ただし `launcher` を `common.toml` から**読む**経路は Goal 9 で導入。
- 分子幾何の**自動取得**。`--param <jobid>.input_coordinate=<path>` のユーザ提供座標取り込みは行う(§7)。
- gem `experiment.toml`/`common.toml` フォーマット採用、gem `metadata.toml`/`status` 再実装(job-manager Lifecycle + `decide_transition` + `tick` が status の権威。parse は exit code のみ)。
- **SBATCH ディレクティブ層**:`_base.bash.j2` 1-12 行目の `#SBATCH`(partition/job-name/time/output/error/**`--rsc`**/mail)は job-manager の `SbatchCmd`/render(`SlurmJobConfig`)が担う領域で**レシピ範囲外**。KUDPC 独自 `--rsc p=:t=:c=:m=` を A1 `SbatchCmd` が出すか否かは**別 issue**(本 spec 非目標。要別途検証)。レシピが移植するのは `_base.bash.j2` の**シェルスクリプト部(13-70 行目相当)**のみ。
- **gaussian-batch への依存**(コード依存も実行時 CLI 依存も)。参照と任意 swap-in フックのみ(§13)。
- experiment DSL / sweep 展開 / 親解決 / JobTemplate 直接 scaffold / 対話 TUI / 既存 flow 再生成・migration / リモートレジストリ / OpenMM。
- path の **R1/R2/R4 は不採用**(§5)。`A1 改変`(`SlurmJobConfig` への launcher 追加等)は**禁止**(CLAUDE.md ポリシー)。

## 3. CLI 形

```
jm --root <ROOT> new [<FLOW-RECIPE>] [--param <JOBID.PARAM=VALUE>]... [--tag <K=V>]... [--print-path]
jm --root <ROOT> new --list
jm --root <ROOT> new <FLOW-RECIPE> --describe
```

| 引数 | 説明 |
|---|---|
| `<FLOW-RECIPE>`(位置, 任意) | FlowRecipe 名。省略時 `blank`。未知名は候補列挙付きエラー。 |
| `--param <JobId>.<param>=<value>` | 任意回。最初の `=` で `key=value`、`key` を最初の `.` で `<JobId>.<param>`。未知 JobId / 未知 param / 型不整合 / `.`・`=` 欠落はエラー。 |
| `--tag <K=V>` | 既存。`flow.toml [tags]`。 |
| `--print-path` | 既存。stdout に `<root>/<uuid>` のみ。 |
| `--list` | FlowRecipe 名 + 1 行説明を列挙して終了。 |
| `--describe` | `<FLOW-RECIPE>` の合成ノードと各 `<JobId>.<param>`(型/既定/ヘルプ)を列挙して終了。 |

`Cmd::New` を `recipe: Option<String>` / `params: Vec<String>` / 既存 `tags` / `print_path` / `list: bool` / `describe: bool` へ拡張。`main()` 分岐を `cmd_new(&root, recipe.as_deref(), &params, &tags, print_path, list, describe)` に。

## 4. アーキテクチャ(二層)

### モジュール配置

```
src/recipes/
  mod.rs           -- 公開 re-export, registries, --param パース, --list/--describe
  job.rs           -- JobTemplate trait, JobArtifacts, JobCtx, RecipeParam/Type, base_preamble()
  flow.rs          -- FlowRecipe trait + 合成アセンブラ(assemble())
  jobs/{g16_opt.rs, parse_g16_out.rs}
  flows/{blank.rs(据置・非分解・バイト同値), g16_opt_parse.rs}
  assets/g16_opt/main.gjf.tmpl
  assets/parse_g16_out/parse_results.py.tmpl
```

- `src/recipes/` は **pyo3 非依存**(`uuid`/`chrono`/`toml`/std のみ)。`jm` `--no-default-features` 必須。
- JobTemplate / FlowRecipe / `base_preamble()` は **純粋**(I/O 無し)。I/O・コピー(input_coordinate)・rollback・launcher 解決は `cmd_new`/render パス。
- `src/lib.rs` から `pub use recipes::{JobTemplate, FlowRecipe, flow_registry, base_preamble, ...}`(**公開 API は追加のみ**)。

### 4.0 共有ベースプリアンブル(`_base.bash.j2` 完全移植)

`gaussian_batch_generator/.../templates/_base.bash.j2` の**シェルスクリプト部**を Rust 関数 `base_preamble()` に完全移植。Jinja 継承(`{% block modules %}`/`{% block body %}`)を**ブロック差込 helper**で写経する。trait に新メソッドは足さず(KISS)、各 JobTemplate の `instantiate()` がこの helper を呼んで `scripts/<JobId>.bash` を組む:

```rust
pub struct PreambleOpts<'a> {
    pub conda_env: &'a str,       // 既定 "analysis"(param 化)
    pub module_block: &'a str,    // {% block modules %} 相当(JobTemplate が供給)
    pub body_block: &'a str,      // {% block body %} 相当(srun 包んだ exec 行)
    pub pixi_manifest: &'a str,   // 既定 ""(空=pixi hook 省略。レシピは自己完結ゆえ通常空)
}
pub fn base_preamble(o: &PreambleOpts<'_>) -> String;
```

`base_preamble()` の固定構造(`_base.bash.j2` 13-70 行と 1:1。`#SBATCH` ヘッダ 1-12 行は **non-goal**:job-manager の SbatchCmd 領域):

```
set -euo pipefail
# inherited conda スタック全消去(env var + conda 関数)
#   ← 学習スキル pixi-conda-stack-reset と同一の load-bearing ブロック。固定文字列(param 化しない)
source "$(conda info --base)/etc/profile.d/conda.sh"
. /usr/share/Modules/init/bash
{module_block}                    ← JobTemplate 供給(下記)
conda activate {conda_env}
{pixi hook(pixi_manifest 非空時のみ)}
# ---- JOB BODY ----
{body_block}                      ← JobTemplate 供給($JM_LAUNCHER 包み。§5.5)
echo "JOB DONE"
exit 0
```

- `module_block`:`g16_opt` → `module restore {module_profile} -f`(`module_profile` 既定 `gaussian_A`、param 化)。`parse_g16_out` → `module restore default -f`(gem post が base 既定を使うのと同じ)。
- サイト固有値のみ param 化:`conda_env`(既定 `analysis`)・`module_profile`(既定 `gaussian_A`)・`pixi_manifest`(既定空)。それ以外は `_base.bash.j2` 同値の固定ボイラープレート。
- `blank` には適用しない(§8。バイト同値要件)。

### 型(Job 層)

```rust
pub enum RecipeParamType { Str, Int, Float, Bool, Path }
pub struct RecipeParam { pub name: &'static str, pub ty: RecipeParamType,
                         pub default: &'static str, pub help: &'static str }

pub struct JobArtifacts {
    pub program: String,                            // "g16" / "python" 等
    pub body: String,                               // flow.toml jobs.<JobId>.body。R3: 絶対 cd + `bash scripts/<JobId>.bash`(薄起動子)
    pub time_limit: Option<String>,                 // [jobs.<JobId>.config].time_limit。partition は常に REPLACE_ME
    pub plan_params: BTreeMap<String, toml::Value>, // → plan.toml [jobs.<JobId>]
    pub sidecars: Vec<GeneratedFile>,               // scripts/<JobId>.bash は base_preamble() で構築済、relpath は "<JobId>/..." 名前空間化済
}
pub struct GeneratedFile { pub relpath: PathBuf, pub contents: String, pub unix_mode: Option<u32> }

pub struct JobCtx<'a> {
    pub job_id: &'a str,
    pub params: &'a BTreeMap<String, toml::Value>,  // 既定 + --param 上書き後の検証済
    pub inputs: &'a BTreeMap<&'static str, String>, // 入力名 → 相対パス `../<producer JobId>/<relpath>`(cwd=自 job dir 前提)
    pub uuid: &'a Uuid, pub created_at: &'a str,
}

pub trait JobTemplate: Send + Sync {
    fn name(&self) -> &'static str;
    fn params(&self) -> &'static [RecipeParam];
    fn inputs(&self) -> &'static [&'static str];
    fn outputs(&self) -> &'static [(&'static str, &'static str)];
    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, RecipeError>;
}
```

### 型(Flow 層)+ 合成アセンブラ

```rust
pub trait FlowRecipe: Send + Sync {
    fn name(&self) -> &'static str;
    fn summary(&self) -> &'static str;
    fn nodes(&self) -> &'static [(&'static str, &'static str)];
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)];
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)];
}
pub fn flow_registry() -> Vec<Box<dyn FlowRecipe>>;          // [Blank, G16OptParse]
pub fn find_flow(name: &str) -> Option<Box<dyn FlowRecipe>>;
pub fn find_job(name: &str) -> Option<Box<dyn JobTemplate>>;
```

**アセンブラ `flow::assemble(recipe, raw_params, tags, uuid, created_at, abs_flow_dir) -> Result<Vec<GeneratedFile>>`**:

1. `recipe.nodes()` 各 `(job_id, tmpl)` を `find_job(tmpl)` 解決(未知=レシピ定義バグ内部エラー)。
2. `--param` を `<JobId>.<param>` でパース → JobId ごと分配 → 各ノード `params()` 既定で埋め、型検証して上書き。
3. `recipe.wiring()` 解決:consumer の各入力名に**相対** `../<producer JobId>/<producer 出力 relpath>` を計算し `JobCtx.inputs` へ。
4. 各ノード `instantiate(&ctx)?` → `JobArtifacts`(sidecar の `scripts/<JobId>.bash` は `base_preamble()` で組済)。body 先頭の絶対 cd は **R3**(§5)。
5. **flow.toml 組立**:`jobs.<JobId>` = {program, body, config(`partition="REPLACE_ME"` + time_limit)}、`edges()` から `[[jobs.<to>.parents]]`。
6. **plan.toml 組立**:`[jobs.<JobId>]` = `plan_params`。ノード集合から構築 → **flow JobId 集合 == plan キー集合**(doctor-clean by construction)。
7. sidecars + flow.toml + plan.toml を `Vec<GeneratedFile>` で返す。

JobTemplate は peer の JobId をハードコードしない(wiring は FlowRecipe がデータ宣言)→ 再利用可能。

### `cmd_new` シーケンス

1. `--list`: `flow_registry()` を `name — summary` 出力で終了。
2. flow 解決: `recipe.unwrap_or("blank")` → `find_flow()`。`None` → bail(候補列挙)。
3. `--describe`: ノードと `<JobId>.<param>` 表を出力して終了。
4. `--tag` パース(既存流用)。
5. `uuid = Uuid::now_v7()`; `resolver = PathResolver::new(root)`; `flow_dir`(絶対); 衝突確認(`exists()` で bail)。
6. `created_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)`。
7. `flow::assemble(.., abs_flow_dir=flow_dir)?`。
8. **input_coordinate 取り込み**:`--param <jid>.input_coordinate=<src>` 指定時 `<src>` を `<flow_dir>/<jid>/input/<basename>` へコピー。`.xyz` は §7 の純 Rust 差込、他形式は copy のみ + sentinel。`<src>` 不在は bail。
9. `create_dir_all`。各 `GeneratedFile` を原子書込。`*.bash`/`*.py` は 0755。
10. 失敗時:`flow_dir` を `remove_dir_all` 巻き戻し → `?` 伝播。
11. 出力(asset 込み列挙)。`--print-path` 時は `<root>/<uuid>` の 1 行のみ。

> **launcher は `cmd_new` では解決しない**(§5.5):sidecar の exec 行は `$JM_LAUNCHER` という**間接参照**を焼くだけ。実値は `jm render`/`submit` 時に render パスが `common.toml` から解決して `batch.bash` に export する。

## 5. flow パス解決 — R3(scaffold 時に絶対 job dir を body へ焼く)

### 制約(実コード確認済み)

- PathResolver:batch.bash = `<root>/<uuid>/.jm/<JobId>/batch.bash`(`.jm` は `<uuid>` 直下)。レシピ sidecar は `.jm` の外 `<root>/<uuid>/<JobId>/...`。
- A1:sbatch へ batch.bash を絶対パスで渡し spool コピー実行 → 実行中 `$0`/`pwd` 不可。job cwd = `SLURM_SUBMIT_DIR`(非決定的)。
- ⇒ 走る batch.bash に自 job dir の錨が無い。

### 採用:R3

`jm new` は scaffold 時に `<root>/<uuid>/<JobId>` を確定的に知る。**flow.toml `body` 先頭に絶対 cd として焼く**:

```toml
[jobs.opt]
program = "g16"
body = """cd "<root>/<uuid>/opt" || exit 1
bash scripts/opt.bash
"""
```

- `body` は薄起動子(絶対 cd 1 行 + `bash scripts/<JobId>.bash`)。重い処理・環境(`_base.bash.j2` 完全移植プリアンブル §4.0)は編集可能 `scripts/<JobId>.bash` に置き cwd=job dir 前提の**純相対**。
- クロスジョブ参照は**相対** `../<producer>/output/main.out`。絶対は body の cd 1 行のみ。
- **core 変更ゼロ**:`flow.rs`/`render_batch_bash`/PyO3/`.pyi`/cwd 契約すべて不変。

| 案 | core 変更 | body 汚染 | cwd 契約 | 判定 |
|---|---|---|---|---|
| **R3(採用)** | **ゼロ** | 絶対 cd 1 行 | 不変 | **採用** |
| R4 | flow.rs 1 行 + env | `$JM_FLOW_DIR` 散在 | 不変 | 却下(ユーザ削除) |
| R2 | `cmd.chdir` | なし | **全ジョブ変更** | 却下 |
| R1 | render シグネチャ | — | 不変 | 却下(公開 API/PyO3 破壊) |

### 既知の小制約(R3)

flow dir 移動/コピー、login↔compute マウント差で body 絶対 cd が破綻。緩和:UUID dir 不動運用 + 安定 root(lustre)。将来 `jm render --rebase-paths` は**別 spec・本 spec 非対象**。**launcher はこの制約を受けない**(read 時解決ゆえ。§5.5)。

### 5.5 launcher — partition と同タイミングの read 時解決(scaffold 焼付けではない)

KUDPC は exec 箇所に必ず `srun`。クラスタ固有値ゆえ `partition` と同じ「一元定義 → 全 job へ default、per 上書き、read 時解決」に揃える(ユーザ指示)。`SlurmJobConfig`(A1)は不変なので **D2 `CommonConfig` に新 field**:

```rust
// D2 gaussian_job_shared::config::common::CommonConfig
pub struct CommonConfig {
    pub slurm_default: SlurmJobConfig,   // A1 — 不変
    pub directories: DirectoryConfig,    // D2
    #[serde(default)]
    pub launcher: Option<String>,        // ★ D2 新 field。common.toml 直下 `launcher = "srun"`
}
```

`#[serde(default)]` ゆえ既存 `common.toml`(`launcher` 無し)もパース可(`CommonConfig` の `#[serde(deny_unknown_fields)]` と両立)。

**partition との対応(厳密同タイミング)**:

| | partition | launcher |
|---|---|---|
| 一元定義 | `common.toml [slurm_default] partition` | `common.toml launcher`(D2 新 field) |
| per 上書き | flow.toml `[jobs.*.config] partition` 明示 | recipe param `--param <JobId>.launcher=`(任意) |
| 解決タイミング | `jm submit`/`render` の read 時 | **同左(`jm submit`/`render` の read 時)** |
| 解決層 | `read_flow` preparse + `merge_with_defaults` | render パス(`fr.common` 保持点) |
| 不在時 fallback | `PartitionMissing` | ハードコード `"srun"`(common 無=KUDPC 正準) |

**機構**:

1. `jm new` は `scripts/<JobId>.bash` の body ブロック exec 行を **`$JM_LAUNCHER` 間接参照**で焼く(scaffold 時に値を解決しない — R3 の絶対パス焼付け哲学を launcher のみ意図的に反転し partition の read 時意味論に合わせる):
   ```
   $JM_LAUNCHER g16 input/main.gjf output/main.out      # g16_opt
   $JM_LAUNCHER python scripts/parse_results.py "../opt/output/main.out"   # parse_g16_out
   ```
   **シェル正当性(重要)**:`$JM_LAUNCHER` は**意図的に未クォート**。空値時に単語分割で消滅させるため(`"$JM_LAUNCHER" g16` だと空値が `argv[0]=""` になり command-not-found。複数語 launcher にも単語分割が正しく作用)。
2. `jm render`/`submit` 時、render パス(`src/runner/flow.rs` submit/render_only。`fr.common` を保持し job ごとに `effective_config`/`params_of` を取り `render_batch_bash` を呼ぶ点)が launcher を**次の優先順位で解決**(partition の「per-job 明示値 > common」と同型):

   | # | 条件 | 解決値 |
   |---|---|---|
   | 1 | plan.toml の `launcher` param が**非空** | その値(per-flow 上書き = flow.toml partition 上書きの類推) |
   | 2 | param 空(既定) かつ `common.launcher = Some(非空)` | その値(例 `"srun"`。クラスタ既定) |
   | 3 | param 空 かつ `common.launcher = Some("")` | **bare**(空文字列。「このクラスタに srun 無」のクラスタ明示) |
   | 4 | param 空 かつ `common.launcher = None`(キー不在) | ハードコード `"srun"`(KUDPC 正準) |

   解決値を `batch.bash` の job-manager runtime-context ブロックに `export JM_LAUNCHER=<resolved>` で注入(ケース 3 は空 export = bare exec)。
   **配線制約の補強**:§下の 3 実装選択肢のいずれを採っても、この優先順位を**単一の `JM_LAUNCHER` に解決してから** export すること(生 `JM_PARAM_LAUNCHER` をそのまま使うだけでは不可 — 空 param がケース 2-4 へフォールバックしないため)。
3. `scripts/<JobId>.bash` は親 `batch.bash` から `JM_LAUNCHER` を環境継承(`bash scripts/<JobId>.bash` は子プロセス)。sidecar を直接実行(ローカル検証)した場合は `JM_LAUNCHER` 未設定 → 未クォートゆえ bare 起動に degrade(ローカルで動く。KUDPC は batch.bash 経由ゆえ常に `srun`)。

**公開シグネチャ不変(R1 制約堅持)**:`render_batch_bash(flow_uuid, jid, parts, params, body)` は**変えない**。launcher は render ループ(`fr.common` 所有)が runtime-context ブロックに注入。**正確な配線は writing-plans で確定**するが、以下 3 制約を課す:(i) A1 不変、(ii) 既存公開 `render_batch_bash` を破壊しない(加法的:解決済 `launcher: Option<&str>` を取る兄弟関数 / `runtime_ctx` map / `JM_PARAM_LAUNCHER` 露出 のいずれか)、(iii) 解決は render 時に**その時点の `common.toml`** から実行。

**純度**:`base_preamble()`/JobTemplate は launcher 値を知らない(`$JM_LAUNCHER` 文字列を埋めるだけ)→ `src/recipes/` の pyo3 非依存・純粋性は保たれる。解決の副作用は render パスのみ。

**ネット効果 = partition と完全同等**:`common.toml launcher` を編集 → `jm render` 再実行で launcher 更新、**再 scaffold 不要**。R3 の絶対 cd 焼付けは scaffold 時のまま(path のみ)、launcher だけ read 時。

## 6. リファレンス整合マッピング(gem ↔ job-manager 二層)

| gem(確立規約) | job-manager 二層表現 |
|---|---|
| `_base.bash.j2` 共有プリアンブル(conda リセット/module/conda activate/末尾) | **`base_preamble()`**(§4.0、完全移植。`#SBATCH` ヘッダは除く=非目標) |
| `{% block modules %}`(`gaussian_g16` は `module restore gaussian_A -f`) | JobTemplate 供給の `module_block`(`g16_opt`→`module restore {module_profile} -f`、既定 `gaussian_A`) |
| `{% block body %}` + srun(gem は python ラッパ内に srun 隠蔽) | `body_block`(job-manager は自己完結ゆえ自前 `$JM_LAUNCHER` 包み。§5.5) |
| step program `gaussian`(`run-g16`) | **JobTemplate `g16_opt`**(program=`g16`) |
| post `gaussian-parse-results`(cclib、β/A2 で γ-pending 未実装) | **JobTemplate `parse_g16_out`**(自前 `parse_results.py`。§13) |
| step 連鎖 + afterok | **FlowRecipe `g16-opt-parse`** |
| `[step.params] route/charge/multiplicity/extra_input` | `g16_opt.params()`。`--param opt.route=...`。`plan.toml [jobs.opt]` |
| coord(.mol2/.xyz)→ gjf 幾何 | `--param opt.input_coordinate=<path>`(§7) |
| `parent_uuids` 出力 consume | FlowRecipe `wiring()` → 相対 `../opt/output/main.out` |
| `common.toml [slurm]`/`[slurm.post]` | `[jobs.opt.config]`(48h)/`[jobs.parse.config]`(1h)。partition 両 `REPLACE_ME` |
| `common.toml`(クラスタ不変値の一元定義) | **`common.toml launcher`**(D2 新 field、§5.5)。`partition` と同フロー |
| `[env].task_basename=main`、`{input,output,derived}/` | `<JobId>/{input,output,derived,scripts}/`、`main` 固定 |
| InChIKey compound | `--param opt.compound=<InChIKey>` |
| status(post が権威) | job-manager Lifecycle/tick が権威。parse は exit code のみ |
| gaussian-batch `main.gjf.j2`/post template | **参照のみ**。依存しない。任意 swap-in は `# REPLACE_ME` フック(§13) |

## 7. v1 JobTemplate / FlowRecipe 詳細

### JobTemplate `g16_opt`

`params()`:

| name | type | default | help |
|---|---|---|---|
| `route` | str | `#p opt b3lyp/6-31g(d)` | Gaussian route 行 |
| `charge` | int | `0` | 全電荷 |
| `multiplicity` | int | `1` | スピン多重度 |
| `extra_input` | str | `` | charge/mult・geometry の後の追加入力 |
| `nproc` | int | `8` | `%nprocshared` |
| `mem` | str | `8GB` | `%mem` |
| `compound` | str | `REPLACE_ME-INCHIKEY` | InChIKey。gjf title + `[tags].compound` |
| `conda_env` | str | `analysis` | `base_preamble()` の `conda activate <env>`(§4.0) |
| `module_profile` | str | `gaussian_A` | `module restore <profile> -f`(§4.0) |
| `pixi_manifest` | path | `` | 空=pixi hook 省略(自己完結ゆえ通常空。§4.0) |
| `launcher` | str | `` | per-flow の launcher 上書き。空=`common.toml launcher`→`srun` に委譲(優先順位は §5.5) |
| `input_coordinate` | path | `` | 分子座標(`.xyz`/`.mol2` 等)。`cmd_new` が `<uuid>/<JobId>/input/` へコピー |

- `inputs()` = `[]`。`outputs()` = `[("gaussian_out","output/main.out")]`。
- `instantiate`:program `"g16"`、`time_limit "48:00:00"`、`plan_params`(パス系は basename のみ)、sidecars:
  - `<JobId>/scripts/<JobId>.bash`(0755)= **`base_preamble()` で構築**:
    - `module_block` = `module restore {module_profile} -f`
    - `conda_env` = param `conda_env`
    - `body_block`(cwd=job dir、純相対、`$JM_LAUNCHER` 未クォート §5.5):
      ```
      mkdir -p output
      $JM_LAUNCHER g16 input/main.gjf output/main.out
      ```
    - 結果は `_base.bash.j2` 同型(set -euo pipefail / conda 全消去 / conda.sh / module init / module restore / conda activate / body / echo JOB DONE; exit 0)
    - `# (optional) gaussian-batch α-reshape 後は gaussian-generate-gjf に差替可。§13` を注記
  - `<JobId>/input/main.gjf`(gem 形式、`{{}}` 差込):
    ```
    %rwf=main.rwf
    %nprocshared={{nproc}}
    %mem={{mem}}
    %chk=main.chk
    {{route}}

    {{compound}}

    {{charge}} {{multiplicity}}
    {{geometry_block}}
    {{extra_input}}
    ```
    - `input_coordinate` 未指定:`{{geometry_block}}` = `<GEOMETRY: REPLACE_ME — 1行1原子 Element x y z。route に geom=connectivity を含むなら空行後 connectivity>`。
    - `.xyz` 指定:`cmd_new` が xyz(行1=原子数 / 行2=コメント / 以降 `Elem x y z`)を**純 Rust** パースし座標行差込。元ファイルも `input/<basename>` 保存。
    - `.mol2` 等:`input/<basename>` コピーのみ。`{{geometry_block}}` は sentinel + 注記。OpenBabel 等は `jm new` に持ち込まない。
- body(flow.toml、R3):`cd "<root>/<uuid>/<JobId>" || exit 1` + `bash scripts/<JobId>.bash`。

### JobTemplate `parse_g16_out`

- `params()` = `[ conda_env(既定 analysis), pixi_manifest(既定空), launcher(既定空) ]`(§4.0 サイト param + §5.5 launcher 上書き。`module_profile` は持たず base 既定 `module restore default -f`)。`inputs()` = `["gaussian_out"]`。`outputs()` = `[]`(`derived/main.mol2` は TODO 拡張点)。
- `instantiate`:program `"python"`、`time_limit "01:00:00"`、`plan_params` = `{ note = "cclib parse + convergence/energy validation" }`、sidecars:
  - `<JobId>/scripts/<JobId>.bash`(0755)= **`base_preamble()` で構築**:
    - `module_block` = `module restore default -f`(gem post と同じ)
    - `conda_env` = param `conda_env`(cclib を持つ env)
    - `body_block`:`$JM_LAUNCHER python scripts/parse_results.py "{{inputs.gaussian_out}}"`(`{{inputs.gaussian_out}}` は wiring が相対 `../opt/output/main.out` に解決。`$JM_LAUNCHER` 未クォート — KUDPC は逐次でも srun 必須 §5.5)
  - `<JobId>/scripts/parse_results.py`(0755、cclib。沈黙成功を避ける):
    - 引数:Gaussian `.out`。`cclib` import 失敗 → 明示メッセージで **exit 2**。
    - 実 pass/fail:(a) パース不可→exit 1、(b) 正常終了マーカ無し→exit 1、(c) opt 収束 False→exit 1、(d) 最終エネルギー非有限→exit 1、全充足で exit 0。stdout に検証済/未検証明記。
    - `# TODO(jm recipe): write derived/main.mol2` を拡張点に。
    - status は Lifecycle/tick が権威。本スクリプトは exit code のみ。
- body(flow.toml、R3):`cd "<root>/<uuid>/<JobId>" || exit 1` + `bash scripts/<JobId>.bash`。

### FlowRecipe `g16-opt-parse`

- `nodes()` = `[("opt","g16_opt"), ("parse","parse_g16_out")]`
- `edges()` = `[("opt","parse","afterok")]`
- `wiring()` = `[("parse","gaussian_out","opt","gaussian_out")]` → `parse` の `{{inputs.gaussian_out}}` = `../opt/output/main.out`
- 生成 `flow.toml`(抜粋):
  ```toml
  uuid = "<uuid>"
  created_at = "<rfc3339>"
  [tags]
  recipe   = "g16-opt-parse"
  compound = "<opt.compound>"

  [jobs.opt]
  program = "g16"
  body = """cd "<root>/<uuid>/opt" || exit 1
  bash scripts/opt.bash
  """
  [jobs.opt.config]
  partition  = "REPLACE_ME"
  time_limit = "48:00:00"

  [jobs.parse]
  program = "python"
  body = """cd "<root>/<uuid>/parse" || exit 1
  bash scripts/parse.bash
  """
  [[jobs.parse.parents]]
  from = "opt"
  kind = "afterok"
  [jobs.parse.config]
  partition  = "REPLACE_ME"
  time_limit = "01:00:00"
  ```
- 生成 `plan.toml`:
  ```toml
  [jobs.opt]
  route = "#p opt b3lyp/6-31g(d)"
  charge = 0
  multiplicity = 1
  extra_input = ""
  nproc = 8
  mem = "8GB"
  compound = "REPLACE_ME-INCHIKEY"
  conda_env = "analysis"
  module_profile = "gaussian_A"
  pixi_manifest = ""
  launcher = ""
  [jobs.parse]
  note = "cclib parse + convergence/energy validation"
  conda_env = "analysis"
  pixi_manifest = ""
  launcher = ""
  ```
- 任意:`common.toml` に `launcher = "srun"`(無くてもハードコード fallback で KUDPC 正準。§5.5)。

## 8. `blank` FlowRecipe(後方互換)

既存 `build_flow_template`/`build_plan_template`(`src/bin/jm.rs:497-574`)を `flows/blank.rs` へ移設。**JobTemplate 非分解**で直接出力(`assemble()`/`base_preamble()` を介さない)し **既存 `jm new` 出力とバイト同値**。`jm new`(無引数)= `jm new blank`。サイドカー無し・R3 絶対 cd 無し・プリアンブル無し・`$JM_LAUNCHER` 無し(既存挙動完全維持)。

## 9. エラーハンドリング

| 状況 | 挙動 |
|---|---|
| 未知 FlowRecipe | `bail!("unknown recipe {name:?}; available: blank, g16-opt-parse")` |
| `--param` に `.`/`=` 欠落 | `bail!("invalid --param: expected <JobId>.<param>=<value>, got {raw}")` |
| 未知 JobId | `bail!("recipe {flow}: no node {jobid}; nodes: opt, parse")` |
| 未知 param / 型不整合 | `bail!("recipe {flow}: job {jobid}: unknown/typed param ...")` |
| `input_coordinate` の src 不在 | `bail!("input_coordinate {src}: not found")`(コピー前検証) |
| FlowRecipe が未知 JobTemplate 参照 | 内部エラー。registry 整合をユニットテストで保証 |
| `flow_dir` 既存 | `bail!("flow dir already exists: {path}")` |
| sidecar/コピー書込失敗 | `flow_dir` 巻き戻し後 `?` 伝播 |
| `--list`/`--describe` | scaffold せず終了 |
| (実行時)`cclib` 未導入 | `parse_results.py` が exit 2 |
| (実行時)`common.toml` に `launcher` キー無し かつ param 空 | エラーにせずハードコード `srun`(§5.5 ケース 4。KUDPC 正準) |
| (実行時)`common.toml launcher = ""` 明示 かつ param 空 | bare exec(§5.5 ケース 3。「このクラスタに srun 無」)。未クォート `$JM_LAUNCHER` 展開で消滅 |
| (実行時)plan.toml `launcher` param 非空 | その値が common に優先(§5.5 ケース 1) |

## 10. テスト

### ユニット(`src/recipes/**`)

- **`base_preamble()`**:出力に `set -euo pipefail`・conda スタック全消去ブロック(`unset -f conda` / `CONDA_` ループの固定文字列)・`source ... conda.sh`・`. /usr/share/Modules/init/bash`・`{module_block}` 差込位置・`conda activate <conda_env>`・末尾 `echo "JOB DONE"`/`exit 0` が `_base.bash.j2` と同順。`pixi_manifest` 空で hook 行が**出ない**、非空で `pixi shell-hook --manifest-path` 行が出る。`#SBATCH` 行を**含まない**(非目標境界の回帰)。
- `g16_opt.instantiate`:program `g16`、sidecar `<JobId>/scripts/<JobId>.bash`(0755)が `base_preamble()` 経由で `module restore gaussian_A -f`(既定)・`conda activate analysis`(既定)・body に **未クォート `$JM_LAUNCHER g16 input/main.gjf output/main.out`** を含む。`--param opt.module_profile=X`/`opt.conda_env=Y` が反映。`input/main.gjf` に `{{}}` 残存無し・gem ヘッダ順、`input_coordinate` 未指定で `<GEOMETRY: REPLACE_ME>`、`.xyz` で座標差込(純 Rust xyz パーサ単体:原子数/コメント/座標行、不正形式 Err)。`outputs()`=`[("gaussian_out","output/main.out")]`。
- `parse_g16_out.instantiate`:program `python`、`module restore default -f`、body に **未クォート `$JM_LAUNCHER python scripts/parse_results.py "../opt/output/main.out"`**(wiring 解決値)、`scripts/parse_results.py`(0755)。
- `assemble(g16-opt-parse)`:flow JobId 集合 == plan キー集合 == `{opt,parse}`、`parse.parents[0]={from:opt,kind:afterok}`、両 config `partition=="REPLACE_ME"`、time 48h/1h 非対称、wiring 相対 `../opt/output/main.out`、opt body 絶対 cd = `flow_dir.join("opt")`。
- パラメータ宛先・レジストリ整合 lint・`blank` バイト同値(プリアンブル/`$JM_LAUNCHER`/R3 cd を**含まない**ことも assert)。

### launcher read 時解決(`src/runner/flow.rs` / `src/persistence/common.rs`)

- `CommonConfig` deserialize:`launcher = "srun"` ありの common.toml で `common.launcher == Some("srun")`、**`launcher` 無し**の既存 common.toml で `None`(`#[serde(default)]` 回帰)、`deny_unknown_fields` と両立。
- `synth_empty_common()` が `launcher: None` を返す(D2 新 field 追従)+ 既存テスト群が壊れない。
- render が `JM_LAUNCHER` を `batch.bash` に export(§5.5 の 4 ケース全網羅):ケース1 plan param `launcher="mpirun"` → `export JM_LAUNCHER='mpirun'`(common より優先)、ケース2 `common.launcher=Some("srun")` + param 空 → `export JM_LAUNCHER='srun'`(quote_for_bash 経由)、ケース3 `common.launcher=Some("")` + param 空 → 空 export(bare)、ケース4 `common.launcher=None` + param 空 → ハードコード `srun`。
- **再 scaffold 不要回帰**:scaffold 後に `common.toml launcher` を書換え → `jm render` 再実行 → batch.bash の `JM_LAUNCHER` のみ更新、`scripts/<JobId>.bash`(`$JM_LAUNCHER` 間接参照)は不変。
- `render_batch_bash` 公開シグネチャ不変の回帰(arity/型)。

### core 不変回帰

- R3 = core 変更ゼロ:`SbatchCmd.chdir` 依然 `None`、`SbatchCmd.env` に `JM_FLOW_DIR` 等新規キー無し、`render_batch_bash` シグネチャ不変。

### 統合(`tests/integration_new_recipes.rs`, `assert_cmd`)

- `--list`/`--describe` 列挙のみ exit 0。
- `jm new g16-opt-parse --param opt.charge=1`:全ファイル生成、gjf に `1 1`、`opt/scripts/opt.bash` に conda 全消去ブロック + `module restore gaussian_A -f` + `$JM_LAUNCHER g16 ...`、opt body cd が実 tempdir 絶対。
- `--param opt.input_coordinate=<tmp.xyz>`:`opt/input/<basename>` コピー + gjf 座標差込。src 不在で exit 非 0 + 巻き戻し。
- doctor-clean:`jm doctor <uuid>` exit 0。
- `jm new g16-opt-parse` → `jm render <uuid>` exit 0、生成 `batch.bash` に `export JM_LAUNCHER='srun'`(common.toml 無でも fallback)。
- 後方互換:`jm new` ≡ `jm new blank` ≡ 既存期待値。

### Python smoke(`python/tests`)

- 正常終了 `.out` で `parse_results.py` exit 0、未収束/切断 exit 1、`cclib` 不在 exit 2。

`MockExecutor`/`InMemoryQuerier`、live SLURM 不要。

## 11. CLAUDE.md 準拠 / 上流変更

- **`### Upstream modification policy` 準拠**:A1 `SlurmJobConfig` は不変(launcher を A1 に足さない)。D2 `CommonConfig` に `launcher: Option<String>` 追加は**正当な D2 変更** — 非上流 seam(recipe param のみ)では `partition` 同等の「クラスタ一元・read 時解決」体験が**原理的に実現不能**(クラスタ不変値の格納先が `CommonConfig` にしか無く、`#[serde(deny_unknown_fields)]` が枠追加を強制する)。これは 2026-05-15 spec Goal#4 を CLAUDE.md ポリシーが置換した結果として許容。
- **D2 調整変更タスク**(coordinated change):D2 リポに `CommonConfig.launcher: Option<String>`(`#[serde(default)]`)を land → 本リポ `Cargo.toml` の D2 rev を bump → `synth_empty_common()` + 関連テストを追従。writing-plans で D2 PR を先行タスク化。
- 生成物は user-authored 入力の**初回 bootstrap のみ**。runtime は `.jm/` 配下のみ書込。launcher 解決は render の既存責務範囲内(新規副作用ファイル無し)。
- `jm` `--no-default-features` → `src/recipes/` pyo3 非依存。`base_preamble()` も純粋・純 Rust(xyz パース含む化学/Python ライブラリ無し)。
- 原子書込(PID サフィックス tmp + rename)を全生成ファイル踏襲。`*.bash`/`*.py` は 0755。
- Out of scope(DSL/sweep/per-flow common/TUI/リモートレジストリ/OpenMM/JobTemplate 直接 scaffold/gaussian-batch 依存/自動幾何取得/`--rsc` 等 SBATCH ヘッダ)非抵触。
- **公開 API/PyO3/`.pyi`/`render_batch_bash` シグネチャ/`flow.rs` cwd 契約すべて不変**。公開追加は `recipes` モジュール + D2 `CommonConfig.launcher` のみ。
- gem の意味論 + `_base.bash.j2` プリアンブル構造を二層で写すがスキーマは job-manager。status は Lifecycle/tick が権威。
- Conventional Commits / per-task commit / stacked PR。

## 12. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| テンプレ層 | 二層(JobTemplate/FlowRecipe) | 単一 Recipe | 再利用、先行例全て二層 |
| **共通プリアンブル** | **`_base.bash.j2` 完全移植 `base_preamble()` + ブロック差込** | 最小プリアンブルのみ / 全 param 化 | gem 実績そのまま、サイト名のみ可変(YAGNI、ユーザ判断) |
| Job 層 CLI 公開 | Flow 層のみ scaffold | JobTemplate も直接 | CLI 最小、YAGNI |
| パラメータ宛先 | `--param <JobId>.<param>` | フラット | `plan.toml [jobs.<JobId>]` 1:1 |
| sidecar 配置 | `<JobId>/{input,output,derived,scripts}/` | フラット | 多ジョブ衝突排除、gem 写し |
| ジョブ本体 | 編集可能 `<JobId>/scripts/<JobId>.bash`、flow.toml body は薄起動子 | body インライン | 化学者が bash 直接編集(ユーザ判断) |
| **path 解決** | **R3:scaffold 時 body に絶対 cd 1 行** | R1/R2/R4 | core 変更ゼロ、gem 先行例整合 |
| **launcher 解決タイミング** | **read 時(`common.toml`→render、partition 同列)** | scaffold 焼付け | 再 scaffold 不要、`common.toml` 編集で全 flow 反映(ユーザ指示) |
| **launcher 格納先** | **D2 `CommonConfig.launcher`** | A1 `SlurmJobConfig` 追加 / recipe param のみ | A1 不変厳守、param のみでは一元・read 時不能(§11) |
| srun 包み | `$JM_LAUNCHER`(未クォート、render 注入) | `srun` 固定リテラル / python ラッパ | KUDPC 必須 + 非 srun/ローカル degrade、gem の隠蔽を job-manager 層で再現 |
| クロスジョブ参照 | 相対 `../<producer>/...` | 絶対/env | flow dir 移動耐性 |
| 幾何入力 | `input_coordinate` scaffold コピー(.xyz 純 Rust 差込) | 自動取得 / OpenBabel 同梱 | `jm new` 化学非依存維持 |
| post 中身 | 自前 cclib `parse_results.py` | gaussian-parse-results | β/A2 未実装(γ-pending、§13) |
| gaussian-batch | 依存せず(参照 + 任意 swap-in) | コード/CLI 依存 | alpha・import rot・未実装(§13) |
| status 権威 | job-manager Lifecycle/tick | gem status 再実装 | 二重実装回避 |
| `blank` | 据置(非分解、プリアンブル/launcher 無し) | JobTemplate 分解 | バイト同値要件 |

## 13. gaussian-batch(β/A2)使用可否評価

`miyake-ken/gaussian-batch` 実コード精査(`gaussian_batch_generator` + `gaussian_batch_cli`):

| 要素 | 状態 | 本 spec での扱い |
|---|---|---|
| テンプレ構造 | `_base.bash.j2`(共有プリアンブル + `{% block modules/body %}`)← `gaussian_g16.bash.j2`/`gaussian_post.bash.j2` 2 leaf + `main.gjf.j2`。Jinja 継承 | **構造を Rust に完全移植**(`base_preamble()` §4.0)。コードは流用せず形を写経 |
| `_base.bash.j2` プリアンブル | 動作する重要ボイラープレート(conda 全消去は学習スキル pixi-conda-stack-reset と同一) | §4.0 で 1:1 移植。`#SBATCH` 1-12 行は除外(job-manager SbatchCmd 領域=非目標) |
| `gaussian_g16.bash.j2` body | `python -m gaussian_compute_runtime run-g16`(srun を **bare**、srun は wrapper 内隠蔽) | job-manager は自己完結ゆえ `$JM_LAUNCHER` 自前包み(§5.5) |
| `gaussian-generate-gjf` | **壊**(`gaussian_job_shared.config.ConfigManager` import → dropped API。xfail(strict)) | 使用不可。`render_gjf` も `ConfigManager` 必須 |
| `gaussian-parse-results` | **未実装**(pyproject に entry 宣言あるがモジュール不在。γ-pending) | 使用不可。**自前 cclib `parse_results.py` が v1**(§7) |
| `gaussian-pipeline`/`generate-batch` | gem フル orchestration 前提 | 不適(job-manager が置換する層) |
| `main.gjf.j2` | `%rwf/%nprocshared/%mem/%chk→route→title→charge mult→atoms→connectivity→extra_input` | **形式一致確認 → 本 spec gjf テンプレは規約準拠**(参照価値) |
| 依存 | `gaussian_job_shared`(D, **private**)/openbabel/jinja2/Python 3.12<3.13 | job-manager は Rust・Python 無し → **コード依存不可** |

**結論**:job-manager は gaussian-batch に**依存しない**(コード依存=言語/Python 非搭載で不可、実行時 CLI 依存=該当 2 entry が壊/未実装で不可)。レシピは**自己完結**。`_base.bash.j2` の**プリアンブル構造のみ**を `base_preamble()` として移植(コード依存ではなく設計写経)。`main.gjf.j2` は形式参照のみ。**α-reshape で `gaussian-generate-gjf`/`gaussian-parse-results` が安定したら** `scripts/<JobId>.bash` の `# REPLACE_ME` フックで任意差替可(本 spec は強制も実装もしない)。CHANGELOG 上 v0.3.0「3 - Alpha」、当該 2 entry は α/γ-pending。
