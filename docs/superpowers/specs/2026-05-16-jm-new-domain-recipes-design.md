# `jm new <flow-recipe>` — 二層レシピ(Job 層 / Flow 層)— design (rev.3)

**Date:** 2026-05-16
**Status:** Draft (rev.3 — 二層アーキテクチャ反映。awaiting user review)
**Reference:**
- 既存エコシステム(整合対象。`git@github.com:miyake-ken/gaussian-experiment-manager.git` "collapsed" + `github.com/miyake-ken/GAUSSIAN_repo/examples`):
  - `experiment.toml` `[step.params]`(`route`/`charge`/`multiplicity`/`extra_input`)
  - **main(`gaussian-run-g16`)→ afterok → post(`gaussian-parse-results`, cclib)** の 2-batch チェーン
  - `.gjf` 形式(`%rwf`/`%nprocshared`/`%mem`/`%chk` → route → title → charge mult → geometry → connectivity → extra_input)
  - `<env.root>/<uuid>/{input,output,derived}/`、`task_basename`(既定 `main`)、InChIKey compound
  - `[slurm]` vs `[slurm.post]`、**OpenMM はエコシステムに一切なし**(確立検証は cclib)
- 二層分離の先行例:nf-core(modules vs subworkflows)/ Snakemake(rule・wrapper vs workflow)/ atomate(Firework vs Workflow)。gem 自体も step program(run-g16 / parse-results)= job 粒度、step 連鎖 = flow 粒度
- `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`(既存 `jm new` sentinel 哲学)
- `src/bin/jm.rs`(`Cmd::New` / `cmd_new` / `build_flow_template` / `build_plan_template` / `atomic_write_str`)
- `src/render/mod.rs`(`render_batch_bash` — 公開 API + Python エクスポート、prod caller は `src/runner/flow.rs:245` のみ)
- `src/runner/flow.rs:262-289`(submit 経路 `SbatchCmd` 構築。`cmd.chdir` 未設定)
- 上流 A1 `slurm-async-runner2/src/sbatch/cmd.rs:133-134`(`SbatchCmd.env` → `--export=ALL,K=V,...`)
- CLAUDE.md(Out of scope / PyO3 境界 / `.jm/` レイアウト / `--no-default-features`)

---

## 1. 問題設定

`jm new`(別 spec)は静的 2-job 雛形のみ。ユーザ要求は「g16 構造最適化 → afterok → 結果検証」のドメインチェーン scaffold。エコシステム精査の結果、確立実装は OpenMM ではなく **cclib による `gaussian-parse-results`**(.out をパースし収束/エネルギー検証)。

さらにユーザ要求により、テンプレートを **再利用可能な二層** に分割する:

- **Job 層(`JobTemplate`)** — 1 バッチ = 1 ジョブの自己完結部品(例 `g16_opt`, `parse_g16_out`)。
- **Flow 層(`FlowRecipe`)** — JobTemplate を DAG に合成(例 `g16-opt-parse` = `g16_opt → parse_g16_out`)。`jm new` が叩く scaffold 単位。

レシピはコードではなく **編集可能な job-manager flow 一式**を生成する(gem に scaffold/new は無く本機能は新規レイヤ)。gem の**ドメイン意味論**を写すが**スキーマは job-manager の `flow.toml`/`plan.toml`**。

## 2. ゴール / 非ゴール

### Goals

1. `jm new` に位置引数 `<flow-recipe>` を追加。`jm new`(無引数)= 組込 `blank`(既存 2-job 雛形、**後方互換**)。`jm new g16-opt-parse --param opt.charge=1` で domain flow 生成。
2. **二層レジストリ**:`JobTemplate`(再利用部品)と `FlowRecipe`(合成)。FlowRecipe のみ scaffold 可能(JobTemplate は内部合成単位、CLI 非公開)。
3. 出力ツリーは Flow 層が構築時 `jm doctor`-clean を保証(flow JobId 集合 == plan `[jobs.*]` キー集合、uuid == ディレクトリ名、親エッジ整合)。
4. domain JobTemplate は flow.toml/plan.toml への寄与に加え、**自分の JobId 名前空間下にサイドカー**(`<JobId>/input/main.gjf`, `<JobId>/scripts/parse_results.py`)を出す。化学者が直接編集可。配置・命名は gem 準拠(`input/`・`output/`・`derived/`、`task_basename = main`)。
5. JobTemplate ごとに型付きパラメータ。CLI は **`--param <JobId>.<param>=<value>`**。`jm new --list`(flow recipe 一覧)/ `jm new <flow-recipe> --describe`(`<JobId>.<param>` 行)。
6. 実行中ジョブが自分のサブディレクトリを解決できるよう submit 経路で `SbatchCmd.env` に `JM_FLOW_DIR` 注入(§5, R4)。
7. v1:JobTemplate `g16_opt` / `parse_g16_out`、FlowRecipe `blank` / `g16-opt-parse`。両レジストリは追加容易。
8. 書き込みは中途半端を残さない(既存 `jm new` rollback 規約踏襲)。

### Non-goals

- `common.toml` 生成・変更(v1)。per-job `[jobs.*.config]`(`SlurmJobConfig`)+ `partition="REPLACE_ME"` sentinel(既存 `jm new` deferred-common 踏襲)。
- **OpenMM**(エコシステム前例なし。ユーザ判断で除外)。
- experiment DSL / sweep 展開 / 親解決(CLAUDE.md "Out of scope")。gem `[[sweep]]`/`parent_uuids` 連鎖は job-manager 責務外。
- gem `experiment.toml`/`common.toml` フォーマット採用、gem `metadata.toml`/`status` 再実装(job-manager の Lifecycle + `decide_transition` + `tick` が status の権威。parse は exit code のみ)。
- **JobTemplate 単体の直接 scaffold**(`jm new g16_opt`)。FlowRecipe のみ scaffold 可能(ユーザ判断、YAGNI)。
- 対話的ウィザード / TUI、既存 flow 再生成・migration・answers-file、リモートレジストリ。
- `render_batch_bash` 公開 API / PyO3 境界変更(R1 不採用, §5)、全ジョブ cwd 契約変更(R2 不採用, §5)。
- 分子幾何の自動取得(gem 上流責務。`input/main.gjf` に geometry sentinel)。

## 3. CLI 形

```
jm --root <ROOT> new [<FLOW-RECIPE>] [--param <JOBID.PARAM=VALUE>]... [--tag <K=V>]... [--print-path]
jm --root <ROOT> new --list
jm --root <ROOT> new <FLOW-RECIPE> --describe
```

| 引数 | 説明 |
|---|---|
| `<FLOW-RECIPE>`(位置, 任意) | FlowRecipe 名。省略時 `blank`。未知名は候補列挙付きエラー。 |
| `--param <JobId>.<param>=<value>` | 任意回。合成ノード `<JobId>` の JobTemplate パラメータを上書き。未知 JobId / 未知 param / 型不整合 / `=`・`.` 欠落はエラー。 |
| `--tag <K=V>` | 既存。`flow.toml [tags]` 反映。 |
| `--print-path` | 既存。stdout に `<root>/<uuid>` のみ。 |
| `--list` | FlowRecipe 名 + 1 行説明を列挙して終了。 |
| `--describe` | `<FLOW-RECIPE>` の合成ノードと各 `<JobId>.<param>`(型/既定/ヘルプ)を列挙して終了。 |

`Cmd::New` を `recipe: Option<String>` / `params: Vec<String>`(`--param`)/ 既存 `tags` / `print_path` / `list: bool` / `describe: bool` へ拡張。`main()` 分岐を `cmd_new(&root, recipe.as_deref(), &params, &tags, print_path, list, describe)` に。

## 4. アーキテクチャ(二層)

### モジュール配置

```
src/recipes/
  mod.rs           -- 公開 re-export, registries, --param <JobId>.<param> パース, --list/--describe 整形
  job.rs           -- JobTemplate trait, JobArtifacts, JobCtx, RecipeParam/Type
  flow.rs          -- FlowRecipe trait + 合成アセンブラ(assemble())
  jobs/
    g16_opt.rs
    parse_g16_out.rs
  flows/
    blank.rs        -- 既存 2-job step1->step2 を据置(JobTemplate 非分解、バイト同値)
    g16_opt_parse.rs-- jobs::{g16_opt, parse_g16_out} を合成
  assets/
    g16_opt/main.gjf.tmpl
    parse_g16_out/parse_results.py.tmpl
```

- `src/recipes/` は **pyo3 非依存**(`uuid`/`chrono`/`toml`/std のみ)。`jm` `--no-default-features` ビルド必須。
- JobTemplate / FlowRecipe は **純粋**(I/O 無し)。I/O・rollback は `cmd_new`。
- `src/lib.rs` から `pub use recipes::{JobTemplate, FlowRecipe, flow_registry, ...}`(**公開 API は追加のみ**)。

### 型(Job 層)

```rust
pub enum RecipeParamType { Str, Int, Float, Bool }
pub struct RecipeParam { pub name: &'static str, pub ty: RecipeParamType,
                         pub default: &'static str, pub help: &'static str }

pub struct JobArtifacts {
    pub program: String,                            // "g16" / "python"
    pub body: String,                               // 先頭 `cd "$JM_FLOW_DIR/<JobId>"`(§5)
    pub time_limit: Option<String>,                 // [jobs.<JobId>.config].time_limit。partition は常に REPLACE_ME
    pub plan_params: BTreeMap<String, toml::Value>, // → plan.toml [jobs.<JobId>]
    pub sidecars: Vec<GeneratedFile>,               // relpath は既に "<JobId>/..." で名前空間化済み
}

pub struct GeneratedFile { pub relpath: PathBuf, pub contents: String, pub unix_mode: Option<u32> }

pub struct JobCtx<'a> {
    pub job_id: &'a str,                            // flow が割り当てた JobId(sidecar/body の名前空間)
    pub params: &'a BTreeMap<String, toml::Value>,  // 既定 + --param <JobId>.* 上書き後の検証済み
    pub inputs: &'a BTreeMap<&'static str, String>, // 入力名 → flow が解決した "$JM_FLOW_DIR/<producer>/..." パス
    pub uuid: &'a Uuid, pub created_at: &'a str,
}

pub trait JobTemplate: Send + Sync {
    fn name(&self) -> &'static str;
    fn params(&self) -> &'static [RecipeParam];
    fn inputs(&self) -> &'static [&'static str];    // 例: parse_g16_out -> ["gaussian_out"]
    fn outputs(&self) -> &'static [(&'static str, &'static str)]; // (名前, JobId 内 relpath) 例: g16_opt -> [("gaussian_out","output/main.out")]
    fn instantiate(&self, ctx: &JobCtx<'_>) -> Result<JobArtifacts, RecipeError>;
}
```

### 型(Flow 層)+ 合成アセンブラ

```rust
pub trait FlowRecipe: Send + Sync {
    fn name(&self) -> &'static str;
    fn summary(&self) -> &'static str;
    fn nodes(&self) -> &'static [(&'static str, &'static str)];  // (JobId, JobTemplate 名)
    fn edges(&self) -> &'static [(&'static str, &'static str, &'static str)]; // (from, to, kind 例 "afterok")
    fn wiring(&self) -> &'static [(&'static str, &'static str, &'static str, &'static str)];
        // (consumer JobId, 入力名, producer JobId, producer 出力名)
}

pub fn flow_registry() -> Vec<Box<dyn FlowRecipe>>;     // [Blank, G16OptParse]
pub fn find_flow(name: &str) -> Option<Box<dyn FlowRecipe>>;
pub fn find_job(name: &str) -> Option<Box<dyn JobTemplate>>; // jobs レジストリ(内部)
```

**アセンブラ `flow::assemble(recipe, raw_params, tags, uuid, created_at) -> Result<Vec<GeneratedFile>>`**:

1. `recipe.nodes()` 各 `(job_id, tmpl)` を `find_job(tmpl)` 解決(未知は内部エラー = レシピ定義バグ)。
2. `--param` を `<JobId>.<param>` でパース → JobId ごとに分配。各ノードの `JobTemplate.params()` 既定で埋め、型検証して上書き。未知 JobId / 未知 param / 型不整合 → bail。
3. `recipe.wiring()` 解決:consumer の各入力名に対し `"$JM_FLOW_DIR/<producer JobId>/<producer 出力 relpath>"` を計算し `JobCtx.inputs` へ。producer 出力名は producer の `JobTemplate.outputs()` から引く。
4. 各ノード `JobTemplate.instantiate(&ctx)?` → `JobArtifacts`。
5. **flow.toml 組立**:`jobs.<JobId>` = {program, body, config(`partition="REPLACE_ME"` + `time_limit`)}、`recipe.edges()` から `[[jobs.<to>.parents]] from=<from> kind=<kind>`。
6. **plan.toml 組立**:`[jobs.<JobId>]` = そのノードの `plan_params`。ノード集合から構築するため **flow JobId 集合 == plan キー集合 を構造的に保証**(doctor-clean by construction)。
7. sidecars(既に `<JobId>/...` 名前空間化)+ flow.toml + plan.toml を `Vec<GeneratedFile>` で返す。

JobTemplate は peer の JobId をハードコードしない(wiring は FlowRecipe がデータで宣言)→ **再利用可能**。将来 `g16-opt-freq-parse` 等は既存 JobTemplate を並べる FlowRecipe 1 個で済む。

### `cmd_new` シーケンス

1. `--list`: `flow_registry()` を `name — summary` 出力で終了。
2. flow 解決: `recipe.unwrap_or("blank")` → `find_flow()`。`None` → bail(候補列挙)。
3. `--describe`: ノードと `<JobId>.<param>` 表(JobTemplate.params 由来)を出力して終了。
4. `--tag` パース(既存流用)。
5. `uuid = Uuid::now_v7()`; `resolver = PathResolver::new(root)`; `flow_dir = resolver.flow_dir(&uuid)`; 衝突確認(`exists()` なら bail、リトライしない)。
6. `created_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)`。
7. `flow::assemble(...)?` → `Vec<GeneratedFile>`。
8. `create_dir_all(&flow_dir)`。各 `GeneratedFile` の親(`<JobId>/input` 等)を `create_dir_all` し `atomic_write_str`。`unix_mode` 指定時 rename 前 `chmod`(Unix-only)。
9. いずれか失敗時:作成済み `flow_dir` を `remove_dir_all` で巻き戻して `?` 伝播。
10. 出力(asset 込み列挙):
    ```
    created flow <uuid> from recipe g16-opt-parse
      <root>/<uuid>/flow.toml
      <root>/<uuid>/plan.toml
      <root>/<uuid>/opt/input/main.gjf
      <root>/<uuid>/parse/scripts/parse_results.py
    next: edit opt/input/main.gjf (geometry), set partition + cluster env in
          flow.toml, then `jm --root <root> render <uuid>`
    ```
    `--print-path` 時は `<root>/<uuid>` の 1 行のみ。

## 5. flow パス解決(R4)+ body 規約

### 制約(実コード確認済み)

`render_batch_bash` はジョブに `JM_FLOW_UUID`/`JM_JOB_ID`/`JM_AXIS_*`/`JM_PARAM_*` のみ export、flow dir パスも `cd` も注入しない。`src/runner/flow.rs:268` で `cmd.chdir` 未設定 → ジョブ cwd は flow dir でない。`render_batch_bash` は公開 API + Python エクスポート(シグネチャ変更は破壊 + `.pyi` 再生成)。

### 採用:R4(submit 経路で `SbatchCmd.env` に `JM_FLOW_DIR` 注入)

A1 `SbatchCmd.env` は `build_argv()` で `--export=ALL,K=V,...`(`cmd.rs:133-134`)。`src/runner/flow.rs` submit 経路に 1 行:

```rust
cmd.env.insert("JM_FLOW_DIR".into(),
    self.resolver.flow_dir(&fr.flow_uuid).to_string_lossy().into_owned());
```

公開 API / PyO3 / `.pyi` / `render_batch_bash` / cwd 契約すべて不変、既存 flow は新 env 無視(純加算)。DryRun/Mock は exec せず無影響。

| 案 | 公開API/PyO3 | cwd契約 | 既存flow | 判定 |
|---|---|---|---|---|
| R1: render シグネチャ変更 | 破壊 | 不変 | 加算 | 却下 |
| R2: `cmd.chdir=Some` | 不変 | 全ジョブ変更 | 挙動変更 | 却下 |
| **R4: submit env 注入(採用)** | 不変 | 不変 | 加算 | **採用** |
| R3: scaffold 時 絶対パス埋込 | 不変 | 不変 | 加算 | 却下(移動破綻) |

`JM_ROOT` 形は CLI 入力 env と同名 2 役の意味過負荷 + join 必要のため不採用。

### body 規約: 先頭で `cd "$JM_FLOW_DIR/<JobId>"`

二層化により各ジョブは **自分の JobId サブディレクトリ**で動く。JobArtifacts.body 先頭は `cd "$JM_FLOW_DIR/<JobId>"`。これで `input/main.gjf` を相対参照でき、Gaussian `%rwf`/`%chk` 相対副生成物・`output/`/`derived/` がそのジョブの名前空間に落ち、合成時 sidecar 衝突が構造的に起きない。クロスジョブ入力は wiring が解決した絶対形 `"$JM_FLOW_DIR/<producer>/output/main.out"` を body に焼き込む。

### 既知の小制約

A1 `render_export` は env キー/値に `,`/`=` を含むと拒否。flow dir パスに通常無いが、含む root では `JM_FLOW_DIR` 注入が `SbatchSpawnError`。極稀。spec 既知制約として明記(将来 `jm doctor` 警告は別 spec)。

## 6. リファレンス整合マッピング(gem ↔ job-manager 二層)

| gem(確立規約) | job-manager 二層表現 |
|---|---|
| step program `gaussian` (`gaussian-run-g16`) | **JobTemplate `g16_opt`**(program=`g16`) |
| post program `gaussian-parse-results`(cclib) | **JobTemplate `parse_g16_out`**(program=`python`) |
| step 連鎖 + afterok(main→post) | **FlowRecipe `g16-opt-parse`**:nodes `[(opt,g16_opt),(parse,parse_g16_out)]`、edges `[(opt,parse,afterok)]` |
| `[step.params] route/charge/multiplicity/extra_input` | `g16_opt` の `params()`。CLI `--param opt.route=...`。`plan.toml [jobs.opt]` に格納(`JM_PARAM_*` 露出) |
| `parent_uuids`(出力→次入力 consume) | FlowRecipe `wiring()`:`(parse,gaussian_out,opt,gaussian_out)`。flow 内明示・JobId 非ハードコード |
| `common.toml [slurm]` / `[slurm.post]` | `[jobs.opt.config]`(time 48h)/ `[jobs.parse.config]`(time 1h)。partition 両方 `REPLACE_ME` |
| `[env].task_basename=main`、`<uuid>/{input,output,derived}/` | flow 内 **`<JobId>/{input,output,derived}/`**(1 ジョブ ≈ 1 gem calc)、`main` 固定 |
| InChIKey compound(gjf title) | `--param opt.compound=<InChIKey>`(既定 sentinel)。gjf title + `[tags].compound` |
| status ファイル(post が権威) | job-manager Lifecycle/tick が権威。parse は exit code のみ |
| `[[sweep]]` / 多段連鎖 | 非対象(CLAUDE.md Out of scope) |

## 7. v1 JobTemplate / FlowRecipe 詳細

### JobTemplate `g16_opt`

- `params()`(gem `[step.params]` 語彙):

| name | type | default | help |
|---|---|---|---|
| `route` | str | `#p opt b3lyp/6-31g(d)` | Gaussian route 行(複数行可、まるごと 1 文字列) |
| `charge` | int | `0` | 全電荷 |
| `multiplicity` | int | `1` | スピン多重度 |
| `extra_input` | str | `` | charge/mult・geometry の後の追加入力 |
| `nproc` | int | `8` | `%nprocshared`(gem は common 由来、job-manager は common 非関与のため明示) |
| `mem` | str | `8GB` | `%mem` |
| `compound` | str | `REPLACE_ME-INCHIKEY` | InChIKey。gjf title + `[tags].compound` |

- `inputs()` = `[]`(root。geometry は sentinel)。`outputs()` = `[("gaussian_out","output/main.out")]`。
- `instantiate`:program `"g16"`、`time_limit "48:00:00"`、sidecar `<JobId>/input/main.gjf`(下記、`{{}}` 差込、geometry sentinel)、`plan_params` = 上記パラメータ、body:
  ```
  cd "$JM_FLOW_DIR/<JobId>"
  # --- cluster environment (EDIT for your site; CLAUDE.md HPC notes) ---
  # REPLACE_ME: e.g. `module load gaussian` / `conda activate <env>`
  mkdir -p output
  g16 input/main.gjf output/main.out
  ```
- `<JobId>/input/main.gjf`(gem 形式):
  ```
  %rwf=main.rwf
  %nprocshared={{nproc}}
  %mem={{mem}}
  %chk=main.chk
  {{route}}

  {{compound}}

  {{charge}} {{multiplicity}}
  <GEOMETRY: REPLACE_ME — 1行1原子 `Element  x  y  z`。route に geom=connectivity を
  含めるなら空行後に connectivity ブロック>
  {{extra_input}}
  ```

### JobTemplate `parse_g16_out`

- `params()` = `[]`(v1 はユーザパラメータ無し)。`inputs()` = `["gaussian_out"]`。`outputs()` = `[]`(`derived/main.mol2` 出力は §下記 TODO 拡張点)。
- `instantiate`:program `"python"`、`time_limit "01:00:00"`、sidecar `<JobId>/scripts/parse_results.py`、`plan_params` = `{ note = "cclib parse + convergence/energy validation" }`、body:
  ```
  cd "$JM_FLOW_DIR/<JobId>"
  # REPLACE_ME: activate the python env that has `cclib`
  python scripts/parse_results.py "{{inputs.gaussian_out}}"
  ```
  (`{{inputs.gaussian_out}}` は wiring が `"$JM_FLOW_DIR/opt/output/main.out"` に解決)
- `scripts/parse_results.py`(cclib。沈黙成功を避ける実 pass/fail):
  - 引数:Gaussian `.out` パス。`cclib` import 失敗時は明示メッセージで **exit 2**(沈黙しない honest failure)。
  - 実 pass/fail:(a) cclib でパース不可 → exit 1、(b) 正常終了マーカ無し → exit 1、(c) opt 収束フラグ False → exit 1、(d) 最終エネルギーが有限実数で取れない → exit 1、すべて満たせば exit 0。検証済/未検証を stdout 明記。
  - `# TODO(jm recipe): write derived/main.mol2`(gem `derived/main.mol2` 相当)を明示拡張点に(v1 は (a)-(d) を本質、derived 出力は任意)。
  - status は job-manager Lifecycle/tick が権威。本スクリプトは exit code のみ。

### FlowRecipe `g16-opt-parse`

- `nodes()` = `[("opt","g16_opt"), ("parse","parse_g16_out")]`
- `edges()` = `[("opt","parse","afterok")]`
- `wiring()` = `[("parse","gaussian_out","opt","gaussian_out")]`
- 生成 `flow.toml`(抜粋):
  ```toml
  uuid = "<uuid>"
  created_at = "<rfc3339>"
  [tags]
  recipe   = "g16-opt-parse"
  compound = "<opt.compound>"

  [jobs.opt]
  program = "g16"
  body = """cd "$JM_FLOW_DIR/opt"
  # REPLACE_ME cluster env
  mkdir -p output
  g16 input/main.gjf output/main.out
  """
  [jobs.opt.config]
  partition  = "REPLACE_ME"
  time_limit = "48:00:00"

  [jobs.parse]
  program = "python"
  body = """cd "$JM_FLOW_DIR/parse"
  # REPLACE_ME python env w/ cclib
  python scripts/parse_results.py "$JM_FLOW_DIR/opt/output/main.out"
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
  [jobs.parse]
  note = "cclib parse + convergence/energy validation"
  ```

### 環境アクティベーション(sentinel)

gem 生成 batch の `source ~/.bashrc; conda activate; module restore` はサイト固有。body 内に `# REPLACE_ME` 環境節を置き `partition=REPLACE_ME` と並ぶクラスタ別編集点として明示(学習スキル `pixi-conda-stack-reset`/`slurm-module-purge-breaks-srun` の HPC 落とし穴回避方針)。具体行はハードコードしない。

## 8. `blank` FlowRecipe(後方互換)

既存 `build_flow_template`/`build_plan_template`(`src/bin/jm.rs:497-574`)を `flows/blank.rs` へ移設。**JobTemplate に分解せず**直接出力(`assemble()` を介さない経路、または blank 専用に flow.toml/plan.toml を直書き)し **既存 `jm new` 出力とバイト同値**を維持(`tests/integration_new.rs` / `src/bin/jm.rs` ユニットがそのまま通る)。`jm new`(無引数)= `jm new blank`。サイドカー無し(flow.toml/plan.toml の 2 ファイルのみ)。

## 9. エラーハンドリング

| 状況 | 挙動 |
|---|---|
| 未知 FlowRecipe | `bail!("unknown recipe {name:?}; available: blank, g16-opt-parse")` |
| `--param` に `.` または `=` 欠落 | `bail!("invalid --param: expected <JobId>.<param>=<value>, got {raw}")` |
| 未知 JobId(レシピノードに無い) | `bail!("recipe {flow}: no node {jobid}; nodes: opt, parse")` |
| 未知 param / 型不整合 | `bail!("recipe {flow}: job {jobid}: unknown/typed param ...")` |
| FlowRecipe が未知 JobTemplate を参照 | 内部エラー(レシピ定義バグ)。`registry()` 整合をユニットテストで保証 |
| `flow_dir` 既存(UUID 衝突) | `bail!("flow dir already exists: {path}")`(リトライしない) |
| sidecar 書込失敗 | 作成済み `flow_dir` を `remove_dir_all` 巻き戻し後 `?` 伝播 |
| `--list`/`--describe` | scaffold せず終了 |
| (実行時)`cclib` 未導入 | `parse_results.py` が明示 exit 2 |

## 10. テスト

### ユニット(`src/recipes/**` `#[cfg(test)] mod tests`)

- **Job 層** `g16_opt.instantiate`:program `g16`、body 先頭 `cd "$JM_FLOW_DIR/<JobId>"`、sidecar relpath が `<JobId>/input/main.gjf`、gjf に `{{}}` 残存無し・gem ヘッダ順(`%rwf`→`%nprocshared`→`%mem`→`%chk`→route→title→`charge mult`)・`<GEOMETRY: REPLACE_ME>`、`outputs()`=`[("gaussian_out","output/main.out")]`。`--param` 反映(route に `=` を含む値で複数 `=` 分割確認)。
- **Job 層** `parse_g16_out.instantiate`:program `python`、`inputs()`=`["gaussian_out"]`、body が `{{inputs.gaussian_out}}` 解決値を含む、sidecar `<JobId>/scripts/parse_results.py`。
- **Flow 層** `assemble(g16-opt-parse)`:flow JobId 集合 == plan キー集合 == `{opt,parse}`、`parse.parents[0]={from:opt,kind:afterok}`、両 config `partition=="REPLACE_ME"`、time_limit 非対称(48h/1h)、parse body に `"$JM_FLOW_DIR/opt/output/main.out"` が焼き込まれている(wiring 解決)。
- パラメータ宛先:`--param opt.charge=1` のみ opt に効き parse に影響しない。未知 JobId/param/型不整合 Err。
- レジストリ整合:全 FlowRecipe の `nodes()` の JobTemplate 名が `find_job` で解決でき、`wiring()` の入出力名が両端 JobTemplate の `inputs()/outputs()` に存在(レシピ定義 lint をテストで)。
- `blank` FlowRecipe:**既存 `jm new` 出力とバイト同値**(回帰防止)。

### 統合(`tests/integration_new_recipes.rs`, `assert_cmd`)

- `jm new --list` が `blank`/`g16-opt-parse` 列挙、exit 0、scaffold 無し。
- `jm new g16-opt-parse --describe` が `opt.route`/`opt.charge`/... を列挙、exit 0、scaffold 無し。
- `jm new g16-opt-parse --param opt.charge=1` → `flow.toml`/`plan.toml`/`opt/input/main.gjf`/`parse/scripts/parse_results.py` 生成、gjf に `1 1`。
- 生成 flow が **doctor-clean**:`jm doctor <uuid>` exit 0(`<JobId>/` サブツリーが doctor を壊さないことを確認)。
- `jm new g16-opt-parse` → `jm render <uuid>` exit 0(ラウンドトリップ)。
- 後方互換:`jm new` ≡ `jm new blank` ≡ 既存期待値。`--print-path`/`--tag` 既存挙動維持。
- 未知レシピ exit 非 0 + 候補列挙。

### submit 経路(`src/runner/flow.rs` / `tests/integration_sp3.rs`)

- `MockExecutor` 記録 `SbatchCmd.env["JM_FLOW_DIR"]` == `resolver.flow_dir(uuid)` 絶対パス(R4 回帰)。`cmd.chdir` 依然 `None`(R2 不採用回帰)。

### Python smoke(`python/tests`)

- 正常終了 `.out` フィクスチャで `parse_results.py` exit 0、未収束/切断で exit 1、`cclib` 不在で exit 2。

`MockExecutor`/`InMemoryQuerier` 使用、live SLURM 不要。

## 11. CLAUDE.md 準拠

- 生成物は user-authored 入力の **初回 bootstrap のみ**。runtime は `.jm/` 配下しか書かない(`jm new` spec §9 bootstrap 容認の sidecar 拡張)。
- `jm` `--no-default-features` → `src/recipes/` pyo3 非依存。
- 原子書込(PID サフィックス tmp + rename)を全生成ファイルで踏襲。
- Out of scope(DSL/sweep/per-flow common/TUI/リモートレジストリ/OpenMM/JobTemplate 直接 scaffold)非抵触。
- 公開 API は追加のみ。`render_batch_bash`/PyO3/`.pyi` 不変(R4 根拠)。
- gem の**意味論**を二層で写すが**スキーマは job-manager**。status は Lifecycle/tick が権威。
- Conventional Commits / per-task commit / stacked PR。

## 12. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| テンプレ層 | **二層(JobTemplate / FlowRecipe)** | 単一 Recipe(rev.2) | 再利用(JobTemplate 並べ替えで新 flow)、nf-core/Snakemake/atomate/gem 全て二層 |
| Job 層 CLI 公開 | Flow 層のみ scaffold 可能 | JobTemplate も直接 | CLI 表面最小、YAGNI(ユーザ判断) |
| パラメータ宛先 | `--param <JobId>.<param>` | フラット | `plan.toml [jobs.<JobId>]` 1:1、同種ジョブ複数で衝突せず、gem per-step 整合 |
| sidecar 配置 | `<JobId>/{input,output,derived,scripts}/` | フラット `input/main.gjf` | 多ジョブ 1 flow dir で衝突を構造排除、gem `<uuid>/{input,...}` を写す |
| クロスジョブ参照 | FlowRecipe `wiring()` がパス解決 | JobTemplate に peer 焼込 | JobTemplate を再利用可能に(peer 非依存) |
| post 中身 | parse-results 規約(cclib) | OpenMM | エコシステム前例ゼロ、ユーザ判断 |
| status 権威 | job-manager Lifecycle/tick | gem status 再実装 | 二重実装回避 |
| flow パス解決 | R4: submit env `JM_FLOW_DIR` | R1/R2/R3/JM_ROOT | 公開 API/PyO3/cwd すべて不変 |
| common.toml | 非関与(partition REPLACE_ME) | gem common 採用 | 既存 deferred-common 踏襲 |
| `blank` | FlowRecipe 据置(非分解) | JobTemplate 分解 | バイト同値要件を最小リスクで |
