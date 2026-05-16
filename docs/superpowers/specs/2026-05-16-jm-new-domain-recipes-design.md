# `jm new <flow-recipe>` — 二層レシピ(Job 層 / Flow 層)— design (rev.4)

**Date:** 2026-05-17
**Status:** Draft (rev.4 — R3 path 解決 + scripts/<JobId>.bash サイドカー + input_coordinate + gaussian-batch 評価反映。awaiting user review)
**Reference:**
- 既存エコシステム(整合対象):
  - `miyake-ken/gaussian-experiment-manager`("collapsed" δ/E)・`miyake-ken/GAUSSIAN_repo/examples`:`experiment.toml [step.params]`(`route`/`charge`/`multiplicity`/`extra_input`)、main→afterok→post 2-batch、`.gjf` 形式、`<env.root>/<uuid>/{input,output,derived}/`、`task_basename`(既定 `main`)、InChIKey compound、`[slurm]`/`[slurm.post]`
  - `miyake-ken/gaussian-batch`(β/A2: `gaussian_batch_generator` + `gaussian_batch_cli`)。**実コード評価(§13)**:Python・alpha。`gaussian-generate-gjf` は import rot で壊、`gaussian-parse-results` は**モジュール未実装(γ-pending)**、`gaussian-pipeline`/`gaussian-generate-batch` は gem フル orchestration 前提。**job-manager は依存しない**。`main.gjf.j2`(`%rwf`/`%nprocshared`/`%mem`/`%chk`→route→title→`charge mult`→atoms→connectivity→extra_input)は本 spec の gjf 形式と一致 → **参照としてのみ有効**。α-reshape 後の任意 swap-in 先として body の `# REPLACE_ME` フックで言及
- 二層分離の先行例:nf-core(modules/subworkflows)/ Snakemake(rule/workflow)/ atomate(Firework/Workflow)/ gem(step program / step 連鎖)
- `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`(既存 `jm new` sentinel 哲学)
- `src/bin/jm.rs`(`Cmd::New` / `cmd_new` / `build_flow_template` / `build_plan_template` / `atomic_write_str`)
- `src/persistence/path.rs`(**PathResolver 真実**:`flow_dir=<root>/<uuid>/`、`batch.bash=<root>/<uuid>/.jm/<JobId>/batch.bash`、`.jm` は `<uuid>` 直下で `<JobId>` は `.jm` の下。user-authored は `.jm` の外・一段上)
- `src/render/mod.rs`(`render_batch_bash` — 公開 API + Python エクスポート。body に `JM_*` のみ export、cd/flow-dir 注入なし)
- `src/runner/flow.rs:262-289`(submit 経路 `SbatchCmd` 構築。`cmd.chdir` 未設定)
- 上流 A1 `slurm-async-runner2/src/sbatch/cmd.rs`(`build_argv` は script を絶対パスで sbatch に渡す。sbatch は spool コピー実行 → 実行中 `$0`/`pwd` は元位置でない。`dispatcher.capture` で起動 cwd 制御なし → job cwd = `SLURM_SUBMIT_DIR` = `jm submit` を叩いた場所)
- CLAUDE.md(Out of scope / PyO3 境界 / `.jm/` レイアウト / `--no-default-features`)

---

## 1. 問題設定

`jm new`(別 spec)は静的 2-job 雛形のみ。ユーザ要求は「g16 構造最適化 → afterok → 結果検証」のドメインチェーン scaffold。エコシステム精査の結果、確立実装は **cclib による .out パース検証**(gem `gaussian-parse-results`。ただし β/A2 では γ-pending 未実装 — §13)。

テンプレートは **再利用可能な二層**:

- **Job 層(`JobTemplate`)** — 1 バッチ = 1 ジョブの自己完結部品(例 `g16_opt`, `parse_g16_out`)。
- **Flow 層(`FlowRecipe`)** — JobTemplate を DAG に合成(例 `g16-opt-parse`)。`jm new` が叩く scaffold 単位。

レシピはコードではなく **編集可能で自己完結な job-manager flow 一式**を生成(gem に scaffold/new は無く本機能は新規レイヤ)。gem の**ドメイン意味論**を写すが**スキーマは job-manager の `flow.toml`/`plan.toml`**。gaussian-batch には依存しない(自己完結。§13)。

## 2. ゴール / 非ゴール

### Goals

1. `jm new` に位置引数 `<flow-recipe>` を追加。`jm new`(無引数)= 組込 `blank`(既存 2-job 雛形、**後方互換**)。`jm new g16-opt-parse --param opt.charge=1` で domain flow 生成。`--param opt.input_coordinate=<path>` で分子座標ファイル(.xyz/.mol2 等)を scaffold 時に取り込む(§7)。
2. **二層レジストリ**:`JobTemplate`(再利用部品)/ `FlowRecipe`(合成)。FlowRecipe のみ scaffold 可能(JobTemplate は内部合成単位、CLI 非公開)。
3. 出力ツリーは Flow 層が構築時 `jm doctor`-clean を保証(flow JobId 集合 == plan `[jobs.*]` キー集合、uuid == ディレクトリ名、親エッジ整合)。
4. domain JobTemplate は flow.toml/plan.toml への寄与に加え、**自分の JobId 名前空間下にサイドカー**を出す:`<JobId>/scripts/<JobId>.bash`(編集可能なジョブ本体ロジック)、`<JobId>/input/main.gjf`、`parse` は `<JobId>/scripts/parse_results.py`。配置・命名は gem 準拠(`input/`・`output/`・`derived/`、`task_basename = main`)。
5. JobTemplate ごとに型付きパラメータ。CLI は **`--param <JobId>.<param>=<value>`**。`jm new --list` / `jm new <flow-recipe> --describe`。
6. v1:JobTemplate `g16_opt` / `parse_g16_out`、FlowRecipe `blank` / `g16-opt-parse`。両レジストリは追加容易。
7. 書き込みは中途半端を残さない(既存 `jm new` rollback 規約踏襲)。
8. **core 変更ゼロ**:path 解決は R3(scaffold 時に flow.toml `body` へ絶対 job dir を焼く)。`flow.rs`/`render_batch_bash`/PyO3/`.pyi`/cwd 契約すべて不変(§5)。

### Non-goals

- `common.toml` 生成・変更(v1)。per-job `[jobs.*.config]`(`SlurmJobConfig`)+ `partition="REPLACE_ME"` sentinel(既存 `jm new` deferred-common 踏襲)。
- 分子幾何の**自動取得**(InChIKey→DB は gem 上流責務)。ただし `--param <jobid>.input_coordinate=<path>` による**ユーザ提供座標ファイルの取り込み**は行う(自動取得ではない。§7)。
- gem `experiment.toml`/`common.toml` フォーマット採用、gem `metadata.toml`/`status` 再実装(job-manager の Lifecycle + `decide_transition` + `tick` が status の権威。parse は exit code のみ)。
- **gaussian-batch への依存**(コード依存も実行時 CLI 依存も)。alpha・`gaussian-generate-gjf` import rot・`gaussian-parse-results` 未実装(§13)。参照と任意 swap-in フックのみ。
- experiment DSL / sweep 展開 / 親解決 / JobTemplate 単体直接 scaffold / 対話 TUI / 既存 flow 再生成・migration / リモートレジストリ / OpenMM。
- path の **R1/R2/R4 はいずれも不採用**(§5)。

## 3. CLI 形

```
jm --root <ROOT> new [<FLOW-RECIPE>] [--param <JOBID.PARAM=VALUE>]... [--tag <K=V>]... [--print-path]
jm --root <ROOT> new --list
jm --root <ROOT> new <FLOW-RECIPE> --describe
```

| 引数 | 説明 |
|---|---|
| `<FLOW-RECIPE>`(位置, 任意) | FlowRecipe 名。省略時 `blank`。未知名は候補列挙付きエラー。 |
| `--param <JobId>.<param>=<value>` | 任意回。最初の `=` で `key=value` 分割、`key` を最初の `.` で `<JobId>.<param>` 分割。合成ノード `<JobId>` の JobTemplate パラメータを上書き。未知 JobId / 未知 param / 型不整合 / `.`・`=` 欠落はエラー。 |
| `--tag <K=V>` | 既存。`flow.toml [tags]` 反映。 |
| `--print-path` | 既存。stdout に `<root>/<uuid>` のみ。 |
| `--list` | FlowRecipe 名 + 1 行説明を列挙して終了。 |
| `--describe` | `<FLOW-RECIPE>` の合成ノードと各 `<JobId>.<param>`(型/既定/ヘルプ)を列挙して終了。 |

`Cmd::New` を `recipe: Option<String>` / `params: Vec<String>` / 既存 `tags` / `print_path` / `list: bool` / `describe: bool` へ拡張。`main()` 分岐を `cmd_new(&root, recipe.as_deref(), &params, &tags, print_path, list, describe)` に。

## 4. アーキテクチャ(二層)

### モジュール配置

```
src/recipes/
  mod.rs           -- 公開 re-export, registries, --param <JobId>.<param> パース, --list/--describe
  job.rs           -- JobTemplate trait, JobArtifacts, JobCtx, RecipeParam/Type
  flow.rs          -- FlowRecipe trait + 合成アセンブラ(assemble())
  jobs/{g16_opt.rs, parse_g16_out.rs}
  flows/{blank.rs(据置・非分解・バイト同値), g16_opt_parse.rs(jobs を合成)}
  assets/g16_opt/{main.gjf.tmpl, g16_opt.bash.tmpl}
  assets/parse_g16_out/{parse.bash.tmpl, parse_results.py.tmpl}
```

- `src/recipes/` は **pyo3 非依存**(`uuid`/`chrono`/`toml`/std のみ)。`jm` `--no-default-features` ビルド必須。
- JobTemplate / FlowRecipe は **純粋**(I/O 無し)。I/O・ファイルコピー(input_coordinate)・rollback は `cmd_new`。
- `src/lib.rs` から `pub use recipes::{JobTemplate, FlowRecipe, flow_registry, ...}`(**公開 API は追加のみ**)。

### 型(Job 層)

```rust
pub enum RecipeParamType { Str, Int, Float, Bool, Path }
pub struct RecipeParam { pub name: &'static str, pub ty: RecipeParamType,
                         pub default: &'static str, pub help: &'static str }

pub struct JobArtifacts {
    pub program: String,                            // "g16" / "python" 等(flow.toml jobs.<JobId>.program)
    pub body: String,                               // flow.toml jobs.<JobId>.body。R3: 先頭 `cd "<ABS job dir>"`、続けて `bash scripts/<JobId>.bash`(薄い起動子)
    pub time_limit: Option<String>,                 // [jobs.<JobId>.config].time_limit。partition は常に REPLACE_ME
    pub plan_params: BTreeMap<String, toml::Value>, // → plan.toml [jobs.<JobId>]
    pub sidecars: Vec<GeneratedFile>,               // relpath は既に "<JobId>/..." で名前空間化済み
}

pub struct GeneratedFile { pub relpath: PathBuf, pub contents: String, pub unix_mode: Option<u32> }

pub struct JobCtx<'a> {
    pub job_id: &'a str,                            // flow が割り当てた JobId(sidecar/path の名前空間)
    pub params: &'a BTreeMap<String, toml::Value>,  // 既定 + --param <JobId>.* 上書き後の検証済み
    pub inputs: &'a BTreeMap<&'static str, String>, // 入力名 → flow が解決した **相対** パス `../<producer JobId>/<relpath>`(cwd = 自 job dir 前提)
    pub uuid: &'a Uuid, pub created_at: &'a str,
}

pub trait JobTemplate: Send + Sync {
    fn name(&self) -> &'static str;
    fn params(&self) -> &'static [RecipeParam];
    fn inputs(&self) -> &'static [&'static str];                  // 例: parse_g16_out -> ["gaussian_out"]
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

pub fn flow_registry() -> Vec<Box<dyn FlowRecipe>>;          // [Blank, G16OptParse]
pub fn find_flow(name: &str) -> Option<Box<dyn FlowRecipe>>;
pub fn find_job(name: &str) -> Option<Box<dyn JobTemplate>>; // jobs レジストリ(内部)
```

**アセンブラ `flow::assemble(recipe, raw_params, tags, uuid, created_at, abs_flow_dir) -> Result<Vec<GeneratedFile>>`**:

1. `recipe.nodes()` 各 `(job_id, tmpl)` を `find_job(tmpl)` 解決(未知は内部エラー = レシピ定義バグ)。
2. `--param` を `<JobId>.<param>` でパース → JobId ごとに分配。各ノードの `JobTemplate.params()` 既定で埋め、型検証して上書き。未知 JobId / 未知 param / 型不整合 → bail。
3. `recipe.wiring()` 解決:consumer の各入力名に **相対パス** `../<producer JobId>/<producer 出力 relpath>` を計算し `JobCtx.inputs` へ(両 job dir は `<uuid>/` 直下の兄弟。cwd = consumer job dir 前提なので相対で安定 = flow dir 移動耐性)。
4. 各ノード `JobTemplate.instantiate(&ctx)?` → `JobArtifacts`。body 先頭の絶対 cd は **R3**:`cd "<abs_flow_dir>/<JobId>" || exit 1`(§5)。
5. **flow.toml 組立**:`jobs.<JobId>` = {program, body, config(`partition="REPLACE_ME"` + `time_limit`)}、`recipe.edges()` から `[[jobs.<to>.parents]] from=<from> kind=<kind>`。
6. **plan.toml 組立**:`[jobs.<JobId>]` = そのノードの `plan_params`。ノード集合から構築 → **flow JobId 集合 == plan キー集合 を構造的に保証**(doctor-clean by construction)。
7. sidecars(既に `<JobId>/...` 名前空間化)+ flow.toml + plan.toml を `Vec<GeneratedFile>` で返す。

JobTemplate は peer の JobId をハードコードしない(wiring は FlowRecipe がデータで宣言)→ **再利用可能**。将来 `g16-opt-freq-parse` 等は既存 JobTemplate を並べる FlowRecipe 1 個で済む。

### `cmd_new` シーケンス

1. `--list`: `flow_registry()` を `name — summary` 出力で終了。
2. flow 解決: `recipe.unwrap_or("blank")` → `find_flow()`。`None` → bail(候補列挙)。
3. `--describe`: ノードと `<JobId>.<param>` 表を出力して終了。
4. `--tag` パース(既存流用)。
5. `uuid = Uuid::now_v7()`; `resolver = PathResolver::new(root)`; `flow_dir = resolver.flow_dir(&uuid)`(**絶対** — `root` は `resolve_root()` で canonicalize 済); 衝突確認(`exists()` なら bail、リトライしない)。
6. `created_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)`。
7. `flow::assemble(.., abs_flow_dir=flow_dir)?` → `Vec<GeneratedFile>`。
8. **input_coordinate 取り込み**(該当パラメータを持つ JobTemplate のみ):`--param <jid>.input_coordinate=<src>` 指定時、`<src>` を `<flow_dir>/<jid>/input/<basename>` へコピー(`cmd_new` の I/O。`<src>` 不在は bail)。`.xyz` は §7 の通り scaffold 時に gjf へ幾何差込、それ以外は copy のみ + gjf は sentinel + 注記。
9. `create_dir_all(&flow_dir)`。各 `GeneratedFile` の親(`<JobId>/input`・`<JobId>/scripts`)を `create_dir_all` し `atomic_write_str`。`*.bash`/`*.py` は `unix_mode=0o755` 指定 → rename 前 `chmod`(Unix-only)。
10. いずれか失敗時:作成済み `flow_dir` を `remove_dir_all` で巻き戻して `?` 伝播。
11. 出力(asset 込み列挙):
    ```
    created flow <uuid> from recipe g16-opt-parse
      <root>/<uuid>/flow.toml
      <root>/<uuid>/plan.toml
      <root>/<uuid>/opt/scripts/opt.bash
      <root>/<uuid>/opt/input/main.gjf
      <root>/<uuid>/parse/scripts/parse.bash
      <root>/<uuid>/parse/scripts/parse_results.py
    next: edit opt/input/main.gjf (geometry) and the cluster-env block in
          opt/scripts/opt.bash, set a real partition, then
          `jm --root <root> render <uuid>`
    ```
    `--print-path` 時は `<root>/<uuid>` の 1 行のみ。

## 5. flow パス解決 — R3(scaffold 時に絶対 job dir を body へ焼く)

### 制約(実コード確認済み)

- PathResolver:batch.bash = `<root>/<uuid>/.jm/<JobId>/batch.bash`(`.jm` は `<uuid>` 直下、`<JobId>` は `.jm` の下)。レシピ sidecar は `.jm` の外 `<root>/<uuid>/<JobId>/...`(CLAUDE.md「`.jm/` はプログラム管理、user-authored は一段上」準拠で `.jm` 配下不可)。
- A1:sbatch へは batch.bash を**絶対パス**で渡し、sbatch はスクリプトを spool にコピーして実行 → 実行中 `$0`/`$BASH_SOURCE`/`pwd` は元位置を指さない。sbatch 提出は cwd 制御なしで起動 → ジョブ cwd = `SLURM_SUBMIT_DIR` = `jm submit` を叩いた場所(flow/job dir ではない、非決定的)。
- ⇒ 走っている batch.bash には自分の `<uuid>/<JobId>/` を知る錨が無い。錨を与える手段は **R3 / R2 / R4** の3択(`$SLURM_SUBMIT_DIR`/`$0` 系は不可)。

### 採用:R3

`jm new` は scaffold 時に `<root>/<uuid>/<JobId>` を確定的に知っている。これを **flow.toml `body` の先頭に絶対 cd として焼く**:

```toml
[jobs.opt]
program = "g16"
body = """cd "<root>/<uuid>/opt" || exit 1
bash scripts/opt.bash
"""
```

- `body` は薄い起動子(絶対 cd 1 行 + `bash scripts/<JobId>.bash`)。**重い処理・環境アクティベーション(`# REPLACE_ME`)は編集可能な `scripts/<JobId>.bash`** に置き、cwd = job dir 前提で**純相対**(`input/main.gjf`, `output/main.out`)。
- クロスジョブ参照は **相対** `../<producer>/output/main.out`(兄弟 job dir)。絶対は body の cd 1 行のみ → flow dir 移動時の修正点は最小(その1行)。
- **core 変更ゼロ**:`flow.rs`/`render_batch_bash`/PyO3/`.pyi`/cwd 契約すべて不変。R4 で消したかった `$JM_FLOW_DIR` も spec から完全除去。
- gem 先行例整合:gem 生成 batch も `--config "<REPO_ROOT>/..."` と render 時に絶対パスを焼く。実クラスタ `env.root=/LARGE0/...` は login/compute 共通の安定 lustre マウント。

| 案 | core 変更 | body 汚染 | cwd 契約 | 判定 |
|---|---|---|---|---|
| **R3(採用)** | **ゼロ** | 絶対 cd 1 行のみ | 不変 | **採用** |
| R4 | flow.rs 1 行 + env | `$JM_FLOW_DIR` 散在 | 不変 | 却下(ユーザが削除) |
| R2 | flow.rs 1 行 `cmd.chdir` | なし(純相対) | **全ジョブ変更** | 却下(blast radius) |
| R1 | render シグネチャ | — | 不変 | 却下(公開 API/PyO3 破壊) |

### 既知の小制約(R3)

flow dir を**移動/コピー**、または login↔compute で**マウントパスが異なる**環境では body の絶対 cd が破綻する。緩和:flow は UUID ディレクトリで同定し移動しない運用 + 安定 root(lustre)前提。spec 既知制約として明記。将来 `jm render --rebase-paths`(body の絶対 cd を現 root へ再焼)は**別 spec・本 spec 非対象**。

## 6. リファレンス整合マッピング(gem ↔ job-manager 二層)

| gem(確立規約) | job-manager 二層表現 |
|---|---|
| step program `gaussian`(`run-g16`) | **JobTemplate `g16_opt`**(program=`g16`、`scripts/<JobId>.bash` が g16 実行) |
| post `gaussian-parse-results`(cclib、β/A2 では γ-pending 未実装) | **JobTemplate `parse_g16_out`**(program=`python`、自前 `parse_results.py`。§13) |
| step 連鎖 + afterok | **FlowRecipe `g16-opt-parse`**:nodes `[(opt,g16_opt),(parse,parse_g16_out)]`、edges `[(opt,parse,afterok)]` |
| `[step.params] route/charge/multiplicity/extra_input` | `g16_opt.params()`。CLI `--param opt.route=...`。`plan.toml [jobs.opt]`(`JM_PARAM_*` 露出) |
| coord ファイル(.mol2/.xyz)→ gjf 幾何 | `--param opt.input_coordinate=<path>`。`cmd_new` が `<uuid>/opt/input/` へコピー(§7) |
| `parent_uuids`(出力→次入力 consume) | FlowRecipe `wiring()`。相対 `../opt/output/main.out` に解決(JobId 非ハードコード) |
| `common.toml [slurm]`/`[slurm.post]` | `[jobs.opt.config]`(time 48h)/ `[jobs.parse.config]`(time 1h)。partition 両方 `REPLACE_ME` |
| `[env].task_basename=main`、`<uuid>/{input,output,derived}/` | flow 内 `<JobId>/{input,output,derived,scripts}/`(1 ジョブ ≈ 1 gem calc)、`main` 固定 |
| InChIKey compound(gjf title) | `--param opt.compound=<InChIKey>`(既定 sentinel)。gjf title + `[tags].compound` |
| status ファイル(post が権威) | job-manager Lifecycle/tick が権威。parse は exit code のみ |
| gaussian-batch `main.gjf.j2` / post template | **参照のみ**(形式一致確認)。依存しない。α-reshape 後の任意 swap-in は body `# REPLACE_ME` フック(§13) |

## 7. v1 JobTemplate / FlowRecipe 詳細

### JobTemplate `g16_opt`

`params()`:

| name | type | default | help |
|---|---|---|---|
| `route` | str | `#p opt b3lyp/6-31g(d)` | Gaussian route 行(複数行可、まるごと 1 文字列) |
| `charge` | int | `0` | 全電荷 |
| `multiplicity` | int | `1` | スピン多重度 |
| `extra_input` | str | `` | charge/mult・geometry の後の追加入力 |
| `nproc` | int | `8` | `%nprocshared` |
| `mem` | str | `8GB` | `%mem` |
| `compound` | str | `REPLACE_ME-INCHIKEY` | InChIKey。gjf title + `[tags].compound` |
| `input_coordinate` | path | `` (空=未指定) | 分子座標ファイル(`.xyz`/`.mol2` 等)。`cmd_new` が `<uuid>/<JobId>/input/` へコピー |

- `inputs()` = `[]`(root)。`outputs()` = `[("gaussian_out","output/main.out")]`。
- `instantiate`:program `"g16"`、`time_limit "48:00:00"`、`plan_params` = 上記(パス系は basename のみ記録)、sidecars:
  - `<JobId>/scripts/<JobId>.bash`(0755):
    ```
    #!/bin/bash
    set -euo pipefail
    # --- cluster environment (EDIT for your site; CLAUDE.md HPC notes) ---
    # REPLACE_ME: e.g. `module load gaussian` / `conda activate <env>`
    # (optional) once gaussian-batch α-reshape lands you may instead call:
    #   gaussian-generate-gjf --config ... ; see §13
    mkdir -p output
    g16 input/main.gjf output/main.out
    ```
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
    - `input_coordinate` 未指定:`{{geometry_block}}` = `<GEOMETRY: REPLACE_ME — 1行1原子 Element x y z。route に geom=connectivity を含むなら空行後に connectivity ブロック>`。
    - `.xyz` 指定:`cmd_new` が xyz(自明形式:行1=原子数 / 行2=コメント / 以降 `Elem x y z`)を**純 Rust で**パースし `{{geometry_block}}` に座標行を差込(化学ライブラリ不要)。元ファイルも `input/<basename>` にコピー保存。
    - `.mol2` 等その他:ファイルを `input/<basename>` にコピーするのみ。`{{geometry_block}}` は sentinel + 注記「`input/<basename>` から幾何を貼るか、gaussian-batch α-reshape 後に `gaussian-generate-gjf` を使う(§13)」。OpenBabel 等の重い変換は `jm new` に持ち込まない。
- body(flow.toml、R3):`cd "<root>/<uuid>/<JobId>" || exit 1` + `bash scripts/<JobId>.bash`。

### JobTemplate `parse_g16_out`

- `params()` = `[]`。`inputs()` = `["gaussian_out"]`。`outputs()` = `[]`(`derived/main.mol2` は TODO 拡張点)。
- `instantiate`:program `"python"`、`time_limit "01:00:00"`、`plan_params` = `{ note = "cclib parse + convergence/energy validation" }`、sidecars:
  - `<JobId>/scripts/<JobId>.bash`(0755):
    ```
    #!/bin/bash
    set -euo pipefail
    # REPLACE_ME: activate the python env that has `cclib`
    python scripts/parse_results.py "{{inputs.gaussian_out}}"
    ```
    (`{{inputs.gaussian_out}}` は wiring が **相対** `../opt/output/main.out` に解決)
  - `<JobId>/scripts/parse_results.py`(0755、cclib。沈黙成功を避ける):
    - 引数:Gaussian `.out` パス。`cclib` import 失敗 → 明示メッセージで **exit 2**。
    - 実 pass/fail:(a) パース不可→exit 1、(b) 正常終了マーカ無し→exit 1、(c) opt 収束 False→exit 1、(d) 最終エネルギーが有限実数で取れない→exit 1、すべて満たせば exit 0。検証済/未検証を stdout 明記。
    - `# TODO(jm recipe): write derived/main.mol2`(gem `derived/main.mol2` 相当)を明示拡張点に。
    - status は job-manager Lifecycle/tick が権威。本スクリプトは exit code のみ。
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
  [jobs.parse]
  note = "cclib parse + convergence/energy validation"
  ```

## 8. `blank` FlowRecipe(後方互換)

既存 `build_flow_template`/`build_plan_template`(`src/bin/jm.rs:497-574`)を `flows/blank.rs` へ移設。**JobTemplate 非分解**で直接出力(`assemble()` を介さない経路)し **既存 `jm new` 出力とバイト同値**(`tests/integration_new.rs` / `src/bin/jm.rs` ユニットがそのまま通る)。`jm new`(無引数)= `jm new blank`。サイドカー無し(flow.toml/plan.toml の 2 ファイルのみ。R3 の絶対 cd も無し — 既存挙動完全維持)。

## 9. エラーハンドリング

| 状況 | 挙動 |
|---|---|
| 未知 FlowRecipe | `bail!("unknown recipe {name:?}; available: blank, g16-opt-parse")` |
| `--param` に `.`/`=` 欠落 | `bail!("invalid --param: expected <JobId>.<param>=<value>, got {raw}")` |
| 未知 JobId | `bail!("recipe {flow}: no node {jobid}; nodes: opt, parse")` |
| 未知 param / 型不整合 | `bail!("recipe {flow}: job {jobid}: unknown/typed param ...")` |
| `input_coordinate` の src 不在/読めない | `bail!("input_coordinate {src}: not found")`(コピー前に検証) |
| FlowRecipe が未知 JobTemplate 参照 | 内部エラー(レシピ定義バグ)。registry 整合をユニットテストで保証 |
| `flow_dir` 既存(UUID 衝突) | `bail!("flow dir already exists: {path}")`(リトライしない) |
| sidecar/コピー書込失敗 | 作成済み `flow_dir` を `remove_dir_all` 巻き戻し後 `?` 伝播 |
| `--list`/`--describe` | scaffold せず終了 |
| (実行時)`cclib` 未導入 | `parse_results.py` が明示 exit 2 |

## 10. テスト

### ユニット(`src/recipes/**`)

- `g16_opt.instantiate`:program `g16`、body が `cd "<abs>/<JobId>"` + `bash scripts/<JobId>.bash`、sidecar relpath `<JobId>/scripts/<JobId>.bash`(0755)・`<JobId>/input/main.gjf`、gjf に `{{}}` 残存無し・gem ヘッダ順、`input_coordinate` 未指定で `<GEOMETRY: REPLACE_ME>`、`.xyz` 指定で座標行差込(純 Rust xyz パーサ単体テスト:原子数行/コメント行/座標行、不正形式 Err)、`outputs()`=`[("gaussian_out","output/main.out")]`。
- `parse_g16_out.instantiate`:program `python`、`inputs()`=`["gaussian_out"]`、`<JobId>/scripts/<JobId>.bash` が `{{inputs.gaussian_out}}` 解決値を含む、`scripts/parse_results.py`(0755)。
- `assemble(g16-opt-parse)`:flow JobId 集合 == plan キー集合 == `{opt,parse}`、`parse.parents[0]={from:opt,kind:afterok}`、両 config `partition=="REPLACE_ME"`、time_limit 非対称(48h/1h)、`parse` の wiring が **相対** `../opt/output/main.out` に解決、opt body の絶対 cd が `flow_dir.join("opt")`。
- パラメータ宛先:`--param opt.charge=1` のみ opt、未知 JobId/param/型不整合 Err、`--param opt.route=#p a=b`(値に `=`/`.`)の分割規則。
- レジストリ整合 lint:全 FlowRecipe の `nodes()` JobTemplate が `find_job` 解決可、`wiring()` 入出力名が両端 `inputs()/outputs()` に存在。
- `blank`:**既存 `jm new` 出力とバイト同値**(回帰防止。R3 の絶対 cd を含まないことも assert)。

### 統合(`tests/integration_new_recipes.rs`, `assert_cmd`)

- `jm new --list` / `--describe`:列挙のみ exit 0、scaffold 無し。
- `jm new g16-opt-parse --param opt.charge=1`:`flow.toml`/`plan.toml`/`opt/scripts/opt.bash`/`opt/input/main.gjf`/`parse/scripts/parse.bash`/`parse/scripts/parse_results.py` 生成、gjf に `1 1`、opt body の cd が実 tempdir 絶対パス。
- `--param opt.input_coordinate=<tmp.xyz>`:`opt/input/<basename>` にコピー + gjf に座標差込。src 不在で exit 非 0 + flow_dir 巻き戻し。
- doctor-clean:`jm doctor <uuid>` exit 0(`<JobId>/` サブツリーが doctor を壊さない)。
- `jm new g16-opt-parse` → `jm render <uuid>` exit 0(ラウンドトリップ)。
- 後方互換:`jm new` ≡ `jm new blank` ≡ 既存期待値。`--print-path`/`--tag` 維持。
- 未知レシピ exit 非 0 + 候補列挙。

### core 不変回帰(`src/runner/flow.rs` / `tests/integration_sp3.rs`)

- **R3 = core 変更ゼロ**の回帰:`SbatchCmd.chdir` 依然 `None`、`SbatchCmd.env` に `JM_FLOW_DIR` 等の新規キーが**入らない**、`render_batch_bash` シグネチャ不変。

### Python smoke(`python/tests`)

- 正常終了 `.out` フィクスチャで `parse_results.py` exit 0、未収束/切断で exit 1、`cclib` 不在で exit 2。

`MockExecutor`/`InMemoryQuerier` 使用、live SLURM 不要。

## 11. CLAUDE.md 準拠

- 生成物は user-authored 入力の **初回 bootstrap のみ**。runtime は `.jm/` 配下しか書かない。R3 は `flow.toml body` を初期生成するだけ(既存 `jm new` と同質)。
- `jm` `--no-default-features` → `src/recipes/` pyo3 非依存。xyz パースも純 Rust(化学ライブラリ無し)。
- 原子書込(PID サフィックス tmp + rename)を全生成ファイルで踏襲。`*.bash`/`*.py` は 0755。
- Out of scope(DSL/sweep/per-flow common/TUI/リモートレジストリ/OpenMM/JobTemplate 直接 scaffold/gaussian-batch 依存/自動幾何取得)非抵触。
- **公開 API/PyO3/`.pyi`/`render_batch_bash`/`flow.rs`/cwd 契約すべて不変**(R3 採用の最大利点)。公開追加は `recipes` モジュールのみ。
- gem の**意味論**を二層で写すが**スキーマは job-manager**。status は Lifecycle/tick が権威。
- Conventional Commits / per-task commit / stacked PR。

## 12. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| テンプレ層 | 二層(JobTemplate/FlowRecipe) | 単一 Recipe | 再利用、先行例全て二層 |
| Job 層 CLI 公開 | Flow 層のみ scaffold | JobTemplate も直接 | CLI 最小、YAGNI(ユーザ判断) |
| パラメータ宛先 | `--param <JobId>.<param>` | フラット | `plan.toml [jobs.<JobId>]` 1:1、同種ジョブ衝突せず |
| sidecar 配置 | `<JobId>/{input,output,derived,scripts}/` | フラット | 多ジョブ衝突を構造排除、gem `<uuid>/{...}` 写し |
| ジョブ本体 | 編集可能 `<JobId>/scripts/<JobId>.bash`、flow.toml body は薄起動子 | body にインライン | 化学者が bash を直接編集(ユーザ判断) |
| **path 解決** | **R3:scaffold 時 body に絶対 cd 1 行** | R1/R2/R4 | **core 変更ゼロ**、`$JM_FLOW_DIR` 完全除去、gem 先行例整合(ユーザ判断) |
| クロスジョブ参照 | 相対 `../<producer>/...` | 絶対/env | flow dir 移動耐性、cwd=job dir 前提で安定 |
| 幾何入力 | `input_coordinate` scaffold コピー(.xyz は純 Rust 差込) | 自動取得 / OpenBabel 同梱 | `jm new` を化学非依存に維持 |
| post 中身 | 自前 cclib `parse_results.py` | gaussian-parse-results | β/A2 で**未実装(γ-pending)**(§13) |
| gaussian-batch | **依存せず**(参照 + 任意 swap-in フック) | コード/CLI 依存 | alpha・import rot・未実装・gem orchestration 前提(§13) |
| status 権威 | job-manager Lifecycle/tick | gem status 再実装 | 二重実装回避 |
| common.toml | 非関与(partition REPLACE_ME) | gem common 採用 | 既存 deferred-common 踏襲 |
| `blank` | FlowRecipe 据置(非分解) | JobTemplate 分解 | バイト同値要件 |

## 13. gaussian-batch(β/A2)使用可否評価

`miyake-ken/gaussian-batch` 実コード精査結果(`gaussian_batch_generator` + `gaussian_batch_cli`):

| 要素 | 状態 | 本 spec での扱い |
|---|---|---|
| `gaussian-generate-gjf` | **壊**(`gaussian_job_shared.config.ConfigManager` を import → dropped API。xfail(strict) テスト) | 使用不可。`render_gjf` も `ConfigManager` 必須で最小契約不可 |
| `gaussian-parse-results` | **未実装**(pyproject に entry 宣言あるがモジュール `parse_results.py` 不在。γ-pending) | 使用不可。**自前 cclib `parse_results.py` が v1**(§7) |
| `gaussian-pipeline` / `gaussian-generate-batch` | gem フル orchestration 前提(experiment.toml/common.toml/UUID dir/metadata) | 不適(job-manager が置換する層) |
| `gaussian-run-job` | 動くが「描画済み bash を渡す」だけ | 不要(job-manager が submit を担う) |
| `main.gjf.j2` テンプレート | `%rwf/%nprocshared/%mem/%chk→route→title→charge mult→atoms→connectivity→extra_input` | **形式一致を確認 → 本 spec の gjf テンプレは規約準拠**(参照価値あり) |
| 依存 | `gaussian_job_shared`(D, **private**)/openbabel/jinja2/Python 3.12<3.13 | コンピュートノードに重い private 依存。job-manager は Rust・Python 無し → **コード依存不可** |

**結論**:job-manager は gaussian-batch に**依存しない**(コード依存=言語/Python 非搭載で不可、実行時 CLI 依存=該当 2 entry が壊/未実装で不可)。レシピは**自己完結**(自前 gjf テンプレ + cclib parse スクリプト)。gaussian-batch の `main.gjf.j2` は形式参照としてのみ使用。**将来 α-reshape で `gaussian-generate-gjf`/`gaussian-parse-results` が安定したら**、`scripts/<JobId>.bash` の `# REPLACE_ME` フックでユーザが任意に差し替え可能(本 spec はその差し替えを強制も実装もしない)。CHANGELOG 上 v0.3.0「3 - Alpha」、当該 2 entry は α/γ-pending。
