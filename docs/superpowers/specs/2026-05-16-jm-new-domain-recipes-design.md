# `jm new <flow-recipe>` — 二層レシピ(Job 層 / Flow 層)— design (rev.6)

**Date:** 2026-05-17
**Status:** Draft (rev.6 — 実 `gaussian_compute_runtime.run_g16`/`parse_results` のアルゴリズムを **job-manager 所有・純 stdlib の `scripts/run.py`/`scripts/parse.py`** で忠実再現(scratch ステージング + `finally` 回収 + srun は g16 subprocess のみ + curated `result.json`)。bash は薄起動子 + プリアンブル + `python scripts/run.py`(bare)。launcher 消費者を bash→run.py に再配置(設計は維持)。scratch_root を D2 `DirectoryConfig` 追加。rev.5 からの差分は §4/§5/§5.5/§5.6/§6/§7/§10/§11/§13。awaiting user review)
**Reference:**
- 既存エコシステム(整合対象):
  - `miyake-ken/gaussian-compute-runtime`(**Group B = compute ノード実体**。実コード精査 §13/§5.5):`run-g16`/`parse-results` の 2 inner subcommand。`run_g16.py` = `prepare_inputs(target→temp)` → `subprocess.run([*launcher, g16, gjf, out], cwd=temp)` → `finally: copy_results(temp→target)`、exit g16>copy。`parse_results.py` = `parse_compound(target_dir)`(Group C cclib)→ `write_json(<target>/result.json)`。`consume/gjf_render.render` = 正準 gjf 形式。これらの**アルゴリズムを写経**(コード/Group D 依存はしない)
  - `miyake-ken/gaussian-experiment-manager`・`miyake-ken/GAUSSIAN_repo/examples`:`experiment.toml [step.params]`(`route`/`charge`/`multiplicity`/`extra_input`)、main→afterok→post、`<env.root>/<uuid>/{input,output,derived}/`、`env.tmp_root`(scratch)、`task_basename`(既定 `main`)、InChIKey compound、`[slurm]`/`[slurm.post]`
  - `miyake-ken/gaussian-batch`(β/A2):`_base.bash.j2`(共有プリアンブル + `{% block modules/body %}`)← 2 leaf + `main.gjf.j2`。`_base.bash.j2` の**シェルプリアンブルを Rust に完全移植**(§4.0)。`gaussian-generate-gjf` 壊・`gaussian-parse-results` 未実装(§13。**`gaussian_compute_runtime` の同名 entry とは別物** — そちらは実装済)
  - **京大 KUDPC**(`https://web.kudpc.kyoto-u.ac.jp/manual/ja/run/batch`):exec 箇所に必ず `srun`。実 `run_g16` は `srun` を **g16 subprocess のみ**に付け(python orchestrator は bare)。job-manager の `run.py` も同層で再現(§5.5)
- 二層分離の先行例:nf-core / Snakemake / atomate / gem
- `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`(既存 `jm new` sentinel 哲学)
- `docs/superpowers/specs/2026-05-15-common-env-defaulting-design.md`(partition の read 時 default 注入。launcher/scratch_root はこれと**同タイミング**)
- `src/bin/jm.rs`(`Cmd::New` / `cmd_new` / `build_flow_template` / `build_plan_template` / `atomic_write_str`)
- `src/persistence/common.rs`(`CommonConfig{slurm_default:SlurmJobConfig(A1), directories:DirectoryConfig(D2)}`、`synth_empty_common`、`merge_with_defaults`。`launcher`/`scratch_root` は D2 新 field)
- `src/persistence/path.rs`(PathResolver:`flow_dir=<root>/<uuid>/`、`.jm` は `<uuid>` 直下。レシピ sidecar は `.jm` 外 `<root>/<uuid>/<JobId>/...`)
- `src/render/mod.rs`(`render_batch_bash(flow_uuid,jid,parts,params,body)` — `JM_FLOW_UUID/JM_JOB_ID/JM_AXIS_*/JM_PARAM_*` を焼く。**シグネチャ不変方針**)
- `src/runner/flow.rs:148-269`(submit/render 経路。`fr.common` 保持 + job ごと `effective_config`/`params_of`。launcher/scratch_root read 時解決の seam)
- 上流 A1 sbatch:script を絶対パスで渡し spool コピー実行。job cwd = `SLURM_SUBMIT_DIR`(非決定的)
- CLAUDE.md(Out of scope / PyO3 境界 / `.jm/` レイアウト / `--no-default-features` / **`### Upstream modification policy`** = A1 不変・D2 必要時可)

---

## 1. 問題設定

ユーザ要求は「g16 構造最適化 → afterok → 結果検証」のドメインチェーン scaffold。**実ジョブの中身は `gaussian_compute_runtime`(Group B)に集約**:gem の `_base.bash.j2` body は `python -m gaussian_compute_runtime run-g16 --config <abs>` を呼ぶだけで、scratch ライフサイクル・g16 起動・回収・parse は全てその Python パッケージ内。rev.5 の「flow dir 内で直接 `g16 input/main.gjf output/main.out`」は実体を写せておらず破綻していた(§5.5 の欠陥表)。

加えて rev.5 までに確立した 2 要件:

- **(A) 共通プリアンブル**:`_base.bash.j2` のシェル部(`set -euo pipefail` / 継承 conda スタック全消去 / module init / `module restore <profile> -f` / `conda activate <env>` / 任意 pixi hook / 末尾)を全 job 共有。
- **(B) クラスタ固有値の一元化**:`partition` と同フロー(common.toml 一元・read 時解決・per 上書き)。本 spec では **launcher**(srun)と **scratch_root**(`env.tmp_root` 相当)。

レシピは**編集可能で自己完結な job-manager flow 一式**を生成。gem の**ドメイン意味論・`run_g16` アルゴリズム・`_base.bash.j2` プリアンブル**を写すが**スキーマは job-manager**。Group B/C/D には依存しない(自己完結。§13)。

## 2. ゴール / 非ゴール

### Goals

1. `jm new [<flow-recipe>]`。無引数=`blank`(後方互換)。`jm new g16-opt-parse --param opt.charge=1`。`--param opt.input_coordinate=<path>` で座標取り込み(§7)。
2. 二層レジストリ:`JobTemplate` / `FlowRecipe`。FlowRecipe のみ scaffold 可能。
3. 構築時 `jm doctor`-clean 保証(flow JobId 集合 == plan キー集合、uuid==dir、親エッジ整合)。
4. domain JobTemplate は flow.toml/plan.toml に加え `<JobId>/` 名前空間下サイドカー:`scripts/<JobId>.bash`(§4.0 プリアンブル + `python scripts/run.py`)、**`scripts/run.py`**(run_g16 アルゴリズム再現、純 stdlib)、`input/main.gjf`;parse は `scripts/<JobId>.bash` + **`scripts/parse.py`**(parse_results 再現、cclib→result.json)。
5. JobTemplate ごと型付き param。CLI `--param <JobId>.<param>=<value>`。`--list`/`--describe`。
6. v1:JobTemplate `g16_opt`/`parse_g16_out`、FlowRecipe `blank`/`g16-opt-parse`。「テンプレ多数」は §4.0 継承で満たし leaf は増やさない(YAGNI)。
7. 書込は中途半端を残さない(rollback 規約踏襲)。
8. **path 解決 = R3**(scaffold 時 flow.toml `body` 先頭に絶対 cd を焼く。§5)。`render_batch_bash` 公開シグネチャ/PyO3/`.pyi`/cwd 契約不変。
9. **D2 1 coordinated PR**:`CommonConfig.launcher: Option<String>` + `DirectoryConfig.scratch_root: Option<PathBuf>`(両 `#[serde(default)]`)。A1 `SlurmJobConfig` は不変。CLAUDE.md `### Upstream modification policy` 準拠。
10. **実 `run_g16`/`parse_results` のアルゴリズムを純 stdlib で忠実再現** + サイトが gem stack 導入済なら `# REPLACE_ME` で `python -m gaussian_compute_runtime` に差替可能。

### Non-goals

- `common.toml` 自動生成(`launcher`/`scratch_root` を**読む**経路のみ導入)。per-job `partition` は `REPLACE_ME` sentinel 据置。
- 分子幾何の自動取得(`input_coordinate` 取り込みは行う)。
- gem `experiment.toml`/`metadata.toml`/`status` 採用・再実装(status は job-manager Lifecycle/tick が権威)。
- **多段 g16 連鎖**(opt→opt2 の親 `derived/main.mol2` 経由幾何受け渡し)。v1 は **opt→parse のみ**(これは `derived/main.mol2` 不要)。`derived/main.mol2` 生成は `parse.py` の明示 TODO 拡張点。
- **SBATCH ディレクティブ層**(`#SBATCH ... --rsc` 等)は job-manager `SbatchCmd`/render 領域でレシピ範囲外。`--rsc` を A1 が出すかは別 issue。
- **Group B/C/D への依存**(コード/実行時/private)。`gaussian_compute_runtime`/`gaussian_job_shared`/`gaussian_job_results` に依存しない。アルゴリズム写経 + 任意 swap-in のみ(§13)。
- DSL / sweep / 親解決 / JobTemplate 直接 scaffold / TUI / 既存 flow 再生成 / リモートレジストリ / OpenMM。
- path の R1/R2/R4・**A1 改変**は不採用/禁止。

## 3. CLI 形

```
jm --root <ROOT> new [<FLOW-RECIPE>] [--param <JOBID.PARAM=VALUE>]... [--tag <K=V>]... [--print-path]
jm --root <ROOT> new --list
jm --root <ROOT> new <FLOW-RECIPE> --describe
```

| 引数 | 説明 |
|---|---|
| `<FLOW-RECIPE>`(位置,任意) | 省略=`blank`。未知名は候補列挙エラー。 |
| `--param <JobId>.<param>=<value>` | 最初の `=`/`.` で分割。未知/型不整合はエラー。 |
| `--tag <K=V>` / `--print-path` / `--list` / `--describe` | 既存 + 列挙系。 |

`Cmd::New` を `recipe: Option<String>`/`params: Vec<String>`/`tags`/`print_path`/`list`/`describe` へ拡張。

## 4. アーキテクチャ(二層)

### モジュール配置

```
src/recipes/
  mod.rs           -- 公開 re-export, registries, --param パース, --list/--describe
  job.rs           -- JobTemplate trait, JobArtifacts, JobCtx, RecipeParam/Type, base_preamble()
  flow.rs          -- FlowRecipe trait + assemble()
  jobs/{g16_opt.rs, parse_g16_out.rs}
  flows/{blank.rs(据置・非分解・バイト同値), g16_opt_parse.rs}
  assets/g16_opt/{main.gjf.tmpl, run.py.tmpl}
  assets/parse_g16_out/parse.py.tmpl
```

- `src/recipes/` は **pyo3 非依存**(`uuid`/`chrono`/`toml`/std)。`jm --no-default-features` 必須。
- JobTemplate/FlowRecipe/`base_preamble()` は **純粋**。I/O・コピー・rollback・launcher/scratch_root 解決は `cmd_new`/render パス。
- `pub use recipes::{JobTemplate, FlowRecipe, flow_registry, base_preamble, ...}`(公開 API は追加のみ)。

### 4.0 共有ベースプリアンブル(`_base.bash.j2` 完全移植)

`_base.bash.j2` のシェル部(13-70 行)を `base_preamble()` に 1:1 移植。`#SBATCH` 1-12 行は非目標(SbatchCmd 領域)。

```rust
pub struct PreambleOpts<'a> {
    pub conda_env: &'a str,       // 既定 "analysis"(param)
    pub module_block: &'a str,    // {% block modules %} 相当(JobTemplate 供給)
    pub body_block: &'a str,      // {% block body %} 相当(= `python scripts/run.py` 等、bare)
    pub pixi_manifest: &'a str,   // 既定 ""(空=pixi hook 省略)
}
pub fn base_preamble(o: &PreambleOpts<'_>) -> String;
```

固定構造(`_base.bash.j2` と同順):`set -euo pipefail` → 継承 conda スタック全消去(`unset -f conda` + `CONDA_*` ループ。**学習スキル pixi-conda-stack-reset と同一、固定文字列・param 化しない**)→ `source "$(conda info --base)/etc/profile.d/conda.sh"` → `. /usr/share/Modules/init/bash` → `{module_block}` → `conda activate {conda_env}` → `{pixi hook(pixi_manifest 非空時)}` → `{body_block}` → `echo "JOB DONE"` → `exit 0`。

- `module_block`:`g16_opt` → `module restore {module_profile} -f`(既定 `gaussian_A`、param)。`parse_g16_out` → `module restore default -f`。
- サイト固有値のみ param:`conda_env`/`module_profile`/`pixi_manifest`。
- `blank` には適用しない(§8)。

### 型(Job 層 / Flow 層)

```rust
pub enum RecipeParamType { Str, Int, Float, Bool, Path }
pub struct RecipeParam { name, ty, default, help }   // すべて &'static str / enum

pub struct JobArtifacts {
    pub program: String,                            // 論理分類 "g16"/"python"(jm ls --program フィルタ用。body は別途 `bash scripts/<JobId>.bash`)
    pub body: String,                               // flow.toml jobs.<JobId>.body。R3: 絶対 cd + `bash scripts/<JobId>.bash`
    pub time_limit: Option<String>,
    pub plan_params: BTreeMap<String, toml::Value>,
    pub sidecars: Vec<GeneratedFile>,               // scripts/<JobId>.bash(base_preamble 済)+ scripts/run.py|parse.py + input/main.gjf。relpath は "<JobId>/..." 名前空間化
}
pub struct GeneratedFile { relpath: PathBuf, contents: String, unix_mode: Option<u32> }
pub struct JobCtx<'a> { job_id, params, inputs(相対 `../<producer>/<relpath>`), uuid, created_at }

pub trait JobTemplate: Send + Sync {
    fn name(&self)->&'static str; fn params(&self)->&'static [RecipeParam];
    fn inputs(&self)->&'static [&'static str];
    fn outputs(&self)->&'static [(&'static str,&'static str)];
    fn instantiate(&self, ctx:&JobCtx<'_>)->Result<JobArtifacts,RecipeError>;
}
pub trait FlowRecipe: Send + Sync {
    fn name(&self)->&'static str; fn summary(&self)->&'static str;
    fn nodes(&self)->&'static [(&'static str,&'static str)];
    fn edges(&self)->&'static [(&'static str,&'static str,&'static str)];
    fn wiring(&self)->&'static [(&'static str,&'static str,&'static str,&'static str)];
}
pub fn flow_registry()->Vec<Box<dyn FlowRecipe>>; pub fn find_flow(&str); pub fn find_job(&str);
```

**`flow::assemble(recipe, raw_params, tags, uuid, created_at, abs_flow_dir)`**:nodes 解決 → `--param` 分配/型検証 → wiring を相対パス解決 → 各 `instantiate()` → flow.toml(`partition="REPLACE_ME"`+time_limit、`edges()`→parents)/ plan.toml(`plan_params`)組立(**flow JobId 集合 == plan キー集合**)→ sidecars+toml を返す。`cmd_new` が input_coordinate コピー・原子書込・rollback を担う。

> **launcher/scratch_root は `cmd_new` では解決しない**(§5.5/§5.6):render 時に `common.toml` から解決し `JM_LAUNCHER`/`JM_SCRATCH_ROOT` を batch.bash に export、`run.py` が消費。

## 5. flow パス解決 — R3 + scratch ステージング

### 5.1 R3(scaffold 時に絶対 job dir を body へ焼く)

sbatch は script を spool コピー実行 → 実行中 `$0`/`pwd` 不可、job cwd=`SLURM_SUBMIT_DIR`(非決定的)。`jm new` は scaffold 時に `<root>/<uuid>/<JobId>` を確定的に知る → flow.toml `body` 先頭に絶対 cd を焼き、**cwd を永続 job dir に固定**:

```toml
[jobs.opt]
program = "g16"     # 論理分類(jm ls --program g16 用)。実行は body→bash→run.py
body = """cd "<root>/<uuid>/opt" || exit 1
bash scripts/opt.bash
"""
```

`body` = 薄起動子。重い処理は編集可能 `scripts/opt.bash`(= §4.0 プリアンブル + `python scripts/run.py`)。R3 の役割は「**永続 job dir に cwd を錨付け**し run.py の相対パス(`input/`,`output/`)を解決可能にする」。core 変更ゼロ(`flow.rs`/`render_batch_bash`/PyO3/cwd 契約不変)。R1/R2/R4 却下理由は rev.5 と同じ。既知制約:flow dir 移動/login↔compute マウント差で絶対 cd 破綻 → UUID dir 不動 + 安定 root 運用、将来 `jm render --rebase-paths` は別 spec。

### 5.2 scratch ステージング(永続 job dir ↔ ノードローカル scratch)

実 `run_g16` の核心。`scripts/run.py`(§7、純 stdlib)が実行:

1. **永続 = 自 job dir**(R3 で cd 済 `<root>/<uuid>/<JobId>/`)。`input/`・`output/` は相対。
2. **scratch = `<JM_SCRATCH_ROOT>/<JM_FLOW_UUID>/<JM_JOB_ID>/`**(§5.6。未設定 fallback = `./.scratch/`)。
3. `prepare_inputs`:`input/` を scratch へ `shutil` コピー。
4. `argv = ([JM_LAUNCHER] if 非空 else []) + [g16_cmd, scratch/main.gjf, scratch/main.out]`;`subprocess.run(argv, cwd=scratch)` — **g16 を cwd=scratch で実行**(`.chk`/`.rwf`/scratch がノードローカル)。`srun`/`g16` PATH 不在(`FileNotFoundError`)→ `failed to launch <argv0>` を stderr + 非ゼロ rc(**黙って 0 を返さない**:afterok post が空 .out を成功扱いするのを防ぐ)。
5. **`finally:` copy_results**:scratch の `main.out`/`.chk` 等を job dir `output/` へ常時回収(g16 失敗でも部分 `.out` を retrieve)。
6. exit 優先順位:**g16 非ゼロ rc 最優先** → 次に copy 失敗で 3 → 全成功 0。順序不変 `prepare→g16(cwd=scratch)→finally copy`。

### 5.5 launcher — read 時解決(消費者は run.py)

設計(D2 `CommonConfig.launcher`・read 時解決・4 ケース優先順位・`JM_LAUNCHER` export)は維持。**rev.5 との差は消費者**:bash ではなく **`run.py` が `JM_LAUNCHER` env を読み g16 subprocess のみを包む**(実 `run_g16` の `launcher=[] if no_srun else ["srun"]` と同層)。bash body は **`python scripts/run.py`(bare、srun なし)** — 実 gem `_base.bash.j2` body が python orchestrator を bare 起動するのと一致。

```rust
// D2 gaussian_job_shared::config::common::CommonConfig
pub struct CommonConfig {
    pub slurm_default: SlurmJobConfig,   // A1 — 不変
    pub directories: DirectoryConfig,    // D2(§5.6 で scratch_root 追加)
    #[serde(default)] pub launcher: Option<String>,   // ★ common.toml 直下 `launcher = "srun"`
}
```

render 時(`src/runner/flow.rs` の `fr.common` 保持点)の解決優先順位(partition「per-job 明示 > common」と同型):

| # | 条件 | 解決値 |
|---|---|---|
| 1 | plan.toml `launcher` param **非空** | その値(per-flow 上書き) |
| 2 | param 空 かつ `common.launcher=Some(非空)` | その値(例 `"srun"`、クラスタ既定) |
| 3 | param 空 かつ `common.launcher=Some("")` | **bare**(「このクラスタに srun 無」) |
| 4 | param 空 かつ `common.launcher=None` | ハードコード `"srun"`(KUDPC 正準) |

解決値を `batch.bash` の runtime-context ブロックに `export JM_LAUNCHER=<resolved>`(`quote_for_bash` 経由)。`run.py` は `os.environ.get("JM_LAUNCHER","")` を読み、非空なら argv 先頭に付与(空=bare、実 `--no-srun` 相当)。**配線制約**:`render_batch_bash` 公開シグネチャ不変。実装は (i) A1 不変 (ii) 既存公開関数を壊さない加法的配線(解決済 `launcher`/`scratch_root` を取る兄弟関数 / runtime_ctx map / `JM_*` 露出のいずれか) (iii) 解決は render 時にその時点の `common.toml` から — を満たし、優先順位を**単一値に解決してから** export(生 `JM_PARAM_*` 直使い不可)。

ネット効果:`common.toml launcher` 編集 → `jm render` 再実行で反映、**再 scaffold 不要**(partition と完全同等)。

### 5.6 scratch_root — read 時解決(launcher と同パターン)

実 flow の `env.tmp_root` 相当。launcher と同じ仕組み・同じ D2 PR:

```rust
// D2 gaussian_job_shared::config::common::DirectoryConfig
pub struct DirectoryConfig {
    pub project_root: PathBuf,
    #[serde(default)] pub scratch_root: Option<PathBuf>,  // ★ common.toml `[directories] scratch_root = "/LARGE0/.../scratch"`
}
```

| # | 条件 | 解決値 |
|---|---|---|
| 1 | plan.toml `scratch_root` param 非空 | その値 |
| 2 | param 空 かつ `directories.scratch_root=Some` | その値(クラスタ既定 scratch、例 lustre scratch) |
| 3 | param 空 かつ `=None` | **未設定** → `JM_SCRATCH_ROOT` を空 export → `run.py` が job dir 内 `./.scratch/` に fallback(機能するが degraded;非 HPC/ローカル smoke で有用) |

render が `export JM_SCRATCH_ROOT=<resolved or 空>`。`run.py` は `os.environ.get("JM_SCRATCH_ROOT") or "<job_dir>/.scratch"` で scratch 親を決める。

## 6. リファレンス整合マッピング(gem ↔ job-manager)

| gem / 実体 | job-manager 表現 |
|---|---|
| `_base.bash.j2` 共有プリアンブル | `base_preamble()`(§4.0、`#SBATCH` 除く) |
| `{% block modules %}` | JobTemplate 供給 `module_block` |
| `python -m gaussian_compute_runtime run-g16 --config`(bash は bare) | `scripts/<JobId>.bash` body = `python scripts/run.py`(bare) |
| `run_g16.py`:prepare_inputs→`[*launcher,g16,gjf,out]`(cwd=temp)→finally copy_results、exit g16>copy | **`scripts/run.py`**(純 stdlib 写経。§5.2/§7) |
| `srun` を g16 subprocess のみに(orchestrator は bare) | `run.py` が `JM_LAUNCHER` で g16 subprocess を包む(§5.5) |
| `env.tmp_root`(scratch) | D2 `DirectoryConfig.scratch_root` → `JM_SCRATCH_ROOT`(§5.6) |
| `gaussian_cmd.command` | recipe param `g16_cmd`(既定 `g16`)→ `JM_PARAM_G16_CMD` |
| `parse_results.py`:`parse_compound(target)`(cclib)→`write_json(<target>/result.json)` | **`scripts/parse.py`**(cclib 写経 → `output/result.json`。§7) |
| `gjf_render.render`(`%nprocshared=rsc.c`/`%mem=rsc.m`/`%chk`/route/title/`chg mult`/`{sym} {x:.6f}…`/extra。**`%rwf` 無し**) | `input/main.gjf` テンプレ(§7。`%rwf` 削除、`{x:.6f}`、nproc/mem は scaffold 既定 + run.py が SLURM 割当 env で上書き) |
| `[step.params] route/charge/multiplicity/extra_input` | `g16_opt.params()` → `plan.toml [jobs.opt]` |
| `common.toml`(クラスタ不変値一元) | `common.toml launcher`(§5.5)+ `[directories] scratch_root`(§5.6)。partition 同フロー |
| `parent derived/main.mol2`→child gjf(`consume-parent-results`) | **非目標(多段 g16)**。`parse.py` の `derived/main.mol2` は TODO 拡張点。v1 opt→parse は不要 |
| status(post 権威) | job-manager Lifecycle/tick 権威。`run.py`/`parse.py` は exit code(+`result.json` 成果物) |
| gaussian-batch `main.gjf.j2` | 形式参照のみ。任意 swap-in は `# REPLACE_ME`(§13) |

## 7. v1 JobTemplate / FlowRecipe 詳細

### JobTemplate `g16_opt`

`params()`:

| name | type | default | help |
|---|---|---|---|
| `route` | str | `#p opt b3lyp/6-31g(d)` | Gaussian route 行 |
| `charge` | int | `0` | 全電荷 |
| `multiplicity` | int | `1` | スピン多重度 |
| `extra_input` | str | `` | charge/mult・geometry の後の追加入力 |
| `nproc` | int | `8` | scaffold 既定 `%nprocshared`(run.py が `$SLURM_CPUS_PER_TASK` で上書き) |
| `mem` | str | `8GB` | scaffold 既定 `%mem`(run.py が SLURM 割当で上書き) |
| `compound` | str | `REPLACE_ME-INCHIKEY` | InChIKey。gjf title + `[tags].compound` |
| `g16_cmd` | str | `g16` | Gaussian バイナリ(実 `gaussian_cmd.command`)→ `JM_PARAM_G16_CMD` |
| `conda_env` | str | `analysis` | プリアンブル `conda activate <env>`(§4.0) |
| `module_profile` | str | `gaussian_A` | `module restore <profile> -f`(§4.0) |
| `pixi_manifest` | path | `` | 空=pixi hook 省略(§4.0) |
| `launcher` | str | `` | per-flow launcher 上書き。空=common→srun に委譲(§5.5 優先順位) |
| `scratch_root` | path | `` | per-flow scratch 上書き。空=common→fallback(§5.6) |
| `input_coordinate` | path | `` | 分子座標(`.xyz`/`.mol2`)。`cmd_new` が `<JobId>/input/` へコピー |

- `inputs()`=`[]`。`outputs()`=`[("gaussian_out","output/main.out")]`。`program`=`"g16"`(分類値;実行は body→`bash scripts/opt.bash`→`python scripts/run.py`)。`time_limit`=`"48:00:00"`。
- sidecars:
  - `scripts/<JobId>.bash`(0755)= `base_preamble()`(`module_block`=`module restore {module_profile} -f`、`conda_env`、`body_block`= `python scripts/run.py`)。
  - **`scripts/run.py`**(0755、純 stdlib。`run_g16` 写経):
    1. `job_dir = os.getcwd()`(R3 で cd 済)。`task="main"`。`g16=os.environ.get("JM_PARAM_G16_CMD","g16")`。`launcher=os.environ.get("JM_LAUNCHER","")`。`scratch_root=os.environ.get("JM_SCRATCH_ROOT") or os.path.join(job_dir,".scratch")`。`scratch=<scratch_root>/<JM_FLOW_UUID>/<JM_JOB_ID>`。
    2. `os.makedirs(scratch, exist_ok=True)`;`input/` を scratch へ `shutil.copytree(..., dirs_exist_ok=True)`(prepare_inputs)。
    3. (任意)`scratch/main.gjf` の `%nprocshared`/`%mem` を `$SLURM_CPUS_PER_TASK`/SLURM mem env から書換(実 resource_spec 由来の再現。env 無ければ scaffold 値据置)。
    4. `argv=([launcher] if launcher else [])+[g16, "main.gjf", "main.out"]`;`rc=subprocess.run(argv, cwd=scratch).returncode`;`FileNotFoundError`→`error: failed to launch {argv[0]}` stderr + `rc=2`(黙って 0 禁止)。
    5. `finally:` `output/` へ `main.out`/`main.chk`/`*.log` 等を copy back(無くても続行、copy 例外は記録)。
    6. exit:`rc!=0` ならそれ、elif copy 失敗 `3`、else `0`。
    7. 末尾コメント:`# REPLACE_ME: gem stack 導入済なら全体を `python -m gaussian_compute_runtime run-g16 --config <abs gem toml>` に差替可(§13)`。
  - `scripts/run.py` は `subprocess`/`shutil`/`os`/`sys` のみ(化学/Group D ライブラリ無し)。
  - `input/main.gjf`(gem `gjf_render` 形式に整合、`{{}}` 差込):
    ```
    %nprocshared={{nproc}}
    %mem={{mem}}
    %chk=main.chk
    {{route}}

    {{compound}}

    {{charge}} {{multiplicity}}
    {{geometry_block}}
    {{extra_input}}
    ```
    **`%rwf` 行なし**(実 `gjf_render.render` と一致)。`input_coordinate` 未指定 → `{{geometry_block}}`=`<GEOMETRY: REPLACE_ME — 1原子1行 Elem x y z>`;`.xyz` → 純 Rust パース(行1=原子数/行2=コメント/以降 `Elem x y z`)で `{sym} {x:.6f} {y:.6f} {z:.6f}` 差込 + 原本を `input/<basename>` 保存;`.mol2` 等 → コピーのみ + sentinel(OpenBabel 等は持ち込まない)。
- body(flow.toml、R3):`cd "<root>/<uuid>/opt" || exit 1` + `bash scripts/opt.bash`。

### JobTemplate `parse_g16_out`

- `params()`=`[ conda_env(既定 analysis), pixi_manifest(既定空) ]`(parse は軽量 post = gem `gaussian_post.bash.j2` 同様 **bare 起動**、srun ラップ対象 subprocess も巨大 scratch も無い → `launcher`/`scratch_root` param 不要)。`inputs()`=`["gaussian_out"]`。`outputs()`=`[("result_json","output/result.json")]`。`program`=`"python"`(分類値)。`time_limit`=`"01:00:00"`。
- sidecars:
  - `scripts/<JobId>.bash`(0755)= `base_preamble()`(`module_block`=`module restore default -f`、`conda_env`、`body_block`= `python scripts/parse.py`)。
  - **`scripts/parse.py`**(0755。`parse_results` 写経):
    - `cclib.io.ccread("{{inputs.gaussian_out}}")`(wiring が相対 `../opt/output/main.out` に解決)。`import cclib` 失敗 → `error: cclib not importable` + **exit 2**。
    - 検証:(a) パース不可→1、(b) 正常終了マーカ無し→1、(c) opt 収束 False→1、(d) 最終エネルギー非有限→1。
    - **curated `output/result.json` を atomic write**(`{converged, scf_energy, n_atoms, source, schema:"jm-recipe/1"}` 等、最小スキーマ)。`parse_results` の `write_json` 相当。書込失敗→3、全 OK→0。stdout に要約。
    - `# TODO(jm recipe): write derived/main.mol2`(多段 g16 連鎖用拡張点。v1 非対象)。
    - `# REPLACE_ME: gem stack 導入済なら `python -m gaussian_compute_runtime parse-results --config <abs>` に差替可(§13)`。
    - status は Lifecycle/tick 権威。本スクリプトは exit code + `result.json` 成果物。
- body(flow.toml、R3):`cd "<root>/<uuid>/parse" || exit 1` + `bash scripts/parse.bash`。

### FlowRecipe `g16-opt-parse`

- `nodes()`=`[("opt","g16_opt"),("parse","parse_g16_out")]`、`edges()`=`[("opt","parse","afterok")]`、`wiring()`=`[("parse","gaussian_out","opt","gaussian_out")]`(→ `parse` の `{{inputs.gaussian_out}}`=`../opt/output/main.out`)。
- 生成 `flow.toml`(抜粋):
  ```toml
  uuid="<uuid>"
  created_at="<rfc3339>"
  [tags]
  recipe="g16-opt-parse"
  compound="<opt.compound>"
  [jobs.opt]
  program="g16"
  body="""cd "<root>/<uuid>/opt" || exit 1
  bash scripts/opt.bash
  """
  [jobs.opt.config]
  partition="REPLACE_ME"
  time_limit="48:00:00"
  [jobs.parse]
  program="python"
  body="""cd "<root>/<uuid>/parse" || exit 1
  bash scripts/parse.bash
  """
  [[jobs.parse.parents]]
  from="opt"
  kind="afterok"
  [jobs.parse.config]
  partition="REPLACE_ME"
  time_limit="01:00:00"
  ```
- 生成 `plan.toml`:
  ```toml
  [jobs.opt]
  route="#p opt b3lyp/6-31g(d)"
  charge=0
  multiplicity=1
  extra_input=""
  nproc=8
  mem="8GB"
  compound="REPLACE_ME-INCHIKEY"
  g16_cmd="g16"
  conda_env="analysis"
  module_profile="gaussian_A"
  pixi_manifest=""
  launcher=""
  scratch_root=""
  [jobs.parse]
  conda_env="analysis"
  pixi_manifest=""
  ```
- 任意:`common.toml` に `launcher="srun"` / `[directories] scratch_root="/.../scratch"`(無くても fallback で機能。§5.5/§5.6)。

## 8. `blank` FlowRecipe(後方互換)

既存 `build_flow_template`/`build_plan_template`(`src/bin/jm.rs:497-574`)を `flows/blank.rs` へ移設。**非分解**で直接出力、**既存 `jm new` 出力とバイト同値**。`jm new`≡`jm new blank`。サイドカー/プリアンブル/run.py/R3 cd すべて無し(既存挙動完全維持)。

## 9. エラーハンドリング

| 状況 | 挙動 |
|---|---|
| 未知 FlowRecipe / 未知 JobId / 未知 param / 型不整合 / `--param` 構文不正 | `bail!` + 候補列挙 |
| `input_coordinate` src 不在 | `bail!` (コピー前検証) |
| `flow_dir` 既存 | `bail!`(リトライしない) |
| sidecar/コピー書込失敗 | `flow_dir` を `remove_dir_all` 巻き戻し後 `?` 伝播 |
| `--list`/`--describe` | scaffold せず終了 |
| (実行時)`JM_SCRATCH_ROOT` 空 | run.py が `<job_dir>/.scratch/` fallback(§5.6 ケース3) |
| (実行時)`srun`/g16 PATH 不在 | run.py:`failed to launch …` stderr + 非ゼロ(黙って 0 禁止) |
| (実行時)g16 非ゼロ | run.py:`finally` で copy 後その rc を返す(g16>copy) |
| (実行時)copy_results 失敗 | run.py:記録し exit 3(g16 成功時) |
| (実行時)`cclib` 不在 | parse.py exit 2 |
| (実行時)common に `launcher` 無 & param 空 | ハードコード `srun`(§5.5 ケース4) |
| (実行時)`common.toml launcher=""` & param 空 | bare(§5.5 ケース3) |

## 10. テスト

### ユニット(`src/recipes/**`)

- `base_preamble()`:`_base.bash.j2` 同順(conda 全消去固定文字列・`{module_block}` 位置・`conda activate <env>`・末尾 `echo "JOB DONE"`/`exit 0`)、`pixi_manifest` 空/非空で hook 行有無、`#SBATCH` を含まない。
- `g16_opt.instantiate`:`program="g16"`(分類値)、body=R3 cd+`bash scripts/opt.bash`、sidecar `scripts/opt.bash`(0755、`module restore gaussian_A -f`+`conda activate analysis`+body=`python scripts/run.py`、**srun を含まない**)、`scripts/run.py`(0755)に prepare/`subprocess.run(...,cwd=scratch)`/`finally` copy/exit-precedence/`failed to launch`/`# REPLACE_ME` 文字列、`run.py` が `cclib`/Group D を import しない(静的 grep)、`input/main.gjf` に **`%rwf` 無し**・`{{}}` 残存無し・`.xyz` で `{x:.6f}` 差込(純 Rust xyz パーサ単体:原子数/コメント/不正形式 Err)、`outputs()`=`[("gaussian_out","output/main.out")]`。
- `parse_g16_out.instantiate`:body=`python scripts/parse.py`、`scripts/parse.py`(0755)が cclib・`output/result.json` atomic write・exit 0/1/2・`# TODO derived/main.mol2`・`# REPLACE_ME`、`outputs()`=`[("result_json","output/result.json")]`。
- `assemble(g16-opt-parse)`:flow JobId 集合==plan キー集合=={opt,parse}、`parse.parents[0]={from:opt,kind:afterok}`、両 `partition=="REPLACE_ME"`、time 48h/1h、wiring 相対 `../opt/output/main.out`、opt body cd=`flow_dir.join("opt")`。
- パラメータ宛先・registry 整合 lint・`blank` バイト同値(プリアンブル/run.py/`$JM_*`/R3 cd を含まないことも assert)。

### run.py アルゴリズム回帰(Python smoke `python/tests`、g16/srun を stub)

- 順序 `prepare→g16(cwd=scratch)→finally copy` を call-order で固定(実 `test_run_g16.py` 同型)。
- g16 非ゼロ rc がそのまま伝播し copy も走る(g16>copy)。prepare 失敗で g16/copy スキップ + 非ゼロ。copy 失敗 + g16 成功 → exit 3。`subprocess.run` が `FileNotFoundError` → 非ゼロ + `failed to launch` に launcher 名。
- `JM_SCRATCH_ROOT` 空 → `<job_dir>/.scratch/` 使用。`JM_LAUNCHER` 空 → argv 先頭に srun 無し、非空 → 付与。
- `parse.py`:正常 `.out` フィクスチャ→exit 0 + `output/result.json` 生成・スキーマ、未収束/切断→1、`cclib` 不在→2。

### launcher/scratch_root read 時解決(`src/runner/flow.rs`/`src/persistence/common.rs`)

- `CommonConfig` deserialize:`launcher`/`[directories] scratch_root` あり→`Some`、無し(既存 common.toml)→`None`(`#[serde(default)]`)、`deny_unknown_fields` と両立。`synth_empty_common()` が両者 `None`/既定。
- render export 4/3 ケース:`JM_LAUNCHER`(§5.5 ケース1-4)・`JM_SCRATCH_ROOT`(§5.6 ケース1-3)を batch.bash に正しく焼く。plan param がそれぞれ common に優先。
- **再 scaffold 不要回帰**:`common.toml` の launcher/scratch_root 書換 → `jm render` 再実行で batch.bash の `JM_*` のみ更新、sidecar(`scripts/*.py`,`*.bash`)不変。
- `render_batch_bash` 公開シグネチャ不変(arity/型)。R3 = core 変更ゼロ(`SbatchCmd.chdir` 依然 None・新規 env キー無し)。

### 統合(`tests/integration_new_recipes.rs`)

- `--list`/`--describe` exit 0・scaffold 無し。
- `jm new g16-opt-parse --param opt.charge=1`:全ファイル生成、gjf に `1 1`・`%rwf` 無し、`opt/scripts/opt.bash` に conda 全消去 + `module restore gaussian_A -f` + `python scripts/run.py`、opt body cd=実 tempdir 絶対。
- `--param opt.input_coordinate=<tmp.xyz>`:`opt/input/<basename>` コピー + gjf 座標差込。src 不在で非 0 + 巻戻し。
- `jm doctor <uuid>` exit 0。`jm new g16-opt-parse`→`jm render <uuid>` exit 0、batch.bash に `export JM_LAUNCHER='srun'`(common 無でも fallback)・`export JM_SCRATCH_ROOT=...`。
- 後方互換:`jm new`≡`jm new blank`≡既存期待値。

`MockExecutor`/`InMemoryQuerier`、live SLURM 不要。

## 11. CLAUDE.md 準拠 / 上流変更

- **`### Upstream modification policy` 準拠**:A1 `SlurmJobConfig` 不変。**D2 1 coordinated PR** = `CommonConfig.launcher: Option<String>` + `DirectoryConfig.scratch_root: Option<PathBuf>`(両 `#[serde(default)]`)→ D2 リポへ land → 本リポ `Cargo.toml` D2 rev bump → `synth_empty_common()` + 関連テスト追従。非上流 seam(recipe param のみ)では partition 同等の「クラスタ一元・read 時解決」が原理的に不能(`#[serde(deny_unknown_fields)]` が枠追加を強制)→ D2 変更が正当。2026-05-15 Goal#4 を CLAUDE.md ポリシーが置換した結果として許容。writing-plans で D2 PR を先行タスク化。
- 生成物は user-authored 入力の初回 bootstrap のみ。runtime は `.jm/` と `<JobId>/output/`・`<JobId>/.scratch/`(後者はジョブ自身の作業領域、user dir 内)。launcher/scratch_root 解決は render 既存責務内(新規副作用ファイル無し)。
- `jm --no-default-features` → `src/recipes/` pyo3 非依存。`base_preamble()`・xyz パーサは純 Rust。**`scripts/run.py`/`parse.py` は job-manager 所有の生成物**で `subprocess`/`shutil`/`os`(parse は `cclib` のみ)。Group B/C/D を import しない。
- 原子書込(PID サフィックス tmp+rename)を全生成ファイル。`*.bash`/`*.py` は 0755。
- Out of scope(DSL/sweep/per-flow common/TUI/リモートレジストリ/OpenMM/JobTemplate 直接 scaffold/Group B-C-D 依存/自動幾何取得/多段 g16/`--rsc` SBATCH ヘッダ)非抵触。
- **公開 API/PyO3/`.pyi`/`render_batch_bash` シグネチャ/`flow.rs` cwd 契約すべて不変**。公開追加は `recipes` モジュール + D2 2 field のみ。
- gem 意味論 + `run_g16` アルゴリズム + `_base.bash.j2` プリアンブルを写すがスキーマは job-manager。status は Lifecycle/tick 権威。
- Conventional Commits / per-task commit / stacked PR。

## 12. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| テンプレ層 | 二層 | 単一 Recipe | 再利用、先行例全て二層 |
| 共通プリアンブル | `_base.bash.j2` 完全移植 `base_preamble()` | 最小/全 param | gem 実績、サイト名のみ可変 |
| **実ジョブ表現** | **job-manager 所有 純 stdlib `run.py`/`parse.py`(run_g16/parse_results 写経)+ `# REPLACE_ME` swap-in** | bash 直書き / `gaussian_compute_runtime` 委譲 / スケルトンのみ | 忠実再現 + 自己完結(§13)、Group B/C/D 不要、gem 導入済なら任意差替(ユーザ選択) |
| scratch | run.py が prepare→g16(cwd=scratch)→finally copy | flow dir 直実行 | 巨大 `.rwf` を lustre に置かない、KUDPC tmp 規約、失敗時 `.out` 回収 |
| srun 層 | run.py が g16 subprocess のみ包む(bash は bare) | bash で `$JM_LAUNCHER python …` | 実 run_g16 と同層、orchestrator は bare(gem 一致) |
| launcher 格納/解決 | D2 `CommonConfig.launcher`・read 時・4 ケース | A1 追加 / recipe param のみ / scaffold 焼付 | A1 不変、partition 同フロー、再 scaffold 不要(ユーザ指示) |
| scratch_root 格納/解決 | D2 `DirectoryConfig.scratch_root`・read 時・job dir fallback | recipe param のみ / TMPDIR 自動のみ | launcher と一貫、クラスタ tmp 明示可、fallback で機能(ユーザ選択) |
| path 解決 | R3(scaffold 時 body 絶対 cd、永続 job dir 錨付け) | R1/R2/R4 | core 変更ゼロ |
| gjf | `gjf_render` 整合(`%rwf` 削除・`{x:.6f}`・nproc/mem は scaffold 既定+run.py が SLURM 上書き) | rev.5 `%rwf` 付き独立 param | 実 renderer 一致 |
| 幾何入力 | `input_coordinate` scaffold コピー(.xyz 純 Rust) | 自動取得/OpenBabel | `jm new` 化学非依存 |
| parse 出力 | curated `output/result.json`(自前 cclib) | exit-code のみ / gaussian-parse-results | 実 parse_results 一致(成果物)、自己完結 |
| 多段 g16 連鎖 | v1 非対象(`derived/main.mol2` は TODO) | v1 で対応 | opt→parse は不要、YAGNI |
| status 権威 | Lifecycle/tick | gem status 再実装 | 二重実装回避 |
| `blank` | 据置(非分解) | 分解 | バイト同値 |

## 13. Group B/C/D 使用可否評価

| 要素 | 状態 | 本 spec での扱い |
|---|---|---|
| `gaussian_compute_runtime.run_g16`(Group B) | **実装済・clean**(`prepare_inputs`→`[*launcher,g16,gjf,out]`(cwd=temp)→`finally copy_results`、exit g16>copy。tests 50/118/174/202 で固定) | **アルゴリズムを `scripts/run.py` に写経**(コード/import 依存はしない) |
| `gaussian_compute_runtime.parse_results`(Group B) | **実装済・clean**(`parse_compound`→`write_json(<target>/result.json)`) | **アルゴリズムを `scripts/parse.py` に写経**(自前 cclib、curated `result.json`) |
| `consume/gjf_render.render`(Group B) | 純関数 gjf:`%nprocshared=rsc.c`/`%mem=rsc.m`/`%chk`/route/title/`chg mult`/`{sym} {x:.6f}`/extra、**`%rwf` 無し** | gjf テンプレを**形式整合**(§7。`%rwf` 削除) |
| `gaussian_job_shared`(Group D, **private**)`ConfigManager`/`JobPaths`/`PathResolver`/`fs.{prepare_inputs,copy_results}` | run_g16 が依存。private・SSH | **依存しない**。`run.py` が prepare/copy を `shutil` で最小再実装、paths は `JM_*` env + R3 cwd から導出 |
| `gaussian_job_results`(Group C)`parse_compound`/`to_json`/`write_json` | parse_results が依存 | **依存しない**。`parse.py` が `cclib` 直叩き + 自前最小 `result.json` スキーマ |
| `gaussian-batch`(A2)`gaussian-generate-gjf`/`gaussian-parse-results` | 壊/未実装(α/γ-pending) | 不使用。**`gaussian_compute_runtime` の同名 entry とは別物**(そちらは実装済 = 写経元) |
| `_base.bash.j2`(A2) | 共有プリアンブル(conda 全消去は学習スキル pixi-conda-stack-reset と同一) | §4.0 で 1:1 移植(`#SBATCH` 除く) |

**結論**:job-manager は Group B/C/D に**依存しない**(Rust・Python 非搭載・private)。`run_g16`/`parse_results`/`gjf_render` の**アルゴリズムと形式を純 stdlib + cclib で写経**し自己完結。`prepare_inputs`/`copy_results` の Group D 内ファイル選択詳細は不可視 → `run.py` は「`input/` 全体を scratch へ、scratch の `main.out`/`main.chk`/`*.log` を `output/` へ」という保守的・自己完結な定義を採る(実 Group D と完全一致は保証しない;swap-in で差替可能)。**gem stack 導入済サイト**は `scripts/run.py`/`parse.py` 末尾の `# REPLACE_ME` で `python -m gaussian_compute_runtime {run-g16,parse-results} --config <abs gem toml>` に任意差替(本 spec は強制も実装もしない)。CHANGELOG 上 `gaussian_compute_runtime` v0.2.0「3-Alpha」。
