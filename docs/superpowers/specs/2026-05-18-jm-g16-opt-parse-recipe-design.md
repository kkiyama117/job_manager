# `jm new g16-opt-parse` — kudpc 向け g16 構造最適化レシピ — design (独立案A)

**Date:** 2026-05-18(2026-05-20 改訂:rev.7 reject 確定 / H1 revoke (a)→(b) 反映)
**Status:** v1 landed (PR #27 / PR #28); H1 permanent fix (b) per-job
`SbatchCmd.chdir` landed (issue #29 / this branch).
**関係性:** 案A は独立正本(2026-05-20 ユーザ決定で rev.7 廃案)。rev.7
(`2026-05-16-jm-new-domain-recipes-design.md`)系統の D2 一元化は v2 拡張点
として §4.2 / §11 に残置 — 「分岐点」ではなく「v2 候補」と読み替えること。
**Reference:**
- 参照リポ `miyake-ken/GAUSSIAN_repo`(docs+レシピ メタリポ。直接呼び出さず参照のみ):
  - `gaussian_compute_runtime`(Group B):`run-g16` = `prepare_inputs(target→temp)` →
    `subprocess.run([*launcher, g16, gjf, out], cwd=temp)` → `finally: copy_results(temp→target)`、
    exit g16>copy。`parse-results` = cclib parse → `result.json`。**アルゴリズムを写経**(コード依存しない)
  - `gaussian-batch`(A2)`_base.bash.j2`:共有シェルプリアンブル + `{% block modules/body %}`
  - `examples/generate_slurm_batch/`:`#SBATCH --rsc p=:t=:c=:m=`(kudpc 固有)、partition `gr10641a`、
    `/LARGE0/gr10641/...`、`module restore gaussian_A -f` / `default -f`、`conda activate analysis`、
    `<env.root>/<uuid>/{input,output,derived}/`、`env.tmp_root`(scratch)、`task_basename`(既定 `main`)
  - 京大 KUDPC マニュアル:exec 箇所に `srun`。実 `run_g16` は `srun` を g16 subprocess のみに付け
    python orchestrator は bare
- `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`(既存 `jm new` sentinel 哲学・`blank`)
- `docs/superpowers/specs/2026-05-15-common-env-defaulting-design.md`(partition の read 時 default 注入)
- `src/bin/jm.rs`(`Cmd::New`/`cmd_new`/`build_flow_template`/`build_plan_template`/`atomic_write_str`)
- `src/render/mod.rs`(`render_batch_bash` — 全 plan param を `export JM_PARAM_<NAME>=<quoted>` に焼く。**シグネチャ不変**)
- `src/runner/flow.rs:262-289`(`SbatchCmd` 組立。`cmd.rsc = config.resource_spec` 既存配線)
- `src/persistence/path.rs`(`PathResolver`:`flow_dir=<root>/<uuid>/`)
- CLAUDE.md(Out of scope / PyO3 境界 / `.jm/` レイアウト / `--no-default-features` /
  `### Upstream modification policy` = A1 不変・D2 必要時可)

---

## 1. 問題設定

ユーザ要求は「g16 構造最適化 → afterok → 結果検証」のドメインチェーン scaffold を kudpc で
回すこと。新 flow ごとの UUID v7 採番・`<root>/<uuid>/` 作成・flow.toml/plan.toml 手書き・
JobId 整合は退屈でミスを生む。`jm new <flow-recipe>` でこれを一発化し、かつ実 `run_g16`/
`parse_results` の**アルゴリズムを純 stdlib で写経した自己完結スクリプト**を同梱する。

参照エコシステム(`gaussian_compute_runtime` 他)は private/SSH 依存があり直接は呼べないため、
**依存はせずアルゴリズム・形式・プリアンブルのみを写経**する。gem stack 導入済サイトでは
生成スクリプト末尾の `# REPLACE_ME` で `python -m gaussian_compute_runtime` に任意差替可能
(本 spec は強制も実装もしない)。

## 2. ゴール / 非ゴール

### Goals

1. `jm new [<flow-recipe>]`(無引数=`blank`、後方互換バイト同値)。`jm new g16-opt-parse
   --param opt.charge=1`、`--param opt.input_coordinate=<path>`、`--list`/`--describe`、
   `--tag k=v`、`--print-path`。
2. 二層レジストリ `JobTemplate` / `FlowRecipe`(FlowRecipe のみ scaffold 可)。
3. 生成直後に `jm doctor <uuid>` clean(flow JobId 集合 == plan キー集合、uuid==dir、
   親エッジ整合)、`jm render`/`jm submit --dry-run` がエラーなく通る。
4. 実 `run_g16`/`parse_results` の**アルゴリズムを純 stdlib で写経**(`scripts/run.py`/`parse.py`)、
   末尾 `# REPLACE_ME` で gem stack へ任意 swap-in。Group B/C/D 非依存(自己完結)。
5. **v1 は job-manager 内完結**:`launcher`/`scratch_root`/`g16_cmd` は recipe param(plan.toml)
   → 既存 render が `JM_PARAM_*` を焼く。**上流(A1/D2)変更ゼロ・render/コア変更ゼロ**。
6. 書き込みは中途半端を残さない(rollback 規約踏襲)。
7. path 解決 = **R3' + (b) per-job chdir**(2026-05-20 改訂):
   - **R3'**(scaffold 側、不変): 生成 `scripts/run.py`/`parse.py` 冒頭へ絶対
     `JOB_DIR` 定数を焼き、スクリプトを **内部 cwd 非依存**にする(I/O は
     `os.path.join(JOB_DIR, ...)` で絶対化)。flow.toml `body` は
     `bash scripts/<JobId>.bash` のみで **cd 無し**。
   - **(b) per-job chdir**(submit 側、2026-05-20 追加・issue #29 / PR #27 H1
     revoke): `FlowRunner::submit` が per-job
     `SbatchCmd.chdir = Some(<flow_dir>/<job_id>/)` を注入し、SLURM が job cwd
     を `<flow_dir>/<job_id>/` に固定 → 相対 `scripts/<id>.bash` が解決される。
   - **flow.rs cwd 契約は更新**(従来「chdir=None 据置」→「per-job chdir
     注入」)。`render_batch_bash` 公開シグネチャ/PyO3/`.pyi` は不変。
     `blank`/user 既存 flow も同じ submit-cwd 規約に収束 — body 起動が cwd
     依存だった場合に挙動変化する可能性あり(README v1 caveat 参照)。
   - 参照 `run-g16` の cwd 非依存性は内部(R3')+ submit-cwd 固定(b)の
     合算で達成され、結果として参照と同じ「cwd を一切信用しない」性質に到達する。

### Non-goals

- **D2 協調PR によるクラスタ一元化**(`common.toml launcher` / `[directories] scratch_root`)
  → **v2 拡張点に切り出し**(§4.2 / §11)。rev.7 はこれを v1 に同梱するが、本案は v1 から外す。
- `common.toml` 自動生成。partition / `resource_spec`(kudpc `--rsc`)は既存
  `[jobs.*.config]` + `REPLACE_ME` sentinel(配線 `cmd.rsc = config.resource_spec` は既存・不変)。
- 多段 g16 連鎖(opt→opt2 の `derived/main.mol2` 幾何受け渡し)。v1 は opt→parse のみ。
- 分子幾何の自動取得(`input_coordinate` 取り込みは行う)。
- gem `experiment.toml`/`metadata.toml`/`status` 採用・再実装(status は job-manager
  Lifecycle/tick が権威)。
- `#SBATCH` ディレクティブ層(`--rsc` 等)はレシピ範囲外(job-manager `SbatchCmd`/render 領域)。
- Group B/C/D へのコード/実行時/private 依存。
- DSL / sweep / 親解決 / JobTemplate 直接 scaffold / TUI / 既存 flow 再生成 /
  リモートレジストリ / OpenMM。
- path の R1/R2/R4・A1 改変は不採用/禁止。

## 3. CLI 形

```
jm --root <ROOT> new [<FLOW-RECIPE>] [--param <JOBID.PARAM=VALUE>]... [--tag <K=V>]... [--print-path]
jm --root <ROOT> new --list
jm --root <ROOT> new <FLOW-RECIPE> --describe
```

| 引数 | 説明 |
|---|---|
| `<FLOW-RECIPE>`(位置・任意) | 省略=`blank`。未知名は候補列挙エラー。 |
| `--param <JobId>.<param>=<value>` | 最初の `.` で `<JobId>`/`<param>`、最初の `=` で `param`/`value` 分割。未知/型不整合/構文不正は候補列挙付き `bail!`。 |
| `--tag <K=V>` / `--print-path` | 既存 `jm new` 仕様踏襲。 |
| `--list` / `--describe` | scaffold せず一覧/詳細を出力し exit 0。 |

`Cmd::New` を `{ recipe: Option<String>, params: Vec<String>, tags: Vec<String>,
print_path: bool, list: bool, describe: bool }` へ拡張。`main()` の match に分岐を足す。

## 4. アーキテクチャ(二層レシピ)

### 4.0 モジュール配置(`src/recipes/`、pyo3 非依存)

```
src/recipes/
  mod.rs            -- 公開 re-export, registries, --param パース, --list/--describe
  job.rs            -- JobTemplate trait, JobArtifacts, JobCtx, RecipeParam/Type, base_preamble()
  flow.rs           -- FlowRecipe trait + assemble()
  jobs/{g16_opt.rs, parse_g16_out.rs}
  flows/{blank.rs, g16_opt_parse.rs}
  assets/_base.bash.j2                 -- 上流 _base.bash.j2 のシェル部を embed(include_str!)
  assets/g16_opt/{main.gjf.tmpl, run.py.tmpl}
  assets/parse_g16_out/parse.py.tmpl
```

- 依存は `uuid`/`chrono`/`toml`/`minijinja`/std のみ(minijinja=純 Rust・C/Python 非依存・
  pyo3 非依存。libpython 非リンク契約に非抵触)。`jm --no-default-features` でクリーンビルド必須。
- JobTemplate/FlowRecipe/`base_preamble()` は**純粋関数**。I/O・コピー・rollback・
  input_coordinate 取り込みは `cmd_new` 側。
- 公開 API 追加は `recipes` モジュールのみ。`render_batch_bash`/PyO3/`.pyi`/`flow.rs` cwd
  契約は不変。

### 4.1 共有ベースプリアンブル `base_preamble()`(minijinja)

上流 `gaussian-batch` の `_base.bash.j2` のシェル部を `src/recipes/assets/_base.bash.j2` として
embed(`include_str!`)し **minijinja** で描画する(`#SBATCH` 1–12 行は SbatchCmd 領域なので
template から除外)。Rust 1:1 手移植は採らない(上流が Jinja2 リテラルゆえ embed が DRY・
drift 耐性で優る。Tera ではなく minijinja を選ぶ理由=Jinja2 構文互換)。`run.py`/`parse.py`/
`main.gjf` は本 v1 では minijinja 化せず sentinel(`{{NAME}}` + `str::replace`)据置
(デリミタ衝突回避・YAGNI)。

```rust
pub struct PreambleOpts<'a> {
    pub conda_env: &'a str,     // 既定 "analysis"
    pub module_block: &'a str,  // {% block modules %} 相当(JobTemplate 供給)
    pub body_block: &'a str,    // {% block body %} 相当(= "python scripts/run.py" 等、bare)
    pub pixi_manifest: &'a str, // 空=pixi hook 省略
}
pub fn base_preamble(o: &PreambleOpts<'_>) -> String;
```

固定構造(`_base.bash.j2` と同順。minijinja template が出力する契約):
`set -euo pipefail` → 継承 conda スタック全消去(`unset -f conda` + `CONDA_*` ループ。
**学習スキル pixi-conda-stack-reset と同一・固定文字列・param 化しない** → template 内では
`{% raw %}…{% endraw %}` で囲いバイト同値を保証)→
`source "$(conda info --base)/etc/profile.d/conda.sh"` → `. /usr/share/Modules/init/bash` →
`{% block modules %}` → `conda activate {{ conda_env }}` →
`{% if pixi_manifest %}{pixi hook}{% endif %}` → `{% block body %}` →
`echo "JOB DONE"` → `exit 0`。

- `module_block`:`g16_opt` → `module restore {{ module_profile }} -f`(既定 `gaussian_A`)。
  `parse_g16_out` → `module restore default -f`。
- bash は改行/空白に敏感 → minijinja の whitespace control(`{%- -%}`)を明示調整。
  §10 に **`_base.bash.j2` バイト同値回帰**(特に conda-reset 区間 vs 学習スキル
  pixi-conda-stack-reset、余分な空行/インデント無し)を必須テストとして残す。
- `blank` には適用しない(§8)。

### 4.2 launcher / scratch_root / g16_cmd — recipe param のみ(案A・render 変更ゼロ)

`src/render/mod.rs:render_batch_bash` は plan.toml の全 param を
`export JM_PARAM_<NAME>=<quote_for_bash(value)>` として既に焼く。本案はこの既存経路を
そのまま使い、新規 env も render 時解決ロジックも導入しない。

| 値 | plan.toml 既定 | batch.bash 露出(既存 render) | `run.py` 消費 |
|---|---|---|---|
| `launcher` | `"srun"` | `export JM_PARAM_LAUNCHER='srun'` | `os.environ.get("JM_PARAM_LAUNCHER","")`。非空→argv 先頭付与、空→bare(実 `--no-srun` 相当) |
| `scratch_root` | `""` | `export JM_PARAM_SCRATCH_ROOT=''` | `os.environ.get("JM_PARAM_SCRATCH_ROOT") or "<job_dir>/.scratch"` |
| `g16_cmd` | `"g16"` | `export JM_PARAM_G16_CMD='g16'` | `os.environ.get("JM_PARAM_G16_CMD","g16")` |

ネット効果:`plan.toml` を編集 → `jm render` 再実行で `JM_PARAM_*` のみ更新、
sidecar(`run.py`/`parse.py`/`*.bash`)は不変(再 scaffold 不要)。

> **v2 拡張点(本 spec 非対象・rev.7 との分岐点)**:クラスタ既定の一元化が必要になったら
> D2 `CommonConfig.launcher: Option<String>` + `DirectoryConfig.scratch_root: Option<PathBuf>`
> (両 `#[serde(default)]`)を **1 coordinated PR** で追加し、partition と同型の read 時解決
> (優先順位:plan param 非空 > common > ハードコード `srun`)に拡張する。v1 の plan param
> 経路はその下位互換として残る。これを v1 から外すことで、本案 v1 は **クロスリポ協調PR・
> Cargo rev bump・`synth_empty_common` 追従・A1/D2 リスクをすべて回避**し、純粋に job-manager
> 内で land/test できる。
>
> **v2 拡張点 TODO(ユーザ指示で明示保持・v1 未実装)**:**per-task CLI + 各タスク
> `common.toml`** の足場化。各 JobTemplate の `# REPLACE_ME` を、当該タスク専用
> `common.toml`(クラスタ不変値 + per-step 設定)を引数に取る
> `python -m gaussian_compute_runtime <step> --config <per-task common.toml> --uuid <uuid>`
> 形へ昇格できるよう v2 で per-task CLI 規約と per-task `common.toml` スキーマを定義する。
> これは上の D2 一元化と同じ v2 で扱う(参照 `run-g16` の `--config`/`--uuid` 形に対応)。
> **v1 は `# REPLACE_ME` sentinel + R3' の自己完結スクリプトのみで、CLI/common 足場は
> 作らない**(YAGNI;ただし TODO として spec に残置)。

### 4.3 型(Job 層 / Flow 層)

```rust
pub enum RecipeParamType { Str, Int, Float, Bool, Path }
pub struct RecipeParam { name, ty, default, help }   // すべて &'static str / enum

pub struct JobArtifacts {
    pub program: String,                  // 論理分類 "g16"/"python"(jm ls --program 用)
    pub body: String,                     // flow.toml jobs.<JobId>.body。R3': "bash scripts/<JobId>.bash"(cd 無し。job dir は run.py/parse.py の絶対 JOB_DIR 定数で解決)
    pub time_limit: Option<String>,
    pub plan_params: BTreeMap<String, toml::Value>,
    pub sidecars: Vec<GeneratedFile>,     // scripts/<JobId>.bash + scripts/run.py|parse.py + input/main.gjf
}
pub struct GeneratedFile { relpath: PathBuf, contents: String, unix_mode: Option<u32> }
pub struct JobCtx<'a> { job_id, params, inputs(相対 "../<producer>/<relpath>"), uuid, created_at }

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

**`flow::assemble(recipe, raw_params, tags, uuid, created_at, abs_flow_dir)`**:
nodes 解決 → `--param` 分配/型検証 → wiring 相対パス解決 → 各 `instantiate()` →
flow.toml(`partition="REPLACE_ME"` + time_limit、`edges()`→parents)/
plan.toml(`plan_params`)組立(**flow JobId 集合 == plan キー集合**)→ sidecars+toml を返す。
`cmd_new` が input_coordinate コピー・原子書込・rollback を担う。

## 5. flow パス解決 — R3'(cwd 非依存)+ scratch ステージング

### 5.1 R3'(scaffold 時に絶対 job dir を run.py/parse.py 定数へ焼く・cwd 非依存)

sbatch は script を spool コピー実行 → 実行中 `$0`/`pwd` 不定、job cwd=`SLURM_SUBMIT_DIR`
(非決定的)。**参照 `run-g16` はこの問題を `cd` ではなく「絶対パスを引数で渡し
スクリプト内で絶対解決し cwd を一切読まない」設計で回避している**(参照 batch.bash の
body は `python -m gaussian_compute_runtime run-g16 --config "<abs common.toml>"
--uuid "<uuid>"` の 1 行のみ・`cd` 皆無 → スナップショット
`gaussian-batch .../snapshots/g16_root_step.bash.snap` で確認済)。本案 v1 は gem
common.toml を持たない(写経・自己完結)が、**同じ cwd 非依存性**を取り込む:

`jm new` は scaffold 時に `<root>/<uuid>/<JobId>` を確定的に知る → 生成
`scripts/run.py`/`parse.py` の冒頭へ絶対 `JOB_DIR` 定数を sentinel swap-in
(`{{JOB_DIR}}` + `str::replace`、§4.1 と同機構。値=`abs_flow_dir.join(<JobId>)`、
§4.3 `assemble(..., abs_flow_dir)` が既に保持)で焼き込む:

```python
# scripts/run.py 冒頭(scaffold が絶対値を swap-in)
JOB_DIR = "<root>/<uuid>/opt"   # cwd ではなくこの絶対定数で input/ output/ を解決
```

flow.toml `body` は `cd` を **持たず** 薄起動子のみ:

```toml
[jobs.opt]
program = "g16"     # 論理分類(jm ls --program g16 用)。実行は body→bash→run.py
body = """bash scripts/opt.bash
"""
```

重い処理は編集可能 `scripts/opt.bash`(= §4.1 プリアンブル + `python scripts/run.py`)。
R3' の役割は「`run.py`/`parse.py` が `os.getcwd()` ではなく焼込み `JOB_DIR` 絶対定数
から `input/`・`output/`(parse は相対 wiring 入力)を解決し、**SLURM の cwd 非決定性に
一切依存しない**(=参照 `run-g16` と同じ性質)」こと。**§4.2 のネット効果不変**:`JOB_DIR` は param 非依存の
scaffold 一回限り定数なので、plan param 編集 → `jm render` 再実行で sidecar は不変
(再 scaffold 不要)のまま。**R3' 不変条件**:焼込 `JOB_DIR` は**必ず絶対**でなければならない(相対だと実行時に
SLURM の非決定 cwd で再解決され R3' が無効化する)。`--root` は相対指定され得るので
`cmd_new` が `assemble` 呼出前に flow_dir を `std::path::absolute`(symlink 非解決=
login↔compute マウント安定)で絶対化する。既知制約:flow dir 移動 / login↔compute
マウント差で焼込絶対パス破綻(R3 の body cd と同等の制約。脆さの所在を body→script に
移しただけで増えない)→ UUID dir 不動 + 安定 root 運用で回避、将来
`jm render --rebase-paths` は別 spec。

**2026-05-20 改訂(R3' + (b) per-job chdir):** R3' は scaffold 側で
「スクリプト内部の cwd 非依存」を達成するが、**ランチャ起動そのもの**
(`bash scripts/<id>.bash` / `python scripts/run.py`)は相対パスのため、SLURM
ジョブの cwd が `<flow_dir>/<job_id>/` でないと起動段階で `No such file or
directory` で失敗する(PR #27 review H1 / issue #29)。これは scaffold だけでは
塞げない gap で、**submit 側の手当て**が必要。

恒久解 **(b)**:`FlowRunner::submit` で per-job
`SbatchCmd.chdir = Some(<flow_dir>/<job_id>/)` を注入(`src/runner/flow.rs`)。
A1 既存 public field の使用のみで A1 immutable 非抵触。SLURM が `--chdir` で
job cwd を job_dir に固定 → 相対 `scripts/<id>.bash` が解決される。R3' の
内部 cwd 非依存性は不変(焼込 `JOB_DIR` 絶対定数が `os.getcwd()` を一切読まない
性質を維持)、したがって R3' は「scaffold 側保険」として残存 — submit cwd が
何らかの理由で job_dir 外でも内部 I/O は破綻しない二段防御になる。

**core 変更1点**:`flow.rs` の submit-cwd 規約が「chdir=None 据置」から
「per-job chdir=<flow_dir>/<job_id>/」へ変わる。`render_batch_bash` 公開
シグネチャ/PyO3/`.pyi` は不変。`blank`/user 既存 flow も同 cwd に乗る
(`blank` の `echo hello` 等は無影響、cwd に依存するユーザ body のみ意識)。
README/`docs/recipes.md` の v1 caveat に「全 flow の job cwd は
`<flow_dir>/<job_id>/`」と明記すること。

**(a) との関係**:PR #27 暫定 `1124c7f`(body・body_block を絶対パス化)は
本 (b) で **完全 revoke**。recipes 層は薄相対起動子 `bash scripts/<id>.bash` /
`python scripts/run.py` に戻る。回帰ガードは Rust 単体・統合の assert を反転
(相対 form を assert、絶対 form を負 assert)+ Python smoke の cwd 非依存
ガード(`cwd != job_dir` で絶対パス起動)を残置(R3' 内部不変の独立検証)。

### 5.2 scratch ステージング(永続 job dir ↔ ノードローカル scratch)

実 `run_g16` の核心。`scripts/run.py`(§7、純 stdlib)が実行:

1. **永続 = 自 job dir**(R3' 焼込 `JOB_DIR=<root>/<uuid>/<JobId>/` 絶対定数。cwd 非依存)。
   `input/`・`output/` は `os.path.join(JOB_DIR, ...)` で絶対化。
2. **scratch = `<scratch_root>/<JM_FLOW_UUID>/<JM_JOB_ID>/`**(`scratch_root` =
   `JM_PARAM_SCRATCH_ROOT` or `<job_dir>/.scratch`)。
3. `prepare_inputs`:`input/` を scratch へ `shutil.copytree(dirs_exist_ok=True)`。
4. `argv = ([launcher] if launcher else []) + [g16, "main.gjf", "main.out"]`;
   `subprocess.run(argv, cwd=scratch)` — **g16 を cwd=scratch で実行**(`.chk`/`.rwf`/scratch が
   ノードローカル)。`srun`/`g16` PATH 不在(`FileNotFoundError`)→ `error: failed to launch
   <argv0>` を stderr + `rc=2`(**黙って 0 を返さない**:afterok の parse が空 .out を成功
   扱いするのを防ぐ)。
5. **`finally:` copy_results**:scratch の `main.out`/`main.chk`/`*.log` 等を job dir
   `output/` へ常時回収(g16 失敗でも部分 `.out` を retrieve)。
6. exit 優先順位:**g16 非ゼロ rc 最優先** → 次に copy 失敗で 3 → 全成功 0。順序不変
   `prepare→g16(cwd=scratch)→finally copy`。

実 `run_g16` と同層で **`srun` は g16 subprocess のみ**に付け、bash body は
`python scripts/run.py`(bare、srun なし)。実 gem `_base.bash.j2` body が python
orchestrator を bare 起動するのと一致。

## 6. リファレンス整合マッピング(参照リポ ↔ job-manager)

| 参照リポ / 実体 | job-manager 表現 |
|---|---|
| `_base.bash.j2` 共有プリアンブル | `base_preamble()`(§4.1、`#SBATCH` 除く) |
| `{% block modules %}` | JobTemplate 供給 `module_block` |
| `python -m gaussian_compute_runtime run-g16`(bash は bare) | `scripts/<JobId>.bash` body = `python scripts/run.py`(bare) |
| `run_g16.py`:prepare→`[*launcher,g16,gjf,out]`(cwd=temp)→finally copy、exit g16>copy | `scripts/run.py`(純 stdlib 写経。§5.2/§7) |
| **パス解決 = cwd 非依存**:body は `--config "<abs common.toml>" --uuid "<uuid>"` を渡すのみ・**`cd` 皆無**、orchestrator が絶対 `<env.root>/<uuid>/...` を内部解決(snapshot で実証) | **R3'**:scaffold が `run.py`/`parse.py` 冒頭へ絶対 `JOB_DIR` 定数を焼込、body は `bash scripts/<JobId>.bash` のみ・**`cd` 無し**(§5.1)。gem common が無いだけで cwd 非依存性は参照と同一 |
| `srun` を g16 subprocess のみに(orchestrator は bare) | `run.py` が `JM_PARAM_LAUNCHER` で g16 subprocess を包む(§4.2) |
| `env.tmp_root`(scratch) | recipe param `scratch_root` → `JM_PARAM_SCRATCH_ROOT`(§4.2)。**v2 で D2 一元化** |
| `gaussian_cmd.command` | recipe param `g16_cmd`(既定 `g16`)→ `JM_PARAM_G16_CMD` |
| `parse_results.py`:cclib parse → `write_json(result.json)` | `scripts/parse.py`(cclib 写経 → `output/result.json`。§7) |
| `gjf_render.render`(`%nprocshared`/`%mem`/`%chk`/route/title/`chg mult`/`{x:.6f}`/extra。**`%rwf` 無し**) | `input/main.gjf` テンプレ(§7。`%rwf` 削除、nproc/mem は scaffold 既定 + run.py が SLURM 割当で上書き) |
| `[step.params] route/charge/multiplicity/extra_input` | `g16_opt.params()` → `plan.toml [jobs.opt]` |
| `#SBATCH --rsc p=:t=:c=:m=`(kudpc) | `[jobs.*.config] resource_spec` / `common.toml [slurm_default]` → 既存 `cmd.rsc=config.resource_spec`(§4.3。レシピ範囲外) |
| `common.toml`(クラスタ不変値一元) | v1: 非関与(plan param)。**v2 拡張点**:`common.toml launcher`/`[directories] scratch_root` |
| `parent derived/main.mol2`→child gjf | **非目標(多段 g16)**。`parse.py` の `derived/main.mol2` は TODO 拡張点 |
| status(post 権威) | job-manager Lifecycle/tick 権威。`run.py`/`parse.py` は exit code + `result.json` 成果物 |

## 7. v1 JobTemplate / FlowRecipe 詳細

### JobTemplate `g16_opt`

`params()`(plan.toml `[jobs.opt]`):

| name | type | default | help |
|---|---|---|---|
| `route` | str | `#p opt b3lyp/6-31g(d)` | Gaussian route 行 |
| `charge` | int | `0` | 全電荷 |
| `multiplicity` | int | `1` | スピン多重度 |
| `extra_input` | str | `` | charge/mult・geometry の後の追加入力 |
| `nproc` | int | `8` | scaffold 既定 `%nprocshared`(run.py が `$SLURM_CPUS_PER_TASK` で上書き) |
| `mem` | str | `8GB` | scaffold 既定 `%mem`(run.py が SLURM 割当で上書き) |
| `compound` | str | `REPLACE_ME-INCHIKEY` | InChIKey。gjf title + `[tags].compound` |
| `g16_cmd` | str | `g16` | Gaussian バイナリ → `JM_PARAM_G16_CMD` |
| `conda_env` | str | `analysis` | プリアンブル `conda activate <env>`(§4.1) |
| `module_profile` | str | `gaussian_A` | `module restore <profile> -f`(§4.1) |
| `pixi_manifest` | path | `` | 空=pixi hook 省略(§4.1) |
| `launcher` | str | `srun` | 空=bare(§4.2)。v1 は recipe param のみ |
| `scratch_root` | path | `` | 空=`<job_dir>/.scratch/` fallback(§4.2) |
| `input_coordinate` | path | `` | 分子座標(`.xyz`/`.mol2`)。`cmd_new` が `opt/input/` へコピー |

- `inputs()`=`[]`。`outputs()`=`[("gaussian_out","output/main.out")]`。`program`=`"g16"`
  (分類値;実行は body→`bash scripts/opt.bash`→`python scripts/run.py`)。
  `time_limit`=`"48:00:00"`。
- sidecars:
  - `scripts/opt.bash`(0755)= `base_preamble()`(`module_block`=`module restore
    {module_profile} -f`、`conda_env`、`body_block`=`python scripts/run.py`)。
  - **`scripts/run.py`**(0755、純 stdlib・`subprocess`/`shutil`/`os`/`sys` のみ。`run_g16` 写経):
    1. 冒頭定数 `JOB_DIR = "{{JOB_DIR}}"`(scaffold が `abs_flow_dir.join("opt")` を
       sentinel swap-in。R3'・cwd 非依存)。`job_dir=JOB_DIR`(`os.getcwd()` は使わない)。
       `task="main"`。`input/`・`output/` は `os.path.join(JOB_DIR, ...)` で絶対化。
       `g16=os.environ.get("JM_PARAM_G16_CMD","g16")`。
       `launcher=os.environ.get("JM_PARAM_LAUNCHER","")`。
       `scratch_root=os.environ.get("JM_PARAM_SCRATCH_ROOT") or os.path.join(job_dir,".scratch")`。
       `scratch=<scratch_root>/<JM_FLOW_UUID>/<JM_JOB_ID>`。
    2. `os.makedirs(scratch, exist_ok=True)`;`input/` を scratch へ
       `shutil.copytree(..., dirs_exist_ok=True)`(prepare_inputs)。
    3. (任意)`scratch/main.gjf` の `%nprocshared`/`%mem` を `$SLURM_CPUS_PER_TASK`/
       SLURM mem env で書換(env 無ければ scaffold 値据置)。
    4. `argv=([launcher] if launcher else [])+[g16,"main.gjf","main.out"]`;
       `rc=subprocess.run(argv, cwd=scratch).returncode`;`FileNotFoundError`→
       `error: failed to launch {argv[0]}` stderr + `rc=2`(黙って 0 禁止)。
    5. `finally:` `output/` へ `main.out`/`main.chk`/`*.log` 等を copy back
       (無くても続行、copy 例外は記録)。
    6. exit:`rc!=0` ならそれ、elif copy 失敗 `3`、else `0`。
    7. 末尾:`# REPLACE_ME: gem stack 導入済なら全体を
       python -m gaussian_compute_runtime run-g16 --config <abs gem toml> に差替可`。
  - `input/main.gjf`(gem `gjf_render` 形式整合、`{{}}` 差込):
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
    **`%rwf` 行なし**(実 `gjf_render.render` と一致)。`input_coordinate` 未指定 →
    `{{geometry_block}}`=`<GEOMETRY: REPLACE_ME — Elem x y z を1原子1行>`;`.xyz` →
    純 Rust パース(行1=原子数/行2=コメント/以降 `Elem x y z`)で
    `{sym} {x:.6f} {y:.6f} {z:.6f}` 差込 + 原本を `input/<basename>` 保存;`.mol2` 等 →
    コピーのみ + sentinel(OpenBabel 非持込)。
- body(flow.toml、R3'):`bash scripts/opt.bash`(**cd 無し**。job dir は run.py の
  焼込 `JOB_DIR` 絶対定数で解決)。

### JobTemplate `parse_g16_out`

- `params()`=`[ conda_env(既定 analysis), pixi_manifest(既定空) ]`(parse は軽量 post =
  bare 起動、srun ラップ対象 subprocess も巨大 scratch も無 → `launcher`/`scratch_root`
  param 不要)。`inputs()`=`["gaussian_out"]`。
  `outputs()`=`[("result_json","output/result.json")]`。`program`=`"python"`。
  `time_limit`=`"01:00:00"`。
- sidecars:
  - `scripts/parse.bash`(0755)= `base_preamble()`(`module_block`=`module restore
    default -f`、`conda_env`、`body_block`=`python scripts/parse.py`)。
  - **`scripts/parse.py`**(0755。`parse_results` 写経):
    - 冒頭定数 `JOB_DIR = "{{JOB_DIR}}"`(scaffold が `abs_flow_dir.join("parse")` を
      sentinel swap-in。R3'・cwd 非依存)。
    - `gaussian_out = os.path.normpath(os.path.join(JOB_DIR, "{{inputs.gaussian_out}}"))`
      (wiring 相対 `../opt/output/main.out` を `JOB_DIR` 基準で絶対化 → `<...>/opt/output/main.out`)。
      `cclib.io.ccread(gaussian_out)`。`import cclib` 失敗 →
      `error: cclib not importable` + **exit 2**。
    - 検証:(a) パース不可→1、(b) 正常終了マーカ無→1、(c) opt 収束 False→1、
      (d) 最終エネルギー非有限→1。
    - **curated `os.path.join(JOB_DIR,"output","result.json")` を atomic write**
      (`{converged, scf_energy, n_atoms, source, schema:"jm-recipe/1"}`)。
      書込失敗→3、全 OK→0。stdout に要約。
    - `# TODO(jm recipe): write derived/main.mol2`(多段 g16 連鎖用拡張点・v1 非対象)。
    - `# REPLACE_ME: gem stack 導入済なら
      python -m gaussian_compute_runtime parse-results --config <abs> に差替可`。
    - status は Lifecycle/tick 権威。本スクリプトは exit code + `result.json` 成果物。
  - `parse.py` は `cclib` + stdlib のみ(Group C/D ライブラリ無し)。
- body(flow.toml、R3'):`bash scripts/parse.bash`(**cd 無し**。入出力は parse.py の
  焼込 `JOB_DIR` 絶対定数で解決)。

### FlowRecipe `g16-opt-parse`

`nodes()`=`[("opt","g16_opt"),("parse","parse_g16_out")]`、
`edges()`=`[("opt","parse","afterok")]`、
`wiring()`=`[("parse","gaussian_out","opt","gaussian_out")]`
(→ `parse` の `{{inputs.gaussian_out}}`=`../opt/output/main.out`)。

生成 `flow.toml`:
```toml
uuid="<uuid>"
created_at="<rfc3339>"
[tags]
recipe="g16-opt-parse"
compound="<opt.compound>"
[jobs.opt]
program="g16"
body="""bash scripts/opt.bash
"""
[jobs.opt.config]
partition="REPLACE_ME"
time_limit="48:00:00"
[jobs.parse]
program="python"
body="""bash scripts/parse.bash
"""
[[jobs.parse.parents]]
from="opt"
kind="afterok"
[jobs.parse.config]
partition="REPLACE_ME"
time_limit="01:00:00"
```

生成 `plan.toml`(flow JobId 集合 == plan キー集合 = {opt,parse}):
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
launcher="srun"
scratch_root=""
[jobs.parse]
conda_env="analysis"
pixi_manifest=""
```

任意:kudpc 実運用では `<root>/common.toml [slurm_default] partition="gr10641a"` +
`resource_spec`(`p=1:t=56:c=56:m=56G`)を設定 → 既存 `cmd.rsc` 配線で sbatch へ。

## 8. `blank` FlowRecipe(後方互換)

既存 `build_flow_template`/`build_plan_template`(`src/bin/jm.rs`)を `flows/blank.rs` へ
移設。**非分解**で直接出力、**既存 `jm new` 出力とバイト同値**。`jm new`≡`jm new blank`。
サイドカー/プリアンブル/run.py/R3' 焼込定数すべて無し(既存挙動完全維持)。

## 9. エラーハンドリング

| 状況 | 挙動 |
|---|---|
| 未知 FlowRecipe / JobId / param / 型不整合 / `--param` 構文不正 | `bail!` + 候補列挙 |
| `input_coordinate` src 不在 | コピー前検証で `bail!` |
| `flow_dir` 既存(UUID 衝突) | `bail!`(リトライしない) |
| sidecar/コピー書込失敗 | `flow_dir` を `remove_dir_all` 巻戻し後 `?` 伝播 |
| `--list`/`--describe` | scaffold せず exit 0 |
| (実行時)`JM_PARAM_SCRATCH_ROOT` 空 | run.py が `<job_dir>/.scratch/` fallback |
| (実行時)`srun`/g16 PATH 不在 | run.py `failed to launch …` stderr + 非ゼロ(黙って 0 禁止) |
| (実行時)g16 非ゼロ | `finally` で copy 後その rc を返す(g16>copy) |
| (実行時)copy 失敗(g16 成功時) | run.py exit 3 |
| (実行時)`cclib` 不在 | parse.py exit 2 |

## 10. テスト

### ユニット(`src/recipes/**`)

- `base_preamble()`(minijinja):`_base.bash.j2` 同順(conda 全消去固定文字列が
  `{% raw %}` でバイト同値=学習スキル pixi-conda-stack-reset 一致・`{% block modules %}`
  位置・`conda activate <env>`・末尾 `echo "JOB DONE"`/`exit 0`)、`pixi_manifest` 空/非空で
  hook 行有無、`#SBATCH` 非含有、minijinja whitespace-control 回帰(余分な空行/インデント無)。
- `g16_opt.instantiate`:`program="g16"`、body=`bash scripts/opt.bash`(**cd を含まない**
  ことを assert)、`scripts/run.py`(0755)冒頭に `JOB_DIR = "<abs>"`(= `JobCtx`/
  `assemble` の `abs_flow_dir.join("opt")`、`{{JOB_DIR}}` sentinel 残存無)・**`os.getcwd()`
  非使用**(静的 grep)、`scripts/opt.bash`(0755、`module restore gaussian_A -f`+
  `conda activate analysis`+`python scripts/run.py`、**srun 非含有**)、`scripts/run.py` に
  prepare/`subprocess.run(...,cwd=scratch)`/`finally` copy/exit-precedence/`failed to
  launch`/`# REPLACE_ME`、`run.py` が `cclib`/Group D を import しない(静的 grep)、
  `input/main.gjf` に **`%rwf` 無**・`{{}}` 残存無・`.xyz` で `{x:.6f}` 差込
  (純 Rust xyz パーサ単体:原子数/コメント/不正形式 Err)、
  `outputs()`=`[("gaussian_out","output/main.out")]`。
- `parse_g16_out.instantiate`:body=`bash scripts/parse.bash`(**cd を含まない**ことを
  assert)、`scripts/parse.py`(0755)冒頭に `JOB_DIR = "<abs>"`(= `abs_flow_dir.join
  ("parse")`、`{{JOB_DIR}}` 残存無)・入力を `os.path.join(JOB_DIR,"../opt/output/main.out")`
  で絶対化・`os.getcwd()` 非使用、cclib・`output/result.json` atomic write・exit 0/1/2/3・
  `# TODO derived/main.mol2`・`# REPLACE_ME`、`outputs()`=`[("result_json","output/result.json")]`。
- `assemble(g16-opt-parse)`:flow JobId 集合==plan キー集合=={opt,parse}、
  `parse.parents[0]={from:opt,kind:afterok}`、両 `partition=="REPLACE_ME"`、time 48h/1h、
  wiring 相対 `../opt/output/main.out`、opt body=`bash scripts/opt.bash`(cd 無し)で
  `opt/scripts/run.py` 内 `JOB_DIR` == `flow_dir.join("opt")` 絶対値。
- パラメータ宛先・registry 整合 lint・`blank` バイト同値(プリアンブル/run.py/`$JM_*`/
  `JOB_DIR` 焼込定数/`cd` を含まないことも assert)。

### run.py アルゴリズム回帰(Python smoke `python/tests`、g16/srun を stub)

- 順序 `prepare→g16(cwd=scratch)→finally copy` を call-order で固定。
- g16 非ゼロ rc がそのまま伝播し copy も走る(g16>copy)。prepare 失敗で g16/copy
  スキップ + 非ゼロ。copy 失敗 + g16 成功 → exit 3。`subprocess.run` が
  `FileNotFoundError` → 非ゼロ + `failed to launch` に launcher 名。
- `JM_PARAM_SCRATCH_ROOT` 空 → `<job_dir>/.scratch/` 使用。`JM_PARAM_LAUNCHER` 空 →
  argv 先頭に srun 無し、非空 → 付与。
- `parse.py`:正常 `.out` フィクスチャ→exit 0 + `output/result.json` 生成・スキーマ、
  未収束/切断→1、`cclib` 不在→2。

### 統合(`tests/integration_new_recipes.rs`)

- `--list`/`--describe` exit 0・scaffold 無し。
- `jm new g16-opt-parse --param opt.charge=1`:全ファイル生成、gjf に `1 1`・`%rwf` 無し、
  `opt/scripts/opt.bash` に conda 全消去 + `module restore gaussian_A -f` +
  `python scripts/run.py`、opt body=`bash scripts/opt.bash`(cd 無し)で
  `opt/scripts/run.py` の `JOB_DIR` が実 tempdir 絶対パス。
- `--param opt.input_coordinate=<tmp.xyz>`:`opt/input/<basename>` コピー + gjf 座標差込。
  src 不在で非 0 + 巻戻し。
- `jm doctor <uuid>` exit 0。`jm new g16-opt-parse`→`jm render <uuid>` exit 0、
  batch.bash に `export JM_PARAM_LAUNCHER='srun'`・`export JM_PARAM_SCRATCH_ROOT=''`。
- 後方互換:`jm new`≡`jm new blank`≡既存期待値。

`MockExecutor`/`InMemoryQuerier`、live SLURM 不要(CLAUDE.md 準拠)。

## 11. CLAUDE.md 準拠 / 上流変更

- **`### Upstream modification policy` 準拠**:A1 `SlurmJobConfig` 不変。**v1 は D2 変更も
  不要**(launcher/scratch_root を recipe param に留めるため)。D2 一元化は §4.2 の v2
  拡張点へ切り出し ── これが rev.7 との最大の意図的分岐点(rev.7 は D2 1 coordinated PR を
  v1 に同梱)。本案は v1 のクロスリポ結合・Cargo rev bump・`synth_empty_common` 追従を
  ゼロにする。
- 生成物は user-authored 入力の初回 bootstrap のみ。runtime は `.jm/` と
  `<JobId>/output/`・`<JobId>/.scratch/`(後者はジョブ自身の作業領域、user dir 内)。
- `jm --no-default-features` → `src/recipes/` pyo3 非依存。`base_preamble()` は embedded
  `_base.bash.j2` を minijinja(純 Rust)で描画、xyz パーサは純 Rust。minijinja 追加は
  通常 crate 依存(libpython 非リンク契約に非抵触)。`scripts/run.py`/`parse.py` は
  job-manager 所有の生成物で `subprocess`/`shutil`/`os`(parse は `cclib` のみ)。
  Group B/C/D を import しない。
- 原子書込(PID サフィックス tmp+rename)を全生成ファイル。`*.bash`/`*.py` は 0755。
- Out of scope(DSL/sweep/per-flow common/TUI/リモートレジストリ/OpenMM/JobTemplate
  直接 scaffold/Group B-C-D 依存/自動幾何取得/多段 g16/`--rsc` SBATCH ヘッダ)非抵触。
- **公開 API/PyO3/`.pyi`/`render_batch_bash` シグネチャは不変**。
  **`flow.rs` cwd 契約は (b) で更新**:per-job
  `SbatchCmd.chdir = Some(<flow_dir>/<job_id>/)` を注入 — A1 既存 public field
  使用のみ(A1 immutable 非抵触)、`render_batch_bash` も `cd` 注入しない。
  公開追加は `recipes` モジュールのみ(v1 は D2 field 追加すら無し)。
- gem 意味論 + `run_g16` アルゴリズム + `_base.bash.j2` プリアンブルを写すがスキーマは
  job-manager。status は Lifecycle/tick 権威。
- Conventional Commits / per-task commit / stacked PR。

## 12. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| テンプレ層 | 二層 | 単一 Recipe | 再利用、先行例(nf-core/Snakemake/atomate/gem)全て二層 |
| 共通プリアンブル | 上流 `_base.bash.j2` embed + minijinja(`base_preamble()` シグネチャ不変) | Rust 1:1 手移植 / 最小 / 全 param | gem 実績、上流 DRY・drift 耐性、サイト名のみ可変 |
| テンプレ展開エンジン | minijinja を §4.1 プリアンブルに限定採用 | cookiecutter/Baker(Python)・kickstart(CLI)・Tera・全面 minijinja | Python/CLI 系は libpython 非リンク契約に抵触。Tera は Jinja2 非互換。`run.py`/`parse.py` はほぼリテラル → デリミタ衝突回避で sentinel 据置(YAGNI) |
| 実ジョブ表現 | 自己完結 純 stdlib `run.py`/`parse.py`(写経)+ `# REPLACE_ME` swap-in | bash 直書き / `gaussian_compute_runtime` 委譲 / スケルトンのみ | 忠実再現 + 自己完結、Group B/C/D 不要、gem 導入済なら任意差替 |
| scratch | run.py が prepare→g16(cwd=scratch)→finally copy | flow dir 直実行 | 巨大 `.rwf` を lustre に置かない、KUDPC tmp 規約、失敗時 `.out` 回収 |
| srun 層 | run.py が g16 subprocess のみ包む(bash は bare) | bash で `$JM_* python …` | 実 run_g16 と同層、orchestrator は bare(gem 一致) |
| **launcher/scratch_root 格納** | **v1: recipe param のみ(render/上流変更ゼロ)。v2: D2 一元化** | v1 から D2 1 coordinated PR(=rev.7) | **rev.7 との分岐点**。v1 を job-manager 内完結にしクロスリポ結合・Cargo rev bump・A1/D2 リスクを回避。一元化は実証後の clean な v2 |
| path 解決 | **R3' + (b) per-job chdir**(scaffold は run.py/parse.py 冒頭へ絶対 `JOB_DIR` 定数で内部 cwd 非依存、submit は `SbatchCmd.chdir=<flow_dir>/<job_id>/` を注入し job cwd を固定。body は cd 無し・薄相対起動子) | R3(body 絶対 cd)/R1/R2/R4/(a) 絶対 body | scaffold 側 R3' は不変(内部 cwd 非依存保険)+ submit 側 (b) で起動 cwd を確定 → 参照 `run-g16` の cwd 非依存性に二段防御で到達。A1 不変、`flow.rs` cwd 契約のみ 1 行更新。(a) 絶対 body は PR #27 暫定で issue #29 にて (b) へ恒久移行 |
| gjf | `gjf_render` 整合(`%rwf` 削除・`{x:.6f}`・nproc/mem は scaffold 既定+run.py が SLURM 上書き) | `%rwf` 付き独立 param | 実 renderer 一致 |
| 幾何入力 | `input_coordinate` scaffold コピー(.xyz 純 Rust) | 自動取得/OpenBabel | `jm new` 化学非依存 |
| parse 出力 | curated `output/result.json`(自前 cclib) | exit-code のみ | 実 parse_results 一致(成果物)、自己完結 |
| 多段 g16 連鎖 | v1 非対象(`derived/main.mol2` は TODO) | v1 で対応 | opt→parse は不要、YAGNI |
| status 権威 | Lifecycle/tick | gem status 再実装 | 二重実装回避 |
| `blank` | 据置(非分解) | 分解 | バイト同値 |

## 13. rev.7 との差分サマリ(マージ時の指針)

| 項目 | 本案A(2026-05-18) | rev.7(2026-05-16) |
|---|---|---|
| launcher/scratch_root | **v1: recipe param のみ・`JM_PARAM_*`・render/上流変更ゼロ**。D2 一元化は v2 拡張点 | v1 に D2 1 coordinated PR(`CommonConfig.launcher`/`DirectoryConfig.scratch_root`)同梱・read 時解決 |
| run.py の env キー | `JM_PARAM_LAUNCHER`/`JM_PARAM_SCRATCH_ROOT`/`JM_PARAM_G16_CMD`(既存 render 経路) | `JM_LAUNCHER`/`JM_SCRATCH_ROOT`(render 時解決した単一値を新規 export) |
| v1 の上流結合 | **ゼロ**(job-manager 内完結) | D2 リポ PR + 本リポ Cargo rev bump + `synth_empty_common` 追従 |
| path 解決(cwd) | **R3' + (b) per-job chdir**(2026-05-20):scaffold が run.py/parse.py 冒頭へ絶対 `JOB_DIR` 定数を焼込、flow.toml body は `bash scripts/<JobId>.bash` のみ・**`cd` 無し**、`FlowRunner::submit` で per-job `SbatchCmd.chdir=<flow_dir>/<job_id>/` を注入し SLURM 起動 cwd を固定。**flow.rs cwd 契約 1 行更新** | (廃案)**R3**:flow.toml body 先頭に絶対 `cd "<root>/<uuid>/<JobId>"` |
| v2 拡張点 TODO | **per-task CLI + 各タスク `common.toml`**(`python -m gaussian_compute_runtime <step> --config <per-task common.toml> --uuid` 形へ任意 swap-in できる足場)を D2 一元化(§4.2)と併せ v2 で実装。v1 は `# REPLACE_ME` sentinel のみで足場化し**実装しない** | rev.7 は per-task CLI/common 分離を明示せず(D2 一元化に内包) |
| 二層レシピ / `g16_opt`+`parse_g16_out` / 自己完結 run.py・parse.py / minijinja プリアンブル / `%rwf` 削除 / `# REPLACE_ME` swap-in / `blank` バイト同値 | 同一(両案共通) | 同一 |
| Plan 構成 | 単一 Plan(recipes core + CLI/render。D2 Plan 不要) | Plan A(D2)/B(core)/C(CLI) の 3 本 |

**2026-05-20 改訂(rev.7 reject 確定):** ユーザ verbatim
*"I rejected rev.7 and redo by #27 and #28. so base is #29 and other docs in
#27 and #28."* → rev.7 系統は廃案。本案A 単独正本。「マージ指針」は不要。

**v2 への引継ぎ(本 spec が将来の v2 spec の前段に残すもの):**
- §4.2 / §11 の D2 一元化(`common.toml launcher` / `[directories] scratch_root` +
  read 時解決)は v2 拡張点として温存。
- **per-task CLI + 各タスク `common.toml`** = (c) `python -m gaussian_compute_runtime
  <step> --config <per-task common.toml> --uuid` 形への昇格。v2 の主目玉。
- (b) per-job chdir は **v1 で land 済**(本 spec §5.1 / §6 / §11 / §12 / Goal #7
  / issue #29)。(c) への移行時に (b) を続行するか撤回するかは v2 で判断
  (case 1: (c) のみで launcher が module 起動になるなら (b) 不要、case 2: 互換性の
  ため (b) 据置)。
