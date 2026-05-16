# `jm new <recipe>` — ドメイン固有レシピ — design (rev.2)

**Date:** 2026-05-16
**Status:** Draft (rev.2 — established-ecosystem 整合反映。awaiting user review)
**Reference:**
- 既存エコシステム(整合対象。`git@github.com:miyake-ken/gaussian-experiment-manager.git` "collapsed" + `github.com/miyake-ken/GAUSSIAN_repo/examples`):
  - `experiment.toml` / `common.toml` スキーマ、`[step.params]`(`route`/`charge`/`multiplicity`/`extra_input`)
  - **main batch(`gaussian-run-g16`)→ afterok → post batch(`gaussian-parse-results`, cclib)** の 2-batch チェーン
  - `.gjf` 形式(`%rwf`/`%nprocshared`/`%mem`/`%chk` → route → title → charge mult → geometry → connectivity → extra_input)
  - `<env.root>/<uuid>/{input,output,derived}/` レイアウト、`task_basename`(既定 `main`)、InChIKey compound
  - `[slurm]` vs `[slurm.post]`(post 用 per-field override)
  - **重要**: 当該エコシステム全体に **OpenMM は一切登場しない**(`grep openmm` ヒット 0)。「結果検証」の確立実装は cclib による .out パース + 収束/エネルギー検証
- `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`(既存 `jm new` の sentinel 哲学)
- `src/bin/jm.rs`(`Cmd::New` / `cmd_new` / `build_flow_template` / `build_plan_template` / `atomic_write_str`)
- `src/render/mod.rs`(`render_batch_bash` — 公開 API + Python エクスポート, prod caller は `src/runner/flow.rs:245` のみ)
- `src/runner/flow.rs:262-289`(submit 経路の `SbatchCmd` 構築。`cmd.chdir` 未設定)
- 上流 A1 `slurm-async-runner2/src/sbatch/cmd.rs:133-134`(`SbatchCmd.env` → `--export=ALL,K=V,...`)
- CLAUDE.md(Out of scope / PyO3 境界 / `.jm/` レイアウト / `--no-default-features` 制約)

---

## 1. 問題設定

`jm new`(別 spec)は静的な 2-job `step1 → step2` 雛形を生成する汎用 scaffold。ユーザ要求は「g16 で構造最適化入力を作り、正常終了後(`afterok`)に Python で結果を検証する」ドメイン固有チェーンの scaffold。

rev.1 では検証を OpenMM と仮定したが、ユーザ提示の既存エコシステム(gem + GAUSSIAN_repo)を精査した結果、**「g16 opt 実行 → afterok → 結果検証」の確立実装は OpenMM ではなく cclib ベースの `gaussian-parse-results`**(.out をパースし収束/エネルギーを検証、終端 status を書く post バッチ)であった。OpenMM はエコシステム全体で未使用。ユーザ判断により本レシピは **parse-results 規約へ整合**(OpenMM は本 spec 非対象)。

`jm` レシピは、この確立済みドメイン規約(パラメータ語彙・main→post afterok・gjf 形式・input/output/derived レイアウト)を **job-manager の `flow.toml`/`plan.toml` で表現する橋渡し**として設計する。gem 自体は "collapsed"(畳まれた)Python 実装で、job-manager(Rust)が現行の orchestrator。レシピはコードではなく **編集可能な job-manager flow 一式**を生成する(gem に scaffold/new は存在せず、本機能は新規レイヤ)。

## 2. ゴール / 非ゴール

### Goals

1. `jm new` に**位置引数 `<recipe>`** を追加。`jm new`(無引数)= 組込 `blank` レシピ(既存 2-job 雛形、**後方互換**)。`jm new g16-opt-parse --param k=v` で domain レシピ生成。
2. レシピは **Rust 側の型付きレジストリ**(B パターン)。出力ツリーは構築時点で `jm doctor`-clean を保証(flow JobId 集合 == plan `[jobs.*]` キー集合、uuid == ディレクトリ名、親エッジ整合)。
3. domain レシピは **flow.toml / plan.toml に加えサイドカー**(`input/main.gjf`, `scripts/parse_results.py`)を生成。化学者が直接編集できる。レイアウトとファイル名は gem 規約準拠(`input/`・`output/`・`derived/`、`task_basename = main`)。
4. レシピごとに型付きパラメータ(名前 / 既定値 / ヘルプ)。`jm new <recipe> --param KEY=VALUE`(繰返し可)。`jm new --list` / `jm new <recipe> --describe`。
5. 実行中ジョブが自分の flow ディレクトリを解決できるよう、submit 経路で `SbatchCmd.env` に `JM_FLOW_DIR`(flow dir 絶対パス)を注入(§5, R4)。
6. v1 レシピ: `blank` + `g16-opt-parse` の 2 つ。レジストリは追加容易。
7. 書き込みは中途半端を残さない(既存 `jm new` の rollback 規約踏襲)。

### Non-goals

- `common.toml` の生成・変更(v1)。gem は `[slurm]`/`[slurm.post]`/`[env]`/`[gaussian_cmd]` を common に持つが、job-manager 側は per-job `[jobs.*.config]`(`SlurmJobConfig`)で表現し `partition = "REPLACE_ME"` sentinel(既存 `jm new` の deferred-common 踏襲)。
- **OpenMM**(rev.1 の誤前提。エコシステムに前例なし。ユーザ判断で除外)。
- experiment DSL / sweep 展開 / 親解決(CLAUDE.md "Out of scope"。利用者はトポロジを書かず名前付きレシピ + スカラパラメータのみ。gem の `[[sweep]]`/`parent_uuids` 連鎖は job-manager の責務外)。
- gem の `experiment.toml`/`common.toml` フォーマットそのものの採用(job-manager の契約は `flow.toml`/`plan.toml`。レシピは gem の**ドメイン意味論**を写すが、**スキーマは job-manager のもの**)。
- gem の `metadata.toml`/`status` ファイル再実装(job-manager は Lifecycle + `decide_transition` + `tick` が status の権威。parse ジョブは exit code で success/fail を表すだけ)。
- 対話的ウィザード / TUI / プロンプト、既存 flow の再生成・migration・answers-file、リモートレシピレジストリ。
- `render_batch_bash` 公開 API / PyO3 境界変更(R1 不採用, §5)、全ジョブ cwd 契約変更(R2 不採用, §5)。
- 分子幾何の自動取得(InChIKey → 構造 DB 参照は gem の上流責務。レシピは `input/main.gjf` に geometry sentinel を置く)。

## 3. CLI 形

```
jm --root <ROOT> new [<RECIPE>] [--param <KEY=VALUE>]... [--tag <KEY=VALUE>]... [--print-path]
jm --root <ROOT> new --list
jm --root <ROOT> new <RECIPE> --describe
```

| 引数 | 説明 |
|---|---|
| `<RECIPE>`(位置, 任意) | レジストリ内のレシピ名。省略時 `blank`。未知名は候補列挙付きエラー。 |
| `--param <KEY=VALUE>` | 任意回。レシピ定義パラメータを上書き。未知キー / 型不整合 / `=` 無しはエラー。 |
| `--tag <KEY=VALUE>` | 既存。`flow.toml [tags]` 反映。全レシピ共通。 |
| `--print-path` | 既存。stdout に `<root>/<uuid>` のみ。 |
| `--list` | レシピ名 + 1 行説明を列挙して終了。 |
| `--describe` | `<RECIPE>` のパラメータ(名前 / 型 / 既定値 / ヘルプ)を列挙して終了。 |

`Cmd::New` を `recipe: Option<String>` / `params: Vec<String>`(`--param`) / 既存 `tags` / `print_path` / `list: bool` / `describe: bool` へ拡張。`main()` 分岐を `cmd_new(&root, recipe.as_deref(), &params, &tags, print_path, list, describe)` に。

## 4. アーキテクチャ

### モジュール配置

```
src/recipes/
  mod.rs                 -- Recipe trait, RecipeParam, RecipeCtx, GeneratedFile,
                            registry(), find(), parse/validate params, --list/--describe 整形
  blank.rs               -- 既存 2-job step1->step2 を Recipe 化(build_flow/plan_template 移設)
  g16_opt_parse.rs       -- domain レシピ + asset テンプレート(include_str! or const)
  assets/
    g16_opt_parse/
      main.gjf.tmpl          -- Gaussian 入力テンプレート({{placeholder}}, gem 形式)
      parse_results.py.tmpl  -- cclib 検証スクリプト
```

- `src/recipes/` は **pyo3 非依存**(`uuid`/`chrono`/`toml`/std のみ)。`jm` は `--no-default-features` ビルド必須。
- レシピは **純粋**:`generate(ctx) -> Result<Vec<GeneratedFile>, RecipeError>`。I/O・rollback は `cmd_new`。
- `src/lib.rs` から `pub use recipes::{Recipe, registry, ...}`(**公開 API は追加のみ**。既存破壊なし)。

### 型

```rust
pub struct GeneratedFile {
    pub relpath: PathBuf,       // "flow.toml"/"plan.toml"/"input/main.gjf"/"scripts/parse_results.py"
    pub contents: String,
    pub unix_mode: Option<u32>, // 既定 None。parse_results.py は body 経由 `python` 実行のため exec ビット不要
}

pub enum RecipeParamType { Str, Int, Float, Bool }

pub struct RecipeParam {
    pub name: &'static str,
    pub ty: RecipeParamType,
    pub default: &'static str,
    pub help: &'static str,
}

pub struct RecipeCtx<'a> {
    pub uuid: &'a Uuid,
    pub created_at: &'a str,                       // RFC3339 UTC
    pub tags: &'a BTreeMap<String, String>,
    pub params: &'a BTreeMap<String, toml::Value>, // 既定 + --param 上書き後の検証済み値
}

pub trait Recipe: Send + Sync {
    fn name(&self) -> &'static str;
    fn summary(&self) -> &'static str;
    fn params(&self) -> &'static [RecipeParam];
    fn generate(&self, ctx: &RecipeCtx<'_>) -> Result<Vec<GeneratedFile>, RecipeError>;
}

pub fn registry() -> Vec<Box<dyn Recipe>>;       // [Blank, G16OptParse]
pub fn find(name: &str) -> Option<Box<dyn Recipe>>;
```

`RecipeError`(`thiserror`): `UnknownParam{name}` / `ParamTypeMismatch{name,expected,got}` / `Internal(String)`。CLI で `anyhow` に載せ替え、候補列挙付きメッセージ。

### 動作シーケンス(`cmd_new`)

1. `--list`: `registry()` を `name — summary` 出力で終了。
2. レシピ解決: `recipe.unwrap_or("blank")` → `find()`。`None` → bail(候補列挙)。
3. パラメータ: `recipe.params()` 既定値で `BTreeMap` を作り `--param` を型検証して上書き。未知キー / 型不整合 → bail。
4. `--describe`: 解決済みパラメータ表を出力して終了(scaffold しない)。
5. `--tag` パース(既存流用)。
6. `uuid = Uuid::now_v7()`; `resolver = PathResolver::new(root)`; `flow_dir = resolver.flow_dir(&uuid)`; 衝突確認(`exists()` なら bail、リトライしない)。
7. `created_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)`。
8. `recipe.generate(&ctx)?` → `Vec<GeneratedFile>`。レシピは内部で flow JobId 集合 == plan `[jobs.*]` キー集合を保証(doctor-clean by construction)。
9. `create_dir_all(&flow_dir)`。各 `GeneratedFile` の親(`input/`・`scripts/`)を `create_dir_all` し `atomic_write_str` で書く。`unix_mode` 指定時は rename 前 `chmod`(Unix-only)。
10. いずれか失敗時: 作成済み `flow_dir` を `remove_dir_all` で巻き戻して `?` 伝播。
11. 出力(既存形踏襲、asset 込み列挙):
    ```
    created flow <uuid> from recipe g16-opt-parse
      <root>/<uuid>/flow.toml
      <root>/<uuid>/plan.toml
      <root>/<uuid>/input/main.gjf
      <root>/<uuid>/scripts/parse_results.py
    next: edit input/main.gjf (geometry block), set a real partition +
          cluster env in flow.toml, then `jm --root <root> render <uuid>`
    ```
    `--print-path` 時は `<root>/<uuid>` の 1 行のみ。

## 5. flow パス解決(R4)— submit 経路 `cmd.env` 注入

### 制約(実コード確認済み)

- `render_batch_bash`(`src/render/mod.rs`)はジョブに `JM_FLOW_UUID`/`JM_JOB_ID`/`JM_AXIS_*`/`JM_PARAM_*` のみ export。flow ディレクトリパスも `cd` も注入しない。
- `src/runner/flow.rs:268` の `SbatchCmd` 構築で `cmd.chdir` 未設定 → ジョブ cwd は flow dir でない。`examples/full` も絶対ログパスで「保証 cwd 無し」が既存前提。
- `render_batch_bash` は `src/lib.rs:38` 公開 + `src/py_export/render.rs`/`mod.rs:103` で Python エクスポート。シグネチャ変更は公開 API + PyO3 境界破壊 + `.pyi` 再生成を伴う。

### 採用: R4(submit 経路で `SbatchCmd.env` に `JM_FLOW_DIR` 注入)

A1 `SbatchCmd.env` は `build_argv()` で `--export=ALL,K=V,...`(`slurm-async-runner2/src/sbatch/cmd.rs:133-134`)。`ALL` 付きで投入環境を保ったまま追加環境変数をジョブへ渡せる。`src/runner/flow.rs` submit 経路に 1 行:

```rust
cmd.env.insert(
    "JM_FLOW_DIR".into(),
    self.resolver.flow_dir(&fr.flow_uuid).to_string_lossy().into_owned(),
);
```

公開 API / PyO3 / `.pyi` 不変、`render_batch_bash` 不変、cwd 契約不変、既存 flow は新 env を無視(純加算)。DryRun/Mock は exec せず無影響(Mock はコール記録のみ、テストで `cmd.env` assert 可)。

| 案 | 公開API/PyO3 | cwd契約 | 既存flow | 判定 |
|---|---|---|---|---|
| R1: render シグネチャ変更 export | 破壊 | 不変 | 加算 | 却下 |
| R2: `cmd.chdir=Some(flow_dir)` | 不変 | 全ジョブ変更 | 挙動変更 | 却下 |
| **R4: submit `cmd.env` 注入(採用)** | 不変 | 不変 | 加算 | **採用** |
| R3: scaffold 時 絶対パス埋込 | 不変 | 不変 | 加算 | 却下(移動/コピー破綻) |

`JM_ROOT` 形は CLI 入力 env と同名 2 役の意味過負荷 + body join が必要なため不採用(将来 root 横断が要れば別途追加可)。

### recipe body 規約: 先頭で `cd "$JM_FLOW_DIR"`

domain レシピの `body` は先頭で `cd "$JM_FLOW_DIR"`。これで `input/main.gjf` 等を相対で書け、Gaussian の `%rwf`/`%chk` 相対副生成物・`output/` も flow dir に落ちる(`--chdir`(R2)不要)。

### 既知の小制約

A1 `render_export` は env キー/値に `,`/`=` を含むと拒否(`cmd.rs` テスト `BAD,KEY`/`BAD=KEY` 確認)。flow dir パスに通常含まれないが、`,`/`=` を含む root では `JM_FLOW_DIR` 注入が `SbatchSpawnError`。極めて稀。spec 既知制約として明記(将来の `jm doctor` 警告は別 spec)。

## 6. リファレンス整合マッピング(gem ↔ job-manager)

| gem(確立規約) | job-manager 表現(本レシピ) |
|---|---|
| `[[step]] program="gaussian" calc_type="opt"` + post `gaussian-parse-results` | `flow.toml` 2-job: `[jobs.opt] program="g16"` → afterok → `[jobs.parse] program="python"` |
| `[step.params] route/charge/multiplicity/extra_input` | `plan.toml [jobs.opt]` に同名で格納(`JM_PARAM_*` にも露出)。scaffold 時 `input/main.gjf` へ差込 |
| `common.toml [slurm]` / `[slurm.post]`(post 用 override) | `flow.toml [jobs.opt.config]`(大きめ) / `[jobs.parse.config]`(小さめ)。`partition="REPLACE_ME"` 両方(common 非関与) |
| `[env].task_basename = "main"`、`<uuid>/{input,output,derived}/` | flow dir 配下 `input/main.gjf`、body が `output/main.out`/`derived/` を作る。`task_basename` は `main` 固定(YAGNI) |
| `gaussian-run-g16 --config --uuid`(B のラッパ) | `jm` には当該 CLI/config 無し → body で `g16 input/main.gjf output/main.out` を直接実行 |
| `gaussian-parse-results`(C, cclib, 終端 status 書込) | body で `python scripts/parse_results.py output/main.out`。**status は job-manager の Lifecycle/tick が権威**、parse は exit code で success/fail |
| InChIKey compound(`compounds=[...]`、gjf title) | `--param compound=<InChIKey>`(既定 sentinel)。gjf title 行に使用。`[tags]` にも記録 |
| `[[sweep]]` / `parent_uuids` 連鎖 | **非対象**(CLAUDE.md Out of scope。単一 opt→parse のみ) |

## 7. `g16-opt-parse` レシピ詳細

### パラメータ(gem `[step.params]` 語彙に整合)

| name | type | default | help |
|---|---|---|---|
| `route` | str | `#p opt b3lyp/6-31g(d)` | Gaussian route 行(複数行可。gem 同様まるごと 1 文字列) |
| `charge` | int | `0` | 全電荷 |
| `multiplicity` | int | `1` | スピン多重度 |
| `extra_input` | str | `` (空) | charge/mult・geometry の後に付す追加入力(gem `extra_input`。connectivity 補助行など) |
| `nproc` | int | `8` | `%nprocshared`(gem は common の resource_spec.t 由来。job-manager は common 非関与のため明示パラメータ) |
| `mem` | str | `8GB` | `%mem` |
| `compound` | str | `REPLACE_ME-INCHIKEY` | InChIKey。gjf title 行 + `[tags].compound` に使用 |

`route`/`%chk=main.chk`/`%rwf=main.rwf` は gem 形式準拠。geometry は発明不能のため sentinel。

### 生成物

**`flow.toml`**(§5 body 規約 + gem の opt(大)/post(小)config 差を反映):

```toml
# Generated by `jm new g16-opt-parse` on <rfc3339>.
uuid       = "<uuid>"
created_at = "<rfc3339>"

[tags]
recipe   = "g16-opt-parse"
compound = "<compound>"
# --tag k=v も反映

[jobs.opt]
program = "g16"
body = """
cd "$JM_FLOW_DIR"
# --- cluster environment (EDIT for your site; see CLAUDE.md HPC notes) ---
# REPLACE_ME: e.g. `module load gaussian` / `conda activate <env>`
mkdir -p output
g16 input/main.gjf output/main.out
"""
[jobs.opt.config]
partition  = "REPLACE_ME"
time_limit = "48:00:00"

[jobs.parse]
program = "python"
body = """
cd "$JM_FLOW_DIR"
# REPLACE_ME: activate the python env that has `cclib`
python scripts/parse_results.py output/main.out
"""
[[jobs.parse.parents]]
from = "opt"
kind = "afterok"
[jobs.parse.config]
partition  = "REPLACE_ME"
time_limit = "01:00:00"
```

**`plan.toml`**(flow JobId 集合と一致。gem `[step.params]` 語彙でパラメータ記録=軽量な由来 + `JM_PARAM_*` 露出):

```toml
[jobs.opt]
route        = "#p opt b3lyp/6-31g(d)"
charge       = 0
multiplicity = 1
extra_input  = ""
nproc        = 8
mem          = "8GB"

[jobs.parse]
note = "cclib parse + convergence/energy validation of output/main.out"
```

**`input/main.gjf`**(gem 形式。scaffold 時 `{{...}}` 差込、geometry は sentinel):

```
%rwf=main.rwf
%nprocshared={{nproc}}
%mem={{mem}}
%chk=main.chk
{{route}}

{{compound}}

{{charge}} {{multiplicity}}
<GEOMETRY: REPLACE_ME — 1行1原子で `Element  x  y  z`。route に geom=connectivity を
含める場合は空行後に connectivity ブロックを記入>
{{extra_input}}
```

**`scripts/parse_results.py`**(cclib ベース。沈黙成功を避ける実 pass/fail):

- 引数: `output/main.out` パス。
- 依存: `cclib`(import 失敗時は `cclib not installed` を明示し exit 2 — 沈黙しない honest failure)。
- **実 pass/fail**:
  - (a) cclib で `.out` をパースできなければ exit 1(切断/破損)。
  - (b) Gaussian 正常終了マーカが無ければ exit 1。
  - (c) opt: 幾何収束フラグ(`ccData` の最適化収束)が False なら exit 1。
  - (d) 最終 SCF/全エネルギーが有限実数で取得できなければ exit 1。
  - すべて満たせば exit 0。検証済み/未検証項目を stdout に明記。
- **任意の派生生成(gem `derived/main.mol2` 相当)**: パース幾何を `derived/main.mol2` に書く処理を `# TODO(jm recipe): write derived/main.mol2` として明示拡張点に(v1 は (a)-(d) の pass/fail を本質とし、derived 出力は任意)。
- job-manager の Lifecycle/tick が status の権威。本スクリプトは exit code のみで success/fail を表す(gem の status ファイル再実装はしない)。

### 環境アクティベーション(sentinel)

gem の生成 batch は `source ~/.bashrc; conda activate analysis; module restore <X> -f` を含むがこれはサイト固有。レシピは body 内に **`# REPLACE_ME` コメントの環境アクティベーション節**を置き、`partition=REPLACE_ME` と並ぶ「クラスタ別編集点」として明示(学習スキル `pixi-conda-stack-reset` / `slurm-module-purge-breaks-srun` の HPC 落とし穴を踏まない方針)。具体 `module`/`conda` 行はハードコードしない。

## 8. `blank` レシピ(後方互換)

既存 `build_flow_template` / `build_plan_template`(`src/bin/jm.rs:497-574`)を `src/recipes/blank.rs` の `Blank` へ移設。出力は **バイト同値**(既存 `tests/integration_new.rs` / `src/bin/jm.rs` ユニットテストがそのまま通る)。`jm new`(無引数)= `jm new blank`。`GeneratedFile` は flow.toml / plan.toml の 2 つのみ(`input/`・`scripts/` 無し)。

## 9. エラーハンドリング

| 状況 | 挙動 |
|---|---|
| 未知レシピ | `bail!("unknown recipe {name:?}; available: blank, g16-opt-parse")` |
| `--param` に `=` 無し | `bail!("invalid --param: expected key=value, got {raw}")` |
| 未知パラメータキー | `bail!("recipe {recipe}: unknown param {key}; valid: ...")` |
| パラメータ型不整合 | `bail!("recipe {recipe}: param {key} expects {ty}, got {raw}")` |
| `flow_dir` 既存(UUID 衝突) | `bail!("flow dir already exists: {path}")`(既存。リトライしない) |
| asset 書込失敗 | 作成済み `flow_dir` を `remove_dir_all` で巻き戻して `?` 伝播 |
| `--list` / `--describe` | scaffold せず終了(副作用なし) |
| (実行時)`cclib` 未導入 | `parse_results.py` が明示メッセージで exit 2(沈黙しない) |

## 10. テスト

### ユニット(`src/recipes/**` の `#[cfg(test)] mod tests`)

- `Blank::generate` 出力が `JobFlow`/`ExperimentPlan` に直接パース、`{step1,step2}`、`step2.parents[0]={from:step1,kind:afterok}`、両 config `partition=="REPLACE_ME"`。**既存 `jm new` 出力とバイト同値**(回帰防止)。
- `G16OptParse::generate`: flow JobId 集合 == plan キー集合 == `{opt, parse}`、`parse.parents[0]={from:opt,kind:afterok}`、両 config `partition=="REPLACE_ME"`、opt/parse の `time_limit` が gem 同様に非対称(48h / 1h)、両 body 先頭 `cd "$JM_FLOW_DIR"`。
- `input/main.gjf`: `{{...}}` 残存無し、`--param route='#p opt pbe1pbe/def2svp' charge=1 multiplicity=2 nproc=16 mem=16GB compound=ABC` 反映、gem ヘッダ順(`%rwf`→`%nprocshared`→`%mem`→`%chk`→route→title→`charge mult`)、`<GEOMETRY: REPLACE_ME>` sentinel 存在。
- パラメータ検証: 既定適用 / 型変換(int/float/bool)/ 未知キー Err / 型不整合 Err / `--param route=a=b` → value=`a=b`(複数 `=` は最初で分割、route に `=` 含む現実ケース)。
- `registry()` に `blank`/`g16-opt-parse` を含み名前ユニーク。

### 統合(`tests/integration_new_recipes.rs`, `assert_cmd`)

- `jm new --list` が 2 レシピ列挙、exit 0、scaffold 無し。
- `jm new g16-opt-parse --describe` がパラメータ表出力、exit 0、scaffold 無し。
- `jm new g16-opt-parse --param charge=1` → `flow.toml`/`plan.toml`/`input/main.gjf`/`scripts/parse_results.py` 生成、gjf に `1 1` 反映。
- 生成 flow が **doctor-clean**: `jm doctor <uuid>` exit 0(`input/`・`scripts/` の存在が doctor を壊さない — doctor は TOML パース + plan 網羅 + uuid/dir + parents + log dir のみ検査し未知ファイルを拒否しないことを確認)。
- `jm new g16-opt-parse` → `jm render <uuid>` exit 0(ラウンドトリップ。`partition=REPLACE_ME` で render は通る)。
- 後方互換: `jm new`(無引数)と `jm new blank` が同一出力で既存 `jm new` 期待値と一致。`--print-path` / `--tag env=prod` 既存挙動維持。
- 未知レシピ `jm new nope` が exit 非 0 + 候補列挙。

### submit 経路(`src/runner/flow.rs` テスト / `tests/integration_sp3.rs`)

- `MockExecutor` で submit し、記録 `SbatchCmd.env["JM_FLOW_DIR"]` が `resolver.flow_dir(uuid)` 絶対パスに一致(R4 回帰)。
- `cmd.chdir` が依然 `None`(R2 不採用回帰)。

### Python smoke(`python/tests`)

- `examples/replica/ROSDSFDQCJNGOL-UHFFFAOYSA-O/main.out` 形の正常終了フィクスチャで `parse_results.py` exit 0、未収束/切断フィクスチャで exit 1、`cclib` 不在で exit 2。

`MockExecutor`/`InMemoryQuerier` 使用、live SLURM 不要(CLAUDE.md 準拠)。

## 11. CLAUDE.md 準拠チェック

- `flow.toml`/`plan.toml`/`input/main.gjf`/`scripts/parse_results.py` は user-authored 入力の **初回 bootstrap のみ**。runtime(render/submit/tick)は依然 `.jm/` 配下しか書かない(`jm new` spec §9 の bootstrap 容認を sidecar に拡張)。
- `jm` `--no-default-features` 必須 → `src/recipes/` は pyo3 非依存。
- アトミック書込(PID サフィックス tmp + rename)を全生成ファイルで踏襲。
- Out of scope(DSL / sweep / per-flow common / TUI / リモートレジストリ / OpenMM)に抵触しない。利用者はトポロジを書かず名前付きレシピ + スカラパラメータのみ。
- 公開 API は追加のみ(`recipes` モジュール re-export)。`render_batch_bash` 含む既存公開 API / PyO3 境界 / `.pyi` を変更しない(R4 採用根拠)。
- レシピは gem の**ドメイン意味論**(route/charge/multiplicity/extra_input、opt→afterok→parse、gjf 形式、input/output/derived)を写すが、**スキーマは job-manager の `flow.toml`/`plan.toml`**(gem 形式は採用しない)。status の権威は job-manager の Lifecycle/tick(gem `status` 非再実装)。
- Conventional Commits / per-task commit / stacked PR。

## 12. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| post(検証)中身 | parse-results 規約(cclib, .out 検証, exit code) | OpenMM / 両立 | エコシステムに OpenMM 前例ゼロ、確立実装は cclib parse。ユーザ判断 |
| パラメータ語彙 | gem `route`/`charge`/`multiplicity`/`extra_input`(+ nproc/mem/compound) | method/basis 分割(rev.1 発明) | 既存規約整合、route 行まるごとが gem 流儀 |
| ファイルレイアウト | gem `input/main.gjf` + `output/`/`derived/`(`task_basename=main`) | rev.1 `assets/opt.gjf` | 既存規約整合、化学者の慣れたレイアウト |
| status 権威 | job-manager Lifecycle/tick(parse は exit code のみ) | gem `status` 再実装 | 二重実装回避、job-manager の責務分担に整合 |
| レシピ定義 substrate | B: Rust 型付きレジストリ | A: ファイルテンプレ / C: リモート | doctor-clean by construction、型検証、単一チーム |
| CLI 表面 | 1c: `jm new [<recipe>]` 位置引数 | 別コマンド / `--recipe` フラグ | 単一メンタルモデル、既存配管再利用 |
| 配置 | `src/recipes/`(同 package) | 別 crate | doctor/型と密結合で保証最強 |
| flow パス解決 | R4: submit `cmd.env` 注入 `JM_FLOW_DIR` | R1/R2/R3/`JM_ROOT` | 公開 API/PyO3/cwd 契約すべて不変、加算のみ |
| common.toml | 非関与(`partition=REPLACE_ME` sentinel) | gem common 採用 | 既存 `jm new` deferred-common 踏襲、事故源回避 |
