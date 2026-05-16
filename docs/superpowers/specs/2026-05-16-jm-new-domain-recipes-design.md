# `jm new <recipe>` — ドメイン固有レシピ — design

**Date:** 2026-05-16
**Status:** Draft (awaiting user review)
**Reference:**
- `docs/superpowers/specs/2026-05-16-jm-new-boilerplate-design.md`(既存 `jm new` の前提・sentinel 哲学)
- `src/bin/jm.rs`(`Cmd::New` / `cmd_new` / `build_flow_template` / `build_plan_template` / `atomic_write_str`)
- `src/render/mod.rs`(`render_batch_bash` — **公開 API + Python エクスポート**, prod caller は `src/runner/flow.rs:245` のみ)
- `src/runner/flow.rs:262-289`(submit 経路の `SbatchCmd` 構築。現状 `cmd.chdir` 未設定)
- 上流 A1 `slurm-async-runner2/src/sbatch/cmd.rs:133-134`(`SbatchCmd.env` → `--export=ALL,K=V,...`)
- CLAUDE.md(Out of scope / PyO3 境界 / `.jm/` レイアウト / `--no-default-features` 制約)

---

## 1. 問題設定

`jm new`(別 spec)は「静的な 2-job `step1 → step2`(afterok)雛形 + `--tag`」を生成する汎用 scaffold である。実運用では「g16 で構造最適化入力を作り、正常終了後(`afterok`)に Python スクリプトで OpenMM を使って結果を検証する」のような**ドメイン固有の依存付き多段チェーン**を毎回手で組むのは退屈でミスを生む。

横断調査(atomate / nf-core / AiiDA / jobflow / cookiecutter 等)の結論:依存付き多段チェーンをテンプレート化する全ツールは **依存トポロジをレシピ定義側に固定し、利用者にはパラメータだけ渡させる**(トポロジは渡させない)。最も近い先行例は atomate v1(バンドルされた宣言的依存グラフを名前で選び、薄い関数がパラメータを差し込む)。これは `jm` の `flow.toml`/`plan.toml`(明示的 `JobEdge`/`afterok`)の TOML 版そのもの。

本 spec は `jm new` を「名前付きドメインレシピを選んでパラメータを差し込む」コマンドへ拡張する。

## 2. ゴール / 非ゴール

### Goals

1. `jm new` に**位置引数 `<recipe>`** を追加。`jm new`(無引数)= 組込 `blank` レシピ(= 既存 2-job 雛形、**後方互換**)。`jm new g16-opt-openmm-check --param k=v` で domain レシピを生成。
2. レシピは **Rust 側の型付きレジストリ**(B パターン)。出力ツリーは構築時点で `jm doctor`-clean を保証(flow JobId 集合 == plan `[jobs.*]` キー集合、uuid == ディレクトリ名、親エッジ整合)。
3. domain レシピは **flow.toml / plan.toml に加えサイドカーファイル**(`assets/opt.gjf`, `assets/check_openmm.py`)を生成し、化学者が直接編集できる。
4. レシピごとに型付きパラメータ(名前 / 既定値 / ヘルプ)。`jm new <recipe> --param KEY=VALUE`(繰返し可)。`jm new --list`(レシピ一覧)、`jm new <recipe> --describe`(パラメータ一覧)。
5. 実行中ジョブが自分の flow ディレクトリを解決できるよう、**submit 経路で `SbatchCmd.env` に `JM_FLOW_DIR`(flow dir 絶対パス)を注入**する(§5)。
6. 生成 v1 レシピ: `blank` + `g16-opt-openmm-check` の 2 つ。レジストリは追加容易。
7. 書き込みは中途半端を残さない(既存 `jm new` の rollback 規約踏襲)。

### Non-goals

- `common.toml` の生成・変更(v1)。`partition` は既存 `jm new` と同じ `REPLACE_ME` sentinel。
- experiment DSL / sweep 展開 / 親解決(CLAUDE.md "Out of scope" 準拠 — 利用者はトポロジを書かず、名前付きレシピ + スカラパラメータのみ)。
- 対話的ウィザード / TUI / プロンプト(SLURM/CI 非対話前提)。
- 既存 flow の再生成・migration・answers-file による再 scaffold(copier `update` 相当は将来別 spec)。
- リモートレシピレジストリ(Pattern C)。単一チームには過剰、in-tree バンドルのみ。
- `render_batch_bash` の公開 API / PyO3 境界変更(R1 不採用。§5)。
- 全ジョブの cwd 契約変更(R2 不採用。§5)。
- OpenMM の分子別力場割当の自動化(`check_openmm.py` は実行可能スケルトン + 明示 TODO 拡張点。§7)。

## 3. CLI 形

```
jm --root <ROOT> new [<RECIPE>] [--param <KEY=VALUE>]... [--tag <KEY=VALUE>]... [--print-path]
jm --root <ROOT> new --list
jm --root <ROOT> new <RECIPE> --describe
```

| 引数 | 説明 |
|---|---|
| `<RECIPE>`(位置, 任意) | レジストリ内のレシピ名。省略時 `blank`。未知名は候補列挙付きでエラー。 |
| `--param <KEY=VALUE>` | 任意回。レシピ定義のパラメータを上書き。未知キー / 型不整合はエラー。`=` 無しはエラー(既存 `--tag` と同じパース)。 |
| `--tag <KEY=VALUE>` | 既存。`flow.toml [tags]` に反映。全レシピ共通。 |
| `--print-path` | 既存。stdout に `<root>/<uuid>` のみ。 |
| `--list` | レシピ名 + 1 行説明を列挙して終了(scaffold しない)。 |
| `--describe` | `<RECIPE>` のパラメータ(名前 / 型 / 既定値 / ヘルプ)を列挙して終了。 |

`Cmd::New` を以下へ拡張(clap):

```rust
New {
    /// Recipe name (positional). Omitted = "blank" (back-compat 2-job boilerplate).
    recipe: Option<String>,
    /// Repeatable. KEY=VALUE recipe parameter overrides.
    #[arg(long = "param", value_name = "KEY=VALUE")]
    params: Vec<String>,
    /// Repeatable. KEY=VALUE pairs written into flow.toml [tags].
    #[arg(long = "tag", value_name = "KEY=VALUE")]
    tags: Vec<String>,
    #[arg(long)]
    print_path: bool,
    /// List available recipes and exit.
    #[arg(long)]
    list: bool,
    /// Print the selected recipe's parameters and exit.
    #[arg(long)]
    describe: bool,
}
```

`main()` の `Cmd::New` 分岐を `cmd_new(&root, recipe.as_deref(), &params, &tags, print_path, list, describe)` に拡張。

## 4. アーキテクチャ

### モジュール配置

```
src/recipes/
  mod.rs                     -- Recipe trait, RecipeParam, RecipeCtx, GeneratedFile,
                                registry(), parse/validate params, --list/--describe 整形
  blank.rs                   -- 既存 2-job step1->step2 を Recipe 化(build_flow/plan_template を移設)
  g16_opt_openmm_check.rs    -- domain レシピ + asset テンプレート(include_str! or const)
  assets/
    g16_opt_openmm_check/
      opt.gjf.tmpl           -- Gaussian 入力テンプレート({{placeholder}})
      check_openmm.py.tmpl   -- OpenMM 検証スケルトン
```

- `src/recipes/` は **pyo3 非依存**(`uuid`/`chrono`/`toml`/標準ライブラリのみ)。`jm` は `--no-default-features` でビルドされるため必須制約。
- レシピは **純粋**(I/O を持たない):`generate(ctx) -> Result<Vec<GeneratedFile>, RecipeError>`。ファイル書き込み・ディレクトリ作成・rollback は `cmd_new` が担当(既存 `jm new` の `atomic_write_str` を asset 数分ループ)。
- `src/lib.rs` から `pub use recipes::{Recipe, registry, ...}` を再エクスポート(downstream/テスト用。公開 API 追加であり既存 API の破壊はしない)。

### 型

```rust
/// 生成する 1 ファイル。relpath は flow ディレクトリ相対。
pub struct GeneratedFile {
    pub relpath: PathBuf,      // "flow.toml" / "plan.toml" / "assets/opt.gjf" / "assets/check_openmm.py"
    pub contents: String,
    pub unix_mode: Option<u32>, // assets/*.py 等の実行ビットが要れば。既定 None(0644 相当)
}

pub enum RecipeParamType { Str, Int, Float, Bool }

pub struct RecipeParam {
    pub name: &'static str,
    pub ty: RecipeParamType,
    pub default: &'static str,   // 文字列表現。型は ty で解釈
    pub help: &'static str,
}

pub struct RecipeCtx<'a> {
    pub uuid: &'a Uuid,
    pub created_at: &'a str,            // RFC3339 UTC
    pub tags: &'a BTreeMap<String, String>,
    pub params: &'a BTreeMap<String, toml::Value>, // 既定 + --param 上書き後の検証済み値
}

pub trait Recipe: Send + Sync {
    fn name(&self) -> &'static str;
    fn summary(&self) -> &'static str;
    fn params(&self) -> &'static [RecipeParam];
    fn generate(&self, ctx: &RecipeCtx<'_>) -> Result<Vec<GeneratedFile>, RecipeError>;
}

pub fn registry() -> Vec<Box<dyn Recipe>>; // [Blank, G16OptOpenmmCheck]
pub fn find(name: &str) -> Option<Box<dyn Recipe>>;
```

`RecipeError`(`thiserror`):`UnknownParam{name}` / `ParamTypeMismatch{name, expected, got}` / `Internal(String)`。CLI 層で `anyhow` に載せ替え、未知レシピ・未知パラメータは候補列挙付きメッセージ。

### 動作シーケンス(`cmd_new`)

1. `--list`: `registry()` を `name — summary` で出力して `Ok(())`。
2. レシピ解決: `recipe.unwrap_or("blank")` → `find()`。`None` → `bail!("unknown recipe {name:?}; available: {...}")`。
3. パラメータ:`recipe.params()` の既定値で `BTreeMap` を作り、`--param` を型に従い検証して上書き。未知キー / 型不整合 → `bail!`。
4. `--describe`: 解決済みパラメータ表を出力して `Ok(())`(scaffold しない)。
5. `--tag` パース(既存ロジック流用)。
6. `uuid = Uuid::now_v7()`; `resolver = PathResolver::new(root)`; `flow_dir = resolver.flow_dir(&uuid)`; 衝突確認(既存と同じ。`flow_dir.exists()` なら bail、リトライしない)。
7. `created_at` = `Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)`。
8. `ctx` を組み `recipe.generate(&ctx)?` → `Vec<GeneratedFile>`。
   - レシピは内部で **flow JobId 集合 == plan `[jobs.*]` キー集合** を構築時に保証(doctor-clean by construction)。
9. `create_dir_all(&flow_dir)`。各 `GeneratedFile` について親(`assets/`)を `create_dir_all` し `atomic_write_str` で書き込み。`unix_mode` 指定があれば rename 前に `chmod`(`batch.bash` の 0600 と同じ Unix-only パターン)。
10. いずれか失敗時:作成済み `flow_dir` を `remove_dir_all` で巻き戻してから `?` 伝播(既存規約)。
11. 出力(既存形を踏襲、生成ファイル列挙を asset 込みに):
    ```
    created flow <uuid> from recipe <name>
      <root>/<uuid>/flow.toml
      <root>/<uuid>/plan.toml
      <root>/<uuid>/assets/opt.gjf
      <root>/<uuid>/assets/check_openmm.py
    next: edit assets/opt.gjf (geometry), set a real partition, then
          `jm --root <root> render <uuid>`
    ```
    `--print-path` 時は `<root>/<uuid>` の 1 行のみ(既存)。

## 5. flow パス解決(R4)— submit 経路 `cmd.env` 注入

### 制約(実コード確認済み)

- `render_batch_bash`(`src/render/mod.rs`)はジョブに `JM_FLOW_UUID` / `JM_JOB_ID` / `JM_AXIS_*` / `JM_PARAM_*` のみ export。**flow ディレクトリへのパスも `cd` も注入しない**。
- `src/runner/flow.rs:268` で `SbatchCmd` を構築する際 **`cmd.chdir` 未設定**(`None`)。よってジョブの cwd は `jm submit` 投入ディレクトリで、flow dir ではない。`examples/full` のログパスも絶対(`/work/...`)で「保証された cwd は無い」のが既存前提。
- `render_batch_bash` は `src/lib.rs:38` で公開、`src/py_export/render.rs` + `mod.rs:103` で **Python エクスポート**。シグネチャ変更は公開 Rust API + PyO3 境界の破壊 + `.pyi` 再生成を伴う。

### 採用案: R4(submit 経路で `SbatchCmd.env` に `JM_FLOW_DIR` 注入)

A1 `SbatchCmd.env: HashMap<String,String>` は `build_argv()` で `--export=ALL,K=V,...` になる(`slurm-async-runner2/src/sbatch/cmd.rs:133-134`)。`ALL` 付きのため投入環境を保ったままジョブへ追加環境変数を渡せる。

`src/runner/flow.rs` の submit 経路(`SbatchCmd::new(...)` 直後、`cmd.dependency` 設定付近)に 1 行:

```rust
cmd.env.insert(
    "JM_FLOW_DIR".into(),
    self.resolver.flow_dir(&fr.flow_uuid).to_string_lossy().into_owned(),
);
```

- 公開 API / PyO3 / `.pyi` 不変。`render_batch_bash` 不変。
- cwd 契約不変(`--chdir` を使わない)。既存 flow は新 env を無視するだけ(純加算)。
- DryRun / Mock executor は exec しないので無影響(Mock はコール記録のみ。テストで `cmd.env` を assert 可能)。

### 比較(却下案)

| 案 | 公開API/PyO3/.pyi | cwd契約 | 既存flow | 判定 |
|---|---|---|---|---|
| R1: `render_batch_bash` に `JM_FLOW_DIR` export(シグネチャ変更) | **破壊** | 不変 | 加算 | 却下(API/PyO3 破壊) |
| R2: `cmd.chdir = Some(flow_dir)` | 不変 | **全ジョブ変更** | 挙動変更 | 却下(blast radius 大) |
| **R4: submit 経路 `cmd.env` 注入(採用)** | 不変 | 不変 | 加算 | **採用** |
| R3: scaffold 時に絶対パス埋込 | 不変 | 不変 | 加算 | 却下(flow 移動/コピーで破綻、UUID ポータビリティと不整合) |

`JM_ROOT` 形も検討したが、CLI 入力 env としての `JM_ROOT`(CLAUDE.md 環境変数表)と「ジョブから見える anchor」で**同名 2 役の意味過負荷**、かつ body 側 join(`$JM_ROOT/$JM_FLOW_UUID`)が必要なため不採用。将来ジョブが root 横断を要すれば別途 `JM_ROOT` を追加可(本 spec の R4 はそれを妨げない)。

### recipe body 規約: 先頭で `cd "$JM_FLOW_DIR"`

domain レシピが生成する `body` は **先頭で `cd "$JM_FLOW_DIR"`** する。これにより:

- サイドカー参照が `assets/opt.gjf` の相対パスで書ける(可読性)。
- Gaussian の `%chk` / `%rwf` 等の相対副生成物、`opt.log` 等が flow dir に落ちる(`--chdir`(R2)無しで意図通り)。

例(`g16-opt-openmm-check`):

```toml
[jobs.opt]
program = "g16"
body = """
cd "$JM_FLOW_DIR"
g16 < assets/opt.gjf > opt.log
"""

[jobs.check]
program = "python"
body = """
cd "$JM_FLOW_DIR"
python assets/check_openmm.py opt.log
"""
[[jobs.check.parents]]
from = "opt"
kind = "afterok"
```

### 既知の小制約

A1 `render_export` は env の **キー/値に `,` または `=` を含むと拒否**(`cmd.rs` テスト群 `BAD,KEY` / `BAD=KEY` / `1,2` で確認)。flow dir パス(`<root>/<uuid>`、uuid は hex+ハイフン)に通常これらは含まれないが、`,`/`=` を含む root パスでは `JM_FLOW_DIR` 注入が `SbatchSpawnError` になる。極めて稀。spec 既知制約として明記し、`jm doctor` での将来警告は別 spec とする(本 spec 非対象)。

## 6. `g16-opt-openmm-check` レシピ詳細

### パラメータ

| name | type | default | help |
|---|---|---|---|
| `method` | str | `B3LYP` | DFT 汎関数 / 理論手法 |
| `basis` | str | `6-31G(d)` | 基底関数 |
| `charge` | int | `0` | 全電荷 |
| `mult` | int | `1` | スピン多重度 |
| `nproc` | int | `8` | `%nprocshared` |
| `mem` | str | `8GB` | `%mem` |
| `title` | str | `jm g16 opt (jm recipe)` | Gaussian タイトル行 |

`partition` はパラメータにしない(既存 `jm new` の deferred-common を踏襲し `flow.toml [jobs.*.config] partition = "REPLACE_ME"`)。幾何は発明できないため `assets/opt.gjf` に sentinel を置く。

### 生成物

`flow.toml`(抜粋。§5 の body 規約反映):

```toml
# Generated by `jm new g16-opt-openmm-check` on <rfc3339>.
uuid       = "<uuid>"
created_at = "<rfc3339>"

[tags]
# --tag 反映。recipe = "g16-opt-openmm-check" を既定タグとして付与(由来記録)

[jobs.opt]
program = "g16"
body = """
cd "$JM_FLOW_DIR"
g16 < assets/opt.gjf > opt.log
"""
[jobs.opt.config]
partition = "REPLACE_ME"

[jobs.check]
program = "python"
body = """
cd "$JM_FLOW_DIR"
python assets/check_openmm.py opt.log
"""
[[jobs.check.parents]]
from = "opt"
kind = "afterok"
[jobs.check.config]
partition = "REPLACE_ME"
```

`plan.toml`(flow JobId 集合と一致。レシピパラメータを記録 = 軽量な「由来 + 再現メモ」。`JM_PARAM_*` としても露出):

```toml
[jobs.opt]
method = "B3LYP"
basis  = "6-31G(d)"
charge = 0
mult   = 1
nproc  = 8
mem    = "8GB"

[jobs.check]
note = "OpenMM validation of opt.log (see assets/check_openmm.py)"
```

`assets/opt.gjf`(scaffold 時に `{{...}}` をパラメータで置換。幾何は sentinel):

```
%nprocshared={{nproc}}
%mem={{mem}}
%chk=opt.chk
#p opt {{method}}/{{basis}}

{{title}}

{{charge}} {{mult}}
<GEOMETRY: REPLACE_ME — 1行1原子で `Element  x  y  z` を記入>

```

`assets/check_openmm.py`(実行可能スケルトン。沈黙成功を避ける):

- 引数: `opt.log` パス。
- **実 pass/fail**: (a) `Normal termination of Gaussian` が無ければ `sys.exit(1)`、(b) 最終 standard orientation 幾何をパースできなければ `exit(1)`、(c) 最終 SCF エネルギーが有限実数でなければ `exit(1)`。
- **明示 TODO 拡張点**(分子別力場割当は scaffold で推測不能):パース済み幾何を使った OpenMM system 構築・MM エネルギー検証を書く場所を `# TODO(jm recipe): ...` で明示。未実装でも (a)-(c) が通れば `exit(0)`(end-to-end で `MockExecutor` 緑、かつ「何を検証済み/未検証か」を stdout に明記して honest)。
- 依存(`openmm` 等)は import を TODO ブロック内に遅延させ、(a)-(c) のみなら標準ライブラリで動く。

## 7. `blank` レシピ(後方互換)

既存 `build_flow_template` / `build_plan_template`(`src/bin/jm.rs:497-574`)の内容を `src/recipes/blank.rs` の `Blank` 実装へ移設。出力は**バイト同値**(既存 `tests/integration_new.rs` / `src/bin/jm.rs` のユニットテストがそのまま通ること)。`jm new`(無引数)= `jm new blank`。`assets/` は生成しない(`GeneratedFile` は flow.toml / plan.toml の 2 つのみ)。

## 8. エラーハンドリング

| 状況 | 挙動 |
|---|---|
| 未知レシピ | `bail!("unknown recipe {name:?}; available: blank, g16-opt-openmm-check")` |
| `--param` に `=` 無し | `bail!("invalid --param: expected key=value, got {raw}")` |
| 未知パラメータキー | `bail!("recipe {recipe}: unknown param {key}; valid: ...")` |
| パラメータ型不整合 | `bail!("recipe {recipe}: param {key} expects {ty}, got {raw}")` |
| `flow_dir` 既存(UUID 衝突) | `bail!("flow dir already exists: {path}")`(既存。リトライしない) |
| asset 書込失敗 | 作成済み `flow_dir` を `remove_dir_all` で巻き戻して `?` 伝播(既存規約) |
| `--list` / `--describe` | scaffold せず終了(副作用なし) |

## 9. テスト

### ユニット(`src/recipes/**` の `#[cfg(test)] mod tests`)

- `Blank::generate` 出力が `toml::from_str::<JobFlow>` / `ExperimentPlan` で直接パース、`{step1,step2}` 一致、`step2.parents[0] = {from:step1, kind:afterok}`、両 config `partition == "REPLACE_ME"`。**既存 `jm new` 出力とバイト同値**を assert(回帰防止)。
- `G16OptOpenmmCheck::generate` 出力: flow JobId 集合 == plan キー集合 == `{opt, check}`、`check.parents[0] = {from:opt, kind:afterok}`、両 config `partition == "REPLACE_ME"`、body 先頭が `cd "$JM_FLOW_DIR"`。
- `assets/opt.gjf` に `{{...}}` 残存が無い、`--param method=PBE0 basis=def2-SVP` 反映、`<GEOMETRY: REPLACE_ME>` sentinel 存在。
- パラメータ検証: 既定値適用 / 型変換(int/float/bool)/ 未知キー Err / 型不整合 Err / `--param a=b=c` → value=`b=c`。
- `registry()` が `blank` と `g16-opt-openmm-check` を含み名前ユニーク。

### 統合(`tests/integration_new_recipes.rs`, `assert_cmd`)

- `jm new --list` が 2 レシピを列挙、exit 0、scaffold 無し(tempdir に新規ディレクトリが増えない)。
- `jm new g16-opt-openmm-check --describe` がパラメータ表を出力、exit 0、scaffold 無し。
- `jm new g16-opt-openmm-check --param method=PBE0` → `flow.toml`/`plan.toml`/`assets/opt.gjf`/`assets/check_openmm.py` 生成、`opt.gjf` に `PBE0` 反映。
- 生成 flow が **doctor-clean**: `jm doctor <uuid>` exit 0(`tests/doctor_examples.rs` と同じ判定。`assets/` の存在が doctor を壊さないことを確認 — doctor は TOML パース + plan 網羅 + uuid/dir + parents + log dir のみ検査し未知ファイルを拒否しない)。
- `jm new g16-opt-openmm-check` → `jm render <uuid>` exit 0(ラウンドトリップ。`partition=REPLACE_ME` で render は通る)。
- 後方互換: `jm new`(無引数)と `jm new blank` が同一出力で、既存 `jm new` 期待値と一致。`--print-path`、`--tag env=prod` 既存挙動維持。
- 未知レシピ `jm new nope` が exit 非 0 + 候補列挙。

### submit 経路(`src/runner/flow.rs` のテスト / `tests/integration_sp3.rs`)

- `MockExecutor` で submit し、記録された `SbatchCmd.env["JM_FLOW_DIR"]` が `resolver.flow_dir(uuid)` の絶対パスに一致(R4 回帰テスト)。
- `cmd.chdir` が依然 `None` であること(R2 を採らない回帰)。

### Python smoke(任意, `python/tests`)

- 既知の正常終了 Gaussian `opt.log` フィクスチャに対し `check_openmm.py` が exit 0、未収束フィクスチャで exit 1。

`MockExecutor` / `InMemoryQuerier` を使い live SLURM 不要(CLAUDE.md 準拠)。

## 10. CLAUDE.md 準拠チェック

- `flow.toml`/`plan.toml`/`assets/*` は user-authored 入力の **初回 bootstrap のみ**。runtime(render/submit/tick)は依然 `.jm/` 配下しか書かない(既存規約と整合。`jm new` spec §9 の bootstrap 容認を sidecar に拡張)。
- `jm` `--no-default-features` 必須 → `src/recipes/` は pyo3 非依存(`uuid`/`chrono`/`toml`/std のみ)。
- アトミック書込(PID サフィックス tmp + rename)を全生成ファイルで踏襲。`assets/*.py` の実行ビットは rename 前 `chmod`(`batch.bash` 0600 と同じ Unix-only パターン)。
- Out of scope(DSL / sweep / per-flow common / TUI / リモートレジストリ)に抵触しない。利用者はトポロジを書かず名前付きレシピ + スカラパラメータのみ。
- Conventional Commits / per-task commit / stacked PR。レシピ追加はレジストリ 1 行 + ファイル 1 群でコード変更を局所化。
- 公開 API は **追加のみ**(`recipes` モジュール re-export)。`render_batch_bash` 含む既存公開 API / PyO3 境界 / `.pyi` を変更しない(R4 採用の根拠)。

## 11. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| レシピ定義 substrate | B: Rust 型付きレジストリ | A: ファイルテンプレ / C: リモート | doctor-clean by construction、型検証、単一チーム |
| CLI 表面 | 1c: `jm new [<recipe>]` 位置引数 | 別コマンド / `--recipe` フラグ | 単一メンタルモデル、既存配管全再利用、Draft spec 改訂が安価 |
| 配置 | `src/recipes/`(同 package) | 別 crate | doctor/型と密結合で保証最強、atomate も同方式 |
| 生成物 | サイドカーファイル | インライン body のみ | 化学者が `.gjf`/`.py` を直接編集 |
| flow パス解決 | R4: submit `cmd.env` 注入 `JM_FLOW_DIR` | R1 / R2 / R3 / `JM_ROOT` | 公開 API / PyO3 / cwd 契約すべて不変、加算のみ |
| OpenMM チェック | 実 pass/fail + 明示 TODO 拡張点 | 完全自動 / 全 TODO 沈黙成功 | scaffold で力場推測不能、かつ沈黙成功を回避 |
