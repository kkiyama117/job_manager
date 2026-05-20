# `jm new` recipe v2 — folder-portable env-var path injection + per-task common.toml

**Date:** 2026-05-20
**Status:** Draft (awaiting user review)
**Supersedes (partial):** `2026-05-18-jm-g16-opt-parse-recipe-design.md` (案A v1) の
path 解決セクション(§5.1 R3' + §11 `flow.rs` cwd 契約 + §4.2 v2 box)。v1 仕様の
他項目(二層レシピ / `JobTemplate` / `FlowRecipe` / `base_preamble()` / `blank` バイト
同値 / `# REPLACE_ME` swap-in / scratch ステージング / xyz パーサ等)は v2 でも不変。
**Reference:**
- 案A v1 spec(`2026-05-18-jm-g16-opt-parse-recipe-design.md`)— v2 はこの差分設計
- issue #29 trade-off 表(rev.7 reject 後の文脈で再評価)
- ユーザ要求(verbatim, 2026-05-20):
  *"I want you to do v2 and avoid writing absolute path if you can (for safety
  of moving folders to other location"*

---

## 1. 問題設定

案A v1(PR #27/#28 land 済)は機能するが、以下の二点が残課題:

1. **scaffold-baked absolute path**(R3')— `scripts/run.py` / `parse.py` 冒頭の
   `JOB_DIR = "/abs/.../<uuid>/opt"`、および (a) 暫定 land 済の絶対 body
   `bash "/abs/.../<uuid>/opt/scripts/opt.bash"`。これらは scaffold 時に焼き込まれ、
   **flow_dir を別 location に移動すると壊れる**(login↔compute マウント差・
   ローカル↔NFS 移動・将来の archive 等)。
2. **クラスタ既定の per-task 一元化**(spec §4.2 v2 box 未実装)— launcher /
   scratch_root / g16_cmd 等のサイト config が `plan.toml` の per-step param 上で
   重複し、サイト変更時に N 箇所修正が必要。

ユーザ意向(2026-05-20):
- v2 では(c)`python -m gaussian_compute_runtime <subcommand>` 起動への swap-in 経路を
  確立しつつ、scaffold の絶対 path 焼き込みを **可能な限り排除**する。
  実 subcommand 名 / CLI 形は audit
  (`docs/superpowers/specs/2026-05-20-gaussian-compute-runtime-audit.md` / PR #37 / issue #32)
  に従う:`run-g16` / `parse-results` / `consume-parent-results`(ハイフン区切り)。
  ただし 2026-05-20 / D-α v0.2.0 時点では γ(`consume-parent-results`)のみ動作、
  前 2 つは **B-α migration 待ちで現状 broken** = 当面 v1 self-contained 維持
  (audit §4.1 / §4.2 / §12)。
- gem stack は v1 と同様 **任意 swap-in**(`# REPLACE_ME` 哲学維持)。
  デフォルト body は self-contained `scripts/<id>.py` を起動するが、env-var-base で
  cwd / folder 非依存。
- per-task `common.toml` 内のサイト固有絶対 path(`scratch_root=/LARGE0/...`)は
  許容、flow_dir 配下への絶対 path のみ禁止。
- 既存 v1 flow は不変動作 + 手動再 scaffold(自動 migration はスコープ外)。

## 2. ゴール / 非ゴール

### Goals

1. **scaffold が flow_dir 絶対 path を一切焼き込まない**(`grep "<flow_dir abs>"
   scaffold-output` が 0 件)。サイト固有絶対 path(`scratch_root` 等)は common.toml の
   key-value としてのみ許容(folder 移動と独立)。
2. **render-time env var 注入で動的 path 解決**(`JM_FLOW_DIR`/`JM_JOB_DIR` を
   `render_batch_bash` が export 文として焼き込む。値は submit/render 時動的計算 =
   移動後 `jm render` で再焼き込み = portable)。
3. **scripts の env-var-base 化**(`JOB_DIR = os.environ["JM_JOB_DIR"]`、絶対定数
   焼き込み撤回)。
4. **per-task `common.toml`** スキーマ確定 + `<flow_dir>/<job_id>/common.toml` 生成。
   launcher / g16_cmd / scratch_root / conda_env / module_profile / pixi_manifest を
   一元化(従来の plan.toml per-step param からの抽出)。
5. **`# REPLACE_ME` swap-in 維持**(任意で `python -m gaussian_compute_runtime
   <subcommand>` へ手動差替可能)。デフォルトは self-contained。
   実 CLI 形は audit §4 / §13(stable-contract one-liner)に従う:
   - γ(`consume-parent-results`): `python -m gaussian_compute_runtime
     consume-parent-results --config <runtime common.toml> <child_uuid_v7>`
     (`--config` は **runtime 用 common.toml** = §4.3 の v2 per-task `common.toml` と
     **別ファイル / 別 schema**。名前衝突については §4.3 注を参照)。
     `<child_uuid_v7>` は canonical lowercase UUID v7 — job-manager の flow UUID
     (`Uuid::new_v4()`)とは **桁レイアウトが異なる**(audit §11 表)。
   - `run-g16` / `parse-results`: D-α v0.2.0 で ImportError(`ConfigManager` /
     `JobPaths` 削除済)= B-α migration まで swap-in 非対応(audit §4.1 / §4.2 / §12)。
   - 当面の swap-in 可能候補は γ のみ。run-g16 / parse-results は v1 self-contained 維持。
6. **`SbatchCmd.chdir` は不変**(`None` 据置 = core 契約変更ゼロ、blank/user-flow 無影響)。
   cwd 非依存性は body の `$JM_JOB_DIR` 展開 + script 内 env var 参照で達成。
7. **既存 v1 flow との後方互換**:既に scaffold 済の `<root>/<uuid>/` は `jm render`
   再実行で v2 形式 batch.bash に更新される(env injection が追加されるが scripts は
   読まない = 無害 no-op)。新規 scaffold は v2 形式(env-var-base script + per-task
   common.toml)を出力。

### Non-goals

- D2(`gaussian_job_shared`)`CommonConfig.launcher` / `[directories] scratch_root`
  追加(= rev.7 idea)— **rev.7 reject 確定** により本 v2 では取り扱わない。
- `python -m gaussian_compute_runtime` を **デフォルト** にする(任意 swap-in に留める)。
  なお 2026-05-20 / D-α v0.2.0 時点で swap-in 可能な subcommand は γ
  (`consume-parent-results`)のみ — run-g16 / parse-results は B-α migration 待ち
  (audit §12 risk matrix)。
- `jm migrate v1→v2`(既存 flow の自動書換)— 手動再 scaffold で運用、v3 拡張点へ。
- 多段 g16 連鎖、自動幾何取得、status 二重実装 — v1 と同じ Out of scope。

## 3. CLI 形

v1 と同一(`jm new [<flow-recipe>] [--param ...] [--tag ...] [--print-path] [--list]
[<recipe> --describe]`)。v2 で新 sub-command 追加なし。

## 4. アーキテクチャ

### 4.1 全体俯瞰(v1 からの差分)

```text
<root>/<uuid>/
├── flow.toml                         # v2: body は env-var-展開済み絶対起動子(scaffold 不変)
├── plan.toml                         # v2: site config を抜き plan-only(route/charge 等)に痩せる
├── opt/
│   ├── common.toml                   # ★ v2 NEW: per-task site config
│   ├── scripts/
│   │   ├── opt.bash                  # base_preamble は不変
│   │   └── run.py                    # ★ v2 NEW: env-var-base(JOB_DIR = os.environ[...])
│   └── input/main.gjf                # v1 と同じ
├── parse/
│   ├── common.toml                   # ★ v2 NEW
│   ├── scripts/{parse.bash, parse.py} # ★ v2 NEW: env-var-base
└── .jm/
    ├── flow.effective.toml
    └── <job_id>/batch.bash           # ★ v2: + export JM_FLOW_DIR / JM_JOB_DIR
```

> **In-flux disclaimer**(audit §8 / §12):上記レイアウトは **job-manager 自身のもの**で、
> `gaussian-compute-runtime` の `PathResolver` レイアウト
> (`<env.root>/<uuid_v7>/{input,output,derived}/`)とは **独立**。runtime swap-in を
> 行う場合、ユーザは runtime 用 common.toml の `[env].root` を別途設定する(v2 spec は
> 関与しない)。**runtime 側 PathResolver レイアウトは user-flagged in-flux**
> (2026-05-20 user signal:"folder structure / job flow may be rewritten")—
> v2 spec / recipes は scaffold に runtime レイアウト literal を焼き込まない方針で、
> 将来の runtime rewrite に対する surgical な影響範囲を保つ。

### 4.2 `render_batch_bash` シグネチャ拡張

```rust
// v1 (現行):
pub fn render_batch_bash(
    flow_uuid: &Uuid,
    jid: &JobId,
    parts: &JobIdParts<'_>,
    params: &BTreeMap<String, toml::Value>,
    body: &str,
) -> String;

// v2:
pub fn render_batch_bash(
    flow_uuid: &Uuid,
    jid: &JobId,
    parts: &JobIdParts<'_>,
    params: &BTreeMap<String, toml::Value>,
    body: &str,
    abs_flow_dir: &Path,   // ★ NEW
    abs_job_dir: &Path,    // ★ NEW
) -> String;
```

注入される env exports(`JM_FLOW_UUID`/`JM_JOB_ID`/`JM_PARAM_*` の後、body の前):

```bash
# --- v2 job-manager dynamic path env (resolved at render time) ---
export JM_FLOW_DIR='/abs/.../<uuid>'
export JM_JOB_DIR='/abs/.../<uuid>/<job_id>'
```

**「絶対 path だが scaffold ではなく render の出力」**= folder 移動後 `jm render` で
再焼き込みされる = portable。`batch.bash` は `.jm/` 配下の program-managed area
(ユーザ編集対象外)で、`jm submit` / `jm render` から都度再生成される性質。

**PyO3 binding**(`py_export/render.rs`):同じ拡張を適用。Python 側
`render_batch_bash(flow_uuid, jid, parts, params, body, abs_flow_dir, abs_job_dir)`。
`.pyi` 再生成必要。

**caller**:`src/runner/flow.rs` `FlowRunner::submit` 内、`self.resolver` から
`flow_dir`/`flow_dir.join(jid.0)` を渡す。

### 4.3 per-task `common.toml` schema

`<flow_dir>/<job_id>/common.toml`(scaffold 時に書き込み、`jm new` 経路でのみ生成):

> **注:命名衝突あり**。本節 schema の `common.toml` は **job-manager 固有の
> per-task site config**(`scripts/<id>.py` が tomllib で直接読む)で、
> `gaussian-compute-runtime` の `--config` が指す `common.toml`(audit §6 / §13:
> `[slurm]` / `[slurm.resource_spec]` / `[env]` / `[gaussian_cmd]` 構造の
> `gaussian_job_shared.dataclasses.common.CommonConfig` serialized 形)とは
> **別ファイル / 別 schema** であり、内容に互換性はない(名前のみ偶然一致)。
> v2 swap-in 経路で runtime を呼ぶ場合、ユーザは runtime 用 common.toml を別途
> 用意する必要がある(v2 spec は scaffold しない = ユーザ責任)。
> v3 で曖昧さ解消が必要になれば v2 側を `jm-common.toml` に rename することを
> 検討(現状 v2 内部のみ使用のため衝突は実害なし)。

```toml
# Generated by `jm new <recipe>`. Per-task job-manager site config —
# read via tomllib by scripts/<id>.py. NOTE: this is NOT the runtime's
# --config schema (see audit §6 for the runtime form; names collide).
# WARNING: avoid absolute paths to flow_dir / sidecars — only cluster-fixed
# absolute paths (scratch_root, module bases) are allowed here.

launcher       = "srun"            # empty = bare
g16_cmd        = "g16"
scratch_root   = ""                # empty = $JM_JOB_DIR/.scratch fallback
                                   # cluster-fixed abs OK e.g. "/LARGE0/.../tmp"
conda_env      = "analysis"
module_profile = "gaussian_A"      # parse_g16_out: "default"
pixi_manifest  = ""                # empty = skip pixi hook
```

**禁止事項**:`flow_dir` / `<job_id>` 配下への絶対 path を書かない。env var
(`$JM_JOB_DIR`)経由で動的解決する。

**scripts での消費**(`scripts/run.py`):

```python
import os, tomllib  # py3.11+
JOB_DIR = os.environ["JM_JOB_DIR"]
with open(os.path.join(JOB_DIR, "common.toml"), "rb") as f:
    C = tomllib.load(f)
launcher     = C.get("launcher", "")
g16_cmd      = C.get("g16_cmd", "g16")
scratch_root = C.get("scratch_root") or os.path.join(JOB_DIR, ".scratch")
conda_env    = C.get("conda_env", "analysis")  # 注:bash 側で既に activate 済、参考用
```

(v1 では `JM_PARAM_LAUNCHER` 等を env var 経由で読んでいた。v2 では common.toml の
直接読み込みに移行 → site config の表現が表形式で人間可読、`jm render` 不要で
変更可能。)

`plan.toml` は v2 で **痩せる**:`route`/`charge`/`multiplicity`/`extra_input`/
`nproc`/`mem`/`compound` のみ残し、launcher/scratch_root/g16_cmd/conda_env/
module_profile/pixi_manifest は common.toml へ移管。`input_coordinate` は v1 と同じ
scaffold 時消費(plan には出ない)。

### 4.4 R4 path 解決(R3' supersede)

| 層 | v1(R3' = 廃止) | v2(R4) |
|---|---|---|
| `flow.toml` body | `bash "/abs/.../scripts/<id>.bash"`(R3' (a) 暫定) | `bash "$JM_JOB_DIR/scripts/<id>.bash"`(env-var-展開済み絶対起動子・scaffold 不変) |
| `scripts/<id>.bash` body_block | `python "/abs/.../scripts/<id>.py"` | `python "$JM_JOB_DIR/scripts/<id>.py"` |
| `scripts/run.py` 冒頭 | `JOB_DIR = "/abs/.../<uuid>/opt"`(R3' 絶対定数) | `JOB_DIR = os.environ["JM_JOB_DIR"]` |
| `batch.bash` env exports | `JM_FLOW_UUID`/`JM_JOB_ID`/`JM_PARAM_*` のみ | + `JM_FLOW_DIR`/`JM_JOB_DIR`(render 時動的) |
| `SbatchCmd.chdir` | `None`(変更検討対象 = (b)) | `None`(確定・不変) |

**R4 不変条件**:
- scaffold 出力(`flow.toml`/`plan.toml`/`scripts/*.py`/`scripts/*.bash`/
  `common.toml`/`input/*.gjf`)に flow_dir 絶対 path が出現しない(grep で 0 件)。
- `batch.bash` の `JM_FLOW_DIR`/`JM_JOB_DIR` は render 時のみ計算され、submit 時に
  `jm render` 再実行で再評価される。
- bash 変数展開(`"$JM_JOB_DIR/..."`)は SLURM ジョブ実行時に評価される — sbatch が
  script を spool コピーしても、コピー先で展開されるのは export 済みの絶対値
  (= render 時の絶対 path)で、cwd / spool location に依存しない。

**Folder 移動シナリオ**:
1. 旧 location で `jm render` → batch.bash に `export JM_JOB_DIR=/old/...` 焼かれる。
2. ユーザが flow_dir を新 location へ `mv`。
3. 新 location で `jm render <uuid>` → batch.bash 上書き、`export JM_JOB_DIR=/new/...`。
4. `jm submit <uuid>` → SLURM が新 absolute で起動。スクリプトも env 経由で新 location
   を解決。

(pending job が古い path を参照する場合は SLURM 側の問題で、案A v1 (a) や (b) でも
同じ。これは v2 のスコープ外。)

### 4.5 scratch ステージング(v1 §5.2 から差分)

`scripts/run.py` の scratch ロジック自体は v1 と同じ(prepare → g16(cwd=scratch)→
finally copy)。差分は `scratch_root` の取得経路のみ:

```python
# v1: scratch_root = os.environ.get("JM_PARAM_SCRATCH_ROOT") or <fallback>
# v2: common.toml から
scratch_root = C.get("scratch_root") or os.path.join(JOB_DIR, ".scratch")
```

`<scratch_root>/<JM_FLOW_UUID>/<JM_JOB_ID>/` の構造は v1 と同じ。

## 5. v2 JobTemplate 詳細

### `g16_opt`(差分)

`params()`(plan.toml `[jobs.opt]`):

| name | type | default | help |
|---|---|---|---|
| `route` | str | `#p opt b3lyp/6-31g(d)` | (v1 と同じ) |
| `charge` | int | `0` | |
| `multiplicity` | int | `1` | |
| `extra_input` | str | `` | |
| `nproc` | int | `8` | |
| `mem` | str | `8GB` | |
| `compound` | str | `REPLACE_ME-INCHIKEY` | |
| `input_coordinate` | path | `` | (v1 と同じ scaffold 時消費) |

**v1 から削除(common.toml へ移管)**:`g16_cmd`/`conda_env`/`module_profile`/
`pixi_manifest`/`launcher`/`scratch_root`。

sidecars(v2):
- `scripts/<id>.bash`(`base_preamble()` 出力、v1 と同じ)。
  - `module_block` は scaffold 時 default を埋め込み(common.toml 編集後の自動同期は
    しない — common.toml 変更時はユーザが bash を手動編集 or 再 scaffold)。
  - `body_block` = `python "$JM_JOB_DIR/scripts/run.py"`(env-var-展開、scaffold 不変)。
- `scripts/run.py`(env-var-base、cwd 非依存、`os.environ["JM_JOB_DIR"]` で全 path
  解決 + `os.path.join(JOB_DIR, "common.toml")` で site config 読み込み)。
- `input/main.gjf`(v1 と同じ)。
- **NEW** `common.toml`(per-task site config、§4.3)。

flow.toml body(v2):

```toml
[jobs.opt]
program = "g16"
body = """bash "$JM_JOB_DIR/scripts/opt.bash"
"""
```

(bash 変数展開、scaffold 時には文字列リテラル `$JM_JOB_DIR` がそのまま残る = 絶対
path 焼き込みなし。)

### `parse_g16_out`(差分)

`params()` は v1 から **痩せ**:`conda_env`/`pixi_manifest` も common.toml へ。
v2 では plan.toml に `[jobs.parse]` は **空 table**(`{}`)になる(plan params が
存在しない場合の最小形式)。

`scripts/parse.py`(env-var-base):

```python
import os, tomllib
JOB_DIR = os.environ["JM_JOB_DIR"]
gaussian_out = os.path.normpath(os.path.join(JOB_DIR, "../opt/output/main.out"))
# (wiring の relative input path は v1 と同じく scaffold 時 sentinel swap-in、
#  ただし JOB_DIR 起点で解決 = folder portable)
```

### FlowRecipe `g16-opt-parse`(v2)

`nodes`/`edges`/`wiring` 構造は v1 と同じ。生成 `flow.toml`/`plan.toml` は §4.3 の
schema 痩せに合わせて更新。

生成 `flow.toml`(v2):

```toml
uuid="<uuid>"
created_at="<rfc3339>"
[tags]
recipe="g16-opt-parse"
compound="<opt.compound>"

[jobs.opt]
program="g16"
body="""bash "$JM_JOB_DIR/scripts/opt.bash"
"""
[jobs.opt.config]
partition="REPLACE_ME"
time_limit="48:00:00"

[jobs.parse]
program="python"
body="""bash "$JM_JOB_DIR/scripts/parse.bash"
"""
[[jobs.parse.parents]]
from="opt"
kind="afterok"
[jobs.parse.config]
partition="REPLACE_ME"
time_limit="01:00:00"
```

生成 `plan.toml`(v2、痩せ後):

```toml
[jobs.opt]
route="#p opt b3lyp/6-31g(d)"
charge=0
multiplicity=1
extra_input=""
nproc=8
mem="8GB"
compound="REPLACE_ME-INCHIKEY"

[jobs.parse]
```

(`parse` は plan params なし → 空 table。`flow JobId 集合 == plan キー集合` 不変条件は
維持 — 集合等価で値の有無は問わない。)

per-task common.toml(opt, parse 各):

```toml
# <flow_dir>/opt/common.toml
launcher       = "srun"
g16_cmd        = "g16"
scratch_root   = ""
conda_env      = "analysis"
module_profile = "gaussian_A"
pixi_manifest  = ""
```

```toml
# <flow_dir>/parse/common.toml
launcher       = ""
g16_cmd        = ""             # parse は g16 を呼ばないので無視されるが schema 一様性のため
scratch_root   = ""             # 同上
conda_env      = "analysis"
module_profile = "default"
pixi_manifest  = ""
```

## 6. 既存 v1 flow との後方互換

v1 で scaffold した既存 flow(`<root>/<uuid>/`)は:
- `flow.toml` body は `bash "/abs/.../scripts/<id>.bash"`(v1 (a) 形式)。
- `scripts/run.py` 冒頭は `JOB_DIR = "/abs/.../<uuid>/opt"`(v1 R3' 焼込)。

v2 を実装した job-manager で `jm render <v1-uuid>` を実行:
- batch.bash に `JM_FLOW_DIR`/`JM_JOB_DIR` が**追加** export される。
- ただし scripts は env var を読まない(v1 焼込絶対定数を使う)→ env var 注入は
  **無害 no-op**、v1 挙動は完全に保持される。
- `jm submit` も同じく動作変化なし(scripts 内部 cwd 非依存性は焼込定数で達成済)。

**v1 → v2 移行は強制しない**。新規 scaffold は v2 形式で出るが、既存 flow は
**触らない限り v1 のまま動く**。

ユーザが folder 移動したい既存 v1 flow を新 location に portable にするには
`jm new g16-opt-parse` で **新規 scaffold + 入力ファイル手動コピー** を推奨
(v3 で `jm migrate v1→v2` を検討)。

## 7. `blank` FlowRecipe(後方互換 v2)

v1 同様、`build_flow_template` / `build_plan_template` は bytewise 不変。`blank` は
v2 でも `JM_FLOW_DIR`/`JM_JOB_DIR` の env export を受け取るが、blank の body は
ユーザ自由なので影響なし(env を参照しない body は env を無視する)。

`blank` には per-task common.toml は **生成しない**(`jm new` が
`recipe_name == "blank"` 分岐で skip)。これも v1 バイト同値保証。

## 8. エラーハンドリング

v1 から差分のみ:

| 状況 | 挙動 |
|---|---|
| (実行時)`common.toml` 不在 | `scripts/run.py` が `FileNotFoundError` → 非ゼロ + stderr に明示メッセージ |
| (実行時)`common.toml` 不正 TOML | tomllib `TOMLDecodeError` 伝播 → 非ゼロ + stderr |
| (実行時)`JM_JOB_DIR` 未 export | `KeyError` → 非ゼロ + stderr に「v2 expects render-injected JM_JOB_DIR」 |
| (scaffold)`common.toml` 書込失敗 | `cmd_new` rollback(`flow_dir` を `remove_dir_all`)|
| (scaffold)flow_dir 絶対 path が scaffold 出力に出現 | テスト(§9)が CI で検出 |

v1 のエラーケース(input_coordinate 不在/`flow_dir` 既存/sidecar 書込失敗/
`srun`/g16 PATH 不在/`cclib` 不在)は v1 と同じ挙動。

## 9. テスト

### ユニット

- `render_batch_bash`(`src/render/mod.rs`):新シグネチャで `JM_FLOW_DIR`/
  `JM_JOB_DIR` の export 行が出ることを assert。`abs_flow_dir`/`abs_job_dir` が
  bash quote される。既存 `JM_FLOW_UUID`/`JM_JOB_ID`/`JM_PARAM_*` の動作不変。
- `g16_opt.instantiate` / `parse_g16_out.instantiate`:
  - body が `bash "$JM_JOB_DIR/scripts/<id>.bash"` (literal `$JM_JOB_DIR` 含む) を
    assert、絶対 path 焼込なしを負 assert。
  - `scripts/run.py` 冒頭が `JOB_DIR = os.environ["JM_JOB_DIR"]` を含み
    `JOB_DIR = "/...` のような絶対 path 焼込なしを負 assert。
  - `common.toml` sidecar が生成され、schema(launcher/g16_cmd/scratch_root/
    conda_env/module_profile/pixi_manifest)を含む。
  - `plan.toml` `[jobs.opt]` から `launcher`/`scratch_root`/`g16_cmd`/`conda_env`/
    `module_profile`/`pixi_manifest` が消えていることを assert。
- `recipes::flow::assemble`:`Assembled.sidecars` に `<id>/common.toml` が含まれる。
- **R4 不変条件回帰ガード**:scaffold 出力全体に flow_dir 絶対 path が出現しない
  ことを assert(`assemble_default()` で `flow_dir = /work/root/<uuid>` を渡し、
  生成された全 sidecar contents + flow.toml + plan.toml + common.toml に
  `/work/root/<uuid>` 文字列が出現しないか)。

### Python smoke(`python/tests/test_recipe_run_py.py` / `test_recipe_parse_py.py`)

v2 形式 fixture に refactor:
- `_materialize` を env-var 注入対応に。`run.py` / `parse.py` template の
  `JOB_DIR = os.environ["JM_JOB_DIR"]` を fixture でもそのまま使い、
  `subprocess.run(..., env={"JM_JOB_DIR": str(job), ...})` で起動。
- `common.toml` を fixture で書き込み、scripts が tomllib で読むことを検証。
- v1 fixture(`{{JOB_DIR}}` sentinel swap-in)は **削除**(v1 scaffold モードを
  recipes が出さなくなるため、回帰の意味を失う)。

### 統合(`tests/integration_new_recipes.rs`)

- v2 scaffold 後:
  - `<flow_dir>/<job_id>/common.toml` の存在 + schema 確認。
  - `flow.toml` body に `bash "$JM_JOB_DIR/scripts/opt.bash"` literal 含有。
  - `flow.toml` 全体に `<flow_dir>` 絶対 path 文字列が含まれないことを assert
    (= R4 不変条件)。
  - `scripts/run.py` の `JOB_DIR = os.environ["JM_JOB_DIR"]` 含有 + 絶対 path 焼込
    なしを assert。
- `jm render <uuid>` 後:
  - batch.bash に `export JM_FLOW_DIR='<abs>'` / `export JM_JOB_DIR='<abs>'` 含有。
  - **folder 移動 test**:scaffold → `mv <flow_dir> <new>` → 新 location で
    `jm render <uuid>`(新 root + uuid 指定)→ 新 batch.bash の `JM_JOB_DIR` が
    新 location。古い絶対 path は scaffold 出力に出現しないことも確認。

### CLI smoke(`tests/cli_smoke.rs`)

`jm --help` で recipe 関連オプション(`--list`/`--describe`)が表示されること(v1 と同じ)。

## 10. CLAUDE.md 準拠 / 上流変更

- **A1 / D2 不変**(A1 immutable / D2 modifiable but not modified in v2)。
- **`render_batch_bash` 公開シグネチャは v2 で拡張**(`abs_flow_dir`/`abs_job_dir`
  追加)— minor breaking change、`.pyi` 再生成必要。spec §11(v1)で「render 不変」と
  書いた契約は v2 で **明示的に更新**。
- **`SbatchCmd.chdir` は不変**(`None` 据置)。`flow.rs` cwd 契約も不変。
- **PyO3**:`py_export/render.rs` も同じ signature 拡張。Pyclass Single Owner 非抵触。
- `jm --no-default-features` ビルド維持(`recipes` モジュールは pyo3 非依存)。
- Out of scope は v1 と同じ。
- Conventional Commits / per-task commits / stacked PRs(本 spec を land した後、
  実装は段階 PR で land):
  1. `feat(render)`: `render_batch_bash` シグネチャ拡張 + JM_FLOW_DIR/JM_JOB_DIR 注入
  2. `feat(recipes)`: common.toml schema + 生成 + scripts env-var 化
  3. `refactor(recipes)`: plan.toml から site config 抽出
  4. `test`: R4 不変条件 + folder 移動回帰
  5. `docs`: v2 spec 確定 + README/`docs/recipes.md` 更新
  6. `chore`: stub_gen + ruff format

## 11. トレードオフ要約

| 論点 | 採用 | 却下 | 理由 |
|---|---|---|---|
| Path 解決 | **R4(env-var-base)** | R3'(scaffold 焼込)/ (a) 絶対 body / (b) per-job chdir | folder portable + core 不変 + cwd 非依存 |
| `render_batch_bash` シグネチャ | **拡張(abs paths 追加)** | render の外で env を後付け / `PathResolver` 注入 | render の責務として自然、最小 minor breaking |
| Site config 配置 | **per-task `common.toml`** | per-flow common.toml / D2 schema 追加 | spec §4.2 v2 box 既定、folder 内自己完結、D2 不変 |
| common.toml 中身 | **key=value(flow_dir 絶対 path 禁止、site fixed 絶対 path 許容)** | env var 経由完全注入(common.toml 内に絶対 path 一切なし) | サイト固有 cluster path(`/LARGE0/...`)は config 表現が自然、folder 移動と独立 |
| Body 起動形 | **`python "$JM_JOB_DIR/scripts/<id>.py"`(env-var-展開済み絶対起動子、scaffold 不変)** | 相対 `python scripts/<id>.py` + chdir(b) | core 不変 + cwd 非依存 + portable のトリプル達成 |
| gem stack | **任意 swap-in(`# REPLACE_ME` 維持、現状 γ のみ)** | v2 デフォルト = `python -m gaussian_compute_runtime`(全 subcommand 自動切替) | v1 self-contained 哲学維持、未導入サイトでも動く + 2026-05-20 時点で run-g16/parse-results は B-α migration 待ち(audit §12) |
| 既存 v1 flow | **不変動作(no-op env injection)+ 移行は手動再 scaffold** | 自動 migration | YAGNI、env injection が無害 no-op で済む |

## 12. v1 (案A spec) からの差分まとめ

| 項目 | v1(案A spec 2026-05-18) | v2(本 spec 2026-05-20) |
|---|---|---|
| Path 解決 | R3'(scaffold 絶対 JOB_DIR 焼込)+ (a) 暫定絶対 body | **R4**(scaffold 絶対 path 一切無し、env-var-base 動的解決) |
| `JM_*_DIR` env | 無し | `JM_FLOW_DIR` / `JM_JOB_DIR`(render 時注入) |
| `render_batch_bash` | (flow_uuid, jid, parts, params, body) | + `abs_flow_dir` / `abs_job_dir` |
| `SbatchCmd.chdir` | `None`(変更検討対象 = (b)) | `None`(確定・不変) |
| Site config 配置 | `plan.toml` per-step param | per-task `common.toml`(`<flow_dir>/<job_id>/common.toml`) |
| `plan.toml` 中身 | site config + plan params 混在 | plan params のみ(痩せる) |
| `scripts/run.py` | 冒頭 `JOB_DIR = "/abs..."`(scaffold 焼込) | 冒頭 `JOB_DIR = os.environ["JM_JOB_DIR"]` |
| `flow.toml` body | `bash "/abs/.../scripts/<id>.bash"`((a) 暫定) | `bash "$JM_JOB_DIR/scripts/<id>.bash"`(env 展開) |
| Folder 移動 | 焼込絶対 path で破綻 | `jm render` 再実行で portable |
| 既存 v1 flow | — | 不変動作(env injection が no-op)、移行は手動再 scaffold |
| gem stack | 任意 swap-in(`# REPLACE_ME`) | 同じ(維持) |
| `JM_PARAM_LAUNCHER`/`JM_PARAM_SCRATCH_ROOT`/`JM_PARAM_G16_CMD` | render が export | **削除**(common.toml 直読み化) |

## 13. v3 拡張点

- `jm migrate v1→v2`:既存 v1 flow を自動で v2 形式に書換(`scripts/*.py` の
  `JOB_DIR` を env-var 化、`plan.toml` から site config 抽出 → `common.toml` 生成)。
- `jm render --rescaffold`:common.toml 変更後に scripts を再生成(現在は手動編集)。
- D2 `CommonConfig.launcher` / `[directories] scratch_root`(= 旧 rev.7 idea):
  サイト一元化が必要になった時点で coordinated PR で D2 拡張。per-task common.toml の
  fallback 階層として組み込む(per-task > per-flow > D2 default)。
- `python -m gaussian_compute_runtime` を **v3 デフォルト**(B-α migration 完了で
  `run-g16` / `parse-results` が swap-in 可能になり、gem stack 導入が広まった時点。
  audit §12 の "Highest rewrite risk" 第 2/3 項クリアが前提)。
- `common.toml` の env var interpolation(`scratch_root = "$SCRATCH/g16"` のような
  シェル風展開)— 現状は plain literal のみ。
