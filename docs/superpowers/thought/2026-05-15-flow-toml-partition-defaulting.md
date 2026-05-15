# flow.toml の `partition` 重複をどう解消するか

**Date:** 2026-05-15
**Status:** thought / exploratory (not a spec, not a plan)
**Scope:** `examples/simple/inputs/**/flow.toml` と `common.toml` の `partition` 二重記述

これは設計議論メモであり、決定事項でも実装プランでもない。具体化する場合は
`docs/superpowers/specs/` 配下に SP 番号付きで切り出すこと。

---

## 1. 問題設定

`examples/simple` 等のセットアップでは、ユーザーは少なくとも以下2か所で `partition`
（と関連の SLURM 値）を書き換える必要がある:

- `examples/simple/inputs/common.toml` の `[slurm_default] partition`
- `examples/simple/inputs/<flow_uuid>/flow.toml` の `[jobs.*.config] partition`

加えて将来増える `examples/sweep` や、ユーザーが自分のクラスタで使い回す flow.toml
ごとに同じ書き換えが要る。1ファイルに集約したい、という動機。

## 2. 検討した2案（初期）

### 案A: デフォルト化（common を fallback）

flow.toml で `partition` を省略 → `common.toml` の値を使う。

### 案B: boilerplate 生成コマンド

`jm new-flow --partition long ...` のような CLI を追加し、雛形を吐く。

### 初期評価（誤りを含む）

- 案A: 「`merge_with_defaults` は既に `partition.is_empty()` で common にフォールバックしている。`JobSpec::config` に `#[serde(default)]` を足せば終わり。軽い」
- 案B: 「実装コスト大。後から partition を swap する手間は減らない。`cp -r` で十分なバリエーション数しかまだ無い」

→ **A を採用、B は後でいい** と一度結論を出した。

## 3. 訂正: A1 の型契約

ユーザー指摘「`slurm` の spec は変えられない、partition は REQUIRED では？」を受けて
A1 (`slurm_async_runner` rev `a734a06`) の実物を確認:

```rust
// src/entities/slurm/sbatch_options.rs:170-217
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlurmJobConfig {
    /// queue of job. It is required thing
    pub partition: JobPartition,           // Option ではなく #[serde(default)] も無い

    #[serde(default)] pub time_limit: Option<JobTimeLimit>,
    #[serde(default)] pub log_stdout:  Option<PathBuf>,
    // ... 他は全部 Option + #[serde(default)]
}
```

確定事項:

- A1 の `SlurmJobConfig::partition` は **型として必須**。`Option` でも `#[serde(default)]` でもない。
- 型は `String` ではなく `JobPartition` newtype。
- SLURM 本体は `DefaultPartition` がクラスタに設定されていれば `--partition` 省略可だが、A1 はその逃げ道を型で塞いでいる。

**初期 A 案が「軽い」と評価したのは誤り**:

- `[jobs.*.config]` ブロックごと省略可能化するには `SlurmJobConfig::default()` が必要。
- `JobPartition` が `Option` でない以上、A1 の `Default` 実装を足すか、`partition: Option<JobPartition>` に変えるか、どちらも A1 改修を伴う。
- A1 は「partition is required」を契約として明示しているので、その契約を破る方向は望ましくない。

## 4. 改訂案 A': job-manager 側に Partial ラッパー

A1 を変えず、flow.toml の入力スキーマだけ別型に分ける案。

```rust
// job-manager 側で定義（中間表現）
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PartialSlurmJobConfig {
    pub partition: Option<JobPartition>,    // ここだけ Optional 化
    pub time_limit: Option<JobTimeLimit>,
    // ... 他は A1 と同じく Option<...>
}
```

`merge_with_defaults` を `(CommonConfig, PartialSlurmJobConfig) -> SlurmJobConfig`
に変更、`partition` は `override.partition.unwrap_or_else(|| common.slurm_default.partition.clone())`
で決定。`is_empty()` 空文字 sentinel が消える。

### A' の評価

- Pros: A1 を一切変えない / Pyclass Single Owner も維持 / `[jobs.*.config]` 完全省略可能 / 空文字 sentinel が Option に置き換わって意味論クリーン
- Cons:
  - **公開 API に新型が増える**: `PartialSlurmJobConfig` を `JobSpec.config` の型に使うと、`persistence/flow.rs`, `flow/run.rs`, `flow/topology.rs`, `view.rs`, `search.rs` のテストファクトリと外部利用が全部この型を経由する。
  - **shared package 方針との緊張**: 「non-platform domain で独自構造を作らない」というメモ
    (`feedback_use_shared_package_definitions.md`)
    と一見ぶつかる。`PartialSlurmJobConfig` は newtype を消す行為ではなく「再構成用の中間構造」だが、説明が要る。
  - Round-trip は自然に保てる（書き戻し時も `PartialSlurmJobConfig` で書けば「common 由来」を保存できる）。

## 5. ファクトリ案

ユーザーから「Factory パターンで flow.toml をパースして、省略時に common の partition を入れる方式はどうか」という問い。

これは A' の対立案ではなく、**境界をどこに引くかの違い** と整理できる。

### 実装ルート2系統

#### F1: 生 TOML を context patching（新規型ゼロ）

```rust
pub fn read_flow_factory(
    path: &Path,
    common: &CommonConfig,
) -> Result<JobFlow, JobManagerError> {
    let mut v: toml::Value = toml::from_str(&fs::read_to_string(path)?)?;
    for (_, job) in v["jobs"].as_table_mut()?.iter_mut() {
        let cfg = job.as_table_mut()?
            .entry("config")
            .or_insert_with(|| toml::Value::Table(Default::default()));
        if !cfg.as_table()?.contains_key("partition") {
            cfg.as_table_mut()?.insert(
                "partition".into(),
                toml::Value::String(common.slurm_default.partition.to_string()),
            );
        }
    }
    Ok(v.try_into()?)   // ここで初めて SlurmJobConfig deserialize（partition 補完済み）
}
```

- `JobSpec.config: SlurmJobConfig` のまま。新規型ゼロ。
- `[jobs.*.config]` ブロック自体が無いケースは `or_insert_with` で空テーブルを足して
  patch する二段処理。

#### F2: モジュール内 Partial + ファクトリ関数（実質 A' を private 化）

```rust
mod intern {
    #[derive(Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    pub(super) struct PartialSlurmJobConfig { /* 全 Option */ }
}

pub fn read_flow_factory(path, common: &CommonConfig) -> Result<JobFlow, _> {
    let raw: RawJobFlow<intern::PartialSlurmJobConfig> = toml::from_str(&...)?;
    Ok(raw.materialize(common))
}
```

- Partial 型は `pub(super)` で persistence モジュールに閉じ、**外には出さない**。
- 公開境界は `read_flow_factory` 関数のみ。呼び出し側からは `JobFlow<SlurmJobConfig>` しか見えない。

### A' との比較（F2 = "private 化された A'"）

| 観点 | F1 | F2 | A' (Partial 公開) |
|---|---|---|---|
| 新規公開型 | 0 | 0 | 1 (`PartialSlurmJobConfig`) |
| parse 時の `CommonConfig` 依存 | 必須 | 必須 | 不要（merge 時に依存） |
| `[jobs.*.config]` 完全省略 | 二段処理が必要 | `Default` derive で自動 | `Default` derive で自動 |
| 型安全性（境界点） | `toml::Value` を直接いじる | 型で守られる | 型で守られる |
| Round-trip 保持 | 失う（injected 値が混入） | 失う | 自然に保たれる |
| 5〜6ファイル波及 | なし | なし | あり |
| shared 方針との衝突 | なし | 説明1行で済む | 説明が要る |

### ファクトリ案の Pros

1. **公開 API が変わらない**: 呼び出し側・PyO3 経由 Python・ダウンストリームのテストファクトリは全て `SlurmJobConfig` のまま。A' の最大の Cons（5〜6ファイルへの波及）が**入口だけに局所化**される。
2. **shared package 方針と衝突しない**: A1 の `SlurmJobConfig` をそのまま使う側に立つ。
3. **A1 の "partition is required" 契約をそのまま尊重**: `SlurmJobConfig` のインスタンスが存在する時点で必ず partition が決まっている、というインバリアントが型レベルで保たれる（"半構築" の SlurmJobConfig が型システムに存在しない）。
4. **境界が1点に集約**: 「flow.toml 入力に対するデフォルト適用」が `read_flow_factory` 1関数に集まる。将来 `time_limit` 等を同じ機構で補いたくなったときの追加箇所が明確。
5. **既存の `merge_with_defaults` を呼び続けられる**（F1）/ Option ベースに書き換えてクリーンになる（F2）。

### ファクトリ案の Cons

1. **read 順序の強制**: common.toml を先に読む必要がある。`read_common` と `read_flow` の独立な対称性が崩れる。`Option<&CommonConfig>` 引数で緩和可能だが、エラー設計が要る。
2. **Round-trip 非対称**: `read_flow_factory` で injection した値を `write_flow` で書き戻すと、`common.toml` の値が `flow.toml` にハードコードされて出る。実コード上 `write_flow` を呼ぶのは主にテスト（`roundtrip_write_read_recovers_jobflow`）なので運用上の被害は限定的だが、明示が必要。
3. **F1 は型安全性が下がる**: `toml::Value` の `as_table_mut()` を剥がす操作が `deny_unknown_fields` の保護圏外に入る。F2 ならこれは無い。
4. **`[jobs.*.config]` 完全省略**: F1 は二段処理、F2 は `Default` 一発。**ユーザーが config テーブル丸ごと書かない運用を許したいなら F2**。
5. **テスト整備コスト**: 既存テストの `sample_config()` 群は `SlurmJobConfig` 直接構築なのでファクトリ経路を通らない。ファクトリ自体のテストには `CommonConfig` + TOML 文字列を組み立てるパターンが新規に要る（A' 同様）。
6. **sentinel 検出は別途必要**: ファクトリは「flow 側省略 → common から補う」だけ。common 側の `REPLACE_ME` を弾く責務は持たない。submit 直前の `is_sentinel(partition)` チェックが別途要る（A 系全案で共通）。

## 6. 結論

A1 の "partition is required" を**契約として字面通り尊重する**なら、
**F2（内部 Partial + ファクトリ関数）** が最も筋が良い。

理由:

- F1 は新規型ゼロが魅力だが、`toml::Value` を直接いじる箇所が型安全性の保護圏外で、
  「config テーブル丸ごと省略」を許そうとすると手続き的になる。A1 が型で守ろうとしているものを部分的に台無しにする方向。
- F2 は **本質的に A' と等価** だが、Partial 型を `pub(super)` で閉じ、公開境界を
  「ファクトリ関数」に限定する点で、A' の最大の Cons（公開 API への新型追加 / shared 方針との緊張 / 5〜6ファイル波及）を**全て消せる**。
- B 案（boilerplate 生成）は、F2 を入れて flow.toml が短くなれば `cp -r` で十分なケースが大半。将来必要なら独立に足せばよい。

唯一残る論点は round-trip 問題で、これは「**flow.toml は人間が書く入力ファイルであり、
job-manager が機械的に書き戻すものではない**」という運用ポリシーを CLAUDE.md に1行入れて、
`write_flow` をテスト・デバッグ専用と位置付けるのが妥当。実際 submit/render/tick の主要パスは
flow.toml を read のみで使用しており、書き戻しているのは `roundtrip_write_read_recovers_jobflow`
テスト程度しか無い。

## 7. 進めるなら（spec/plan 化する際の骨子）

1. `persistence/flow.rs` に `pub(super) struct PartialSlurmJobConfig` + `Default` 派生
2. `read_flow` を `read_flow(path, &CommonConfig) -> JobFlow` に変更
3. `merge_with_defaults` を `(CommonConfig, PartialSlurmJobConfig) -> SlurmJobConfig` に書き換え、`is_empty()` 分岐を `Option` ベースに置換
4. `examples/simple/inputs/01999999-.../flow.toml` から `[jobs.opt.config]` / `[jobs.freq.config]` を削除
5. submit 直前に partition が `REPLACE_ME` 等 sentinel と一致したら明示エラー（A 系共通）
6. CLAUDE.md の "Workflow conventions" 付近に "flow.toml は read-only な入力ファイル。`write_flow` はテスト・デバッグ専用" を1行追加
7. Round-trip テスト (`roundtrip_write_read_recovers_jobflow`) のセマンティクスを再定義（ファクトリ経由ではなく直接 `SlurmJobConfig` を持つ `JobFlow` で round-trip させる、等）

## 8. 未確定 / 要検証

- `JobPartition` newtype のシリアル表現（plain string か、`{ value = "..." }` ラッパーか）。F1 で `toml::Value::String(...)` を直接挿入するなら確認必須。`Deserialize` derive が `String` 互換ならそのまま、そうでなければ F2 推奨度がさらに上がる。
- D2 (`gaussian_job_shared`) 側で `JobSpec`/`JobFlow` がどう定義されているか。`config: SlurmJobConfig` を job-manager 側で差し替え可能か（型パラメータ化されているか、固定か）が F2 実装の容易さを左右する。
- `write_flow` の現用ユースケース全件。submit/render/tick の挙動と examples 配下のサンプルを横断確認。

## 9. 参照

- A1 `SlurmJobConfig`: `slurm_async_runner` rev `a734a06`,
  `src/entities/slurm/sbatch_options.rs:170-217`
- 現行 merge: `src/persistence/common.rs:27-67` (`merge_with_defaults`)
- 現行 read/write: `src/persistence/flow.rs`, `src/persistence/common.rs`
- 利用箇所: `src/flow/run.rs:67` (`effective_config`), `src/runner/flow.rs:207`
- examples: `examples/simple/inputs/{common.toml,01999999-.../flow.toml}`
- ユーザーフィードバック: `feedback_use_shared_package_definitions.md`
  （shared から import、独自構造を避ける）
