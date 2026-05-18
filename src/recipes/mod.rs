//! `jm new <flow-recipe>` の二層レシピ(Job 層 / Flow 層)。
//!
//! pyo3 非依存・純 Rust(`minijinja`/`toml`/`uuid`/`chrono`/std のみ)。
//! `jm --no-default-features` でクリーンビルドされる。

pub mod flow;
pub mod flows;
pub mod job;
pub mod jobs;
pub mod xyz;

pub use flow::{Assembled, assemble};
pub use flows::G16OptParse;
pub use job::{
    FlowRecipe, GeneratedFile, JobArtifacts, JobCtx, JobTemplate, PreambleOpts, RecipeError,
    RecipeParam, RecipeParamType, base_preamble,
};
