//! Persistence layer — all TOML file I/O lives here.
//!
//! Submodules are organized by file kind (one TOML schema per submodule).

pub mod path;

pub use path::PathResolver;
