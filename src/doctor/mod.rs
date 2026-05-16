//! `jm doctor` — validate a `<root>` tree's TOML files and structural
//! invariants before `render`/`submit`.

pub mod report;

pub use report::{DoctorReport, Finding, Severity};
