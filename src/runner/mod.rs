//! Orchestration layer.

pub mod flow;
pub mod transition;

pub use flow::FlowRunner;
pub use transition::{Decision, TickResult, decide_transition};
