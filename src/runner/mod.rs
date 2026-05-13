//! Orchestration layer.

pub mod transition;

pub use transition::{Decision, TickResult, decide_transition, tick_many};
