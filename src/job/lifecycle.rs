//! Lifecycle — per-job state machine.
//!
//! Pending は enum value にしない (ファイル不在で表現)。

// Variant order is the state-machine progression (Queued < Running < terminal).
// `Ord`/`PartialOrd` are derived so `Lifecycle` (and `DisplayLifecycle`) can be
// stored in a `BTreeSet` and used by listing aggregation; the relative order of
// the terminal variants (Success/Failed/Skipped) carries no domain meaning.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    Queued,
    Running,
    Success,
    Failed,
    Skipped,
}

impl Lifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Lifecycle::Success | Lifecycle::Failed | Lifecycle::Skipped
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&Lifecycle::Queued).unwrap(),
            "\"queued\""
        );
        assert_eq!(
            serde_json::to_string(&Lifecycle::Success).unwrap(),
            "\"success\""
        );
        assert_eq!(
            serde_json::to_string(&Lifecycle::Skipped).unwrap(),
            "\"skipped\""
        );
    }

    #[test]
    fn deserialize_rejects_pascal_case() {
        let result: Result<Lifecycle, _> = serde_json::from_str("\"Queued\"");
        assert!(result.is_err(), "PascalCase should be rejected");
    }

    #[test]
    fn is_terminal_marks_terminal_states() {
        assert!(!Lifecycle::Queued.is_terminal());
        assert!(!Lifecycle::Running.is_terminal());
        assert!(Lifecycle::Success.is_terminal());
        assert!(Lifecycle::Failed.is_terminal());
        assert!(Lifecycle::Skipped.is_terminal());
    }
}
