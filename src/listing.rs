//! `jm ls` — pure projection/aggregation/formatting for cross-flow listing.

use std::collections::BTreeSet;

use crate::job::lifecycle::Lifecycle;

/// Display-time lifecycle: the 5 `Lifecycle` values plus `Pending`
/// (no `status.toml` on disk — not a real enum value).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DisplayLifecycle {
    Pending,
    Real(Lifecycle),
}

impl DisplayLifecycle {
    /// Short code shown in the `ST` column.
    pub fn code(self) -> &'static str {
        match self {
            DisplayLifecycle::Pending => "PD",
            DisplayLifecycle::Real(Lifecycle::Queued) => "Q",
            DisplayLifecycle::Real(Lifecycle::Running) => "R",
            DisplayLifecycle::Real(Lifecycle::Success) => "OK",
            DisplayLifecycle::Real(Lifecycle::Failed) => "F",
            DisplayLifecycle::Real(Lifecycle::Skipped) => "SK",
        }
    }

    /// Long machine-readable name (used in `--json`).
    pub fn long(self) -> &'static str {
        match self {
            DisplayLifecycle::Pending => "pending",
            DisplayLifecycle::Real(Lifecycle::Queued) => "queued",
            DisplayLifecycle::Real(Lifecycle::Running) => "running",
            DisplayLifecycle::Real(Lifecycle::Success) => "success",
            DisplayLifecycle::Real(Lifecycle::Failed) => "failed",
            DisplayLifecycle::Real(Lifecycle::Skipped) => "skipped",
        }
    }

    /// Parse one token: short code or long name, case-insensitive.
    pub fn parse_token(s: &str) -> Result<DisplayLifecycle, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "pd" | "pending" => Ok(DisplayLifecycle::Pending),
            "q" | "queued" => Ok(DisplayLifecycle::Real(Lifecycle::Queued)),
            "r" | "running" => Ok(DisplayLifecycle::Real(Lifecycle::Running)),
            "ok" | "success" => Ok(DisplayLifecycle::Real(Lifecycle::Success)),
            "f" | "failed" => Ok(DisplayLifecycle::Real(Lifecycle::Failed)),
            "sk" | "skipped" => Ok(DisplayLifecycle::Real(Lifecycle::Skipped)),
            other => Err(format!(
                "unknown status {other:?} (expected one of \
                 pd,q,r,ok,f,sk / pending,queued,running,success,failed,skipped)"
            )),
        }
    }
}

/// Parse a comma-separated `--status` value into a set. Empty/blank input
/// yields an empty set (= no status filter). Whitespace around tokens is
/// trimmed; empty tokens (e.g. trailing comma) are ignored.
pub fn parse_status_set(csv: &str) -> Result<BTreeSet<DisplayLifecycle>, String> {
    let mut out = BTreeSet::new();
    for tok in csv.split(',') {
        if tok.trim().is_empty() {
            continue;
        }
        out.insert(DisplayLifecycle::parse_token(tok)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("PD", DisplayLifecycle::Pending)]
    #[case("pending", DisplayLifecycle::Pending)]
    #[case("Q", DisplayLifecycle::Real(Lifecycle::Queued))]
    #[case("F", DisplayLifecycle::Real(Lifecycle::Failed))]
    #[case("R", DisplayLifecycle::Real(Lifecycle::Running))]
    #[case("Running", DisplayLifecycle::Real(Lifecycle::Running))]
    #[case("ok", DisplayLifecycle::Real(Lifecycle::Success))]
    #[case("SK", DisplayLifecycle::Real(Lifecycle::Skipped))]
    fn parse_token_accepts_code_and_long_case_insensitive(
        #[case] input: &str,
        #[case] expected: DisplayLifecycle,
    ) {
        assert_eq!(DisplayLifecycle::parse_token(input).unwrap(), expected);
    }

    #[test]
    fn parse_token_rejects_unknown() {
        let err = DisplayLifecycle::parse_token("xyz").unwrap_err();
        assert!(err.contains("unknown status"), "got: {err}");
    }

    #[test]
    fn code_and_long_round_trip_for_every_variant() {
        for dl in [
            DisplayLifecycle::Pending,
            DisplayLifecycle::Real(Lifecycle::Queued),
            DisplayLifecycle::Real(Lifecycle::Running),
            DisplayLifecycle::Real(Lifecycle::Success),
            DisplayLifecycle::Real(Lifecycle::Failed),
            DisplayLifecycle::Real(Lifecycle::Skipped),
        ] {
            assert_eq!(DisplayLifecycle::parse_token(dl.code()).unwrap(), dl);
            assert_eq!(DisplayLifecycle::parse_token(dl.long()).unwrap(), dl);
        }
    }

    #[test]
    fn parse_status_set_splits_csv_and_ignores_blanks() {
        let s = parse_status_set("running, F ,").unwrap();
        assert_eq!(s.len(), 2);
        assert!(s.contains(&DisplayLifecycle::Real(Lifecycle::Running)));
        assert!(s.contains(&DisplayLifecycle::Real(Lifecycle::Failed)));
        assert!(parse_status_set("").unwrap().is_empty());
        assert!(parse_status_set("  ").unwrap().is_empty());
    }

    #[test]
    fn parse_status_set_propagates_token_error() {
        assert!(parse_status_set("running,nope").is_err());
    }
}
