//! batch.bash render — env-export style with POSIX single-quote escaping.

/// Convert an axis name or param key into a bash-safe upper-case identifier.
pub fn sanitize_var_name(name: &str) -> String {
    let upper = name.to_ascii_uppercase();
    upper
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// POSIX-safe single-quote escape: `'` → `'\''`, then wrap in single quotes.
pub fn quote_for_bash(value: &str) -> String {
    let escaped = value.replace('\'', r"'\''");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_upper_cases_lowercase() {
        assert_eq!(sanitize_var_name("compound"), "COMPOUND");
    }

    #[test]
    fn sanitize_replaces_hyphen() {
        assert_eq!(sanitize_var_name("my-axis"), "MY_AXIS");
    }

    #[test]
    fn sanitize_replaces_dot() {
        assert_eq!(sanitize_var_name("ax.is"), "AX_IS");
    }

    #[test]
    fn quote_simple_value() {
        assert_eq!(quote_for_bash("hello"), "'hello'");
    }

    #[test]
    fn quote_escapes_single_quote() {
        assert_eq!(quote_for_bash("it's"), r"'it'\''s'");
    }

    #[test]
    fn quote_preserves_newline() {
        assert_eq!(quote_for_bash("a\nb"), "'a\nb'");
    }
}
