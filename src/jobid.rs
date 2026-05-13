//! JobId 命名規約 helper。
//!
//! 規約:
//! - step_id: `[A-Za-z0-9_-]+`、予約名禁止
//! - JobId: `<step_id>` または `<step_id>__<axis>=<idx>__...`
//! - 予約: `flow`, `plan`, `experiment`, `derived`, `status`
//!
//! D2 の `JobId(pub String)` 自身は文字種制約を持たない。本モジュールは
//! Python authoring で「規約に従った JobId 文字列」を作る helper を提供する。

use crate::error::JobManagerError;

const RESERVED_IDS: &[&str] = &["flow", "plan", "experiment", "derived", "status"];

fn valid_step_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

fn valid_job_id_char(c: char) -> bool {
    valid_step_id_char(c) || c == '='
}

/// step_id の検証 (`[A-Za-z0-9_-]+`、予約名禁止)。OK なら入力を返す。
pub fn validate_step_id(s: &str) -> Result<&str, JobManagerError> {
    if s.is_empty() || !s.chars().all(valid_step_id_char) {
        return Err(JobManagerError::InvalidStepId(s.to_string()));
    }
    if RESERVED_IDS.contains(&s) {
        return Err(JobManagerError::ReservedJobId(s.to_string()));
    }
    Ok(s)
}

/// JobId 全体の検証 (文字種 + 予約名 + sweep encoding 整合性)。
pub fn validate_job_id(s: &str) -> Result<&str, JobManagerError> {
    if s.is_empty() || !s.chars().all(valid_job_id_char) {
        return Err(JobManagerError::InvalidJobId(s.to_string()));
    }
    // parse できれば形式 OK
    parse_job_id(s)?;
    Ok(s)
}

/// JobId 文字列を組み立てる。D2 newtype 包装は呼び側 `JobId::from(...)`。
pub fn build_job_id(source_step_id: &str, axis_combo: &[(&str, usize)]) -> String {
    if axis_combo.is_empty() {
        return source_step_id.to_string();
    }
    let mut s = String::with_capacity(source_step_id.len() + axis_combo.len() * 16);
    s.push_str(source_step_id);
    for (ax, idx) in axis_combo {
        s.push_str("__");
        s.push_str(ax);
        s.push('=');
        s.push_str(&idx.to_string());
    }
    s
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobIdParts<'a> {
    pub source_step_id: &'a str,
    pub axis_combo: Vec<(&'a str, usize)>,
}

/// 借用ベース parse (alloc なし)。`&job_id.0` または string literal を渡す。
pub fn parse_job_id(s: &str) -> Result<JobIdParts<'_>, JobManagerError> {
    if s.is_empty() {
        return Err(JobManagerError::InvalidJobId(String::new()));
    }
    let mut iter = s.split("__");
    let source_step_id = iter.next().expect("split yields >=1");
    validate_step_id(source_step_id)?;

    let mut axis_combo: Vec<(&str, usize)> = Vec::new();
    for piece in iter {
        let Some(eq_pos) = piece.find('=') else {
            return Err(JobManagerError::JobIdParseError {
                id: s.to_string(),
                piece: piece.to_string(),
                reason: "expected '<axis>=<idx>'".to_string(),
            });
        };
        let (ax, idx_str) = piece.split_at(eq_pos);
        let idx_str = &idx_str[1..];
        if ax.is_empty() || !ax.chars().all(valid_step_id_char) {
            return Err(JobManagerError::JobIdParseError {
                id: s.to_string(),
                piece: piece.to_string(),
                reason: format!("invalid axis name '{ax}'"),
            });
        }
        let idx: usize = idx_str
            .parse()
            .map_err(|_| JobManagerError::JobIdParseError {
                id: s.to_string(),
                piece: piece.to_string(),
                reason: format!("invalid index '{idx_str}'"),
            })?;
        axis_combo.push((ax, idx));
    }
    Ok(JobIdParts {
        source_step_id,
        axis_combo,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_step_id_accepts_path_safe() {
        assert!(validate_step_id("opt").is_ok());
        assert!(validate_step_id("opt-1").is_ok());
        assert!(validate_step_id("opt_2").is_ok());
        assert!(validate_step_id("Step123").is_ok());
    }

    #[test]
    fn validate_step_id_rejects_reserved() {
        for name in &["flow", "plan", "experiment", "derived", "status"] {
            assert!(matches!(
                validate_step_id(name),
                Err(JobManagerError::ReservedJobId(_))
            ));
        }
    }

    #[test]
    fn validate_step_id_rejects_invalid_chars() {
        assert!(matches!(
            validate_step_id("opt=1"),
            Err(JobManagerError::InvalidStepId(_))
        ));
        assert!(matches!(
            validate_step_id("opt/sub"),
            Err(JobManagerError::InvalidStepId(_))
        ));
        assert!(matches!(
            validate_step_id(""),
            Err(JobManagerError::InvalidStepId(_))
        ));
    }

    #[test]
    fn validate_job_id_accepts_sweep_form() {
        assert!(validate_job_id("opt").is_ok());
        assert!(validate_job_id("opt__compound=0__method=2").is_ok());
    }

    #[test]
    fn validate_job_id_rejects_invalid() {
        assert!(validate_job_id("opt/sub").is_err());
        assert!(validate_job_id("opt__compound=abc").is_err());
    }

    #[test]
    fn build_no_sweep_returns_step_id() {
        assert_eq!(build_job_id("opt", &[]), "opt");
    }

    #[test]
    fn build_with_sweep_encodes_axes() {
        assert_eq!(
            build_job_id("opt", &[("compound", 0), ("method", 2)]),
            "opt__compound=0__method=2"
        );
    }

    #[test]
    fn parse_round_trip_no_sweep() {
        let s = build_job_id("opt", &[]);
        let parts = parse_job_id(&s).unwrap();
        assert_eq!(parts.source_step_id, "opt");
        assert!(parts.axis_combo.is_empty());
    }

    #[test]
    fn parse_round_trip_with_sweep() {
        let s = build_job_id("opt", &[("compound", 0), ("method", 2)]);
        let parts = parse_job_id(&s).unwrap();
        assert_eq!(parts.source_step_id, "opt");
        assert_eq!(parts.axis_combo, vec![("compound", 0), ("method", 2)]);
    }

    #[test]
    fn parse_rejects_malformed() {
        assert!(parse_job_id("opt__nothing").is_err());
        assert!(parse_job_id("opt__compound=abc").is_err());
        assert!(parse_job_id("__compound=0").is_err());
    }

    #[test]
    fn parse_axis_name_must_be_valid() {
        // axis 名に '=' は使えない (区切り文字なので)
        assert!(parse_job_id("opt__c/d=0").is_err());
    }
}
