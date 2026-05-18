//! 最小・依存ゼロの XYZ 座標パーサ。`jm new` は化学ライブラリを持たない
//! ので、g16 gjf の geometry block を作るのに必要な最小機能だけを実装。

/// XYZ を Gaussian gjf geometry block へ変換する。
/// 形式: 1行目=原子数, 2行目=コメント, 以降 `Elem x y z`(f64)。
/// 出力は1原子1行 `{sym} {x:.6} {y:.6} {z:.6}`(末尾改行なし)。
pub fn xyz_to_geometry_block(src: &str) -> Result<String, String> {
    let mut lines = src.lines();
    let count: usize = lines
        .next()
        .ok_or_else(|| "xyz: first line (atom count) missing".to_string())?
        .trim()
        .parse()
        .map_err(|_| "xyz: first line must be an integer atom count".to_string())?;
    lines
        .next()
        .ok_or_else(|| "xyz: comment line (line 2) missing".to_string())?;

    let mut out: Vec<String> = Vec::with_capacity(count);
    for (i, raw) in lines.enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        let mut it = raw.split_whitespace();
        let sym = it
            .next()
            .ok_or_else(|| format!("xyz: atom line {} empty", i + 3))?;
        let parse_coord = |o: Option<&str>, axis: &str| -> Result<f64, String> {
            o.ok_or_else(|| format!("xyz: atom line {} missing {axis} coordinate", i + 3))?
                .parse::<f64>()
                .map_err(|_| format!("xyz: atom line {} has non-numeric {axis} coordinate", i + 3))
        };
        let x = parse_coord(it.next(), "x")?;
        let y = parse_coord(it.next(), "y")?;
        let z = parse_coord(it.next(), "z")?;
        out.push(format!("{sym} {x:.6} {y:.6} {z:.6}"));
    }
    if out.len() != count {
        return Err(format!(
            "xyz: atom count mismatch — header says {count}, found {}",
            out.len()
        ));
    }
    Ok(out.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_atom_xyz() {
        let xyz = "2\nwater fragment\nO  0.0 0.0 0.117\nH 0.0 0.757 -0.467\n";
        let block = xyz_to_geometry_block(xyz).unwrap();
        assert_eq!(
            block,
            "O 0.000000 0.000000 0.117000\nH 0.000000 0.757000 -0.467000"
        );
    }

    #[test]
    fn rejects_atom_count_mismatch() {
        let err = xyz_to_geometry_block("3\nc\nO 0 0 0\nH 0 0 1\n").unwrap_err();
        assert!(err.contains("atom count"), "got: {err}");
    }

    #[test]
    fn rejects_bad_header() {
        let err = xyz_to_geometry_block("notanumber\nc\nO 0 0 0\n").unwrap_err();
        assert!(err.contains("first line"), "got: {err}");
    }

    #[test]
    fn rejects_malformed_atom_line() {
        let err = xyz_to_geometry_block("1\nc\nO 0.0 nope 0.0\n").unwrap_err();
        assert!(err.contains("coordinate"), "got: {err}");
    }

    #[test]
    fn rejects_empty() {
        let err = xyz_to_geometry_block("").unwrap_err();
        assert!(err.contains("first line"), "got: {err}");
    }
}
