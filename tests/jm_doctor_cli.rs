//! `jm doctor` CLI smoke test: clean tree exits 0, broken tree exits non-zero.

use assert_cmd::Command;
use predicates::str::contains;
use tempfile::tempdir;

fn write(p: &std::path::Path, body: &str) {
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(p, body).unwrap();
}

const FLOW: &str = "uuid = \"01999999-0000-7000-8000-000000000000\"\n\
created_at = \"2026-05-15T00:00:00Z\"\n\
[jobs.opt]\nprogram = \"echo\"\nbody = \"x\\n\"\n\
[jobs.opt.config]\npartition = \"long\"\n";

#[test]
fn doctor_clean_tree_exits_zero() {
    let d = tempdir().unwrap();
    let f = d.path().join("01999999-0000-7000-8000-000000000000");
    write(&f.join("flow.toml"), FLOW);
    write(&f.join("plan.toml"), "[jobs.opt]\nnproc = 1\n");

    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", d.path().to_str().unwrap(), "doctor"])
        .assert()
        .success()
        .stdout(contains("summary:"));
}

#[test]
fn doctor_broken_tree_exits_nonzero() {
    let d = tempdir().unwrap();
    let f = d.path().join("01999999-0000-7000-8000-000000000000");
    write(&f.join("flow.toml"), &format!("{FLOW}bogus = 1\n"));
    write(&f.join("plan.toml"), "[jobs.opt]\n");

    Command::cargo_bin("jm")
        .unwrap()
        .args(["--root", d.path().to_str().unwrap(), "doctor"])
        .assert()
        .failure()
        .stdout(contains("FAIL"));
}
