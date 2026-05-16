//! End-to-end tests for `jm new`.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

/// Read the single flow dir name (the minted uuid) under `root`.
fn sole_flow_uuid(root: &std::path::Path) -> String {
    let entries: Vec<_> = std::fs::read_dir(root)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    assert_eq!(entries.len(), 1, "expected exactly one flow dir");
    entries[0].file_name().to_string_lossy().into_owned()
}

#[test]
fn jm_new_creates_flow_and_plan_and_renders() {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;

    let dir = tempdir().unwrap();

    // jm new
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root").arg(dir.path()).arg("new");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("created flow"));

    let uuid_str = sole_flow_uuid(dir.path());
    let uuid = uuid::Uuid::parse_str(&uuid_str).expect("flow dir name is a uuid");

    let resolver = PathResolver::new(dir.path());
    assert!(resolver.flow_toml(&uuid).exists(), "flow.toml missing");
    assert!(resolver.plan_toml(&uuid).exists(), "plan.toml missing");

    // Round-trips through FlowRun::read (no common.toml -> synth fallback;
    // REPLACE_ME partition keeps it off the PartitionMissing path).
    let fr = FlowRun::read(&resolver, uuid).expect("FlowRun::read should succeed");
    assert_eq!(fr.flow.jobs.len(), 2);

    // jm render must succeed on the generated boilerplate.
    let mut render = Command::cargo_bin("jm").unwrap();
    render
        .arg("--root")
        .arg(dir.path())
        .arg("render")
        .arg(&uuid_str);
    render
        .assert()
        .success()
        .stdout(predicate::str::contains("rendered 2 jobs"));
}

#[test]
fn jm_new_print_path_emits_only_the_dir() {
    let dir = tempdir().unwrap();

    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root")
        .arg(dir.path())
        .arg("new")
        .arg("--print-path");
    let out = cmd.assert().success().get_output().stdout.clone();
    let printed = String::from_utf8(out).unwrap();
    let printed = printed.trim();

    let uuid_str = sole_flow_uuid(dir.path());
    let expected = std::fs::canonicalize(dir.path()).unwrap().join(&uuid_str);
    assert_eq!(
        std::path::Path::new(printed),
        expected,
        "print-path output should be exactly <root>/<uuid>"
    );
    assert!(
        !printed.contains("created flow"),
        "print-path must not emit the human banner"
    );
}

#[test]
fn jm_new_writes_tag_into_flow() {
    use job_manager::flow::FlowRun;
    use job_manager::persistence::PathResolver;

    let dir = tempdir().unwrap();
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root")
        .arg(dir.path())
        .arg("new")
        .arg("--tag")
        .arg("env=prod");
    cmd.assert().success();

    let uuid_str = sole_flow_uuid(dir.path());
    let uuid = uuid::Uuid::parse_str(&uuid_str).unwrap();
    let resolver = PathResolver::new(dir.path());
    let fr = FlowRun::read(&resolver, uuid).unwrap();
    assert_eq!(fr.flow.tags.get("env").map(String::as_str), Some("prod"));
}

#[test]
fn jm_new_rejects_malformed_tag() {
    let dir = tempdir().unwrap();
    let mut cmd = Command::cargo_bin("jm").unwrap();
    cmd.arg("--root")
        .arg(dir.path())
        .arg("new")
        .arg("--tag")
        .arg("notakeyvalue");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("expected key=value"));
    // No flow dir should have been created (tag is parsed before mkdir).
    let dirs: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    assert!(dirs.is_empty(), "no flow dir on tag-parse failure");
}
