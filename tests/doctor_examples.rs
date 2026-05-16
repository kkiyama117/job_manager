//! Drift guard: `examples/full/` must stay valid. If an upstream D2/A1
//! struct changes incompatibly, `run_doctor` produces a FAIL and this
//! test (and `jm doctor`) goes red.

use job_manager::{DoctorScope, run_doctor};
use std::path::Path;

#[test]
fn examples_full_has_no_doctor_failures() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/full");
    let report = run_doctor(&root, &DoctorScope::All).expect("run_doctor should not error");
    assert!(
        !report.has_fail(),
        "examples/full has doctor FAILs:\n{report}"
    );
}
