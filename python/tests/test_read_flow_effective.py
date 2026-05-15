"""Smoke test: read_flow → write_flow ↔ read_flow_effective via in-process API.

Exercises the Python binding surface end-to-end without invoking the `jm`
CLI. The full `jm render --effective-only` path is already covered by
Rust integration tests (`tests/integration_sp3.rs`, `tests/cli_smoke.rs`)
and the `write_flow_effective ↔ read_flow_effective` byte-level
round-trip is covered in `src/persistence/flow.rs`, so the Python side
only needs to verify the binding wiring and on-disk layout assumptions.

Keeping this Python-only also lets `pytest` pass against a pre-built
wheel without a Rust toolchain (e.g., a wheel pulled from PyPI), which
was not possible while this test shelled out to `cargo run`.
"""

from __future__ import annotations

from pathlib import Path

import job_manager


def test_read_flow_effective_roundtrip(tmp_path: Path) -> None:
    uuid = "01999999-0000-7000-8000-000000000000"

    common_toml = """
[slurm_default]
partition = "long"

[directories]
project_root = "/tmp/jm-test"
"""
    (tmp_path / "common.toml").write_text(common_toml.strip() + "\n")

    flow_dir = tmp_path / uuid
    flow_dir.mkdir()
    flow_toml = f"""
uuid = "{uuid}"
created_at = "2026-05-15T00:00:00Z"

[jobs.opt]
program = "echo"
body = "true\\n"
# partition は省略、common.toml の "long" が inject される
"""
    (flow_dir / "flow.toml").write_text(flow_toml.strip() + "\n")

    plan_toml = """
[jobs.opt]
"""
    (flow_dir / "plan.toml").write_text(plan_toml.strip() + "\n")

    # 1. read_flow infers <root>/common.toml and bakes its partition into
    #    the materialized body.
    body = job_manager.read_flow(str(flow_dir / "flow.toml"))
    assert uuid in body, f"uuid not preserved by read_flow: {body}"
    assert 'partition = "long"' in body, (
        f"expected default partition baked in by read_flow: {body}"
    )

    # 2. Persist the materialized body where `jm render --effective-only`
    #    would emit it. write_flow uses the same atomic-rename path as
    #    write_flow_effective, so the on-disk bytes are equivalent.
    eff_path = flow_dir / ".jm" / "flow.effective.toml"
    job_manager.write_flow(str(eff_path), body)
    assert eff_path.exists(), f"snapshot not written at {eff_path}"

    # 3. read_flow_effective reads the snapshot without needing
    #    common.toml again — every default must already be baked in.
    snapshot = job_manager.read_flow_effective(str(eff_path))
    assert uuid in snapshot, f"uuid lost on effective roundtrip: {snapshot}"
    assert 'partition = "long"' in snapshot, (
        f"partition lost on effective roundtrip: {snapshot}"
    )
