"""Smoke test: roundtrip via write_flow_effective ↔ read_flow_effective."""

from __future__ import annotations

import subprocess
from pathlib import Path

import job_manager


def test_read_flow_effective_after_render(tmp_path: Path) -> None:
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

    repo_root = Path(__file__).resolve().parents[2]
    cmd = [
        "cargo",
        "run",
        "--bin",
        "jm",
        "--no-default-features",
        "--quiet",
        "--",
        "--root",
        str(tmp_path),
        "render",
        uuid,
        "--effective-only",
    ]
    # 5 min — first invocation rebuilds the jm binary; later runs hit the
    # build cache and finish in seconds. Surface a timeout as a clear test
    # failure rather than a hung CI job.
    r = subprocess.run(
        cmd, cwd=repo_root, capture_output=True, text=True, timeout=300
    )
    assert r.returncode == 0, f"jm render failed: stderr={r.stderr}"

    eff_path = flow_dir / ".jm" / "flow.effective.toml"
    assert eff_path.exists(), f"snapshot not written at {eff_path}"

    body = job_manager.read_flow_effective(str(eff_path))
    assert uuid in body
    assert 'partition = "long"' in body, f"expected default partition baked in: {body}"
