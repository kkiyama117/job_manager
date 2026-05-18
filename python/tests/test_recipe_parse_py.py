import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PARSE_TMPL = REPO / "src/recipes/assets/parse_g16_out/parse.py.tmpl"
FIX = Path(__file__).resolve().parent / "_recipe_fixtures" / "g16_ok.out"


def _materialize(tmp: Path, input_rel: str) -> Path:
    job = tmp / "parse"
    (job / "scripts").mkdir(parents=True)
    # R3': bake absolute JOB_DIR (mirrors scaffold). An absolute input_rel
    # (e.g. the fixture) wins over JOB_DIR via os.path.join semantics.
    body = (
        PARSE_TMPL.read_text()
        .replace("{{JOB_DIR}}", str(job))
        .replace("{{INPUT_REL}}", input_rel)
    )
    (job / "scripts" / "parse.py").write_text(body)
    return job


def _run(job: Path) -> subprocess.CompletedProcess:
    # R3' (a) / H1 regression: launch parse.py by an ABSOLUTE path from a cwd
    # that is NOT the job dir, proving parse.py is cwd-independent (the
    # previous `cwd=job` masked the launcher gap).
    return subprocess.run(
        [sys.executable, str(job / "scripts" / "parse.py")],
        cwd=job.parent,
        capture_output=True,
        text=True,
    )


def test_cclib_missing_exits_2(tmp_path):
    job = _materialize(tmp_path, "missing.out")
    body = (
        (job / "scripts" / "parse.py")
        .read_text()
        .replace("import cclib  # noqa: F401", "raise ImportError('forced')")
    )
    (job / "scripts" / "parse.py").write_text(body)
    cp = _run(job)
    assert cp.returncode == 2
    assert "cclib not importable" in cp.stderr


def test_valid_out_writes_result_json(tmp_path):
    pytest.importorskip("cclib")
    job = _materialize(tmp_path, str(FIX))
    cp = _run(job)
    assert cp.returncode == 0, cp.stderr
    res = json.loads((job / "output" / "result.json").read_text())
    assert res["schema"] == "jm-recipe/1"
    assert res["converged"] is True
    assert res["n_atoms"] >= 1
    assert isinstance(res["scf_energy"], float)


def test_missing_input_exits_1(tmp_path):
    pytest.importorskip("cclib")
    job = _materialize(tmp_path, "nope.out")
    cp = _run(job)
    assert cp.returncode == 1
