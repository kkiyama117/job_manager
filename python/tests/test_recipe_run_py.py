import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUN_TMPL = REPO / "src/recipes/assets/g16_opt/run.py.tmpl"


def _materialize(tmp: Path) -> Path:
    job = tmp / "job"
    (job / "input").mkdir(parents=True)
    (job / "input" / "main.gjf").write_text(
        "%nprocshared=8\n%mem=8GB\n%chk=main.chk\n#p opt\n\nt\n\n0 1\nH 0 0 0\n\n"
    )
    scripts = job / "scripts"
    scripts.mkdir()
    # R3': scaffold bakes the absolute job dir; the smoke harness does the
    # same {{JOB_DIR}} swap-in here so run.py is cwd-independent under test.
    (scripts / "run.py").write_text(
        RUN_TMPL.read_text().replace("{{JOB_DIR}}", str(job))
    )
    return job


def _stub_bin(tmp: Path, name: str, script: str) -> None:
    b = tmp / "bin"
    b.mkdir(exist_ok=True)
    p = b / name
    p.write_text("#!/bin/bash\n" + script)
    p.chmod(0o755)


def _run(job: Path, env: dict) -> subprocess.CompletedProcess:
    # R3' (a) / H1 regression: mirror the scaffold — launch run.py by an
    # ABSOLUTE path from a cwd that is NOT the job dir. This proves run.py is
    # cwd-independent (the previous `cwd=job` masked the launcher gap).
    return subprocess.run(
        [sys.executable, str(job / "scripts" / "run.py")],
        cwd=job.parent,
        env=env,
        capture_output=True,
        text=True,
    )


def base_env(tmp: Path) -> dict:
    e = dict(os.environ)
    e["PATH"] = f"{tmp / 'bin'}:{e['PATH']}"
    e["JM_FLOW_UUID"] = "flowu"
    e["JM_JOB_ID"] = "opt"
    e["JM_PARAM_SCRATCH_ROOT"] = str(tmp / "scratch")
    e["JM_PARAM_LAUNCHER"] = ""
    e["JM_PARAM_G16_CMD"] = "g16"
    return e


def test_success_order_prepare_run_copy(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(tmp_path, "g16", 'echo "ok" > main.out\nexit 0\n')
    cp = _run(job, base_env(tmp_path))
    assert cp.returncode == 0, cp.stderr
    assert (job / "output" / "main.out").read_text().strip() == "ok"


def test_g16_nonzero_propagates_and_still_copies(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(tmp_path, "g16", 'echo "partial" > main.out\nexit 7\n')
    cp = _run(job, base_env(tmp_path))
    assert cp.returncode == 7  # g16 rc has top precedence
    assert (job / "output" / "main.out").read_text().strip() == "partial"


def test_missing_g16_does_not_exit_zero(tmp_path):
    job = _materialize(tmp_path)
    env = base_env(tmp_path)
    env["JM_PARAM_G16_CMD"] = "definitely-not-on-path-xyz"
    cp = _run(job, env)
    assert cp.returncode != 0
    assert "failed to launch" in cp.stderr


def test_launcher_prefixes_argv(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(
        tmp_path,
        "srun",
        'echo "$@" > "$SRUN_ARGS_FILE"\nshift_cmd="${@: -3}"\nexit 0\n',
    )
    _stub_bin(tmp_path, "g16", "exit 0\n")
    env = base_env(tmp_path)
    env["JM_PARAM_LAUNCHER"] = "srun"
    env["SRUN_ARGS_FILE"] = str(tmp_path / "srun_args.txt")
    cp = _run(job, env)
    assert cp.returncode == 0, cp.stderr
    assert "g16 main.gjf main.out" in (tmp_path / "srun_args.txt").read_text()


def test_scratch_root_empty_falls_back_to_dot_scratch(tmp_path):
    job = _materialize(tmp_path)
    _stub_bin(tmp_path, "g16", "echo ok > main.out\nexit 0\n")
    env = base_env(tmp_path)
    env["JM_PARAM_SCRATCH_ROOT"] = ""
    cp = _run(job, env)
    assert cp.returncode == 0, cp.stderr
    assert (job / ".scratch" / "flowu" / "opt" / "main.out").exists()
