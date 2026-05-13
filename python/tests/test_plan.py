"""Python E2E for SP-2 plan + authoring pattern (spec §1.1)."""

from __future__ import annotations

import tempfile
from itertools import product
from pathlib import Path
from uuid import uuid4

import pytest

from job_manager import (
    ExperimentPlan,
    PathResolver,
    build_job_id,
    parse_job_id,
    read_plan,
    write_plan,
)


def test_experiment_plan_construct_and_jobs_getter():
    plan = ExperimentPlan(
        {
            "opt__compound=0": {"route": "# B3LYP/6-31G* opt", "nproc": 16},
            "opt__compound=1": {"route": "# B3LYP/6-31G* opt", "nproc": 16},
        }
    )
    jobs = plan.jobs
    assert len(jobs) == 2
    assert jobs["opt__compound=0"]["route"] == "# B3LYP/6-31G* opt"
    assert jobs["opt__compound=0"]["nproc"] == 16


def test_plan_round_trip():
    plan = ExperimentPlan(
        {
            "opt__c=0": {"route": "# r0", "nproc": 16},
            "opt__c=1": {"route": "# r1", "nproc": 16},
        }
    )
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "plan.toml"
        write_plan(str(p), plan)
        back = read_plan(str(p))
        assert len(back.jobs) == 2
        assert back.jobs["opt__c=0"]["route"] == "# r0"


def test_authoring_pattern_12_jobs():
    """spec §1.1 の Python authoring パターン (sweep + parent in pure Python)."""
    compounds = ["benzene", "toluene", "p-xylene"]
    methods = [
        {"name": "b3lyp", "route": "B3LYP"},
        {"name": "m062x", "route": "M06-2X"},
    ]

    params: dict[str, dict] = {}
    for (i, c), (j, m) in product(enumerate(compounds), enumerate(methods)):
        opt_id = build_job_id("opt", [("compound", i), ("method", j)])
        params[opt_id] = {
            "route": f"# {m['route']}/6-31G* opt",
            "compound": c,
            "nproc": 16,
        }
        freq_id = build_job_id("freq", [("compound", i), ("method", j)])
        params[freq_id] = {
            "route": f"# {m['route']}/6-31G* freq",
            "compound": c,
            "nproc": 16,
        }

    plan = ExperimentPlan(params)
    assert len(plan.jobs) == 12

    # 各 JobId が規約に従う
    for jid in plan.jobs:
        parts = parse_job_id(jid)
        assert parts["source_step_id"] in ("opt", "freq")
        assert len(parts["axis_combo"]) == 2

    # round-trip
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "plan.toml"
        write_plan(str(p), plan)
        back = read_plan(str(p))
        assert len(back.jobs) == 12


def test_pathresolver_plan_toml():
    with tempfile.TemporaryDirectory() as d:
        resolver = PathResolver(d)
        uid = uuid4()
        path = resolver.plan_toml(str(uid))
        # path はまだ存在しないが、parent dir を含むはず
        assert "plan.toml" in str(path)
        assert str(uid) in str(path)


def test_pathresolver_experiment_toml_reserved_for_future():
    """experiment_toml() getter は SP-2 では使わないが、将来用に公開済み。"""
    with tempfile.TemporaryDirectory() as d:
        resolver = PathResolver(d)
        uid = uuid4()
        path = resolver.experiment_toml(str(uid))
        assert "experiment.toml" in str(path)


def test_experiment_plan_rejects_invalid_job_id_char():
    """M-1: 不正文字 (`..`, `/`) を含む job_id key は ValueError を raise する。"""
    with pytest.raises(ValueError):
        ExperimentPlan({"../evil": {"route": "# x"}})
    with pytest.raises(ValueError):
        ExperimentPlan({"opt/sub": {"route": "# x"}})


def test_experiment_plan_rejects_reserved_job_id():
    """M-1: 予約名 (flow/plan/experiment/derived/status) を job_id key として拒否。"""
    for reserved in ("flow", "plan", "experiment", "derived", "status"):
        with pytest.raises(ValueError):
            ExperimentPlan({reserved: {"route": "# x"}})
