"""Python E2E for SP-2 jobid helpers."""

from __future__ import annotations

import pytest

from job_manager import build_job_id, parse_job_id, validate_job_id, validate_step_id


def test_validate_step_id_ok():
    assert validate_step_id("opt") == "opt"
    assert validate_step_id("opt-1") == "opt-1"
    assert validate_step_id("Step_2") == "Step_2"


def test_validate_step_id_rejects_reserved():
    for name in ["flow", "plan", "experiment", "derived", "status"]:
        with pytest.raises(ValueError):
            validate_step_id(name)


def test_validate_step_id_rejects_invalid_chars():
    for bad in ["opt=1", "opt/sub", "", "opt space"]:
        with pytest.raises(ValueError):
            validate_step_id(bad)


def test_build_no_sweep():
    assert build_job_id("opt", []) == "opt"


def test_build_with_sweep():
    assert (
        build_job_id("opt", [("compound", 0), ("method", 2)])
        == "opt__compound=0__method=2"
    )


def test_parse_round_trip():
    s = build_job_id("opt", [("compound", 0), ("method", 2)])
    parts = parse_job_id(s)
    assert parts["source_step_id"] == "opt"
    assert parts["axis_combo"] == [("compound", 0), ("method", 2)]


def test_parse_rejects_malformed():
    with pytest.raises(ValueError):
        parse_job_id("opt__compound=abc")
    with pytest.raises(ValueError):
        parse_job_id("opt__nothing")
    with pytest.raises(ValueError):
        parse_job_id("")


def test_validate_job_id_accepts_sweep_form():
    assert validate_job_id("opt__compound=0__method=2") == "opt__compound=0__method=2"


# --- M-3: Python 公開境界の build_job_id は fallible で source / axis を検証する。 ---


def test_build_job_id_rejects_invalid_source_step_id():
    """M-3: 不正文字 / 予約名を source_step_id に渡すと ValueError。"""
    for bad in ["../evil", "opt/sub", "opt=1", ""]:
        with pytest.raises(ValueError):
            build_job_id(bad, [("compound", 0)])
    for reserved in ["flow", "plan", "experiment", "derived", "status"]:
        with pytest.raises(ValueError):
            build_job_id(reserved, [])


def test_build_job_id_rejects_invalid_axis_name():
    """M-3: axis 名の不正文字も ValueError。"""
    for bad in ["../evil", "ax=1", "ax/sub", ""]:
        with pytest.raises(ValueError):
            build_job_id("opt", [(bad, 0)])


def test_build_job_id_rejects_reserved_axis_name():
    """M-3: axis 名に予約名 (flow/plan/...) を使うと ValueError。"""
    for reserved in ["flow", "plan", "experiment", "derived", "status"]:
        with pytest.raises(ValueError):
            build_job_id("opt", [(reserved, 0)])
