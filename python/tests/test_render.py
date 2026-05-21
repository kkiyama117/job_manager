"""Render pytest — env-export bash content."""

from job_manager import render_batch_bash


def test_render_emits_jm_param_and_axis():
    out = render_batch_bash(
        flow_uuid="01997cdc-0000-7000-8000-000000000000",
        job_id="opt__a=0",
        body="echo hi",
        params={"route": "B3LYP"},
        abs_flow_dir="/work/root/01997cdc-0000-7000-8000-000000000000",
        abs_job_dir="/work/root/01997cdc-0000-7000-8000-000000000000/opt",
    )
    assert "export JM_FLOW_UUID='01997cdc-0000-7000-8000-000000000000'" in out
    assert "export JM_AXIS_A='0'" in out
    assert "export JM_PARAM_ROUTE='B3LYP'" in out
    assert "export JM_FLOW_DIR='/work/root/01997cdc-0000-7000-8000-000000000000'" in out
    assert (
        "export JM_JOB_DIR='/work/root/01997cdc-0000-7000-8000-000000000000/opt'" in out
    )
    assert "echo hi" in out


def test_render_escapes_single_quote_in_param():
    out = render_batch_bash(
        flow_uuid="01997cdc-0000-7000-8000-000000000000",
        job_id="x__a=0",
        body="",
        params={"note": "it's working"},
        abs_flow_dir="/work/root/01997cdc-0000-7000-8000-000000000000",
        abs_job_dir="/work/root/01997cdc-0000-7000-8000-000000000000/x",
    )
    assert "export JM_PARAM_NOTE='it'\\''s working'" in out
