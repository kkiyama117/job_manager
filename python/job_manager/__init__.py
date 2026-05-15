"""job_manager — SLURM job data management."""

from job_manager import _job_manager_core as _core

# SP-3 v2: new types
FlowRun = _core.FlowRun
JobRun = _core.JobRun
Lifecycle = _core.Lifecycle

# SP-3 v2: new pyfunctions
submit_flow = _core.submit_flow
render_batch_bash = _core.render_batch_bash
read_common = _core.read_common
write_common = _core.write_common
read_flow = _core.read_flow
read_flow_effective = _core.read_flow_effective
write_flow = _core.write_flow
read_job_run = _core.read_job_run
write_job_run = _core.write_job_run

# SP-1/SP-2: existing
PathResolver = _core.PathResolver
SearchFilter = _core.SearchFilter
CalcView = _core.CalcView
walk_flows = _core.walk_flows
validate_step_id = _core.validate_step_id
validate_job_id = _core.validate_job_id
build_job_id = _core.build_job_id
parse_job_id = _core.parse_job_id
ExperimentPlan = _core.ExperimentPlan
read_plan = _core.read_plan
write_plan = _core.write_plan

__all__ = [
    # SP-3 v2
    "FlowRun",
    "JobRun",
    "Lifecycle",
    "submit_flow",
    "render_batch_bash",
    "read_common",
    "write_common",
    "read_flow",
    "read_flow_effective",
    "write_flow",
    "read_job_run",
    "write_job_run",
    # SP-1/SP-2
    "PathResolver",
    "SearchFilter",
    "CalcView",
    "walk_flows",
    "validate_step_id",
    "validate_job_id",
    "build_job_id",
    "parse_job_id",
    "ExperimentPlan",
    "read_plan",
    "write_plan",
]
