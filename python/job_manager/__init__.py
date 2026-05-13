"""job_manager — SLURM job data management."""

from job_manager import _job_manager_core as _core

PathResolver = _core.PathResolver
SearchFilter = _core.SearchFilter
Lifecycle = _core.Lifecycle
CalcView = _core.CalcView
walk_flows = _core.walk_flows
# TODO(Phase G.1/G.2): tick_many removed; FlowRunner::tick binding coming in E.5.

# SP-2: jobid helpers
validate_step_id = _core.validate_step_id
validate_job_id = _core.validate_job_id
build_job_id = _core.build_job_id
parse_job_id = _core.parse_job_id

# SP-2: plan
ExperimentPlan = _core.ExperimentPlan
read_plan = _core.read_plan
write_plan = _core.write_plan

__all__ = [
    "PathResolver",
    "SearchFilter",
    "Lifecycle",
    "CalcView",
    "walk_flows",
    # SP-2
    "validate_step_id",
    "validate_job_id",
    "build_job_id",
    "parse_job_id",
    "ExperimentPlan",
    "read_plan",
    "write_plan",
]
