from job_manager import _job_manager_core as _core

if hasattr(_core, "__doc__"):
    __doc__ = _core.__doc__
if hasattr(_core, "__all__"):
    __all__ = list(_core.__all__)
else:
    __all__ = []

# Re-export the compiled `sum_as_string` placeholder so callers can do
# `import job_manager; job_manager.sum_as_string(...)` without reaching
# into the private `_job_manager_core` namespace.
sum_as_string = _core.sum_as_string
if "sum_as_string" not in __all__:
    __all__.append("sum_as_string")
