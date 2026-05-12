"""SP-1 Python API smoke tests."""

from __future__ import annotations

import asyncio
import tempfile

import job_manager


def test_path_resolver_paths_match_layout():
    resolver = job_manager.PathResolver("/work")
    assert str(resolver.root()) == "/work"
    p = resolver.flow_toml("01997cdc-0000-7000-8000-000000000000")
    assert str(p).endswith("flow.toml")


def test_search_filter_construction_with_defaults():
    f = job_manager.SearchFilter()
    assert f.program is None
    assert f.tags == {}
    f2 = job_manager.SearchFilter(program="g16", flow_uuid_prefix="0199")
    assert f2.program == "g16"
    assert f2.flow_uuid_prefix == "0199"


def test_per_job_status_enum_values():
    assert job_manager.PerJobStatus.Queued != job_manager.PerJobStatus.Running


def test_walk_flows_empty_dir_returns_empty_list():
    async def run(root: str) -> list:
        return await job_manager.walk_flows(root)

    with tempfile.TemporaryDirectory() as d:
        result = asyncio.run(run(d))
        assert result == []
