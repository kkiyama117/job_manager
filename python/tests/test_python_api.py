"""SP-1 Python API smoke tests."""

from __future__ import annotations

import asyncio
import tempfile

import pytest

import job_manager


def test_path_resolver_paths_match_layout():
    resolver = job_manager.PathResolver("/work")
    assert str(resolver.root()) == "/work"
    p = resolver.flow_toml("01997cdc-0000-7000-8000-000000000000")
    assert str(p).endswith("flow.toml")


def test_path_resolver_status_file_rejects_traversal():
    """M-2: `..` を含む job_id は ValueError (path traversal 防止)。"""
    resolver = job_manager.PathResolver("/work")
    with pytest.raises(ValueError):
        resolver.status_file("01997cdc-0000-7000-8000-000000000000", "../evil")


def test_path_resolver_status_file_rejects_reserved():
    """M-2: 予約名を job_id として使うのは ValueError。"""
    resolver = job_manager.PathResolver("/work")
    with pytest.raises(ValueError):
        resolver.status_file("01997cdc-0000-7000-8000-000000000000", "flow")


def test_path_resolver_status_file_accepts_valid_job_id():
    """M-2: 規約に従う job_id は通過する (回帰防止)。"""
    resolver = job_manager.PathResolver("/work")
    p = resolver.status_file(
        "01997cdc-0000-7000-8000-000000000000", "opt__compound=0"
    )
    assert "opt__compound=0" in str(p)
    # F2: status file lives under .jm/<JobId>/status.toml (no dot prefix)
    assert str(p).endswith("/.jm/opt__compound=0/status.toml")


def test_search_filter_construction_with_defaults():
    f = job_manager.SearchFilter()
    assert f.program is None
    assert f.tags == {}
    f2 = job_manager.SearchFilter(program="g16", flow_uuid_prefix="0199")
    assert f2.program == "g16"
    assert f2.flow_uuid_prefix == "0199"


def test_lifecycle_enum_values():
    assert job_manager.Lifecycle.Queued != job_manager.Lifecycle.Running


def test_walk_flows_empty_dir_returns_empty_list():
    async def run(root: str) -> list:
        return await job_manager.walk_flows(root)

    with tempfile.TemporaryDirectory() as d:
        result = asyncio.run(run(d))
        assert result == []
