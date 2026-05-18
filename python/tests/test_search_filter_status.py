from job_manager._job_manager_core import SearchFilter


def test_search_filter_accepts_status_string_list():
    f = SearchFilter(status=["running", "F"])
    assert f.status == ["running", "F"]


def test_search_filter_status_defaults_empty():
    f = SearchFilter()
    assert f.status == []
