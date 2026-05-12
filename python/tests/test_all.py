import pytest
import job_manager


def test_sum_as_string():
    assert job_manager.sum_as_string(1, 1) == "2"
