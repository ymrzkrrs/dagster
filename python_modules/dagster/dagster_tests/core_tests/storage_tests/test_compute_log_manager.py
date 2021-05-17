import tempfile

import pytest

from .utils.compute_log_manager import TestComputeLogManager


class TestLocalComputeLogManager(TestComputeLogManager):
    __test__ = True

    @pytest.fixture(scope="function", name="compute_log_manager_config")
    def compute_log_manager_config(self):
        with tempfile.TemporaryDirectory() as tmpdir_path:
            yield {
                "module": "dagster.core.storage.local_compute_log_manager",
                "class": "LocalComputeLogManager",
                "config": {"base_dir": tmpdir_path},
            }