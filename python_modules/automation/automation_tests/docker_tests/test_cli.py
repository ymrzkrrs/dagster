import os

import pytest
from automation.docker.cli import cli
from click.testing import CliRunner
from dagster.utils import file_relative_path


@pytest.fixture(name="runner")
def runner_fixture():
    yield CliRunner()


def test_list(runner):
    images_path = file_relative_path(__file__, "../../automation/docker/images")
    expected_images = "\n".join([f.name for f in os.scandir(images_path) if f.is_dir()])

    assert expected_images in runner.invoke(cli, ["list"]).output
