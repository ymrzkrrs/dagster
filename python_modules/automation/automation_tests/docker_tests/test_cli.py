import pytest
from automation.docker.cli import cli
from click.testing import CliRunner


@pytest.fixture(name="runner")
def runner_fixture():
    yield CliRunner()


@pytest.fixture(name="images_dir")
def images_dir_fixture(tmpdir):
    yield (tmpdir / "images").mkdir()


@pytest.fixture(name="Image")
def image_fixture(images_dir):
    def scaffold_image_files(name):
        (images_dir / name).mkdir()
        (images_dir / name / "Dockerfile").write("FROM hello-world")
        return name

    yield scaffold_image_files


def test_list(runner, images_dir, Image):
    assert not runner.invoke(cli, ["list", "--directory", images_dir]).output

    image1 = Image("image1")
    image2 = Image("image2")

    assert image1 in runner.invoke(cli, ["list", "--directory", images_dir]).output
    assert image2 in runner.invoke(cli, ["list", "--directory", images_dir]).output
