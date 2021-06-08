import pytest
import yaml
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
    def scaffold_image_files(name, versions=None):
        (images_dir / name).mkdir()
        (images_dir / name / "Dockerfile").write("FROM hello-world")

        if versions:
            (images_dir / name / "versions.yaml").write(yaml.dump(versions))

        return name

    yield scaffold_image_files


def test_list(runner, images_dir, Image):
    assert not runner.invoke(cli, ["list", "--directory", images_dir]).output

    image1 = Image("image1")
    image2 = Image("image2")

    assert image1 in runner.invoke(cli, ["list", "--directory", images_dir]).output
    assert image2 in runner.invoke(cli, ["list", "--directory", images_dir]).output


def test_build(runner, images_dir, Image):
    image = Image("image", versions={"test": {}})
    output = runner.invoke(
        cli,
        [
            "build",
            "--name",
            image,
            "--dagster-version",
            "dev",
            "--python-version",
            "test",
            "--directory",
            images_dir,
        ],
    )
    breakpoint()
    pass


def test_build_all():
    pass


def test_push():
    pass


def test_push_all():
    pass


def test_push_dockerhub():
    pass


def test_push_ecr():
    pass
