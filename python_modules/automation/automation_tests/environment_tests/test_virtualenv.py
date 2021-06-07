import os
import subprocess
from contextlib import contextmanager

import pytest


@contextmanager
def virtualenv(venv_dir):
    try:
        old_path = os.environ["PATH"]
        old_virtual_env = os.getenv("VIRTUAL_ENV")
        os.environ["PATH"] = f"{venv_dir}/bin:{old_path}"
        os.environ["VIRTUAL_ENV"] = str(venv_dir)
        yield
    finally:
        os.environ["PATH"] = old_path
        if old_virtual_env is None:
            del os.environ["VIRTUAL_ENV"]
        else:
            os.environ["VIRTUAL_ENV"] = old_virtual_env


@pytest.fixture(name="venv")
def venv_fixture(tmpdir):
    with tmpdir.as_cwd():
        subprocess.check_call(["python3", "-m", "venv", "test_venv"])
        yield tmpdir / "test_venv"


@pytest.fixture(name="package")
def package_fixture(tmpdir):
    packages = (tmpdir / "packages").mkdir()

    class Package:
        def __init__(self, name):
            self.name = name
            self.directory = packages / name
            """
            foo
            ├── foo
            │   └── __init__.py
            └── setup.py
            """
            self.directory.mkdir()
            (self.directory / name).mkdir()
            (self.directory / name / "__init__.py").write("")
            (self.directory / "setup.py").write(
                "from setuptools import setup, find_packages\n"
                f"setup(name='{name}', packages=find_packages())"
            )

    return Package("foo")


def test_virtualenv_contextmanager(venv, package):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(["python3", "-c", f"import {package.name}"])

    with virtualenv(venv):
        subprocess.check_call(["pip", "install", package.directory])
        subprocess.check_call(["python3", "-c", f"import {package.name}"])

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(["python3", "-c", f"import {package.name}"])
