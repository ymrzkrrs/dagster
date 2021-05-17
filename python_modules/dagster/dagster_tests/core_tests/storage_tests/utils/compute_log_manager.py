import os
import random
import string
import sys

import pytest
from dagster import (
    DagsterEventType,
    InputDefinition,
    ModeDefinition,
    execute_pipeline,
    pipeline,
    reconstructable,
    resource,
    solid,
)
from dagster.core.execution.compute_logs import should_disable_io_stream_redirect
from dagster.core.storage.compute_log_manager import ComputeIOType
from dagster.core.test_utils import (
    instance_for_test,
)

HELLO_SOLID = "HELLO SOLID"
HELLO_RESOURCE = "HELLO RESOURCE"
SEPARATOR = os.linesep if (os.name == "nt" and sys.version_info < (3,)) else "\n"


@resource
def resource_a(_):
    print(HELLO_RESOURCE)  # pylint: disable=print-call
    return "A"


@solid
def spawn(_):
    return 1


@solid(input_defs=[InputDefinition("num", int)], required_resource_keys={"a"})
def spew(_, num):
    print(HELLO_SOLID)  # pylint: disable=print-call
    return num


def define_pipeline():
    @pipeline(mode_defs=[ModeDefinition(resource_defs={"a": resource_a})])
    def spew_pipeline():
        spew(spew(spawn()))

    return spew_pipeline


def normalize_file_content(s):
    return "\n".join([line for line in s.replace(os.linesep, "\n").split("\n") if line])


class TestComputeLogManager:
    """
    You can extend this class to easily run these set of tests on any compute log manager. When
    extending, you simply need to override the `event_log_storage` fixture and return your
    implementation of `EventLogStorage`.

    For example:

    ```
    class TestMyComputeLogManagerImplementation(TestComputeLogManager):
        __test__ = True

        @pytest.fixture(scope='function', name='storage')
        def compute_log_manager(self):  # pylint: disable=arguments-differ
            return MyComputeLogManagerImplementation()
    ```
    """

    __test__ = False

    @pytest.fixture(scope="function", name="compute_log_manager_config")
    def compute_log_manager_config(self):
        raise NotImplementedError()

    @pytest.fixture(scope="function", name="instance")
    def instance(self, compute_log_manager_config):
        with instance_for_test({"compute_logs": compute_log_manager_config}) as instance:
            yield instance

    @pytest.mark.skipif(
        should_disable_io_stream_redirect(), reason="compute logs disabled for win / py3.6+"
    )
    def test_compute_log_to_disk(self, instance):
        compute_log_manager = instance.compute_log_manager

        spew_pipeline = define_pipeline()
        result = execute_pipeline(spew_pipeline, instance=instance)
        assert result.success
        compute_steps = [
            event.step_key
            for event in result.step_event_list
            if event.event_type == DagsterEventType.STEP_START
        ]
        for step_key in compute_steps:
            if step_key.startswith("spawn"):
                continue
            compute_io_path = compute_log_manager.get_local_path(
                result.run_id, step_key, ComputeIOType.STDOUT
            )
            assert os.path.exists(compute_io_path)
            with open(compute_io_path, "r") as stdout_file:
                assert normalize_file_content(stdout_file.read()) == HELLO_SOLID

    @pytest.mark.skipif(
        should_disable_io_stream_redirect(), reason="compute logs disabled for win / py3.6+"
    )
    def test_compute_log_to_disk_multiprocess(self, instance):
        spew_pipeline = reconstructable(define_pipeline)
        manager = instance.compute_log_manager
        result = execute_pipeline(
            spew_pipeline,
            run_config={"storage": {"filesystem": {}}, "execution": {"multiprocess": {}}},
            instance=instance,
        )
        assert result.success

        compute_steps = [
            event.step_key
            for event in result.step_event_list
            if event.event_type == DagsterEventType.STEP_START
        ]
        for step_key in compute_steps:
            if step_key.startswith("spawn"):
                continue
            compute_io_path = manager.get_local_path(result.run_id, step_key, ComputeIOType.STDOUT)
            assert os.path.exists(compute_io_path)
            with open(compute_io_path, "r") as stdout_file:
                assert normalize_file_content(stdout_file.read()) == HELLO_SOLID

    @pytest.mark.skipif(
        should_disable_io_stream_redirect(), reason="compute logs disabled for win / py3.6+"
    )
    def test_compute_log_manager(self, instance):
        manager = instance.compute_log_manager
        spew_pipeline = define_pipeline()
        result = execute_pipeline(spew_pipeline, instance=instance)
        assert result.success
        compute_steps = [
            event.step_key
            for event in result.step_event_list
            if event.event_type == DagsterEventType.STEP_START
        ]
        assert len(compute_steps) == 3
        step_key = "spew"
        assert manager.is_watch_completed(result.run_id, step_key)

        stdout = manager.read_logs_file(result.run_id, step_key, ComputeIOType.STDOUT)
        assert normalize_file_content(stdout.data) == HELLO_SOLID

        stderr = manager.read_logs_file(result.run_id, step_key, ComputeIOType.STDERR)
        cleaned_logs = stderr.data.replace("\x1b[34m", "").replace("\x1b[0m", "")
        assert "dagster - DEBUG - spew_pipeline - " in cleaned_logs

        bad_logs = manager.read_logs_file("not_a_run_id", step_key, ComputeIOType.STDOUT)
        assert bad_logs.data is None
        assert not manager.is_watch_completed("not_a_run_id", step_key)

    @pytest.mark.skipif(
        should_disable_io_stream_redirect(), reason="compute logs disabled for win / py3.6+"
    )
    def test_compute_log_manager_subscriptions(self, instance):
        spew_pipeline = define_pipeline()
        step_key = "spew"
        result = execute_pipeline(spew_pipeline, instance=instance)
        stdout_observable = instance.compute_log_manager.observable(
            result.run_id, step_key, ComputeIOType.STDOUT
        )
        stderr_observable = instance.compute_log_manager.observable(
            result.run_id, step_key, ComputeIOType.STDERR
        )
        stdout = []
        stdout_observable.subscribe(stdout.append)
        stderr = []
        stderr_observable.subscribe(stderr.append)
        assert len(stdout) == 1
        assert stdout[0].data.startswith(HELLO_SOLID)
        assert stdout[0].cursor in [12, 13]
        assert len(stderr) == 1
        assert stderr[0].cursor == len(stderr[0].data)
        assert stderr[0].cursor > 400

    @pytest.mark.skipif(
        should_disable_io_stream_redirect(), reason="compute logs disabled for win / py3.6+"
    )
    def test_long_solid_names(self, instance):
        solid_name = "".join(random.choice(string.ascii_lowercase) for x in range(300))

        @pipeline(mode_defs=[ModeDefinition(resource_defs={"a": resource_a})])
        def long_pipeline():
            spew.alias(name=solid_name)()

        manager = instance.compute_log_manager

        result = execute_pipeline(
            long_pipeline,
            instance=instance,
            run_config={"solids": {solid_name: {"inputs": {"num": 1}}}},
        )
        assert result.success

        compute_steps = [
            event.step_key
            for event in result.step_event_list
            if event.event_type == DagsterEventType.STEP_START
        ]

        assert len(compute_steps) == 1
        step_key = compute_steps[0]
        assert manager.is_watch_completed(result.run_id, step_key)

        stdout = manager.read_logs_file(result.run_id, step_key, ComputeIOType.STDOUT)
        assert normalize_file_content(stdout.data) == HELLO_SOLID
