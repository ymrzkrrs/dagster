import os
import signal
import sys
import tempfile
import time
from contextlib import ExitStack, contextmanager

import pendulum
import yaml
from dagster import Shape, check, composite_solid, pipeline, solid
from dagster.config import Field
from dagster.config.config_type import Array
from dagster.core.host_representation.origin import (
    ExternalPipelineOrigin,
    InProcessRepositoryLocationOrigin,
)
from dagster.core.instance import DagsterInstance
from dagster.core.launcher import RunLauncher
from dagster.core.run_coordinator import RunCoordinator, SubmitRunContext
from dagster.core.storage.pipeline_run import (
    PipelineRun,
    PipelineRunStatus,
    PipelineRunsFilter,
    PipelineTarget,
)
from dagster.core.telemetry import cleanup_telemetry_logger
from dagster.core.workspace.context import WorkspaceProcessContext
from dagster.core.workspace.load_target import WorkspaceLoadTarget
from dagster.serdes import ConfigurableClass
from dagster.seven.compat.pendulum import create_pendulum_time, mock_pendulum_timezone
from dagster.utils import merge_dicts
from dagster.utils.error import serializable_error_info_from_exc_info


def step_output_event_filter(pipe_iterator):
    for step_event in pipe_iterator:
        if step_event.is_successful_output:
            yield step_event


def nesting_composite_pipeline(depth, num_children, *args, **kwargs):
    """Creates a pipeline of nested composite solids up to "depth" layers, with a fan-out of
    num_children at each layer.

    Total number of solids will be num_children ^ depth
    """

    @solid
    def leaf_node(_):
        return 1

    def create_wrap(inner, name):
        @composite_solid(name=name)
        def wrap():
            for i in range(num_children):
                solid_alias = "%s_node_%d" % (name, i)
                inner.alias(solid_alias)()

        return wrap

    @pipeline(*args, **kwargs)
    def nested_pipeline():
        comp_solid = create_wrap(leaf_node, "layer_%d" % depth)

        for i in range(depth):
            comp_solid = create_wrap(comp_solid, "layer_%d" % (depth - (i + 1)))

        comp_solid.alias("outer")()

    return nested_pipeline


@contextmanager
def environ(env):
    """Temporarily set environment variables inside the context manager and
    fully restore previous environment afterwards
    """
    previous_values = {key: os.getenv(key) for key in env}
    for key, value in env.items():
        if value is None:
            if key in os.environ:
                del os.environ[key]
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        for key, value in previous_values.items():
            if value is None:
                if key in os.environ:
                    del os.environ[key]
            else:
                os.environ[key] = value


@contextmanager
def instance_for_test(overrides=None, set_dagster_home=True, temp_dir=None):
    with ExitStack() as stack:
        if not temp_dir:
            temp_dir = stack.enter_context(tempfile.TemporaryDirectory())

        # If using the default run launcher, wait for any grpc processes that created runs
        # during test disposal to finish, since they might also be using this instance's tempdir
        instance_overrides = merge_dicts(
            {
                "run_launcher": {
                    "class": "DefaultRunLauncher",
                    "module": "dagster.core.launcher.default_run_launcher",
                    "config": {
                        "wait_for_processes": True,
                    },
                }
            },
            (overrides if overrides else {}),
        )

        if set_dagster_home:
            stack.enter_context(environ({"DAGSTER_HOME": temp_dir}))

        with open(os.path.join(temp_dir, "dagster.yaml"), "w") as fd:
            yaml.dump(instance_overrides, fd, default_flow_style=False)

        with DagsterInstance.from_config(temp_dir) as instance:
            try:
                yield instance
            except:
                sys.stderr.write(
                    "Test raised an exception, attempting to clean up instance:"
                    + serializable_error_info_from_exc_info(sys.exc_info()).to_string()
                    + "\n"
                )
                raise
            finally:
                cleanup_test_instance(instance)


def cleanup_test_instance(instance):
    check.inst_param(instance, "instance", DagsterInstance)
    # To avoid filesystem contention when we close the temporary directory, wait for
    # all runs to reach a terminal state, and close any subprocesses or threads
    # that might be accessing the run history DB.
    instance.run_launcher.join()

    cleanup_telemetry_logger()


def create_run_for_test(
    instance,
    pipeline_name=None,
    run_id=None,
    run_config=None,
    mode=None,
    solids_to_execute=None,
    step_keys_to_execute=None,
    status=None,
    tags=None,
    root_run_id=None,
    parent_run_id=None,
    pipeline_snapshot=None,
    execution_plan_snapshot=None,
    parent_pipeline_snapshot=None,
    external_pipeline_origin=None,
    pipeline_code_origin=None,
):
    target = PipelineTarget(name=pipeline_name, mode=mode)
    return instance.create_run(
        target,
        run_id,
        run_config,
        solids_to_execute,
        step_keys_to_execute,
        status,
        tags,
        root_run_id,
        parent_run_id,
        pipeline_snapshot,
        execution_plan_snapshot,
        parent_pipeline_snapshot,
        external_pipeline_origin=external_pipeline_origin,
        pipeline_code_origin=pipeline_code_origin,
    )


def register_managed_run_for_test(
    instance,
    pipeline_name=None,
    run_id=None,
    run_config=None,
    mode=None,
    solids_to_execute=None,
    step_keys_to_execute=None,
    tags=None,
    root_run_id=None,
    parent_run_id=None,
    pipeline_snapshot=None,
    execution_plan_snapshot=None,
    parent_pipeline_snapshot=None,
):
    return instance.register_managed_run(
        pipeline_name,
        run_id,
        run_config,
        mode,
        solids_to_execute,
        step_keys_to_execute,
        tags,
        root_run_id,
        parent_run_id,
        pipeline_snapshot,
        execution_plan_snapshot,
        parent_pipeline_snapshot,
    )


def poll_for_finished_run(instance, run_id=None, timeout=20, run_tags=None):
    total_time = 0
    interval = 0.01

    filters = PipelineRunsFilter(
        run_ids=[run_id] if run_id else None,
        tags=run_tags,
        statuses=[PipelineRunStatus.SUCCESS, PipelineRunStatus.FAILURE, PipelineRunStatus.CANCELED],
    )

    while True:
        runs = instance.get_runs(filters, limit=1)
        if runs:
            return runs[0]
        else:
            time.sleep(interval)
            total_time += interval
            if total_time > timeout:
                raise Exception("Timed out")


def poll_for_step_start(instance, run_id, timeout=30):
    poll_for_event(instance, run_id, event_type="STEP_START", message=None, timeout=timeout)


def poll_for_event(instance, run_id, event_type, message, timeout=30):
    total_time = 0
    backoff = 0.01

    while True:
        time.sleep(backoff)
        logs = instance.all_logs(run_id)
        matching_events = [
            log_record.dagster_event
            for log_record in logs
            if log_record.is_dagster_event
            and log_record.dagster_event.event_type_value == event_type
        ]
        if matching_events:
            if message is None:
                return
            for matching_message in (event.message for event in matching_events):
                if message in matching_message:
                    return

        total_time += backoff
        backoff = backoff * 2
        if total_time > timeout:
            raise Exception("Timed out")


@contextmanager
def new_cwd(path):
    old = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(old)


def today_at_midnight(timezone_name="UTC"):
    check.str_param(timezone_name, "timezone_name")
    now = pendulum.now(timezone_name)
    return create_pendulum_time(now.year, now.month, now.day, tz=now.timezone.name)


class ExplodingRunLauncher(RunLauncher, ConfigurableClass):
    def __init__(self, inst_data=None):
        self._inst_data = inst_data

        super().__init__()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return ExplodingRunLauncher(inst_data=inst_data)

    def launch_run(self, context):
        raise NotImplementedError("The entire purpose of this is to throw on launch")

    def join(self, timeout=30):
        """Nothing to join on since all executions are synchronous."""

    def can_terminate(self, run_id):
        return False

    def terminate(self, run_id):
        check.not_implemented("Termination not supported")


class MockedRunLauncher(RunLauncher, ConfigurableClass):
    def __init__(self, inst_data=None, bad_run_ids=None):
        self._inst_data = inst_data
        self._queue = []
        self._bad_run_ids = bad_run_ids

        super().__init__()

    def launch_run(self, context):
        run = context.pipeline_run
        check.inst_param(run, "run", PipelineRun)
        check.invariant(run.status == PipelineRunStatus.STARTING)

        if self._bad_run_ids and run.run_id in self._bad_run_ids:
            raise Exception(f"Bad run {run.run_id}")

        self._queue.append(run)
        return run

    def queue(self):
        return self._queue

    @classmethod
    def config_type(cls):
        return Shape({"bad_run_ids": Field(Array(str), is_required=False)})

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    @property
    def inst_data(self):
        return self._inst_data

    def can_terminate(self, run_id):
        return False

    def terminate(self, run_id):
        check.not_implemented("Termintation not supported")


class MockedRunCoordinator(RunCoordinator, ConfigurableClass):
    def __init__(self, inst_data=None):
        self._inst_data = inst_data
        self._queue = []

        super().__init__()

    def submit_run(self, context: SubmitRunContext):
        pipeline_run = context.pipeline_run
        check.inst(pipeline_run.external_pipeline_origin, ExternalPipelineOrigin)
        self._queue.append(pipeline_run)
        return pipeline_run

    def queue(self):
        return self._queue

    @classmethod
    def config_type(cls):
        return Shape({})

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(
            inst_data=inst_data,
        )

    @property
    def inst_data(self):
        return self._inst_data

    def can_cancel_run(self, run_id):
        check.not_implemented("Cancellation not supported")

    def cancel_run(self, run_id):
        check.not_implemented("Cancellation not supported")


def get_terminate_signal():
    if sys.platform == "win32":
        return signal.SIGTERM
    return signal.SIGKILL


def get_crash_signals():
    if sys.platform == "win32":
        return [
            get_terminate_signal()
        ]  # Windows keeps resources open after termination in a way that messes up tests
    else:
        return [get_terminate_signal(), signal.SIGINT]


_mocked_system_timezone = {"timezone": None}


@contextmanager
def mock_system_timezone(override_timezone):
    with mock_pendulum_timezone(override_timezone):
        try:
            _mocked_system_timezone["timezone"] = override_timezone
            yield
        finally:
            _mocked_system_timezone["timezone"] = None


def get_mocked_system_timezone():
    return _mocked_system_timezone["timezone"]


# Test utility for creating a test workspace for a function
class TestInProcessWorkspaceLoadTarget(WorkspaceLoadTarget):
    def __init__(self, origin: InProcessRepositoryLocationOrigin):
        self._origin = origin

    def create_origins(self):
        return [self._origin]


@contextmanager
def in_process_test_workspace(instance, recon_repo):
    with WorkspaceProcessContext(
        instance, TestInProcessWorkspaceLoadTarget(InProcessRepositoryLocationOrigin(recon_repo))
    ) as workspace_process_context:
        yield workspace_process_context.create_request_context()
