import logging
import pytest

from dagster import (
    DagsterInstance,
    ModeDefinition,
    execute_pipeline,
    pipeline,
    resource,
    solid,
)


def get_log_records(pipe, managed_logs):
    instance = DagsterInstance.local_temp(
        overrides={"python_logs": {"managed_python_logs": managed_logs}}
    )
    result = execute_pipeline(pipe, instance=instance)

    event_records = instance.event_log_storage.get_logs_for_run(result.run_id)
    return [er for er in event_records if er.user_message]


@pytest.mark.parametrize(
    "managed_logs,expect_output",
    [
        (["root"], True),
        (["python_logger"], True),
        (["some_logger"], False),
        (["root", "python_logger"], True),
    ],
)
def test_logging_capture_logger_defined_outside(managed_logs, expect_output):
    logger = logging.getLogger("python_logger")
    logger.setLevel(logging.INFO)

    @solid
    def my_solid():
        logger.info("some info")

    @pipeline
    def my_pipeline():
        my_solid()

    log_event_records = [
        lr for lr in get_log_records(my_pipeline, managed_logs) if lr.user_message == "some info"
    ]

    if expect_output:
        assert len(log_event_records) == 1
        log_event_record = log_event_records[0]
        assert log_event_record.step_key == "my_solid"
        assert log_event_record.level == logging.INFO
    else:
        assert len(log_event_records) == 0


@pytest.mark.parametrize(
    "managed_logs,expect_output",
    [
        (["root"], True),
        (["python_logger"], True),
        (["some_logger"], False),
        (["root", "python_logger"], True),
    ],
)
def test_logging_capture_logger_defined_inside(managed_logs, expect_output):
    @solid
    def my_solid():
        logger = logging.getLogger("python_logger")
        logger.setLevel(logging.INFO)
        logger.info("some info")

    @pipeline
    def my_pipeline():
        my_solid()

    log_event_records = [
        lr for lr in get_log_records(my_pipeline, managed_logs) if lr.user_message == "some info"
    ]

    if expect_output:
        assert len(log_event_records) == 1
        log_event_record = log_event_records[0]
        assert log_event_record.step_key == "my_solid"
        assert log_event_record.level == logging.INFO
    else:
        assert len(log_event_records) == 0


@pytest.mark.parametrize(
    "managed_logs,expect_output",
    [
        (["root"], True),
        (["python_logger"], True),
        (["some_logger"], False),
        (["root", "python_logger"], True),
    ],
)
def test_logging_capture_resource(managed_logs, expect_output):

    python_log = logging.getLogger("python_logger")
    python_log.setLevel(logging.DEBUG)

    @resource
    def foo_resource():
        def fn():
            python_log.info("log from resource foo")

        return fn

    @resource
    def bar_resource():
        def fn():
            python_log.info("log from resource bar")

        return fn

    @solid(required_resource_keys={"foo", "bar"})
    def process(context):
        context.resources.foo()
        context.resources.bar()

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"foo": foo_resource, "bar": bar_resource})])
    def my_pipeline():
        process()

    log_event_records = [
        lr
        for lr in get_log_records(my_pipeline, managed_logs)
        if lr.user_message.startswith("log from resource")
    ]

    if expect_output:
        assert len(log_event_records) == 2
        log_event_record = log_event_records[0]
        assert log_event_record.level == logging.INFO
    else:
        assert len(log_event_records) == 0