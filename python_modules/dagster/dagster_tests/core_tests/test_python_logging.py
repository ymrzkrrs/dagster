import logging
import pytest
import re

from dagster import (
    DagsterInstance,
    ModeDefinition,
    execute_pipeline,
    execute_solid,
    pipeline,
    resource,
    solid,
)

RUN_ID_REGEX = r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"


@pytest.fixture
def test_instance():
    return DagsterInstance.local_temp(
        overrides={"python_log_config": {"dagster_managed_logs": [""]}}
    )


def test_logging_capture_logger_defined_outside(test_instance):
    logger = logging.getLogger("some_logger")

    @solid
    def my_solid():
        logger.critical("some critical")

    @pipeline
    def my_pipeline():
        my_solid()

    result = execute_pipeline(my_pipeline, instance=test_instance)

    event_records = test_instance.event_log_storage.get_logs_for_run(result.run_id)
    log_event_records = [er for er in event_records if er.user_message == "some critical"]
    assert len(log_event_records) == 1
    log_event_record = log_event_records[0]
    assert log_event_record.step_key == "my_solid"
    assert log_event_record.level == logging.CRITICAL


def test_logging_capture_logger_defined_inside(test_instance):
    @solid
    def my_solid():
        logger = logging.getLogger("some_logger")
        logger.critical("some critical")

    @pipeline
    def my_pipeline():
        my_solid()

    result = execute_pipeline(my_pipeline, instance=test_instance)

    event_records = test_instance.event_log_storage.get_logs_for_run(result.run_id)
    log_event_records = [er for er in event_records if er.user_message == "some critical"]
    assert len(log_event_records) == 1
    log_event_record = log_event_records[0]
    assert log_event_record.step_key == "my_solid"
    assert log_event_record.level == logging.CRITICAL


def test_solid_python_logging(capsys, test_instance):
    @solid
    def logged_solid():
        python_log = logging.getLogger("python_log")
        python_log.setLevel(logging.DEBUG)
        python_log.debug("test python debug logging from logged_solid")
        python_log.critical("test python critical logging from logged_solid")

    @pipeline
    def pipe():
        logged_solid()

    result = execute_pipeline(pipe, instance=test_instance)
    assert result.success

    captured = capsys.readouterr()

    assert not re.search("test python debug logging from logged_solid", captured.err, re.MULTILINE)
    expected_regex = (
        r"python_log - CRITICAL - [a-f_]* - "
        + RUN_ID_REGEX
        + " logged_solid - test python critical logging from logged_solid"
    )
    assert re.search(expected_regex, captured.err, re.MULTILINE)


def test_resource_python_logging(capsys, test_instance):

    python_log = logging.getLogger("python_log")
    python_log.setLevel(logging.DEBUG)

    @resource
    def foo_resource():
        def fn():
            python_log.critical("test logging from foo resource")

        return fn

    @resource
    def bar_resource():
        def fn():
            python_log.critical("test logging from bar resource")

        return fn

    @solid(required_resource_keys={"foo", "bar"})
    def process(context):
        context.resources.foo()
        context.resources.bar()

    @pipeline(mode_defs=[ModeDefinition(resource_defs={"foo": foo_resource, "bar": bar_resource})])
    def process_pipeline():
        process()

    execute_pipeline(process_pipeline, instance=test_instance)

    captured = capsys.readouterr()

    expected_log_regexes = [
        r"python_log - CRITICAL - process_pipeline - "
        + RUN_ID_REGEX
        + r" - process - test logging from foo resource",
        r"python_log - CRITICAL - process_pipeline - "
        + RUN_ID_REGEX
        + r" - process - test logging from bar resource",
    ]
    for expected_log_regex in expected_log_regexes:
        assert re.search(expected_log_regex, captured.err, re.MULTILINE)
