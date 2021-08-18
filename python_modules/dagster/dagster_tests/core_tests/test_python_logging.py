import logging
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


def test_logging_capture_logger_defined_outside():
    logger = logging.getLogger("some_logger")

    @solid
    def my_solid():
        logger.critical("some critical")

    @pipeline
    def my_pipeline():
        my_solid()

    instance = DagsterInstance.ephemeral()
    result = execute_pipeline(my_pipeline, instance=instance)

    event_records = instance.event_log_storage.get_logs_for_run(result.run_id)
    log_event_records = [er for er in event_records if er.user_message == "some critical"]
    assert len(log_event_records) == 1
    log_event_record = log_event_records[0]
    assert log_event_record.step_key == "my_solid"
    assert log_event_record.level == logging.CRITICAL


def test_logging_capture_logger_defined_inside():
    @solid
    def my_solid():
        logger = logging.getLogger("some_logger")
        logger.critical("some critical")

    @pipeline
    def my_pipeline():
        my_solid()

    instance = DagsterInstance.ephemeral()
    result = execute_pipeline(my_pipeline, instance=instance)

    event_records = instance.event_log_storage.get_logs_for_run(result.run_id)
    log_event_records = [er for er in event_records if er.user_message == "some critical"]
    assert len(log_event_records) == 1
    log_event_record = log_event_records[0]
    assert log_event_record.step_key == "my_solid"
    assert log_event_record.level == logging.CRITICAL


def test_solid_python_logging(capsys):
    @solid
    def logged_solid():
        python_log = logging.getLogger("python_log")
        python_log.setLevel(logging.DEBUG)
        python_log.debug("test python debug logging from logged_solid")
        python_log.critical("test python critical logging from logged_solid")

    result = execute_solid(logged_solid)
    assert result.success

    captured = capsys.readouterr()

    assert not re.search("test python debug logging from logged_solid", captured.err, re.MULTILINE)
    assert re.search("test python critical logging from logged_solid", captured.err, re.MULTILINE)


def test_resource_python_logging(capsys):

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

    execute_pipeline(process_pipeline)

    captured = capsys.readouterr()

    expected_log_regexes = [
        r"python_log - CRITICAL - process_pipeline - [a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-"
        r"[a-f0-9]{12} - process - test logging from foo resource",
        r"python_log - CRITICAL - process_pipeline - [a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-"
        r"[a-f0-9]{12} - process - test logging from bar resource",
    ]
    for expected_log_regex in expected_log_regexes:
        assert re.search(expected_log_regex, captured.err, re.MULTILINE)
