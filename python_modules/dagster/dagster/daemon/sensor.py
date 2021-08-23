import os
import sys
import time
from collections import namedtuple

import pendulum
from dagster import check, seven
from dagster.core.definitions.run_request import JobType
from dagster.core.definitions.sensor import SensorExecutionData
from dagster.core.errors import DagsterError
from dagster.core.host_representation import ExternalPipeline, PipelineSelector
from dagster.core.instance import DagsterInstance
from dagster.core.scheduler.job import JobStatus, JobTickData, JobTickStatus, SensorJobData
from dagster.core.storage.pipeline_run import (
    JobTarget,
    PipelineRun,
    PipelineRunStatus,
    PipelineRunsFilter,
    PipelineTarget,
)
from dagster.core.storage.tags import RUN_KEY_TAG, check_tags
from dagster.core.workspace import IWorkspace
from dagster.utils import merge_dicts
from dagster.utils.error import serializable_error_info_from_exc_info

RECORDED_TICK_STATES = [JobTickStatus.SUCCESS, JobTickStatus.FAILURE]
FULFILLED_TICK_STATES = [JobTickStatus.SKIPPED, JobTickStatus.SUCCESS]

MIN_INTERVAL_LOOP_TIME = 5


class DagsterSensorDaemonError(DagsterError):
    """Error when running the SensorDaemon"""


class SkippedSensorRun(namedtuple("SkippedSensorRun", "run_key existing_run")):
    """Placeholder for runs that are skipped during the run_key idempotence check"""


class SensorLaunchContext:
    def __init__(self, external_sensor, job_state, tick, instance, logger):
        self._external_sensor = external_sensor
        self._instance = instance
        self._logger = logger
        self._job_state = job_state
        self._tick = tick

    @property
    def status(self):
        return self._tick.status

    @property
    def logger(self):
        return self._logger

    @property
    def run_count(self):
        return len(self._tick.run_ids)

    def update_state(self, status, **kwargs):
        skip_reason = kwargs.get("skip_reason")
        cursor = kwargs.get("cursor")
        origin_run_id = kwargs.get("origin_run_id")
        if "skip_reason" in kwargs:
            del kwargs["skip_reason"]

        if "cursor" in kwargs:
            del kwargs["cursor"]

        if "origin_run_id" in kwargs:
            del kwargs["origin_run_id"]
        if kwargs:
            check.inst_param(status, "status", JobTickStatus)

        if status:
            self._tick = self._tick.with_status(status=status, **kwargs)

        if skip_reason:
            self._tick = self._tick.with_reason(skip_reason=skip_reason)

        if cursor:
            self._tick = self._tick.with_cursor(cursor)

        if origin_run_id:
            self._tick = self._tick.with_origin_run(origin_run_id)

    def add_run(self, run_id, run_key=None):
        self._tick = self._tick.with_run(run_id, run_key)

    def _write(self):
        self._instance.update_job_tick(self._tick)
        if self._tick.status in FULFILLED_TICK_STATES:
            last_run_key = (
                self._job_state.job_specific_data.last_run_key
                if self._job_state.job_specific_data
                else None
            )
            if self._tick.run_keys:
                last_run_key = self._tick.run_keys[-1]
            self._instance.update_job_state(
                self._job_state.with_data(
                    SensorJobData(
                        last_tick_timestamp=self._tick.timestamp,
                        last_run_key=last_run_key,
                        min_interval=self._external_sensor.min_interval_seconds,
                        cursor=self._tick.cursor,
                    )
                )
            )

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # Log the error if the failure wasn't an interrupt or the daemon generator stopping
        if exception_value and not isinstance(exception_value, (KeyboardInterrupt, GeneratorExit)):
            error_data = serializable_error_info_from_exc_info(sys.exc_info())
            self.update_state(JobTickStatus.FAILURE, error=error_data)

        self._write()

        self._instance.purge_job_ticks(
            self._job_state.job_origin_id,
            tick_status=JobTickStatus.SKIPPED,
            before=pendulum.now("UTC").subtract(days=7).timestamp(),  #  keep the last 7 days
        )


def _check_for_debug_crash(debug_crash_flags, key):
    if not debug_crash_flags:
        return

    kill_signal = debug_crash_flags.get(key)
    if not kill_signal:
        return

    os.kill(os.getpid(), kill_signal)
    time.sleep(10)
    raise Exception("Process didn't terminate after sending crash signal")


def execute_sensor_iteration_loop(instance, workspace, logger, until=None):
    """
    Helper function that performs sensor evaluations on a tighter loop, while reusing grpc locations
    within a given daemon interval.  Rather than relying on the daemon machinery to run the
    iteration loop every 30 seconds, sensors are continuously evaluated, every 5 seconds. We rely on
    each sensor definition's min_interval to check that sensor evaluations are spaced appropriately.
    """
    manager_loaded_time = pendulum.now("UTC").timestamp()

    RELOAD_LOCATION_MANAGER_INTERVAL = 60

    workspace_iteration = 0
    start_time = pendulum.now("UTC").timestamp()
    while True:
        start_time = pendulum.now("UTC").timestamp()
        if until and start_time >= until:
            # provide a way of organically ending the loop to support test environment
            break

        if start_time - manager_loaded_time > RELOAD_LOCATION_MANAGER_INTERVAL:
            workspace.cleanup()
            manager_loaded_time = pendulum.now("UTC").timestamp()
            workspace_iteration = 0

        yield from execute_sensor_iteration(instance, logger, workspace, workspace_iteration)
        loop_duration = pendulum.now("UTC").timestamp() - start_time
        sleep_time = max(0, MIN_INTERVAL_LOOP_TIME - loop_duration)
        time.sleep(sleep_time)
        yield
        workspace_iteration += 1


def execute_sensor_iteration(
    instance, logger, workspace, workspace_iteration=None, debug_crash_flags=None
):
    check.inst_param(workspace, "workspace", IWorkspace)
    check.inst_param(instance, "instance", DagsterInstance)
    sensor_jobs = [
        s
        for s in instance.all_stored_job_state(job_type=JobType.SENSOR)
        if s.status == JobStatus.RUNNING
    ]
    if not sensor_jobs:
        if not workspace_iteration:
            logger.info("Not checking for any runs since no sensors have been started.")
        yield
        return

    for job_state in sensor_jobs:
        sensor_debug_crash_flags = (
            debug_crash_flags.get(job_state.job_name) if debug_crash_flags else None
        )
        error_info = None
        try:
            origin = job_state.origin.external_repository_origin.repository_location_origin
            repo_location = workspace.get_location(origin)

            repo_name = job_state.origin.external_repository_origin.repository_name

            if not repo_location.has_repository(repo_name):
                raise DagsterSensorDaemonError(
                    f"Could not find repository {repo_name} in location {repo_location.name} to "
                    + f"run sensor {job_state.job_name}. If this repository no longer exists, you can "
                    + "turn off the sensor in the Dagit UI.",
                )

            external_repo = repo_location.get_repository(repo_name)
            if not external_repo.has_external_sensor(job_state.job_name):
                raise DagsterSensorDaemonError(
                    f"Could not find sensor {job_state.job_name} in repository {repo_name}. If this "
                    "sensor no longer exists, you can turn it off in the Dagit UI.",
                )

            now = pendulum.now("UTC")
            if _is_under_min_interval(job_state, now):
                continue

            tick = instance.create_job_tick(
                JobTickData(
                    job_origin_id=job_state.job_origin_id,
                    job_name=job_state.job_name,
                    job_type=JobType.SENSOR,
                    status=JobTickStatus.STARTED,
                    timestamp=now.timestamp(),
                )
            )

            _check_for_debug_crash(sensor_debug_crash_flags, "TICK_CREATED")

            external_sensor = external_repo.get_external_sensor(job_state.job_name)
            with SensorLaunchContext(
                external_sensor, job_state, tick, instance, logger
            ) as tick_context:
                _check_for_debug_crash(sensor_debug_crash_flags, "TICK_HELD")
                yield from _evaluate_sensor(
                    tick_context,
                    instance,
                    workspace,
                    repo_location,
                    external_repo,
                    external_sensor,
                    job_state,
                    sensor_debug_crash_flags,
                )
        except Exception:  # pylint: disable=broad-except
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            logger.error(
                "Sensor daemon caught an error for sensor {sensor_name} : {error_info}".format(
                    sensor_name=job_state.job_name,
                    error_info=error_info.to_string(),
                )
            )
        yield error_info


def _evaluate_sensor(
    context,
    instance,
    workspace,
    repo_location,
    external_repo,
    external_sensor,
    job_state,
    sensor_debug_crash_flags=None,
):
    context.logger.info(f"Checking for new runs for sensor: {external_sensor.name}")
    sensor_runtime_data = repo_location.get_external_sensor_execution_data(
        instance,
        external_repo.handle,
        external_sensor.name,
        job_state.job_specific_data.last_tick_timestamp if job_state.job_specific_data else None,
        job_state.job_specific_data.last_run_key if job_state.job_specific_data else None,
        job_state.job_specific_data.cursor if job_state.job_specific_data else None,
    )

    yield

    assert isinstance(sensor_runtime_data, SensorExecutionData)
    if not sensor_runtime_data.run_requests:
        if sensor_runtime_data.pipeline_run_reactions:
            for pipeline_run_reaction in sensor_runtime_data.pipeline_run_reactions:
                origin_run_id = pipeline_run_reaction.pipeline_run.run_id
                message = (
                    f'Sensor "{external_sensor.name}" processed failure of run {origin_run_id}.'
                )

                if pipeline_run_reaction.error:
                    context.logger.error(
                        f"Got a reaction request for run {origin_run_id} but execution errorred: {pipeline_run_reaction.error}"
                    )
                    context.update_state(
                        JobTickStatus.FAILURE,
                        cursor=sensor_runtime_data.cursor,
                        error=pipeline_run_reaction.error,
                    )
                else:
                    # log to the original pipeline run
                    instance.report_engine_event(
                        message=message, pipeline_run=pipeline_run_reaction.pipeline_run
                    )
                    context.logger.info(
                        f"Completed a reaction request for run {origin_run_id}: {message}"
                    )
                    context.update_state(
                        JobTickStatus.SUCCESS,
                        cursor=sensor_runtime_data.cursor,
                        origin_run_id=origin_run_id,
                    )
        elif sensor_runtime_data.skip_message:
            context.logger.info(
                f"Sensor returned false for {external_sensor.name}, skipping: "
                f"{sensor_runtime_data.skip_message}"
            )
            context.update_state(
                JobTickStatus.SKIPPED,
                skip_reason=sensor_runtime_data.skip_message,
                cursor=sensor_runtime_data.cursor,
            )
        else:
            context.logger.info(f"Sensor returned false for {external_sensor.name}, skipping")
            context.update_state(JobTickStatus.SKIPPED, cursor=sensor_runtime_data.cursor)

        yield
        return

    pipeline_selector = PipelineSelector(
        location_name=repo_location.name,
        repository_name=external_repo.name,
        pipeline_name=external_sensor.pipeline_name,
        solid_selection=external_sensor.solid_selection,
    )
    subset_pipeline_result = repo_location.get_subset_external_pipeline_result(pipeline_selector)
    external_pipeline = ExternalPipeline(
        subset_pipeline_result.external_pipeline_data,
        external_repo.handle,
    )

    skipped_runs = []
    for run_request in sensor_runtime_data.run_requests:
        run = _get_or_create_sensor_run(
            context, instance, repo_location, external_sensor, external_pipeline, run_request
        )

        if isinstance(run, SkippedSensorRun):
            skipped_runs.append(run)
            yield
            continue

        _check_for_debug_crash(sensor_debug_crash_flags, "RUN_CREATED")

        error_info = None

        try:
            context.logger.info(
                "Launching run for {sensor_name}".format(sensor_name=external_sensor.name)
            )
            instance.submit_run(run.run_id, workspace)
            context.logger.info(
                "Completed launch of run {run_id} for {sensor_name}".format(
                    run_id=run.run_id, sensor_name=external_sensor.name
                )
            )
        except Exception:  # pylint: disable=broad-except
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            context.logger.error(
                f"Run {run.run_id} created successfully but failed to launch: " f"{str(error_info)}"
            )

        yield error_info

        _check_for_debug_crash(sensor_debug_crash_flags, "RUN_LAUNCHED")

        context.add_run(run_id=run.run_id, run_key=run_request.run_key)

    if skipped_runs:
        run_keys = [skipped.run_key for skipped in skipped_runs]
        skipped_count = len(skipped_runs)
        context.logger.info(
            f"Skipping {skipped_count} {'run' if skipped_count == 1 else 'runs'} for sensor "
            f"{external_sensor.name} already completed with run keys: {seven.json.dumps(run_keys)}"
        )

    if context.run_count:
        context.update_state(JobTickStatus.SUCCESS, cursor=sensor_runtime_data.cursor)
    else:
        context.update_state(JobTickStatus.SKIPPED, cursor=sensor_runtime_data.cursor)

    yield


def _is_under_min_interval(job_state, now):
    if not job_state.job_specific_data:
        return False

    if not job_state.job_specific_data.last_tick_timestamp:
        return False

    if not job_state.job_specific_data.min_interval:
        return False

    elapsed = now.timestamp() - job_state.job_specific_data.last_tick_timestamp
    return elapsed < job_state.job_specific_data.min_interval


def _get_or_create_sensor_run(
    context, instance, repo_location, external_sensor, external_pipeline, run_request
):

    if not run_request.run_key:
        return _create_sensor_run(
            instance, repo_location, external_sensor, external_pipeline, run_request
        )

    existing_runs = instance.get_runs(
        PipelineRunsFilter(
            tags=merge_dicts(
                PipelineRun.tags_for_sensor(external_sensor),
                {RUN_KEY_TAG: run_request.run_key},
            )
        )
    )

    if len(existing_runs):
        run = existing_runs[0]
        if run.status != PipelineRunStatus.NOT_STARTED:
            # A run already exists and was launched for this time period,
            # but the scheduler must have crashed before the tick could be put
            # into a SUCCESS state
            context.logger.info(f"Skipping run for {run_request.run_key}, found {run.run_id}.")
            return SkippedSensorRun(run_key=run_request.run_key, existing_run=run)
        else:
            context.logger.info(
                f"Run {run.run_id} already created with the run key "
                f"`{run_request.run_key}` for {external_sensor.name}"
            )
            return run

    context.logger.info(f"Creating new run for {external_sensor.name}")

    return _create_sensor_run(
        instance, repo_location, external_sensor, external_pipeline, run_request
    )


def _create_sensor_run(instance, repo_location, external_sensor, external_pipeline, run_request):
    external_execution_plan = repo_location.get_external_execution_plan(
        external_pipeline,
        run_request.run_config,
        external_sensor.mode,
        step_keys_to_execute=None,
        known_state=None,
    )
    execution_plan_snapshot = external_execution_plan.execution_plan_snapshot

    pipeline_tags = external_pipeline.tags or {}
    check_tags(pipeline_tags, "pipeline_tags")
    tags = merge_dicts(
        merge_dicts(pipeline_tags, run_request.tags),
        PipelineRun.tags_for_sensor(external_sensor),
    )
    if run_request.run_key:
        tags[RUN_KEY_TAG] = run_request.run_key

    if external_pipeline.snapshot_represents_job:
        target = JobTarget(name=external_sensor.pipeline_name)
    else:
        target = PipelineTarget(name=external_sensor.pipeline_name, mode=external_sensor.mode)

    return instance.create_run(
        target=target,
        run_id=None,
        run_config=run_request.run_config,
        solids_to_execute=external_pipeline.solids_to_execute,
        step_keys_to_execute=None,
        status=PipelineRunStatus.NOT_STARTED,
        solid_selection=external_sensor.solid_selection,
        root_run_id=None,
        parent_run_id=None,
        tags=tags,
        pipeline_snapshot=external_pipeline.pipeline_snapshot,
        execution_plan_snapshot=execution_plan_snapshot,
        parent_pipeline_snapshot=external_pipeline.parent_pipeline_snapshot,
        external_pipeline_origin=external_pipeline.get_external_origin(),
        pipeline_code_origin=external_pipeline.get_python_origin(),
    )
