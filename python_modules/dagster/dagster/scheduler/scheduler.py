import datetime
import os
import sys
import time

import pendulum
from dagster import check
from dagster.core.events import EngineEventData
from dagster.core.host_representation import ExternalPipeline, PipelineSelector, RepositoryLocation
from dagster.core.instance import DagsterInstance
from dagster.core.scheduler.job import JobState, JobStatus, JobTickData, JobTickStatus, JobType
from dagster.core.scheduler.scheduler import DEFAULT_MAX_CATCHUP_RUNS, DagsterSchedulerError
from dagster.core.storage.pipeline_run import (
    JobTarget,
    PipelineRun,
    PipelineRunStatus,
    PipelineRunsFilter,
    PipelineTarget,
)
from dagster.core.storage.tags import RUN_KEY_TAG, SCHEDULED_EXECUTION_TIME_TAG, check_tags
from dagster.core.workspace import IWorkspace
from dagster.seven.compat.pendulum import to_timezone
from dagster.utils import merge_dicts
from dagster.utils.error import serializable_error_info_from_exc_info


class _ScheduleLaunchContext:
    def __init__(self, tick, instance, logger):
        self._instance = instance
        self._logger = logger
        self._tick = tick

    def update_state(self, status, **kwargs):
        self._tick = self._tick.with_status(status=status, **kwargs)

    def add_run(self, run_id, run_key=None):
        self._tick = self._tick.with_run(run_id, run_key)

    def _write(self):
        self._instance.update_job_tick(self._tick)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # Log the error if the failure wasn't an interrupt or the daemon generator stopping
        if exception_value and not isinstance(exception_value, (KeyboardInterrupt, GeneratorExit)):
            error_data = serializable_error_info_from_exc_info(sys.exc_info())
            self.update_state(JobTickStatus.FAILURE, error=error_data)

        self._write()


_SCHEDULER_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S%z"


def execute_scheduler_iteration(instance, workspace, logger, max_catchup_runs):
    end_datetime_utc = pendulum.now("UTC")
    yield from launch_scheduled_runs(
        instance, workspace, logger, end_datetime_utc, max_catchup_runs
    )


def launch_scheduled_runs(
    instance,
    workspace,
    logger,
    end_datetime_utc,
    max_catchup_runs=DEFAULT_MAX_CATCHUP_RUNS,
    debug_crash_flags=None,
):
    check.inst_param(instance, "instance", DagsterInstance)
    check.inst_param(workspace, "workspace", IWorkspace)

    schedules = [
        s
        for s in instance.all_stored_job_state(job_type=JobType.SCHEDULE)
        if s.status == JobStatus.RUNNING
    ]

    if not schedules:
        logger.info("Not checking for any runs since no schedules have been started.")
        yield
        return

    schedule_names = ", ".join([schedule.job_name for schedule in schedules])
    logger.info(f"Checking for new runs for the following schedules: {schedule_names}")

    for schedule_state in schedules:
        error_info = None
        try:
            origin = schedule_state.origin.external_repository_origin.repository_location_origin
            repo_location = workspace.get_location(origin)
            yield from launch_scheduled_runs_for_schedule(
                instance,
                logger,
                schedule_state,
                workspace,
                repo_location,
                end_datetime_utc,
                max_catchup_runs,
                (debug_crash_flags.get(schedule_state.job_name) if debug_crash_flags else None),
            )
        except Exception:  # pylint: disable=broad-except
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            logger.error(
                f"Scheduler caught an error for schedule {schedule_state.job_name} : {error_info.to_string()}"
            )
        yield error_info


def launch_scheduled_runs_for_schedule(
    instance,
    logger,
    schedule_state,
    workspace,
    repo_location,
    end_datetime_utc,
    max_catchup_runs,
    debug_crash_flags=None,
):
    check.inst_param(instance, "instance", DagsterInstance)
    check.inst_param(schedule_state, "schedule_state", JobState)
    check.inst_param(end_datetime_utc, "end_datetime_utc", datetime.datetime)
    check.inst_param(repo_location, "repo_location", RepositoryLocation)

    latest_tick = instance.get_latest_job_tick(schedule_state.job_origin_id)

    if not latest_tick:
        start_timestamp_utc = schedule_state.job_specific_data.start_timestamp
    elif latest_tick.status == JobTickStatus.STARTED:
        # Scheduler was interrupted while performing this tick, re-do it
        start_timestamp_utc = latest_tick.timestamp
    else:
        start_timestamp_utc = latest_tick.timestamp + 1

    schedule_name = schedule_state.job_name
    repo_name = schedule_state.origin.external_repository_origin.repository_name

    if not repo_location.has_repository(repo_name):
        raise DagsterSchedulerError(
            f"Could not find repository {repo_name} in location {repo_location.name} to "
            + f"run schedule {schedule_name}. If this repository no longer exists, you can "
            + "turn off the schedule in the Dagit UI.",
        )

    external_repo = repo_location.get_repository(repo_name)

    if not external_repo.has_external_schedule(schedule_name):
        raise DagsterSchedulerError(
            f"Could not find schedule {schedule_name} in repository {repo_name}. If this "
            "schedule no longer exists, you can turn it off in the Dagit UI.",
        )

    external_schedule = external_repo.get_external_schedule(schedule_name)

    timezone_str = external_schedule.execution_timezone
    if not timezone_str:
        timezone_str = "UTC"
        logger.warn(
            f"Using UTC as the timezone for {external_schedule.name} as it did not specify "
            "an execution_timezone in its definition."
        )

    tick_times = []
    for next_time in external_schedule.execution_time_iterator(start_timestamp_utc):
        if next_time.timestamp() > end_datetime_utc.timestamp():
            break

        tick_times.append(next_time)

    if not tick_times:
        logger.info(f"No new runs for {schedule_name}")
        return

    if not external_schedule.partition_set_name and len(tick_times) > 1:
        logger.warning(f"{schedule_name} has no partition set, so not trying to catch up")
        tick_times = tick_times[-1:]
    elif len(tick_times) > max_catchup_runs:
        logger.warning(f"{schedule_name} has fallen behind, only launching {max_catchup_runs} runs")
        tick_times = tick_times[-max_catchup_runs:]

    if len(tick_times) == 1:
        tick_time = tick_times[0].strftime(_SCHEDULER_DATETIME_FORMAT)
        logger.info(f"Evaluating schedule `{schedule_name}` at {tick_time}")
    else:
        times = ", ".join([time.strftime(_SCHEDULER_DATETIME_FORMAT) for time in tick_times])
        logger.info(f"Evaluating schedule `{schedule_name}` at the following times: {times}")

    for schedule_time in tick_times:
        schedule_timestamp = schedule_time.timestamp()
        if latest_tick and latest_tick.timestamp == schedule_timestamp:
            tick = latest_tick
            logger.info("Resuming previously interrupted schedule execution")

        else:
            tick = instance.create_job_tick(
                JobTickData(
                    job_origin_id=external_schedule.get_external_origin_id(),
                    job_name=schedule_name,
                    job_type=JobType.SCHEDULE,
                    status=JobTickStatus.STARTED,
                    timestamp=schedule_timestamp,
                )
            )

            _check_for_debug_crash(debug_crash_flags, "TICK_CREATED")

        with _ScheduleLaunchContext(tick, instance, logger) as tick_context:
            _check_for_debug_crash(debug_crash_flags, "TICK_HELD")

            yield from _schedule_runs_at_time(
                instance,
                logger,
                workspace,
                repo_location,
                external_repo,
                external_schedule,
                schedule_time,
                tick_context,
                debug_crash_flags,
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


def _schedule_runs_at_time(
    instance,
    logger,
    workspace,
    repo_location,
    external_repo,
    external_schedule,
    schedule_time,
    tick_context,
    debug_crash_flags,
):
    schedule_name = external_schedule.name

    pipeline_selector = PipelineSelector(
        location_name=repo_location.name,
        repository_name=external_repo.name,
        pipeline_name=external_schedule.pipeline_name,
        solid_selection=external_schedule.solid_selection,
    )

    subset_pipeline_result = repo_location.get_subset_external_pipeline_result(pipeline_selector)
    external_pipeline = ExternalPipeline(
        subset_pipeline_result.external_pipeline_data,
        external_repo.handle,
    )

    schedule_execution_data = repo_location.get_external_schedule_execution_data(
        instance=instance,
        repository_handle=external_repo.handle,
        schedule_name=external_schedule.name,
        scheduled_execution_time=schedule_time,
    )
    yield

    if not schedule_execution_data.run_requests:
        logger.info(f"No run requests returned for {external_schedule.name}, skipping")

        # Update tick to skipped state and return
        tick_context.update_state(JobTickStatus.SKIPPED)
        return

    for run_request in schedule_execution_data.run_requests:
        run = _get_existing_run_for_request(instance, external_schedule, schedule_time, run_request)
        if run:
            if run.status != PipelineRunStatus.NOT_STARTED:
                # A run already exists and was launched for this time period,
                # but the scheduler must have crashed before the tick could be put
                # into a SUCCESS state

                logger.info(
                    f"Run {run.run_id} already completed for this execution of {external_schedule.name}"
                )
                tick_context.add_run(run_id=run.run_id, run_key=run_request.run_key)
                yield
                continue
            else:
                logger.info(
                    f"Run {run.run_id} already created for this execution of {external_schedule.name}"
                )
        else:
            run, errors = _create_scheduler_run(
                instance,
                logger,
                schedule_time,
                repo_location,
                external_schedule,
                external_pipeline,
                run_request,
            )
            for error in errors:
                yield error

        _check_for_debug_crash(debug_crash_flags, "RUN_CREATED")

        if run.status != PipelineRunStatus.FAILURE:
            try:
                instance.submit_run(run.run_id, workspace)
                logger.info(f"Completed scheduled launch of run {run.run_id} for {schedule_name}")
            except Exception:  # pylint: disable=broad-except
                error_info = serializable_error_info_from_exc_info(sys.exc_info())
                logger.error(
                    f"Run {run.run_id} created successfully but failed to launch: {str(serializable_error_info_from_exc_info(sys.exc_info()))}"
                )
                yield error_info

        _check_for_debug_crash(debug_crash_flags, "RUN_LAUNCHED")
        tick_context.add_run(run_id=run.run_id, run_key=run_request.run_key)
        _check_for_debug_crash(debug_crash_flags, "RUN_ADDED")
        yield

    _check_for_debug_crash(debug_crash_flags, "TICK_SUCCESS")
    tick_context.update_state(JobTickStatus.SUCCESS)


def _get_existing_run_for_request(instance, external_schedule, schedule_time, run_request):
    tags = merge_dicts(
        PipelineRun.tags_for_schedule(external_schedule),
        {
            SCHEDULED_EXECUTION_TIME_TAG: to_timezone(schedule_time, "UTC").isoformat(),
        },
    )
    if run_request.run_key:
        tags[RUN_KEY_TAG] = run_request.run_key
    runs_filter = PipelineRunsFilter(tags=tags)
    existing_runs = instance.get_runs(runs_filter)
    if not len(existing_runs):
        return None
    return existing_runs[0]


def _create_scheduler_run(
    instance,
    logger,
    schedule_time,
    repo_location,
    external_schedule,
    external_pipeline,
    run_request,
):
    run_config = run_request.run_config
    schedule_tags = run_request.tags

    execution_plan_errors = []
    execution_plan_snapshot = None

    try:
        external_execution_plan = repo_location.get_external_execution_plan(
            external_pipeline,
            run_config,
            external_schedule.mode,
            step_keys_to_execute=None,
            known_state=None,
        )
        execution_plan_snapshot = external_execution_plan.execution_plan_snapshot
    except Exception:  # pylint: disable=broad-except
        execution_plan_errors.append(serializable_error_info_from_exc_info(sys.exc_info()))

    pipeline_tags = external_pipeline.tags or {}
    check_tags(pipeline_tags, "pipeline_tags")
    tags = merge_dicts(pipeline_tags, schedule_tags)

    tags[SCHEDULED_EXECUTION_TIME_TAG] = to_timezone(schedule_time, "UTC").isoformat()
    if run_request.run_key:
        tags[RUN_KEY_TAG] = run_request.run_key

    if external_pipeline.snapshot_represents_job:
        target = JobTarget(name=external_schedule.pipeline_name)
    else:
        target = PipelineTarget(name=external_schedule.pipeline_name, mode=external_schedule.mode)

    # If the run was scheduled correctly but there was an error creating its
    # run config, enter it into the run DB with a FAILURE status
    possibly_invalid_pipeline_run = instance.create_run(
        target=target,
        run_id=None,
        run_config=run_config,
        solids_to_execute=external_pipeline.solids_to_execute,
        step_keys_to_execute=None,
        solid_selection=external_pipeline.solid_selection,
        status=(
            PipelineRunStatus.FAILURE
            if len(execution_plan_errors) > 0
            else PipelineRunStatus.NOT_STARTED
        ),
        root_run_id=None,
        parent_run_id=None,
        tags=tags,
        pipeline_snapshot=external_pipeline.pipeline_snapshot,
        execution_plan_snapshot=execution_plan_snapshot,
        parent_pipeline_snapshot=external_pipeline.parent_pipeline_snapshot,
        external_pipeline_origin=external_pipeline.get_external_origin(),
        pipeline_code_origin=external_pipeline.get_python_origin(),
    )

    if len(execution_plan_errors) > 0:
        for error in execution_plan_errors:
            instance.report_engine_event(
                error.message,
                possibly_invalid_pipeline_run,
                EngineEventData.engine_error(error),
            )
        instance.report_run_failed(possibly_invalid_pipeline_run)
        error_string = "\n".join([error.to_string() for error in execution_plan_errors])
        logger.error(f"Failed to fetch execution plan for {external_schedule.name}: {error_string}")
    return (possibly_invalid_pipeline_run, execution_plan_errors)
