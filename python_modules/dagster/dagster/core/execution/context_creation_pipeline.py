import logging
import sys
from abc import ABC, abstractproperty
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from dagster import check
from dagster.core.definitions import (
    ExecutorDefinition,
    IntermediateStorageDefinition,
    ModeDefinition,
    PipelineDefinition,
)
from dagster.core.definitions.executor import check_cross_process_constraints
from dagster.core.definitions.pipeline_base import IPipeline
from dagster.core.definitions.resource import ScopedResourcesBuilder
from dagster.core.errors import DagsterError, DagsterUserCodeExecutionError
from dagster.core.events import DagsterEvent
from dagster.core.execution.memoization import validate_reexecution_memoization
from dagster.core.execution.plan.plan import ExecutionPlan
from dagster.core.execution.resources_init import (
    get_required_resource_keys_to_init,
    resource_initialization_manager,
)
from dagster.core.execution.retries import RetryMode
from dagster.core.executor.init import InitExecutorContext
from dagster.core.instance import DagsterInstance
from dagster.core.log_manager import DagsterLogManager, DagsterLoggingMetadata
from dagster.core.storage.init import InitIntermediateStorageContext
from dagster.core.storage.intermediate_storage import IntermediateStorage
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.core.storage.type_storage import construct_type_storage_plugin_registry
from dagster.core.system_config.objects import ResolvedRunConfig
from dagster.loggers import default_loggers, default_system_loggers
from dagster.utils import EventGenerationManager
from dagster.utils.error import serializable_error_info_from_exc_info

from .context.logger import InitLoggerContext
from .context.system import (
    ExecutionData,
    IPlanContext,
    PlanData,
    PlanExecutionContext,
    PlanOrchestrationContext,
)

if TYPE_CHECKING:
    from dagster.core.executor.base import Executor
    from dagster.core.execution.plan.outputs import StepOutputHandle


def initialize_console_manager(pipeline_run: Optional[PipelineRun]) -> DagsterLogManager:
    # initialize default colored console logger
    loggers = []
    for logger_def, logger_config in default_system_loggers():
        loggers.append(
            logger_def.logger_fn(
                InitLoggerContext(
                    logger_config, logger_def, run_id=pipeline_run.run_id if pipeline_run else None
                )
            )
        )
    return DagsterLogManager(
        DagsterLoggingMetadata(
            pipeline_tags=pipeline_run.tags if pipeline_run and pipeline_run.tags else {}
        ),
        loggers,
    )


def construct_intermediate_storage_data(
    storage_init_context: InitIntermediateStorageContext,
) -> IntermediateStorage:
    return storage_init_context.intermediate_storage_def.intermediate_storage_creation_fn(
        storage_init_context
    )


def executor_def_from_config(
    mode_definition: ModeDefinition, resolved_run_config: ResolvedRunConfig
) -> ExecutorDefinition:
    selected_executor = resolved_run_config.execution.execution_engine_name
    if selected_executor is None:
        if len(mode_definition.executor_defs) == 1:
            return mode_definition.executor_defs[0]

        check.failed(
            f"No executor selected but there are {len(mode_definition.executor_defs)} options."
        )

    else:
        for executor_def in mode_definition.executor_defs:
            if executor_def.name == selected_executor:
                return executor_def

        check.failed(
            f'Could not find executor "{selected_executor}". This should have been caught at config validation time.'
        )


# This represents all the data that is passed *into* context creation process.
# The remainder of the objects generated (e.g. loggers, resources) are created
# using user-defined code that may fail at runtime and result in the emission
# of a pipeline init failure events. The data in this object are passed all
# over the place during the context creation process so grouping here for
# ease of argument passing etc.
class ContextCreationData(NamedTuple):
    pipeline: IPipeline
    resolved_run_config: ResolvedRunConfig
    pipeline_run: PipelineRun
    mode_def: ModeDefinition
    intermediate_storage_def: IntermediateStorageDefinition
    executor_def: ExecutorDefinition
    instance: DagsterInstance
    resource_keys_to_init: AbstractSet[str]
    execution_plan: ExecutionPlan

    @property
    def pipeline_def(self) -> PipelineDefinition:
        return self.pipeline.get_definition()


def create_context_creation_data(
    pipeline: IPipeline,
    execution_plan: ExecutionPlan,
    run_config: Dict[str, Any],
    pipeline_run: PipelineRun,
    instance: DagsterInstance,
) -> "ContextCreationData":
    pipeline_def = pipeline.get_definition()
    resolved_run_config = ResolvedRunConfig.build(pipeline_def, run_config, mode=pipeline_run.mode)

    mode_def = pipeline_def.get_mode_definition(pipeline_run.mode)
    intermediate_storage_def = resolved_run_config.intermediate_storage_def_for_mode(mode_def)
    executor_def = executor_def_from_config(mode_def, resolved_run_config)

    return ContextCreationData(
        pipeline=pipeline,
        resolved_run_config=resolved_run_config,
        pipeline_run=pipeline_run,
        mode_def=mode_def,
        intermediate_storage_def=intermediate_storage_def,
        executor_def=executor_def,
        instance=instance,
        resource_keys_to_init=get_required_resource_keys_to_init(
            execution_plan, pipeline_def, resolved_run_config, intermediate_storage_def
        ),
        execution_plan=execution_plan,
    )


def create_plan_data(
    context_creation_data: "ContextCreationData", raise_on_error: bool, retry_mode: RetryMode
) -> PlanData:
    return PlanData(
        pipeline=context_creation_data.pipeline,
        pipeline_run=context_creation_data.pipeline_run,
        instance=context_creation_data.instance,
        execution_plan=context_creation_data.execution_plan,
        raise_on_error=raise_on_error,
        retry_mode=retry_mode,
    )


def create_execution_data(
    context_creation_data: "ContextCreationData",
    scoped_resources_builder: ScopedResourcesBuilder,
    intermediate_storage: IntermediateStorage,
) -> ExecutionData:
    return ExecutionData(
        scoped_resources_builder=scoped_resources_builder,
        intermediate_storage=intermediate_storage,
        intermediate_storage_def=context_creation_data.intermediate_storage_def,
        resolved_run_config=context_creation_data.resolved_run_config,
        pipeline_def=context_creation_data.pipeline_def,
        mode_def=context_creation_data.pipeline_def.get_mode_definition(
            context_creation_data.resolved_run_config.mode
        ),
    )


TContextType = TypeVar("TContextType", bound=IPlanContext)


class ExecutionContextManager(Generic[TContextType], ABC):
    def __init__(
        self,
        event_generator: Generator[Union[DagsterEvent, TContextType], None, None],
        raise_on_error: Optional[bool] = False,
    ):
        self._manager = EventGenerationManager[TContextType](
            generator=event_generator, object_cls=self.context_type, require_object=raise_on_error
        )

    @abstractproperty
    def context_type(self) -> Type[TContextType]:
        pass

    def prepare_context(self) -> Iterable[DagsterEvent]:  # ode to Preparable
        return self._manager.generate_setup_events()

    def get_context(self) -> TContextType:
        return self._manager.get_object()

    def shutdown_context(self) -> Iterable[DagsterEvent]:
        return self._manager.generate_teardown_events()

    def get_generator(self) -> Generator[Union[DagsterEvent, IPlanContext], None, None]:
        return self._manager.generator


def execution_context_event_generator(
    pipeline: IPipeline,
    execution_plan: ExecutionPlan,
    run_config: Dict[str, Any],
    pipeline_run: PipelineRun,
    instance: DagsterInstance,
    retry_mode: RetryMode,
    scoped_resources_builder_cm: Optional[
        Callable[..., EventGenerationManager[ScopedResourcesBuilder]]
    ] = None,
    intermediate_storage: Optional[IntermediateStorage] = None,
    raise_on_error: Optional[bool] = False,
    output_capture: Optional[Dict["StepOutputHandle", Any]] = None,
) -> Generator[Union[DagsterEvent, PlanExecutionContext], None, None]:
    scoped_resources_builder_cm = cast(
        Callable[..., EventGenerationManager[ScopedResourcesBuilder]],
        check.opt_callable_param(
            scoped_resources_builder_cm,
            "scoped_resources_builder_cm",
            default=resource_initialization_manager,
        ),
    )

    execution_plan = check.inst_param(execution_plan, "execution_plan", ExecutionPlan)
    pipeline_def = pipeline.get_definition()

    run_config = check.dict_param(run_config, "run_config", key_type=str)
    pipeline_run = check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
    instance = check.inst_param(instance, "instance", DagsterInstance)

    intermediate_storage = check.opt_inst_param(
        intermediate_storage, "intermediate_storage_data", IntermediateStorage
    )
    raise_on_error = check.bool_param(raise_on_error, "raise_on_error")

    context_creation_data = create_context_creation_data(
        pipeline,
        execution_plan,
        run_config,
        pipeline_run,
        instance,
    )

    log_manager = create_log_manager(context_creation_data)
    resource_defs = pipeline_def.get_required_resource_defs_for_mode(
        context_creation_data.resolved_run_config.mode
    )

    resources_manager = scoped_resources_builder_cm(
        resource_defs=resource_defs,
        resource_configs=context_creation_data.resolved_run_config.resources,
        log_manager=log_manager,
        execution_plan=execution_plan,
        pipeline_run=context_creation_data.pipeline_run,
        resource_keys_to_init=context_creation_data.resource_keys_to_init,
        instance=instance,
        emit_persistent_events=True,
        pipeline_def_for_backwards_compat=pipeline_def,
    )
    yield from resources_manager.generate_setup_events()
    scoped_resources_builder = check.inst(resources_manager.get_object(), ScopedResourcesBuilder)

    intermediate_storage = create_intermediate_storage(
        context_creation_data,
        intermediate_storage,
        scoped_resources_builder,
    )

    execution_context = PlanExecutionContext(
        plan_data=create_plan_data(context_creation_data, raise_on_error, retry_mode),
        execution_data=create_execution_data(
            context_creation_data, scoped_resources_builder, intermediate_storage
        ),
        log_manager=log_manager,
        output_capture=output_capture,
    )

    _validate_plan_with_context(execution_context, execution_plan)

    yield execution_context
    yield from resources_manager.generate_teardown_events()


class PlanOrchestrationContextManager(ExecutionContextManager[PlanOrchestrationContext]):
    def __init__(
        self,
        context_event_generator: Callable[
            ..., Generator[Union[DagsterEvent, PlanOrchestrationContext], None, None]
        ],
        pipeline: IPipeline,
        execution_plan: ExecutionPlan,
        run_config: Dict[str, Any],
        pipeline_run: PipelineRun,
        instance: DagsterInstance,
        raise_on_error: Optional[bool] = False,
        output_capture: Optional[Dict["StepOutputHandle", Any]] = None,
        executor_defs: Optional[List[ExecutorDefinition]] = None,
    ):
        event_generator = context_event_generator(
            pipeline,
            execution_plan,
            run_config,
            pipeline_run,
            instance,
            raise_on_error,
            executor_defs,
            output_capture,
        )
        super(PlanOrchestrationContextManager, self).__init__(event_generator)

    @property
    def context_type(self) -> Type[PlanOrchestrationContext]:
        return PlanOrchestrationContext


def orchestration_context_event_generator(
    pipeline: IPipeline,
    execution_plan: ExecutionPlan,
    run_config: Dict[str, Any],
    pipeline_run: PipelineRun,
    instance: DagsterInstance,
    raise_on_error: bool,
    executor_defs: Optional[List[ExecutorDefinition]],
    output_capture: Optional[Dict["StepOutputHandle", Any]],
) -> Generator[Union[DagsterEvent, PlanOrchestrationContext], None, None]:
    check.invariant(executor_defs is None)
    context_creation_data = create_context_creation_data(
        pipeline,
        execution_plan,
        run_config,
        pipeline_run,
        instance,
    )

    log_manager = create_log_manager(context_creation_data)

    try:
        executor = create_executor(context_creation_data)

        execution_context = PlanOrchestrationContext(
            plan_data=create_plan_data(context_creation_data, raise_on_error, executor.retries),
            log_manager=log_manager,
            executor=executor,
            output_capture=output_capture,
        )

        _validate_plan_with_context(execution_context, execution_plan)

        yield execution_context
    except DagsterError as dagster_error:
        dagster_error = cast(DagsterUserCodeExecutionError, dagster_error)
        user_facing_exc_info = (
            # pylint does not know original_exc_info exists is is_user_code_error is true
            # pylint: disable=no-member
            dagster_error.original_exc_info
            if dagster_error.is_user_code_error
            else sys.exc_info()
        )
        error_info = serializable_error_info_from_exc_info(user_facing_exc_info)

        event = DagsterEvent.pipeline_failure(
            pipeline_context_or_name=pipeline_run.pipeline_name,
            context_msg=(
                f'Pipeline failure during initialization for pipeline "{pipeline_run.pipeline_name}". '
                "This may be due to a failure in initializing the executor or one of the loggers."
            ),
            error_info=error_info,
        )
        log_manager.log_dagster_event(
            level=logging.ERROR, msg=event.message or "", dagster_event=event
        )
        yield event

        if raise_on_error:
            raise dagster_error


class PlanExecutionContextManager(ExecutionContextManager[PlanExecutionContext]):
    def __init__(
        self,
        pipeline: IPipeline,
        execution_plan: ExecutionPlan,
        run_config: Dict[str, Any],
        pipeline_run: PipelineRun,
        instance: DagsterInstance,
        retry_mode: RetryMode,
        scoped_resources_builder_cm: Optional[
            Callable[..., EventGenerationManager[ScopedResourcesBuilder]]
        ] = None,
        raise_on_error: Optional[bool] = False,
        output_capture: Optional[Dict["StepOutputHandle", Any]] = None,
    ):
        super(PlanExecutionContextManager, self).__init__(
            execution_context_event_generator(
                pipeline,
                execution_plan,
                run_config,
                pipeline_run,
                instance,
                retry_mode,
                scoped_resources_builder_cm,
                raise_on_error=raise_on_error,
                output_capture=output_capture,
            )
        )

    @property
    def context_type(self) -> Type[PlanExecutionContext]:
        return PlanExecutionContext


# perform any plan validation that is dependent on access to the pipeline context
def _validate_plan_with_context(
    pipeline_context: IPlanContext, execution_plan: ExecutionPlan
) -> None:
    validate_reexecution_memoization(pipeline_context, execution_plan)


def create_intermediate_storage(
    context_creation_data: ContextCreationData,
    intermediate_storage_data: Optional[IntermediateStorage],
    scoped_resources_builder: ScopedResourcesBuilder,
) -> IntermediateStorage:

    resolved_run_config, pipeline_def, intermediate_storage_def, pipeline_run = (
        context_creation_data.resolved_run_config,
        context_creation_data.pipeline_def,
        context_creation_data.intermediate_storage_def,
        context_creation_data.pipeline_run,
    )
    intermediate_storage_data = (
        intermediate_storage_data
        if intermediate_storage_data
        else construct_intermediate_storage_data(
            InitIntermediateStorageContext(
                pipeline_def=pipeline_def,
                mode_def=context_creation_data.mode_def,
                intermediate_storage_def=intermediate_storage_def,
                intermediate_storage_config=resolved_run_config.intermediate_storage.intermediate_storage_config,
                pipeline_run=pipeline_run,
                instance=context_creation_data.instance,
                resolved_run_config=resolved_run_config,
                type_storage_plugin_registry=construct_type_storage_plugin_registry(
                    pipeline_def, intermediate_storage_def
                ),
                resources=scoped_resources_builder.build(
                    context_creation_data.intermediate_storage_def.required_resource_keys,
                ),
            )
        )
    )

    return intermediate_storage_data


def create_executor(context_creation_data: ContextCreationData) -> "Executor":
    check.inst_param(context_creation_data, "context_creation_data", ContextCreationData)
    init_context = InitExecutorContext(
        pipeline=context_creation_data.pipeline,
        executor_def=context_creation_data.executor_def,
        executor_config=context_creation_data.resolved_run_config.execution.execution_engine_config,
        instance=context_creation_data.instance,
    )
    check_cross_process_constraints(init_context)
    return context_creation_data.executor_def.executor_creation_fn(init_context)


@contextmanager
def scoped_pipeline_context(
    execution_plan: ExecutionPlan,
    pipeline: IPipeline,
    run_config: Dict[str, Any],
    pipeline_run: PipelineRun,
    instance: DagsterInstance,
    scoped_resources_builder_cm: Optional[
        Callable[..., EventGenerationManager[ScopedResourcesBuilder]]
    ] = resource_initialization_manager,
    raise_on_error: Optional[bool] = False,
) -> Generator[PlanExecutionContext, None, None]:
    """Utility context manager which acts as a very thin wrapper around
    `pipeline_initialization_manager`, iterating through all the setup/teardown events and
    discarding them.  It yields the resulting `pipeline_context`.

    Should only be used where we need to reconstruct the pipeline context, ignoring any yielded
    events (e.g. PipelineExecutionResult, dagstermill, unit tests, etc)
    """
    check.inst_param(execution_plan, "execution_plan", ExecutionPlan)
    check.inst_param(pipeline, "pipeline", IPipeline)
    check.dict_param(run_config, "run_config", key_type=str)
    check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
    check.inst_param(instance, "instance", DagsterInstance)
    check.callable_param(scoped_resources_builder_cm, "scoped_resources_builder_cm")

    initialization_manager = PlanExecutionContextManager(
        pipeline,
        execution_plan,
        run_config,
        pipeline_run,
        instance,
        RetryMode.DISABLED,
        scoped_resources_builder_cm=scoped_resources_builder_cm,
        raise_on_error=raise_on_error,
    )
    for _ in initialization_manager.prepare_context():
        pass

    try:
        yield check.inst(initialization_manager.get_context(), PlanExecutionContext)
    finally:
        for _ in initialization_manager.shutdown_context():
            pass


def create_log_manager(context_creation_data: ContextCreationData) -> DagsterLogManager:
    check.inst_param(context_creation_data, "context_creation_data", ContextCreationData)

    pipeline_def, mode_def, resolved_run_config, pipeline_run = (
        context_creation_data.pipeline_def,
        context_creation_data.mode_def,
        context_creation_data.resolved_run_config,
        context_creation_data.pipeline_run,
    )

    # The following logic is tightly coupled to the processing of logger config in
    # python_modules/dagster/dagster/core/system_config/objects.py#config_map_loggers
    # Changes here should be accompanied checked against that function, which applies config mapping
    # via ConfigurableDefinition (@configured) to incoming logger configs. See docstring for more details.

    loggers = []
    for logger_key, logger_def in mode_def.loggers.items() or default_loggers().items():
        if logger_key in resolved_run_config.loggers:
            loggers.append(
                logger_def.logger_fn(
                    InitLoggerContext(
                        resolved_run_config.loggers.get(logger_key, {}).get("config"),
                        logger_def,
                        pipeline_def=pipeline_def,
                        run_id=pipeline_run.run_id,
                    )
                )
            )

    if not loggers:
        for (logger_def, logger_config) in default_system_loggers():
            loggers.append(
                logger_def.logger_fn(
                    InitLoggerContext(
                        logger_config,
                        logger_def,
                        pipeline_def=pipeline_def,
                        run_id=pipeline_run.run_id,
                    )
                )
            )

    handlers = context_creation_data.instance.get_handlers()

    return DagsterLogManager(
        logging_metadata=get_logging_metadata(pipeline_run),
        loggers=loggers,
        handlers=handlers,
    )


def _create_context_free_log_manager(
    instance: DagsterInstance, pipeline_run: PipelineRun, pipeline_def: PipelineDefinition
) -> DagsterLogManager:
    """In the event of pipeline initialization failure, we want to be able to log the failure
    without a dependency on the PlanExecutionContext to initialize DagsterLogManager.
    Args:
        pipeline_run (dagster.core.storage.pipeline_run.PipelineRun)
        pipeline_def (dagster.definitions.PipelineDefinition)
    """
    check.inst_param(instance, "instance", DagsterInstance)
    check.inst_param(pipeline_run, "pipeline_run", PipelineRun)
    check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)

    loggers = []
    # Use the default logger
    for (logger_def, logger_config) in default_system_loggers():
        loggers += [
            logger_def.logger_fn(
                InitLoggerContext(
                    logger_config,
                    logger_def,
                    pipeline_def=pipeline_def,
                    run_id=pipeline_run.run_id,
                )
            )
        ]

    return DagsterLogManager(get_logging_metadata(pipeline_run), loggers, instance.get_handlers())


def get_logging_metadata(pipeline_run: PipelineRun) -> DagsterLoggingMetadata:
    check.opt_inst_param(pipeline_run, "pipeline_run", PipelineRun)
    return DagsterLoggingMetadata(
        run_id=pipeline_run.run_id,
        pipeline_name=pipeline_run.pipeline_name,
        pipeline_tags=pipeline_run.tags,
    )
