from asyncio import Queue, get_event_loop
from enum import Enum
from typing import Any, AsyncGenerator, Dict, Union

from dagster import check
from dagster.core.workspace.context import WorkspaceProcessContext
from graphene import Schema
from graphql.error import GraphQLError
from graphql.error import format_error as format_graphql_error
from graphql.execution import ExecutionResult
from rx import Observable
from starlette import status
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import QueryParams
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from .templates.playground import TEMPLATE


class GraphQLWS(str, Enum):
    PROTOCOL = "graphql-ws"

    # https://github.com/apollographql/subscriptions-transport-ws/blob/master/PROTOCOL.md
    CONNECTION_INIT = "connection_init"
    CONNECTION_ACK = "connection_ack"
    CONNECTION_ERROR = "connection_error"
    CONNECTION_TERMINATE = "connection_terminate"
    CONNECTION_KEEP_ALIVE = "ka"
    START = "start"
    DATA = "data"
    ERROR = "error"
    COMPLETE = "complete"
    STOP = "stop"


class DagitGraphQL:
    def __init__(
        self,
        schema: Schema,
        process_context: WorkspaceProcessContext,
        app_path_prefix: str = "",
        middleware: list = None,
    ):
        self._schema = schema
        self._process_context = process_context
        self._app_path_prefix = app_path_prefix
        self._middleware = check.opt_list_param(middleware, "middleware")

    async def http_endpoint(self, request: Request):
        """
        fork of starlette GraphQLApp to allow for
            * our context type (crucial)
            * our GraphiQL playground (could change)
        """

        if request.method == "GET":
            # render graphiql
            if "text/html" in request.headers.get("Accept", ""):
                text = TEMPLATE.replace("{{ app_path_prefix }}", self._app_path_prefix)
                return HTMLResponse(text)

            data: Union[Dict[str, str], QueryParams] = request.query_params

        elif request.method == "POST":
            content_type = request.headers.get("Content-Type", "")

            if "application/json" in content_type:
                data = await request.json()
            elif "application/graphql" in content_type:
                body = await request.body()
                text = body.decode()
                data = {"query": text}
            elif "query" in request.query_params:
                data = request.query_params
            else:
                return PlainTextResponse(
                    "Unsupported Media Type",
                    status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                )

        else:
            return PlainTextResponse(
                "Method Not Allowed", status_code=status.HTTP_405_METHOD_NOT_ALLOWED
            )

        if "query" not in data:
            return PlainTextResponse(
                "No GraphQL query found in the request",
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        query = data["query"]
        variables = data.get("variables")
        operation_name = data.get("operationName")

        # context manager? scoping?
        context = self._process_context.create_request_context(request)

        result = await run_in_threadpool(  # threadpool = aio event loop
            self._schema.execute,
            query,
            variables=variables,
            operation_name=operation_name,
            context=context,
            middleware=self._middleware,
        )

        error_data = [format_graphql_error(err) for err in result.errors] if result.errors else None
        response_data = {"data": result.data}
        if error_data:
            response_data["errors"] = error_data
        status_code = status.HTTP_400_BAD_REQUEST if result.errors else status.HTTP_200_OK

        return JSONResponse(response_data, status_code=status_code)

    async def ws_endpoint(self, websocket: WebSocket):
        """
        Implementation of websocket ASGI endpoint for GraphQL.
        Once we are free of conflicting deps, we should be able to use an impl from
        strawberry-graphql or the like.
        """

        observables = {}
        tasks = {}

        await websocket.accept(subprotocol=GraphQLWS.PROTOCOL)

        try:
            while (
                websocket.client_state != WebSocketState.DISCONNECTED
                and websocket.application_state != WebSocketState.DISCONNECTED
            ):
                message = await websocket.receive_json()
                operation_id = message.get("id")
                message_type = message.get("type")

                if message_type == GraphQLWS.CONNECTION_INIT:
                    await websocket.send_json({"type": GraphQLWS.CONNECTION_ACK})

                elif message_type == GraphQLWS.CONNECTION_TERMINATE:
                    await websocket.close()
                elif message_type == GraphQLWS.START:
                    try:
                        data = message["payload"]
                        query = data["query"]
                        variables = data.get("variables")
                        operation_name = data.get("operation_name")

                        # correct scoping?
                        request_context = self._process_context.create_request_context(websocket)
                        async_result = self._schema.execute(
                            query,
                            variables=variables,
                            operation_name=operation_name,
                            context=request_context,
                            middleware=self._middleware,
                            allow_subscriptions=True,
                        )
                    except GraphQLError as error:
                        payload = format_graphql_error(error)
                        await _send_message(websocket, GraphQLWS.ERROR, payload, operation_id)
                        continue

                    if isinstance(async_result, ExecutionResult):
                        if not async_result.errors:
                            check.failed(
                                f"Only expect non-async result on error, got {async_result}"
                            )
                        payload = format_graphql_error(async_result.errors[0])  # type: ignore
                        await _send_message(websocket, GraphQLWS.ERROR, payload, operation_id)
                        continue

                    # in the future we should get back async gen directly, back compat for now
                    disposable, async_gen = _disposable_and_async_gen_from_obs(async_result)

                    observables[operation_id] = disposable
                    tasks[operation_id] = get_event_loop().create_task(
                        handle_async_results(async_gen, operation_id, websocket)
                    )
                elif message_type == GraphQLWS.STOP:
                    if operation_id not in observables:
                        return

                    observables[operation_id].dispose()
                    del observables[operation_id]

                    tasks[operation_id].cancel()
                    del tasks[operation_id]

        except WebSocketDisconnect:
            pass
        finally:
            for operation_id in observables:
                observables[operation_id].dispose()
                tasks[operation_id].cancel()


async def handle_async_results(results: AsyncGenerator, operation_id: str, websocket: WebSocket):
    try:
        async for result in results:
            payload = {"data": result.data}

            if result.errors:
                payload["errors"] = [format_graphql_error(err) for err in result.errors]

            await _send_message(websocket, GraphQLWS.DATA, payload, operation_id)
    except Exception as error:  # pylint: disable=broad-except
        if not isinstance(error, GraphQLError):
            error = GraphQLError(str(error))

        await _send_message(
            websocket,
            GraphQLWS.DATA,
            {"data": None, "errors": [format_graphql_error(error)]},
            operation_id,
        )

    if (
        websocket.client_state != WebSocketState.DISCONNECTED
        and websocket.application_state != WebSocketState.DISCONNECTED
    ):
        await _send_message(websocket, GraphQLWS.COMPLETE, None, operation_id)


async def _send_message(
    websocket: WebSocket,
    type_: GraphQLWS,
    payload: Any,
    operation_id: str,
) -> None:
    data = {"type": type_, "id": operation_id}

    if payload is not None:
        data["payload"] = payload

    return await websocket.send_json(data)


def _disposable_and_async_gen_from_obs(obs: Observable):
    """
    Compatability layer for legacy Observable to async generator

    This should be removed and subscription resolvers changed to
    return async generators after removal of flask & gevent based dagit.
    """
    queue: Queue = Queue()

    disposable = obs.subscribe(on_next=queue.put_nowait)

    async def async_gen():
        while True:
            i = await queue.get()
            yield i

    return disposable, async_gen()
