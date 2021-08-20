import gzip
import io
from functools import partial
from os import path
from typing import List

from dagster import DagsterInstance
from dagster import __version__ as dagster_version
from dagster.cli.workspace.cli_target import get_workspace_process_context_from_kwargs
from dagster.core.debug import DebugRunPayload
from dagster.core.storage.compute_log_manager import ComputeIOType
from dagster.core.workspace.context import WorkspaceProcessContext
from dagster_graphql import __version__ as dagster_graphql_version
from dagster_graphql.schema import create_schema
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles

from .graphql import DagitGraphQL
from .version import __version__

ROOT_ADDRESS_STATIC_RESOURCES = [
    "/manifest.json",
    "/favicon.ico",
    "/favicon.png",
    "/asset-manifest.json",
    "/robots.txt",
    "/favicon_failed.ico",
    "/favicon_pending.ico",
    "/favicon_success.ico",
]


async def dagit_info_endpoint(_request):
    return JSONResponse(
        {
            "dagit_version": __version__,
            "dagster_version": dagster_version,
            "dagster_graphql_version": dagster_graphql_version,
        }
    )


async def download_debug_file_endpoint(
    context: WorkspaceProcessContext,
    request: Request,
):
    run_id = request.path_params["run_id"]
    run = context.instance.get_run_by_id(run_id)
    debug_payload = DebugRunPayload.build(context.instance, run)

    result = io.BytesIO()
    with gzip.GzipFile(fileobj=result, mode="wb") as file:
        debug_payload.write(file)

    result.seek(0)  # be kind, please rewind

    return StreamingResponse(result, media_type="application/gzip")


async def download_compute_logs_endpoint(
    context: WorkspaceProcessContext,
    request: Request,
):
    run_id = request.path_params["run_id"]
    step_key = request.path_params["step_key"]
    file_type = request.path_params["file_type"]

    file = context.instance.compute_log_manager.get_local_path(
        run_id,
        step_key,
        ComputeIOType(file_type),
    )

    if not path.exists(file):
        raise HTTPException(404)

    return FileResponse(
        context.instance.compute_log_manager.get_local_path(
            run_id,
            step_key,
            ComputeIOType(file_type),
        ),
        filename=f"{run_id}_{step_key}.{file_type}",
    )


def index_endpoint(
    base_dir: str,
    app_path_prefix: str,
    request: Request,  # pylint:disable=unused-argument
):
    """
    Serves root html
    """
    index_path = path.join(base_dir, "./webapp/build/index.html")

    try:
        with open(index_path) as f:
            rendered_template = f.read()
            return HTMLResponse(
                rendered_template.replace('href="/', f'href="{app_path_prefix}/')
                .replace('src="/', f'src="{app_path_prefix}/')
                .replace("__PATH_PREFIX__", app_path_prefix)
            )
    except FileNotFoundError:
        raise Exception(
            """
            Can't find webapp files.
            If you are using dagit, then probably it's a corrupted installation or a bug.
            However, if you are developing dagit locally, your problem can be fixed by running
            "make rebuild_dagit" in the project root.
            """
        )


def create_root_static_endpoints(base_dir: str) -> List[Route]:
    def _static_file(file_path):
        return Route(
            file_path,
            lambda _: FileResponse(path=path.join(base_dir, f"./webapp/build{file_path}")),
        )

    return [_static_file(f) for f in ROOT_ADDRESS_STATIC_RESOURCES]


def create_routes(
    process_context: WorkspaceProcessContext,
    app_path_prefix: str,
):
    graphql_schema = create_schema()
    dagit_graphql = DagitGraphQL(graphql_schema, process_context, app_path_prefix)
    static_resources_dir = path.dirname(__file__)
    bound_index_endpoint = partial(index_endpoint, static_resources_dir, app_path_prefix)

    return [
        Route("/dagit_info", dagit_info_endpoint),
        Route(
            "/graphql",
            dagit_graphql.http_endpoint,
            name="graphql-http",
            methods=["GET", "POST"],
        ),
        WebSocketRoute(
            "/graphql",
            dagit_graphql.ws_endpoint,
            name="graphql-ws",
        ),
        # download file endpoints
        Route(
            "/download/{run_id:str}/{step_key:str}/{file_type:str}",
            partial(download_compute_logs_endpoint, process_context),
        ),
        Route(
            "/download_debug/{run_id:str}",
            partial(download_debug_file_endpoint, process_context),
        ),
        # static resources addressed at /static/
        Mount(
            "/static",
            StaticFiles(
                directory=path.join(static_resources_dir, "webapp/build/static"),
                check_dir=False,
            ),
            name="static",
        ),
        # static resources addressed at /vendor/
        Mount(
            "/vendor",
            StaticFiles(
                directory=path.join(static_resources_dir, "webapp/build/vendor"),
                check_dir=False,
            ),
            name="vendor",
        ),
        # specific static resources addressed at /
        *create_root_static_endpoints(static_resources_dir),
        Route("/{path:path}", bound_index_endpoint),
        Route("/", bound_index_endpoint),
    ]


def create_app(
    process_context: WorkspaceProcessContext,
    debug: bool,
    app_path_prefix: str,
):
    return Starlette(
        debug=debug,
        routes=create_routes(
            process_context,
            app_path_prefix,
        ),
    )


def default_app():
    instance = DagsterInstance.get()
    process_context = get_workspace_process_context_from_kwargs(
        instance=instance,
        version=__version__,
        read_only=False,
        kwargs={},
    )
    return create_app(process_context, app_path_prefix="", debug=True)
