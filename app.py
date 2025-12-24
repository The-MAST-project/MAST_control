from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse, RedirectResponse

from common.config import Config
from common.const import Const
from control.controller import Controller
from control.data_server import DataServer


@asynccontextmanager
async def lifespan(fast_app: FastAPI):
    Controller().startup()
    yield
    Controller().shutdown()


app = FastAPI(
    default_response_class=ORJSONResponse,
    title="The MAST control API",
    # lifespan=lifespan,
)

# origins = [
#     "http://localhost",
#     "http://mast-wis-control",
#     "http://mast-wis-control.weizmann.ac.il"
#     "http://localhost:3000",
#     "http://mast-wis-control:3000",
#     "http://mast-wis-control.weizmann.ac.il:3000"
# ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket(Const.BASE_CONTROL_PATH + "/activity_notification_client")
async def activity_notification_ws(websocket: WebSocket):
    await Controller().activity_notification_client(websocket)


app.include_router(Controller().api_router)
app.include_router(DataServer().api_router)


@app.get("/favicon.ico")
def read_favicon():
    return RedirectResponse(url="/static/favicon.ico")


if __name__ == "__main__":
    cfg = Config()
    server_conf = cfg.get_service("control")
    assert server_conf is not None, "cannot get server_conf"

    uvicorn_server = uvicorn.Server(
        config=uvicorn.Config(
            app=app, host=server_conf.listen_on, port=server_conf.port
        )
    )
    import logging

    from common.mast_logging import init_log

    logger = logging.Logger("uvicorn")
    init_log(
        logger,
        level=logging.DEBUG,
    )
    logger.info(
        f"Starting MAST control server on {server_conf.listen_on}:{server_conf.port}..."
    )
    uvicorn_server.run()
