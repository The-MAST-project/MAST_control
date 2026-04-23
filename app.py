from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse, RedirectResponse

from MAST_common.config import Config
from control.controller import Controller
from control.data_server import DataServer


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"{app.title} starting up...")
    controller = Controller()
    yield
    # Shutdown
    logger.info(f"{app.title} shutting down...")
    controller.shutdown()
    controller.executor.shutdown(wait=True)
    controller.config_timer.cancel()
    controller.fetch_timer.cancel()


app = FastAPI(
    default_response_class=ORJSONResponse,
    title="MAST_control",
    lifespan=lifespan,
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

    from MAST_common.mast_logging import init_log

    logger = logging.Logger("uvicorn")
    init_log(
        logger,
        level=logging.DEBUG,
    )
    logger.info(
        f"Starting MAST control server on {server_conf.listen_on}:{server_conf.port}..."
    )
    uvicorn_server.run()
