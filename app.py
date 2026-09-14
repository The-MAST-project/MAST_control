import argparse
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from common.config import Config
from common.mast_logging import configure_logging, get_logger
from control.controller import Controller
from control.data_server import DataServer

# Logging is configured once, here, before anything logs. Every 'mast.*' logger
# inherits the handlers and level from root by propagation.
# Precedence: --log-level > MAST_LOG_LEVEL > default.
_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--log-level", default=None, help="DEBUG, INFO, WARNING, ... (overrides MAST_LOG_LEVEL)")
configure_logging(_parser.parse_known_args()[0].log_level)

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"{app.title} starting up...")

    # Track the configuration database from here on. Opt-in by design (MAST_common#96): it
    # starts a thread holding a change-stream cursor, so it wants an owner with a lifetime,
    # and Config() is constructed by --help, by tests and by one-shot scripts.
    #
    # Not optional for this service, though it reads like it. Controller.managed_sites()
    # reads the sites live on every call, having replaced a copy that a 30-second timer kept
    # in step; the store is what makes that lookup current, and it reloads only while
    # something is watching. Without this call get_sites() returns the same snapshot for the
    # life of the process, and an edit to 'sites' reaches a running controller only at the
    # next restart -- with nothing to say so, since the lookup still looks live.
    #
    # Here rather than in the __main__ block below, which is where MAST_unit puts it: this
    # runs under any ASGI server, including `uvicorn app:app`, where __main__ never executes.
    # Never fatal -- a controller that cannot watch still runs on what it loaded.
    Config().start_watching()

    controller = Controller()
    yield
    # Shutdown
    logger.info(f"{app.title} shutting down...")
    # Paired with start_watching above, and before the controller goes down: the watcher
    # thread is a daemon, but stopping it explicitly closes the change stream rather than
    # leaving the cursor to the server's timeout.
    Config().stop_watching()
    controller.shutdown()
    controller.executor.shutdown(wait=True)
    controller.fetch_timer.cancel()


app = FastAPI(
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

    uvicorn_server = uvicorn.Server(config=uvicorn.Config(app=app, host=server_conf.listen_on, port=server_conf.port))

    logger.info(f"Starting MAST control server on {server_conf.listen_on}:{server_conf.port}...")
    uvicorn_server.run()
