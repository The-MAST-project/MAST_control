import uvicorn
from fastapi import FastAPI
from common.config import Config
from control.controller import router as controller_router
from control.controller import controller

from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager


@asynccontextmanager
def lifespan(fast_app: FastAPI):
    controller.startup()
    yield
    controller.shutdown()


app = FastAPI(
    default_response_class=ORJSONResponse,
    title="The MAST control API",
    # lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(controller_router)


if __name__ == '__main__':
    cfg = Config()
    server_conf = cfg.get_service('control')
    uvicorn_config = uvicorn.Config(app=app, host=server_conf['listen_on'], port=server_conf['port'])
    uvicorn_server = uvicorn.Server(config=uvicorn_config)
    uvicorn_server.run()
