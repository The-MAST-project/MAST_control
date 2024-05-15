import uvicorn
from fastapi import FastAPI
from common.config import Config
from scheduling.scheduler import router as scheduler_router
from scheduling.scheduler import scheduler

from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager


@asynccontextmanager
def lifespan(fast_app: FastAPI):
    scheduler.startup()
    yield
    scheduler.shutdown()


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


app.include_router(scheduler_router)


if __name__ == '__main__':
    cfg = Config()

    uvicorn_config = uvicorn.Config(app=app, host=cfg.toml['server']['host'], port=cfg.toml['server']['port'])
    uvicorn_server = uvicorn.Server(config=uvicorn_config)
    uvicorn_server.run()
