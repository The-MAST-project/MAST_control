import uvicorn
from fastapi import FastAPI, WebSocket
from common.config import Config
from control.controller import router as controller_router
from control.controller import controller

from fastapi.responses import ORJSONResponse, RedirectResponse
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

from common.utils import BASE_CONTROL_PATH
@app.websocket(BASE_CONTROL_PATH + '/activity_notification_client')
async def activity_notification_ws(websocket: WebSocket):
    await controller.activity_notification_client(websocket)

app.include_router(controller_router)

@app.get("/favicon.ico")
def read_favicon():
    return RedirectResponse(url="/static/favicon.ico")

if __name__ == '__main__':
    cfg = Config()
    server_conf = cfg.get_service('control')
    uvicorn_config = uvicorn.Config(app=app, host=server_conf['listen_on'], port=server_conf['port'])
    uvicorn_server = uvicorn.Server(config=uvicorn_config)
    uvicorn_server.run()
