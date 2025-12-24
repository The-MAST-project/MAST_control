import asyncio
import logging
import os
import random
import socket
from pathlib import Path
from typing import Literal, Set, cast

from cachetools import TTLCache, cached
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from common.api import SpecApi, UnitApi
from common.canonical import CanonicalResponse, CanonicalResponse_Ok
from common.config import Config, Site, UnitConfig
from common.const import Const
from common.dlipowerswitch import (
    OutletDomain,
    SwitchedOutlet,
)
from common.interfaces.components import Component
from common.mast_logging import init_log
from common.models.statuses import (
    ShortStatus,
    SpecStatus,
    UnitStatus,
)
from common.networking import NetworkedDevice
from common.tasks.models import TaskAcquisitionPathNotification
from common.utils import (
    RepeatTimer,
    function_name,
)
from control.planning import Planner

logger = logging.getLogger("controller")
init_log(logger)

_units_status_cache = TTLCache(maxsize=20, ttl=30)


@cached(_units_status_cache, key=lambda self: self.name)
def _fetch_status_from_api(self) -> "UnitStatus | None":
    """
    Module-level cached helper. Cache key is unit.name.
    This calls the unit API and returns the parsed UnitStatus (or None).
    """
    response: CanonicalResponse = asyncio.run(self.api.get("status"))
    return cast(UnitStatus, response.value) if response.succeeded else None


class ControllerStatus(BaseModel):
    detected: bool
    spec: dict
    units: dict
    date: str


class ControlledSpec(Component):
    @property
    def was_shut_down(self) -> bool:
        return self._was_shut_down

    @property
    def status(self):
        return self._status

    @property
    def name(self) -> str:
        return "spec"

    def __init__(self, site: Site, controller: "Controller"):
        Component.__init__(self)
        self.site = site
        self.controller = controller
        logger.info(
            f"trying to make a Spec() connection with host='{self.site.spec_host}' ..."
        )
        self.api = SpecApi(site_name=site.name)
        self._status: SpecStatus | None = None
        self.timer = RepeatTimer(
            interval=30 + random.randint(0, 5), function=self.on_timer
        ).start()
        self._was_shut_down = False

    def on_timer(self):
        response: CanonicalResponse = asyncio.run(self.api.get("status"))
        self._status = response.value if response.succeeded else None

    @property
    def detected(self) -> bool:
        if self.api and self.api.client:
            response: CanonicalResponse = self.api.client.get("status")
            return response.value.detected if response.is_success else False
        return False

    @property
    def connected(self) -> bool:
        if self.api and self.api.client:
            response: CanonicalResponse = self.api.client.get("status")
            return response.value.connected if response.is_success else False
        return False

    def abort(self):
        if self.api.client:
            self.api.client.get("abort")

    @property
    def operational(self) -> bool:
        if self.api.client:
            response = self.api.client.get("status")
            return response.value.operational if response.is_success else False
        return False

    @property
    def why_not_operational(self) -> list[str]:
        ret: list[str] = []

        if not self.detected:
            ret.append(f"{self.name}: not detected")
        elif not self.connected:
            ret.append(f"{self.name}: not connected")
        elif self.api.client:
            response: CanonicalResponse = self.api.client.get("status")
            if response.succeeded:
                status = response.value
                if status not in (None, [], "Never"):
                    for reason in status.why_not_operational:  # type: ignore
                        ret.append(reason)

        return ret

    def startup(self):
        if self.api.client:
            self._was_shut_down = False
            self.api.client.get("startup")

    def shutdown(self):
        if self.api.client:
            self._was_shut_down = True
            self.api.client.get("shutdown")


class ControlledUnit(Component, SwitchedOutlet, NetworkedDevice):
    def __init__(self, site_name: str, host: str, controller: "Controller"):
        Component.__init__(self)
        NetworkedDevice.__init__(self, conf={"network": {"host": host}})
        SwitchedOutlet.__init__(
            self, OutletDomain.UnitOutlets, unit_name=host, outlet_name="Computer"
        )
        if not self.is_on():
            self.power_on()
            # TODO: wait till the unit computer boots
        self.powered = self.is_on()
        self._was_shut_down = False

        self.controller = controller
        self.site_name = site_name
        self.api = UnitApi(hostname=host)
        self._status: UnitStatus | None = None

        try:
            response: CanonicalResponse = asyncio.run(self.api.get("status"))
            if response.succeeded:
                logger.info(f"Connected to unit '{host}' successfully.")
                self._status: UnitStatus = cast(UnitStatus, response.value)
            else:
                logger.warning(f"Failed to connect to unit '{host}': {response.errors}")
        except Exception as e:
            logger.error(f"Exception while connecting to unit '{host}': {e}")

        self.timer = RepeatTimer(
            interval=25 + random.randint(0, 5), function=self.on_timer
        ).start()

    def on_timer(self):
        short_status = ShortStatus(
            type="short",
            detected=False,
            powered=self.powered,
            operational=False,
        )

        response: CanonicalResponse = asyncio.run(self.api.get("status"))
        new_status = (
            cast(UnitStatus, response.value) if response.succeeded else short_status
        )
        # update instance cache and shared TTLCache so callers use fresh value
        self._status = new_status
        try:
            _units_status_cache[self.name] = new_status
        except Exception:
            # best-effort: ignore cache errors
            pass

    @property
    def status(self) -> "UnitStatus | ShortStatus | None":
        """
        Return cached unit status. Priority:
        1) in-memory value self._status
        2) shared TTL cache keyed by unit.name (via _fetch_status_from_api)
        3) call API (via cached helper) which will populate the TTL cache
        """
        # prefer the most recent in-memory value
        if self._status is not None:
            return self._status
        # try shared cache
        cached_val = _units_status_cache.get(self.name)
        if cached_val is not None:
            self._status = cached_val
            return cached_val
        # fall back to calling API via cached helper (will populate cache)
        try:
            val = _fetch_status_from_api(self)
        except Exception:
            val = None
        self._status = val
        return val

    @property
    async def detected(self) -> bool:
        response = await self.api.get("status") if self.api else None
        if response and response.value:
            return response.value.detected
        else:
            return False

    @property
    async def connected(self) -> bool:
        response = await self.api.get("status") if self.api else None
        if response and response.value:
            return response.value.connected
        else:
            return False

    @property
    async def operational(self) -> bool:
        response = await self.api.get("status") if self.api else None
        if response and response.value:
            return response.value.operational
        else:
            return False

    @property
    async def why_not_operational(self) -> list[str]:
        ret: list[str] = []
        if not self.detected:
            ret.append(f"{self.name}: not detected")
        elif not self.connected:
            ret.append(f"{self.name}: not connected")
        elif not self.operational:
            if not self.detected:
                ret.append(f"{self.name}: api client not detected")
            else:
                response = await self.api.get("status") if self.api else None
                if response and response.value:
                    for reason in response.value.why_not_operational:
                        ret.append(reason)
        return ret

    @property
    def was_shut_down(self) -> bool:
        return self._was_shut_down

    async def startup(self):
        self._was_shut_down = False
        return await self.api.get("startup")

    async def shutdown(self):
        self._was_shut_down = True
        return await self.api.get("shutdown")

    async def move_to_coordinates(self, ra: float, dec: float):
        return await self.api.get("move_to_coordinates", {"ra": ra, "dec": dec})

    async def expose(self, seconds):
        return await self.api.get("expose", {"seconds": seconds})

    @property
    def name(self) -> str:
        return self.network.hostname if self.network.hostname else "Unknown unit"

    async def abort(self):
        return await self.api.get("abort")


class ActivityNotification(BaseModel):
    initiator: str
    activity: int
    activity_verbal: str
    started: bool
    duration: str | None = None  # [seconds]


class ControlledSite:
    def __init__(self, controller, site_name: str) -> None:
        self.site_name = site_name
        self.controller = controller
        self.controlled_units: dict[str, ControlledUnit] = {}
        self.controlled_spec: ControlledSpec | None = None

        site = [s for s in controller.config.sites if s.name == site_name][0]

        for unit_name in site.deployed_units:
            if unit_name not in site.units_in_maintenance:
                self.controlled_units[unit_name] = ControlledUnit(
                    site_name=site_name,
                    host=unit_name,
                    controller=controller,
                )

        if site.spec_host:
            self.controlled_spec = ControlledSpec(site=site, controller=controller)


class ControllerConfig(BaseModel):
    sites: list[Site] = []


class Controller:
    """
    The Controller:
    - on startup:
      - loads all the targets from files <top>/targets/submitted
      - creates a list units: list[ApiUnit] objects, with Config.NUMBER_OF_UNITS elements (detected units)
      - creates an ApiSpec object
      - creates a list of targets, sorted by merit
      - creates a web service for
        - current list of targets, sorted by merit, editable
        - current list of acquisitions including the one in-progress and those pending
        - status of equipment (units and spec)

    - when needed, i.e. {
          - when a page is refreshed
          - when an acquisition can be started
          - when the acquisitions list is displayed
      }
      - re-sorts the targets according to merit

    - when an acquisition candidate is made:
      - gets the target with the highest merit and allocates units as needed
      - tries to allocate remaining units to other targets, according to their needs
      - when all the operational units are allocated starts the acquisition

    - if the in-progress acquisition succeeds it gets moved to '<top>/acquisitions/completed', if it fails it stays
       in '<top>/acquisitions/pending'
    """

    _instance = None
    _initialized = False

    @property
    def name(self) -> str:
        return socket.gethostname()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Controller, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        try:
            self.planner = Planner(controller=self)
        except Exception as e:
            logger.error(f"failed to initialize Planner: {e}")
            raise Exception(f"failed to initialize Planner: {e}") from e

        self._terminated = False

        self.activity_notification_clients: Set[WebSocket] = set()

        self.config = ControllerConfig()
        self.config.sites = Config().get_sites()
        self.controlled_sites: list[ControlledSite] = []
        for site in self.sites_we_manage:
            self.controlled_sites.append(
                ControlledSite(
                    site_name=site.name,
                    controller=self,
                )
            )

            # for unit_name in site.deployed_units:
            #     self.sites[site.name]["units"][unit_name] = {
            #         "controlled_unit": None,
            #         "status": ShortUnitStatus(
            #             type="short",
            #             detected=False,
            #             powered=False,
            #             operational=False,
            #         ),
            #     }
            #     Thread(target=make_unit, args=[site.name, unit_name]).start()

        self._initialized = True

    @property
    def sites_we_manage(self) -> list[Site]:
        return [s for s in self.config.sites if s.controller_host == self.name]

    @property
    def units_we_manage(self) -> list[str]:
        units = []
        for site in self.config.sites:
            if site.controller_host == self.name:
                units.extend(site.deployed_units)
        return units

    @property
    def units_in_maintenance(self) -> list[str]:
        units = []
        for site in self.config.sites:
            if site.controller_host == self.name:
                units.extend(site.units_in_maintenance)
        return units

    def start_controlling(self):
        self._terminated = False
        self.planner.start_planning()

    def stop_controlling(self):
        self._terminated = True
        self.planner.stop_planning()

    def startup(self):
        self.start_controlling()

    def shutdown(self):
        self.stop_controlling()

    def status(self) -> CanonicalResponse:
        ret = {}

        for site in self.controlled_sites:
            ret[site.site_name] = {}

            if site.controlled_spec:
                ret[site.site_name]["spec"] = site.controlled_spec.status or {
                    "detected": False,
                    "connected": False,
                    "operational": False,
                }

            ret[site.site_name]["units"] = {}
            for unit_name in list(site.controlled_units.keys()):
                ret[site.site_name]["units"][unit_name] = site.controlled_units[
                    unit_name
                ].status or ShortStatus(
                    type="short",
                    detected=False,
                    powered=False,
                    operational=False,
                )

        return CanonicalResponse(value=ret)

    def addressable(self, site_name: str, unit_name: str) -> CanonicalResponse:
        if site_name not in [s.name for s in self.config.sites]:
            return CanonicalResponse(errors=[f"Unknown site '{site_name}'"])

        configured_site = [s for s in self.config.sites if s.name == site_name][0]
        if unit_name not in configured_site.deployed_units:
            return CanonicalResponse(
                errors=[f"Unit '{site_name}:{unit_name}' not deployed"]
            )

        if unit_name in configured_site.units_in_maintenance:
            return CanonicalResponse(
                errors=[f"Unit '{site_name}:{unit_name}' is in maintenance mode"]
            )

        if site_name not in [s.name for s in self.config.sites]:
            return CanonicalResponse(
                errors=[f"Site '{site_name}' not managed by '{self.name}'"]
            )

        site = [s for s in self.config.sites if s.name == site_name][0]
        if unit_name not in list(site.deployed_units):
            return CanonicalResponse(
                errors=[f"Unit '{site_name}:{unit_name}' not deployed"]
            )

        if unit_name in site.units_in_maintenance:
            return CanonicalResponse(
                errors=[f"Unit '{site_name}:{unit_name}' is in maintenance mode"]
            )

        return CanonicalResponse_Ok

    def get_controlled_unit(
        self, site_name: str, unit_name: str
    ) -> ControlledUnit | None:
        controlled_site = [
            site for site in self.controlled_sites if site.site_name == site_name
        ][0]
        controlled_unit = controlled_site.controlled_units.get(unit_name)
        return controlled_unit

    def endpoint_unit_status(self, site_name: str, unit_name: str) -> CanonicalResponse:
        """
        Returns a unit's status

        :param site_name:
        :param unit_name:
        :return:
        """
        ret = self.addressable(site_name=site_name, unit_name=unit_name)
        if not ret.succeeded:
            return ret

        controlled_unit = self.get_controlled_unit(
            site_name=site_name, unit_name=unit_name
        )

        if controlled_unit and controlled_unit.status:
            return CanonicalResponse(value=controlled_unit.status)
        else:
            return CanonicalResponse(
                value=ShortStatus(
                    type="short",
                    detected=False,
                    powered=False,
                    operational=False,
                )
            )

    def endpoint_power_switch_status(
        self, site_name: str, unit_name: str
    ) -> CanonicalResponse:
        ret = self.addressable(site_name=site_name, unit_name=unit_name)
        if not ret.succeeded:
            return ret

        controlled_unit = self.get_controlled_unit(
            site_name=site_name, unit_name=unit_name
        )
        if controlled_unit is None or controlled_unit.power_switch is None:
            return CanonicalResponse(
                errors=[f"power_switch for unit '{site_name}:{unit_name}' is None"]
            )

        try:
            status = controlled_unit.power_switch.status()
        except Exception as ex:
            return CanonicalResponse(errors=[f"exception: {ex}"])
        return CanonicalResponse(value=status)

    def endpoint_get_outlet(
        self, site_name: str, unit_name: str, outlet_name
    ) -> CanonicalResponse:
        ret = self.addressable(site_name=site_name, unit_name=unit_name)
        if not ret.succeeded:
            return ret

        controlled_unit = self.get_controlled_unit(
            site_name=site_name, unit_name=unit_name
        )
        if controlled_unit is None or controlled_unit.power_switch is None:
            return CanonicalResponse(
                errors=[f"power_switch for unit '{site_name}:{unit_name}' is None"]
            )

        try:
            state = controlled_unit.power_switch.get_outlet_state(
                outlet_name=outlet_name
            )
        except ValueError as ex:
            return CanonicalResponse(errors=[f"exception: {ex}"])
        return CanonicalResponse(value=state)

    def endpoint_set_outlet(
        self,
        site_name: str,
        unit_name: str,
        outlet_name: str,
        state: Literal["on", "off", "toggle"],
    ) -> CanonicalResponse:
        ret = self.addressable(site_name=site_name, unit_name=unit_name)
        if not ret.succeeded:
            return ret

        controlled_unit = self.get_controlled_unit(
            site_name=site_name, unit_name=unit_name
        )
        if controlled_unit is None or controlled_unit.power_switch is None:
            return CanonicalResponse(
                errors=[f"power_switch for unit '{site_name}:{unit_name}' is None"]
            )

        if state == "on":
            controlled_unit.power_switch.set_outlet_state(
                outlet_name=outlet_name, state=True
            )
        elif state == "off":
            controlled_unit.power_switch.set_outlet_state(
                outlet_name=outlet_name, state=False
            )
        elif state == "toggle":
            controlled_unit.power_switch.toggle_outlet(outlet_name=outlet_name)

        new_state = controlled_unit.power_switch.get_outlet_state(
            outlet_name=outlet_name
        )
        return CanonicalResponse(value=new_state)

    # async def execute_assigned_plan(self, ulid: str) -> CanonicalResponse:
    #     matching_plans = [
    #         p for p in self.expired_plans_folder.plans if p.plan.ulid == ulid
    #     ]
    #     if len(matching_plans) == 0:
    #         return CanonicalResponse(errors=[f"no matching plan for {ulid=}"])
    #     plan = matching_plans[0]
    #     plan.run_folder = PathMaker.make_run_folder()
    #     os.makedirs(plan.run_folder, exist_ok=True)
    #     os.link(plan.file, os.path.join(plan.run_folder, "task"))
    #     self.task_in_progress = plan
    #     asyncio.create_task(plan.execute(controller=self))
    #     return CanonicalResponse_Ok

    async def task_acquisition_path_notification(
        self, notification: TaskAcquisitionPathNotification
    ):
        """
        Receives locations of products related to a running task:
        - from units: type: 'autofocus' or 'acquisition'
        - from spec: type: 'spec', folder containing the spec's acquisition
        :param notification:
        :return:
        """
        op = function_name()
        if self.task_in_progress is None:
            logger.error(f"{function_name}: no task_in_progress")

        if (
            self.task_in_progress
            and notification.task_id != self.task_in_progress.settings.ulid
        ):
            logger.error(
                f"ignored notification for task '{notification.task_id}' (task in progress '{self.task_in_progress.settings.ulid}"
            )
            return

        src = notification.src
        assert (
            self.task_in_progress is not None
            and self.task_in_progress.settings is not None
            and self.task_in_progress.settings.run_folder is not None
            and notification.initiator.hostname is not None
        )
        dst = (
            Path(self.task_in_progress.settings.run_folder)
            / notification.initiator.hostname
            / notification.link
        )
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(src, dst)
            logger.info(f"{op}: created symlink '{src}' -> '{dst}'")
        except Exception as e:
            logger.error(f"{op}: failed to symlink '{src}' -> '{dst}' (error: {e})")

    async def activity_notification_endpoint(self, notification: ActivityNotification):
        """
        Listens for activity notifications (from units, specs and self) and pushes them via WebSocket
        to all the registered GUIs.

        :param notification:
        :return:
        """
        if not self.activity_notification_clients:
            logger.info(f"{function_name()}: no clients")
            return

        disconnected = []
        for ws in self.activity_notification_clients:
            try:
                logger.info(f"{function_name()}: sending to {ws} ...")
                await ws.send_json(notification.model_dump())
            except WebSocketDisconnect:
                disconnected.append(ws)

        for ws in disconnected:
            self.activity_notification_clients.remove(ws)

    async def activity_notification_client(self, websocket: WebSocket):
        """Receives websocket connections from web GUI"""
        await websocket.accept()
        self.activity_notification_clients.add(websocket)
        logger.info(f"new websocket from {websocket.client}")
        try:
            while True:
                _ = await websocket.receive_text()
        except WebSocketDisconnect:
            logger.info(f"websocket {websocket.client} disconnected")
        finally:
            self.activity_notification_clients.remove(websocket)
            # await websocket.close()

    def endpoint_config_get_users(self):
        return Config().get_users()

    def endpoint_config_get_user(self, user_name: str):
        return Config().get_user(user_name)

    def endpoint_config_get_unit(self, unit_name: str) -> CanonicalResponse:
        unit_config = Config().get_unit(unit_name)
        # logger.debug(f"{function_name}: {unit_name=}, {unit_config=}")
        return (
            CanonicalResponse(value=unit_config)
            if unit_config
            else CanonicalResponse(errors=[f"Unit '{unit_name}' not found"])
        )

    def endpoint_config_set_unit(
        self, unit_name: str, unit_conf: UnitConfig
    ) -> CanonicalResponse:
        Config().set_unit(unit_name, unit_conf)
        return CanonicalResponse_Ok

    def endpoint_config_get_thar_filters(self):
        return Config().get_specs().wheels["ThAr"].filters

    @property
    def api_router(self) -> APIRouter:
        base_path = Const.BASE_CONTROL_PATH
        router = APIRouter()

        tag = "Control"
        router.add_api_route(base_path + "/status", tags=[tag], endpoint=self.status)
        router.add_api_route(base_path + "/startup", tags=[tag], endpoint=self.startup)
        router.add_api_route(
            base_path + "/shutdown", tags=[tag], endpoint=self.shutdown
        )

        tag = "Config"
        router.add_api_route(
            base_path + "/config/users",
            tags=[tag],
            endpoint=self.endpoint_config_get_users,
        )
        router.add_api_route(
            base_path + "/config/user",
            tags=[tag],
            endpoint=self.endpoint_config_get_user,
        )
        router.add_api_route(
            base_path + "/config/get_unit/{unit_name}",
            tags=[tag],
            endpoint=self.endpoint_config_get_unit,
        )
        router.add_api_route(
            base_path + "/config/set_unit/{unit_name}",
            tags=[tag],
            endpoint=self.endpoint_config_set_unit,
        )
        router.add_api_route(
            base_path + "/config/get_thar_filters",
            tags=[tag],
            endpoint=self.endpoint_config_get_thar_filters,
        )

        tag = "Unit"
        router.add_api_route(
            base_path + "/unit/{site_name}/{unit_name}/status",
            tags=[tag],
            endpoint=self.endpoint_unit_status,
        )

        # Power switch routes
        tag = "Power Switch"
        router.add_api_route(
            base_path + "/unit/{site_name}/{unit_name}/power_switch/status",
            tags=[tag],
            endpoint=self.endpoint_power_switch_status,
        )
        router.add_api_route(
            base_path
            + "/unit/{site_name}/{unit_name}/power_switch/get_outlet/{outlet_name}",
            tags=[tag],
            endpoint=self.endpoint_get_outlet,
        )
        router.add_api_route(
            base_path
            + "/unit/{site_name}/{unit_name}/power_switch/set_outlet/{outlet_name}/{state}",
            tags=[tag],
            endpoint=self.endpoint_set_outlet,
            methods=["PUT", "POST"],
        )
        self.planner.add_routes(router)

        # router.add_api_route(base_path + '/{unit}/expose', tags=[tag], endpoint=scheduler.units.{unit}.expose)
        # router.add_api_route(base_path + '/{unit}/move_to_coordinates', tags=[tag], endpoint=scheduler.units.{unit}.move_to_coordinates)

        # router.add_api_route(
        #     plans_base + "/execute_assigned_plan",
        #     tags=[tag],
        #     endpoint=self.execute_assigned_plan,
        # )
        router.add_api_route(
            base_path + "/task_acquisition_path_notification",
            methods=["PUT"],
            tags=[tag],
            endpoint=self.task_acquisition_path_notification,
        )
        router.add_api_route(
            base_path + "/activity_notification",
            methods=["PUT"],
            endpoint=self.activity_notification_endpoint,
        )
        return router


def startup():
    Controller().startup()


def shutdown():
    Controller().shutdown()


if __name__ == "__main__":
    Controller().startup()
