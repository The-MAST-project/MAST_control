import asyncio
import atexit
import logging
import os
import signal
import socket
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock
from typing import Any, Literal, Set

import httpx
from fastapi import APIRouter, WebSocket
from pydantic import BaseModel, ValidationError

from common.activities import Activities, ControllerActivities
from common.api import ControllerApi, SpecApi, UnitApi
from common.canonical import CanonicalResponse, CanonicalResponse_Ok
from common.config import Config, Site, UnitConfig
from common.const import Const
from common.dlipowerswitch import (
    DliPowerSwitch,
)
from common.mast_logging import init_log
from common.models.batches import Batch
from common.models.statuses import (
    BasicStatus,
    ControllerStatus,
    SitesStatus,
    SiteStatus,
    SpecStatus,
)
from common.notifications import UiUpdateNotifications
from common.tasks.models import AcquisitionPathNotification
from common.utils import (
    RepeatTimer,
    function_name,
    time_stamp,
)
from control.planning import Plan, Planner

logger = logging.getLogger("controller")
init_log(logger)

# _units_status_cache = TTLCache(maxsize=20, ttl=30)


# @cached(_units_status_cache, key=lambda self: self.name)
# def _fetch_unit_status_from_api(self) -> "UnitStatus | None":
#     """
#     Module-level cached helper. Cache key is unit.name.
#     This calls the unit API and returns the parsed UnitStatus (or None).
#     """
#     response: CanonicalResponse = asyncio.run(self.api.get("status"))
#     return cast(UnitStatus, response.value) if response.succeeded else None


# class ControlledSpec(Component):
#     @property
#     def was_shut_down(self) -> bool:
#         return self._was_shut_down

#     @property
#     def status(self):
#         return self.spec_status

#     @property
#     def name(self) -> str:
#         return "spec"

#     def __init__(self, site: Site, controller: "Controller"):
#         Component.__init__(self, ControllerActivities)
#         self.site = site
#         self.controller = controller
#         logger.info(
#             f"trying to make a Spec() connection with host='{self.site.spec_host}' ..."
#         )
#         self.api = SpecApi(site_name=site.name)
#         self.spec_status: SpecStatus | None = None
#         self.timer = RepeatTimer(
#             interval=30 + random.randint(0, 5), function=self.on_timer
#         ).start()
#         self._was_shut_down = False

#     def on_timer(self):
#         response: CanonicalResponse = asyncio.run(self.api.get("status"))
#         if response.succeeded and response.value:
#             self.spec_status = response.value

#     @property
#     def detected(self) -> bool:
#         if self.api and self.api.client:
#             response: CanonicalResponse = self.api.client.get("status")
#             return response.value.detected if response.is_success else False
#         return False

#     @property
#     def connected(self) -> bool:
#         if self.api and self.api.client:
#             response: CanonicalResponse = self.api.client.get("status")
#             return response.value.connected if response.is_success else False
#         return False

#     def abort(self):
#         if self.api.client:
#             self.api.client.get("abort")

#     @property
#     def operational(self) -> bool:
#         if self.api.client:
#             response = self.api.client.get("status")
#             return response.value.operational if response.is_success else False
#         return False

#     @property
#     def why_not_operational(self) -> list[str]:
#         ret: list[str] = []

#         if not self.detected:
#             ret.append(f"{self.name}: not detected")
#         elif not self.connected:
#             ret.append(f"{self.name}: not connected")
#         elif self.api.client:
#             response: CanonicalResponse = self.api.client.get("status")
#             if response.succeeded:
#                 status = response.value
#                 if status not in (None, [], "Never"):
#                     for reason in status.why_not_operational:  # type: ignore
#                         ret.append(reason)

#         return ret

#     def startup(self):
#         if self.api.client:
#             self._was_shut_down = False
#             self.api.client.get("startup")

#     def shutdown(self):
#         if self.api.client:
#             self._was_shut_down = True
#             self.api.client.get("shutdown")


# class ControlledUnit(Component, SwitchedOutlet, NetworkedDevice):
#     def __init__(self, site_name: str, host: str, controller: "Controller"):
#         Component.__init__(self, ControlledUnitActivities)
#         NetworkedDevice.__init__(self, conf={"network": {"host": host}})
#         SwitchedOutlet.__init__(
#             self, OutletDomain.UnitOutlets, unit_name=host, outlet_name="Computer"
#         )
#         if not self.is_on():
#             self.power_on()
#             # TODO: wait till the unit computer boots
#         self.powered = self.is_on()
#         self._was_shut_down = False

#         self.controller = controller
#         self.site_name = site_name
#         self.api = UnitApi(hostname=host)
#         self._status: UnitStatus | None = None

#         try:
#             response: CanonicalResponse = asyncio.run(self.api.get("status"))
#             if response.succeeded:
#                 logger.info(f"Connected to unit '{host}' successfully.")
#                 self._status = cast(UnitStatus, response.value)
#             else:
#                 logger.warning(f"Failed to connect to unit '{host}': {response.errors}")
#         except Exception as e:
#             logger.error(f"Exception while connecting to unit '{host}': {e}")

#         self.timer = RepeatTimer(
#             interval=25 + random.randint(0, 5), function=self.on_timer
#         ).start()

#     def on_timer(self):
#         short_status = BasicStatus(
#             detected=False,
#             powered=self.powered,
#             operational=False,
#         )

#         response: CanonicalResponse = asyncio.run(self.api.get("status"))
#         new_status = (
#             cast(UnitStatus, response.value) if response.succeeded else short_status
#         )
#         # update instance cache and shared TTLCache so callers use fresh value
#         self._status = new_status
#         try:
#             _units_status_cache[self.name] = new_status
#         except Exception:
#             # best-effort: ignore cache errors
#             pass

#     @property
#     def status(self) -> "UnitStatus | BasicStatus | None":
#         """
#         Return cached unit status. Priority:
#         1) in-memory value self._status
#         2) shared TTL cache keyed by unit.name (via _fetch_status_from_api)
#         3) call API (via cached helper) which will populate the TTL cache
#         """
#         # prefer the most recent in-memory value
#         if self._status is not None:
#             return self._status
#         # try shared cache
#         cached_val = _units_status_cache.get(self.name)
#         if cached_val is not None:
#             self._status = cached_val
#             return cached_val
#         # fall back to calling API via cached helper (will populate cache)
#         try:
#             val = _fetch_unit_status_from_api(self)
#         except Exception:
#             val = None
#         self._status = val
#         return val

#     @property
#     async def detected(self) -> bool:
#         response = await self.api.get("status") if self.api else None
#         if response and response.value:
#             return response.value.detected
#         else:
#             return False

#     @property
#     async def connected(self) -> bool:
#         response = await self.api.get("status") if self.api else None
#         if response and response.value:
#             return response.value.connected
#         else:
#             return False

#     @property
#     async def operational(self) -> bool:
#         response = await self.api.get("status") if self.api else None
#         if response and response.value:
#             return response.value.operational
#         else:
#             return False

#     @property
#     async def why_not_operational(self) -> list[str]:
#         ret: list[str] = []
#         if not self.detected:
#             ret.append(f"{self.name}: not detected")
#         elif not self.connected:
#             ret.append(f"{self.name}: not connected")
#         elif not self.operational:
#             if not self.detected:
#                 ret.append(f"{self.name}: api client not detected")
#             else:
#                 response = await self.api.get("status") if self.api else None
#                 if response and response.value:
#                     for reason in response.value.why_not_operational:
#                         ret.append(reason)
#         return ret

#     @property
#     def was_shut_down(self) -> bool:
#         return self._was_shut_down

#     async def startup(self):
#         self._was_shut_down = False
#         return await self.api.get("startup")

#     async def shutdown(self):
#         self._was_shut_down = True
#         return await self.api.get("shutdown")

#     async def move_to_coordinates(self, ra: float, dec: float):
#         return await self.api.get("move_to_coordinates", {"ra": ra, "dec": dec})

#     async def expose(self, seconds):
#         return await self.api.get("expose", {"seconds": seconds})

#     @property
#     def name(self) -> str:
#         return self.network.hostname if self.network.hostname else "Unknown unit"

#     async def abort(self):
#         return await self.api.get("abort")


# class ControlledSite:
#     def __init__(self, controller, site_name: str) -> None:
#         self.site_name = site_name
#         self.controller = controller
#         self.controlled_units: dict[str, ControlledUnit | None] | None = None
#         self.controlled_spec: ControlledSpec | None = None

#         site = [s for s in controller.config.sites if s.name == site_name][0]
#         self.controlled_units = {}
#         for unit_name in site.deployed_units:
#             if unit_name not in site.units_in_maintenance:
#                 self.controlled_units[unit_name] = ControlledUnit(
#                     site_name=site_name,
#                     host=unit_name,
#                     controller=controller,
#                 )

#         if site.spec_host:
#             self.controlled_spec = ControlledSpec(site=site, controller=controller)


class ControllerConfig(BaseModel):
    managed_sites: list[Site] = []
    managed_units: dict[
        str, dict[str, UnitConfig | None]
    ] = {}  # site_name -> unit_name -> UnitConfig | None


class CachedValue:
    site_name: str
    machine_name: str
    last_attempt: datetime | None = None
    last_success: datetime | None = None
    interval: timedelta
    value: Any | None = None
    fetcher: Any | None = None  # Future from ThreadPoolExecutor
    api: SpecApi | UnitApi | ControllerApi

    def __init__(
        self,
        interval_seconds: float,
        api: UnitApi | SpecApi | ControllerApi,
        site_name: str,
        machine_name: str,
    ):
        self.interval = timedelta(seconds=interval_seconds)
        self.api = api
        self.site_name = site_name
        self.machine_name = machine_name

    def needs_refresh(self) -> bool:
        """Check if enough time has elapsed since last attempt"""
        if self.last_attempt is None:
            return True  # Never attempted

        now = datetime.now(timezone.utc)
        elapsed = now - self.last_attempt
        return elapsed > self.interval

    def time_since_last_success(self) -> timedelta | None:
        """Return time elapsed since last successful fetch"""
        if self.last_success is None:
            return None

        now = datetime.now(timezone.utc)
        return now - self.last_success


class Controller(Activities):
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

        Activities.__init__(self)
        self.activities = ControllerActivities(0)
        try:
            self.planner = Planner(controller=self)
        except Exception as e:
            logger.error(f"failed to initialize Planner: {e}")
            raise Exception(f"failed to initialize Planner: {e}") from e

        self._terminated = False

        self.activity_notification_clients: Set[WebSocket] = set()
        self.hostname = socket.gethostname().split(".")[0]

        self.preferred_site = None
        match self.hostname:
            case "mast-wis-control":
                self.preferred_site = "ns"

        self.config = ControllerConfig()
        sites = Config().get_sites()
        for site in sites:
            if self.hostname != site.controller_host:
                continue
            self.config.managed_sites.append(site)
            self.config.managed_units[site.name] = {}
            for unit_name in site.deployed_units:
                if unit_name not in site.units_in_maintenance:
                    self.config.managed_units[site.name][unit_name] = Config().get_unit(
                        site.name, unit_name
                    )

        self._shutdown_event: Event = Event()
        self.lock: Lock = Lock()
        self.executor = ThreadPoolExecutor(max_workers=25)

        # Build cache hierarchy: site_name -> {"units": {unit_name: CachedValue}, "spec": CachedValue}
        self.status_cache: dict[str, dict[str, Any]] = {}
        self.power_switches: dict[
            str, dict[str, DliPowerSwitch]
        ] = {}  # power_switches[site_name][unit_name]

        for site in self.config.managed_sites:
            self.status_cache[site.name] = {
                "units": {},
                "spec": None,
                "controller": None,
            }

            if site.name not in self.power_switches:
                self.power_switches[site.name] = {}

            # Create CachedValue for each unit
            for unit_name in site.deployed_units:
                if unit_name not in site.units_in_maintenance:
                    unit_api = UnitApi(hostname=unit_name)
                    self.status_cache[site.name]["units"][unit_name] = CachedValue(
                        interval_seconds=30,
                        api=unit_api,
                        site_name=site.name,
                        machine_name=unit_name,
                    )

                if unit_name not in self.power_switches[site.name]:
                    ps_hostname = unit_name.replace(site.project, site.project + "ps")
                    try:
                        ps_ipaddr = socket.gethostbyname(ps_hostname)
                    except socket.gaierror:
                        ps_ipaddr = None
                        logger.warning(
                            f"{function_name()}: cannot resolve power switch hostname '{ps_hostname}'"
                        )

                    assert site.name in self.config.managed_units
                    assert unit_name in self.config.managed_units[site.name]
                    assert self.config.managed_units[site.name][unit_name] is not None

                    unit_config = self.config.managed_units[site.name][unit_name]
                    assert unit_config is not None
                    ps_config = unit_config.power_switch
                    self.power_switches[site.name][unit_name] = DliPowerSwitch(
                        hostname=ps_hostname,
                        ipaddr=ps_ipaddr,
                        conf=ps_config,
                    )

            # Create CachedValue for spec
            if site.spec_host:
                for existing_site in self.status_cache:
                    # have we already seen this controller machine?
                    if "spec" in self.status_cache[existing_site]:
                        cached_value = self.status_cache[existing_site]
                        if (
                            cached_value["spec"] is not None
                            and cached_value["spec"].machine_name == site.spec_host
                        ):
                            self.status_cache[site.name]["spec"] = cached_value["spec"]
                            break
                else:
                    # nope, make a new entry
                    self.status_cache[site.name]["spec"] = CachedValue(
                        interval_seconds=30,
                        api=SpecApi(site_name=site.name),
                        site_name=site.name,
                        machine_name=site.spec_host,
                    )

            # Create CachedValue for controller
            if site.controller_host:
                for existing_site in self.status_cache:
                    # have we already seen this controller machine?
                    if "controller" in self.status_cache[existing_site]:
                        cached_value = self.status_cache[existing_site]
                        if (
                            cached_value["controller"] is not None
                            and cached_value["controller"].machine_name
                            == site.controller_host
                        ):
                            self.status_cache[site.name]["controller"] = cached_value[
                                "controller"
                            ]
                            break
                else:
                    # nope, make a new entry
                    self.status_cache[site.name]["controller"] = CachedValue(
                        interval_seconds=30,
                        api=ControllerApi(site_name=site.name),
                        site_name=site.name,
                        machine_name=site.controller_host,
                    )

        self.config_timer: RepeatTimer = RepeatTimer(30, self.on_config_timer)
        self.config_timer.start()

        self.fetch_timer: RepeatTimer = RepeatTimer(2, self.on_fetch_timer)
        self.fetch_timer.daemon = (
            False  # Don't make it a daemon so we can clean up properly
        )

        self.refresh()
        self.fetch_timer.start()

        # Register cleanup on program exit
        atexit.register(self._cleanup_on_exit)

        # Register signal handlers for Ctrl+C detection
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.in_progress: Plan | Batch | None = None
        self._initialized = True

    def operational_units(self, site_name: str | None) -> list[str]:
        """Uses cached statuses to get a list of currently operational units for the given site_name (or preferred_site if None)"""
        if site_name is None:
            site_name = self.preferred_site
        if site_name is None:
            logger.error(
                f"{function_name()}: site_name is None and preferred_site is not set"
            )
            return []

        site_cache = self.status_cache.get(site_name)
        if site_cache is None:
            logger.error(
                f"{function_name()}: site '{site_name}' not found in status cache"
            )
            return []

        return [
            unit_name
            for unit_name, cached_value in site_cache["units"].items()
            if cached_value.value and cached_value.value.operational
        ]

    def _cleanup_on_exit(self):
        """Called automatically when Python exits (including Ctrl+C)"""
        logger.info("atexit: Cleaning up Controller")
        self.shutdown()

    def _signal_handler(self, signum, frame):
        """Called when SIGINT (Ctrl+C) or SIGTERM received"""
        logger.info(f"Signal {signum} received, initiating graceful shutdown")
        self._shutdown_event.set()

    def on_config_timer(self):
        self.config.managed_sites = Config().get_sites()

    def on_fetch_timer(self):
        self.refresh()

    def refresh(self):
        """Walk cache and schedule fetches for stale entries"""
        if self._shutdown_event.is_set():
            logger.debug("Shutdown in progress, skipping refresh")
            return

        # Check if timer was cancelled (happens on shutdown)
        if not self.fetch_timer.is_alive():
            logger.info("Timer stopped, assuming shutdown")
            return

        for site_name in [s.name for s in self.config.managed_sites]:
            if site_name not in self.status_cache:
                """
                We have been assigned a new site to manage since last config load
                """
                self.status_cache[site_name] = {
                    "units": {},
                    "spec": None,
                    "controller": None,
                }
            site_cache = self.status_cache[site_name]

            # Check units
            for unit_name, cached_value in site_cache["units"].items():
                if cached_value.needs_refresh() and cached_value.fetcher is None:
                    future = self.executor.submit(self._fetch_status, cached_value)
                    cached_value.fetcher = future

                    future.add_done_callback(
                        lambda f,
                        sn=site_name,
                        cn=unit_name,
                        cv=cached_value: self._on_fetch_complete(f, sn, cn, cv)
                    )

            # Check spec
            if site_cache["spec"] is not None:
                cached_value = site_cache["spec"]
                if cached_value.needs_refresh() and cached_value.fetcher is None:
                    future = self.executor.submit(self._fetch_status, cached_value)
                    cached_value.fetcher = future

                    future.add_done_callback(
                        lambda f,
                        sn=site_name,
                        cn="spec",
                        cv=cached_value: self._on_fetch_complete(f, sn, cn, cv)
                    )

            # Check controller
            if site_cache["controller"] is not None:
                cached_value = site_cache["controller"]
                if cached_value.needs_refresh() and cached_value.fetcher is None:
                    future = self.executor.submit(self._fetch_status, cached_value)
                    cached_value.fetcher = future

                    future.add_done_callback(
                        lambda f,
                        sn=site_name,
                        cn="controller",
                        cv=cached_value: self._on_fetch_complete(f, sn, cn, cv)
                    )

    def _fetch_status(self, cached_value: CachedValue) -> Any:
        """Generic fetch for any component type - detects type from cached_value.api"""
        api_type = type(
            cached_value.api
        ).__name__  # "UnitApi", "SpecApi", "ControllerApi"

        try:
            cached_value.last_attempt = datetime.now(timezone.utc)
            response = asyncio.run(
                cached_value.api.get(
                    "controller_status"
                    if isinstance(cached_value.api, ControllerApi)
                    else "status"
                )
            )

            if response.succeeded:
                return response.value
            else:
                logger.error(f"{function_name()}: {response.errors}")
                return BasicStatus(detected=False, powered=False, operational=False)
        except Exception as e:
            logger.error(f"Error fetching {api_type} status: {e}")
            return BasicStatus(detected=False, powered=False, operational=False)

    def status_from_dict(
        self, api: SpecApi | ControllerApi | UnitApi, data: dict
    ) -> Any:
        from common.models.statuses import (
            BasicStatus,
            ControllerStatus,
            FullUnitStatus,
            SpecStatus,
        )

        expected_status_type = None
        validated_status = None
        match type(api).__name__:
            case "UnitApi":
                expected_status_type = FullUnitStatus
            case "SpecApi":
                expected_status_type = SpecStatus
            case "ControllerApi":
                expected_status_type = ControllerStatus
            case _:
                raise ValueError(f"Unknown API type: {type(api).__name__}")

        try:
            validated_status = expected_status_type.model_validate(data)
        except Exception as e:
            try:
                validated_status = BasicStatus.model_validate(data)
            except Exception as e2:
                logger.error(
                    f"Failed to validate status data for {type(api).__name__}: {e}; also failed BasicStatus: {e2}"
                )
                return BasicStatus(detected=False, powered=False, operational=False)

        return validated_status

    def _on_fetch_complete(
        self,
        future: Future,
        site_name: str,
        component_name: str,  # unit_name, "spec", or "controller"
        cached_value: CachedValue,
    ):
        """Generic callback for any component type"""
        api_type = type(cached_value.api).__name__

        try:
            status = future.result()
            validated_status = self.status_from_dict(cached_value.api, status)
            with self.lock:
                cached_value.value = validated_status
                cached_value.last_success = datetime.now(timezone.utc)
                cached_value.fetcher = None
                logger.debug(
                    f"Updated cache for {site_name}:{component_name} ({api_type}) with '{type(cached_value.value).__name__}'"
                )
        except Exception as e:
            logger.error(f"Error updating cache for {site_name}:{component_name}: {e}")
            with self.lock:
                cached_value.fetcher = None

    def status(self) -> CanonicalResponse:
        """Returns statuses from cache"""
        with self.lock:
            ret = SitesStatus(timestamp=time_stamp(), sites={})

            for site_name in [s.name for s in self.config.managed_sites]:
                site_cache = self.status_cache[site_name]

                unit_statuses = {}
                for unit_name, cached_value in site_cache["units"].items():
                    if cached_value.value:
                        unit_statuses[unit_name] = cached_value.value
                        unit_statuses[unit_name].powered = self.power_switches[
                            site_name
                        ][unit_name].get_outlet_state("Computer")

                spec_status = (
                    site_cache["spec"].value
                    if site_cache["spec"]
                    else SpecStatus(
                        detected=False,
                        powered=False,
                        operational=False,
                        why_not_operational=["No status available"],
                    )
                )

                try:
                    ret.sites[site_name] = SiteStatus(
                        controller=ControllerStatus(
                            powered=True,
                            detected=True,
                            operational=True,
                        ),
                        spec=spec_status,
                        units=unit_statuses,
                    )
                except ValidationError as e:
                    logger.error(f"Validation error for site '{site_name}': {e}")
                    ret.sites[site_name] = SiteStatus(
                        controller=ControllerStatus(
                            powered=True,
                            detected=True,
                            operational=True,
                        ),
                        spec=SpecStatus(
                            detected=False,
                            powered=False,
                            operational=False,
                            why_not_operational=[f"Validation error: {e}"],
                        ),
                        units=unit_statuses,
                    )

            return CanonicalResponse(value=ret.model_dump())

    def endpoint_controller_status(self) -> CanonicalResponse:
        """
        Returns the controller's own status
        """
        return CanonicalResponse(
            value=ControllerStatus(
                powered=True,
                detected=True,
                operational=True,
            )
        )

    def endpoint_unit_status(self, site_name: str, unit_name: str) -> CanonicalResponse:
        """
        Returns a unit's status

        :param site_name:
        :param unit_name:
        :return:
        """
        if site_name not in self.status_cache:
            return CanonicalResponse(errors=[f"no statuses for '{site_name=}'"])
        if unit_name not in self.status_cache[site_name]["units"]:
            return CanonicalResponse(
                errors=[f"no status for '{unit_name=}' of '{site_name=}'"]
            )

        with self.lock:
            return CanonicalResponse(
                value=self.status_cache[site_name]["units"][unit_name].value
            )

    def endpoint_power_switch_status(
        self, site_name: str, unit_name: str
    ) -> CanonicalResponse:
        if (
            site_name not in self.power_switches
            or unit_name not in self.power_switches[site_name]
        ):
            return CanonicalResponse(
                errors=[f"no power_switch for {site_name=}, {unit_name=}"]
            )
        power_switch = self.power_switches[site_name][unit_name]

        try:
            status = power_switch.status()
        except Exception as ex:
            return CanonicalResponse(errors=[f"exception: {ex}"])
        return CanonicalResponse(value=status)

    def endpoint_get_outlet(
        self, site_name: str, unit_name: str, outlet_name
    ) -> CanonicalResponse:
        if (
            site_name not in self.power_switches
            or unit_name not in self.power_switches[site_name]
        ):
            return CanonicalResponse(
                errors=[f"no power_switch for {site_name=}, {unit_name=}"]
            )
        power_switch = self.power_switches[site_name][unit_name]

        try:
            state = power_switch.get_outlet_state(outlet_name=outlet_name)
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
        if (
            site_name not in self.power_switches
            or unit_name not in self.power_switches[site_name]
        ):
            return CanonicalResponse(
                errors=[f"no power_switch for {site_name=}, {unit_name=}"]
            )
        power_switch = self.power_switches[site_name][unit_name]

        if state == "on":
            power_switch.set_outlet_state(outlet_name=outlet_name, state=True)
        elif state == "off":
            power_switch.set_outlet_state(outlet_name=outlet_name, state=False)
        elif state == "toggle":
            power_switch.toggle_outlet(outlet_name=outlet_name)

        new_state = power_switch.get_outlet_state(outlet_name=outlet_name)
        return CanonicalResponse(value=new_state)

    async def execute(self, work: Plan | Batch) -> CanonicalResponse:
        """
        Executes a plan or batch. This is called by the Planner when a plan or batch is ready to be executed.
        """
        if self.in_progress is not None:
            return CanonicalResponse(
                errors=[
                    f"another plan/batch is already in progress: '{self.in_progress.ulid}'"
                ]
            )

        self.in_progress = work
        try:
            await work.execute(controller=self)
        finally:
            self.in_progress = None
        return CanonicalResponse_Ok

    async def task_acquisition_path_notification(
        self, notification: AcquisitionPathNotification
    ):
        """
        Receives locations of products related to a running task:
        - from units: type: 'autofocus' or 'acquisition'
        - from spec: type: 'spec', folder containing the spec's acquisition
        :param notification:
        :return:
        """
        op = function_name()
        if self.in_progress is None:
            logger.error(f"{function_name}: no in_progress")

        if self.in_progress and notification.assignment_id != self.in_progress.ulid:
            logger.error(
                f"ignored notification for plan/batch '{notification.assignment_id}' ('{self.in_progress.ulid=}')"
            )
            return

        src = notification.src
        assert (
            self.in_progress is not None
            and self.in_progress.run_folder is not None
            and notification.initiator.hostname is not None
        )
        dst = (
            Path(self.in_progress.run_folder)
            / notification.initiator.hostname
            / notification.subpath
        )
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(src, dst)
            logger.info(f"{op}: created symlink '{src}' -> '{dst}'")
        except Exception as e:
            logger.error(f"{op}: failed to symlink '{src}' -> '{dst}' (error: {e})")

    async def notifications_endpoint(self, data: UiUpdateNotifications):
        """
        Listens for notifications (from units, specs and self) and pushes them to Django server
        for dissemination to GUI clients.
        """
        op = function_name()
        try:
            logger.info(f"{op}: Received notification from {data.initiator.hostname}")
            logger.debug(f"{op}: Full data: {data.model_dump()}")
        except Exception as e:
            logger.error(f"{op}: Error logging notification: {e}")

        django_url = (
            f"http://{Const.DJANGO_HOST}:{Const.DJANGO_PORT}/api/notifications/"
        )

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    django_url,
                    json=data.model_dump(),
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 200:
                    logger.debug(f"{op}: Notification sent to Django successfully")
                else:
                    logger.warning(
                        f"{op}: Django returned status {response.status_code}: {response.text}"
                    )
        except httpx.RequestError as e:
            logger.error(f"{op}: Failed to send notification to Django: {e}")
        except Exception as e:
            logger.error(f"{op}: Unexpected error sending notification to Django: {e}")

    def endpoint_config_get_users(self):
        return Config().get_users()

    def endpoint_config_get_user(self, user_name: str):
        return Config().get_user(user_name)

    def endpoint_config_get_unit(
        self, site_name: str, unit_name: str
    ) -> CanonicalResponse:
        unit_config = Config().get_unit(site_name, unit_name)
        # logger.debug(f"{function_name}: {unit_name=}, {unit_config=}")
        return (
            CanonicalResponse(value=unit_config)
            if unit_config
            else CanonicalResponse(errors=[f"Unit '{unit_name}' not found"])
        )

    def endpoint_config_sites(self) -> CanonicalResponse:
        return CanonicalResponse(value=Config().get_sites())

    def endpoint_config_set_unit(
        self, site_name: str, unit_name: str, unit_conf: UnitConfig
    ) -> CanonicalResponse:
        Config().set_unit(site_name, unit_name, unit_conf)
        return CanonicalResponse_Ok

    def endpoint_config_get_thar_filters(self) -> CanonicalResponse:
        return CanonicalResponse(value=Config().get_thar_filters())

    def startup(self):
        pass

    def shutdown(self):
        """Gracefully shutdown all resources"""
        if self._shutdown_event.is_set():
            return  # Already shutting down

        logger.info("Controller shutdown initiated")
        self._shutdown_event.set()

        # Stop timers
        if hasattr(self, "config_timer") and self.config_timer:
            self.config_timer.cancel()
        if hasattr(self, "fetch_timer") and self.fetch_timer:
            self.fetch_timer.cancel()

        # Shutdown executor gracefully
        if hasattr(self, "executor"):
            logger.info("Waiting for in-flight tasks to complete...")
            self.executor.shutdown(wait=True)

        logger.info("Controller shutdown complete")

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
            base_path + "/config/get_unit/{site_name}/{unit_name}",
            tags=[tag],
            endpoint=self.endpoint_config_get_unit,
        )
        router.add_api_route(
            base_path + "/config/sites",
            tags=[tag],
            endpoint=self.endpoint_config_sites,
        )
        router.add_api_route(
            base_path + "/config/set_unit/{site_name}/{unit_name}",
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

        tag = "Controller"
        router.add_api_route(
            base_path + "/controller_status",
            tags=[tag],
            endpoint=self.endpoint_controller_status,
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
            base_path + "/notifications",
            methods=["PUT"],
            endpoint=self.notifications_endpoint,
        )
        # router.add_api_route(base_path + '/{unit}/expose', tags=[tag], endpoint=scheduler.units.{unit}.expose)
        # router.add_api_route(base_path + '/{unit}/move_to_coordinates', tags=[tag], endpoint=scheduler.units.{unit}.move_to_coordinates)ned_plan,

        # router.add_api_route(
        #     plans_base + "/execute_assigned_plan",   base_path + "/task_acquisition_path_notification",
        #     tags=[tag],"PUT"],
        #     endpoint=self.execute_assigned_plan,            tags=[tag],

        return router
