"""The safety monitor: polls the observatory's weather service and answers "is it safe?".

One thread asks the service for every sensor of the `davis` station in a single request each
`POLL_INTERVAL`, and feeds the raw values to `common.safety.Safety`, which keeps them per sensor and
decides. Nothing here decides anything: see `Safety` for the rules (stabilization windows, gaps,
stale readings) and `common.config.safety` for the settings, which live in the config DB's `safety`
collection and are re-read on every poll, so an edit takes effect within one interval.

One thread, not one per sensor: `SafetyApi` is a singleton whose `errors` list is shared, so
concurrent calls would overwrite each other's, and one request already returns every sensor.

Safe means no enabled sensor says otherwise -- with none enabled (or no `safety` document for the
site) it is safe.
"""

import asyncio
import datetime
from collections.abc import Callable
from typing import Any

from common.api import SafetyApi
from common.canonical import CanonicalResponse
from common.config import Config
from common.config.local import ConfigError
from common.config.safety import SafetyConfig
from common.config.site import Site
from common.mast_logging import get_logger
from common.safety import POLL_INTERVAL, Safety, SafetyVerdictModel
from common.utils import RepeatTimer

logger = get_logger(__name__)

STATION = "davis"
REQUEST_TIMEOUT = 3.0  # seconds; well inside POLL_INTERVAL


def fetch_station_sensors() -> tuple[dict[str, dict] | None, list[str]]:
    """Every sensor of `STATION`, as the service gives them, or `(None, the reasons it could not be had)`."""
    api = SafetyApi()
    sensors = asyncio.run(api.sensors(STATION, timeout=REQUEST_TIMEOUT))
    return sensors, list(api.errors)


class SafetyMonitor:
    def __init__(
        self,
        *,
        settings: Callable[[], tuple[SafetyConfig | None, Site | None, str]] | None = None,
        fetch: Callable[[], tuple[dict[str, dict] | None, list[str]]] = fetch_station_sensors,
        interval: datetime.timedelta = POLL_INTERVAL,
    ):
        """`settings` gives `(safety document, this machine's site, project)`; by default the config DB's."""
        self.safety = Safety()
        self._settings = settings or self._settings_from_config
        self._fetch = fetch
        self._interval = interval
        self._timer: RepeatTimer | None = None
        self._applied: tuple[Any, Any] | None = None  # (document, site) as last applied, compared by identity
        self._config_error: str | None = None
        self._service_down = False

    # ---- lifecycle ----

    def start(self) -> None:
        if self._timer is not None:
            return
        self._timer = RepeatTimer(self._interval.total_seconds(), self.poll)
        self._timer.start()

    def stop(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    # ---- the question ----

    def verdict(self) -> SafetyVerdictModel:
        return self.safety.verdict()

    def endpoint_is_safe(self) -> CanonicalResponse:
        """`{"is_safe": bool}`, plus `"reasons": [str, ...]` when -- and only when -- it is not safe."""
        return CanonicalResponse(value=self.verdict().model_dump(exclude_none=True))

    # ---- the poll ----

    @staticmethod
    def _settings_from_config() -> tuple[SafetyConfig | None, Site | None, str]:
        config = Config()
        site = config.local_site
        return config.get_safety(), site, site.project if site else config.local.project

    def poll(self) -> None:
        """One cycle. Never raises: a monitor that dies silently leaves the last verdict looking current."""
        try:
            self._refresh_settings()
            self._read_sensors()
        except Exception:
            logger.exception("safety poll failed")

    def _refresh_settings(self) -> None:
        try:
            document, site, project = self._settings()
        except ConfigError as e:
            # Keep what we had. Said once per distinct problem, not every 5 seconds.
            if str(e) != self._config_error:
                logger.error(f"safety settings not applied, keeping the previous ones: {e}")
                self._config_error = str(e)
            return
        if self._config_error is not None:
            logger.info("safety settings are valid again")
            self._config_error = None
        if self._applied is not None and self._applied[0] is document and self._applied[1] is site:
            return  # the config accessors hand back the same object until the collection changes
        self.safety.apply_config(document, project=project, site=site)
        self._applied = (document, site)
        logger.info(f"safety settings applied: sensors={sorted(self.safety.sensors)}, sun={self.safety.sun is not None}")

    def _read_sensors(self) -> None:
        wanted = [name for name, state in self.safety.sensors.items() if state.settings.enabled]
        if not wanted:
            return  # nothing to ask the service

        sensors, errors = self._fetch()
        if sensors is None:
            reason = "; ".join(errors) or "no answer"
            for name in wanted:
                self.safety.report_failure(name, f"weather service: {reason}")
            if not self._service_down:
                logger.warning(f"the weather service is not answering: {reason}")
                self._service_down = True
            return
        if self._service_down:
            logger.info("the weather service is answering again")
            self._service_down = False

        now = datetime.datetime.now(datetime.UTC)
        for name in wanted:
            entry = sensors.get(name)
            if entry is None:
                self.safety.report_failure(name, f"the weather service has no sensor '{name}' for '{STATION}'")
                continue
            try:
                value = float(entry["value"])
                age = now - datetime.datetime.fromtimestamp(float(entry["timestamp"]), tz=datetime.UTC)
            except (KeyError, ValueError, TypeError) as e:
                self.safety.report_failure(name, f"malformed reading {entry!r}: {e!r}")
                continue
            self.safety.ingest(name, (value, age), now=now)
