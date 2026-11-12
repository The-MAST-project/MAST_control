"""A standalone safety monitor: watch `is_safe` over the nights and write down what it does.

It runs a `control.safety.SafetyMonitor` -- the very polling and verdict code control serves
`GET /is_safe` from -- and every `--interval` seconds (20) asks it whether it is safe. It logs

- each change of `is_safe`, with how long the previous state lasted and, when not safe, the
  reasons; and a change of which sensors block while it stays not safe;
- a summary of each observing night (time safe / not safe / warming up, flips, longest not-safe
  stretch, which sensors blocked most) when the night turns, and at exit.

and appends one line per check to `<data-dir>/safety-monitor-<night>.jsonl` with the raw value of
*every* sensor the weather service gave, enabled or not -- for working out what the limits should
be afterwards, which the log alone cannot show.

Settings come from the config DB's `safety` collection (run on a machine that has it) or, with
`--settings FILE`, from a JSON file in the same format, re-read whenever it changes::

    {
        "site": "ns",
        "location": {"latitude": 30.04, "longitude": 35.02, "elevation": 400},
        "sensors": {"wind_speed": {"max": 25.0, "settling": 60}, "rain_rate": {"max": 0.0, "settling": 900}},
        "sun_elevation": {"dawn": -9, "dusk": -12},
    }

`location` exists only in the file (the DB has it in `sites`); it is needed for `sun_elevation`.
At least one sensor must be enabled, or the weather service is never asked.

A windowed sensor is not safe until its window has filled, so the first minutes after a start say
nothing about the weather; they are logged as warming up and left out of the summary.
"""

from __future__ import annotations

import argparse
import datetime
import json
import signal
import threading
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import humanfriendly

from common.config.local import ConfigError
from common.config.safety import SafetyConfig
from common.config.site import Location, Site
from common.mast_logging import configure_logging, get_logger, observing_night
from common.safety import MAX_GAP
from control.safety import REQUEST_TIMEOUT, SafetyMonitor, fetch_station_sensors

PROGRAM = "safety-monitor"
DEFAULT_INTERVAL = 20.0  # seconds between checks
FIRST_CHECK_DELAY = REQUEST_TIMEOUT + 2  # seconds: lets the first poll land before the first check

logger = get_logger(__name__)

Settings = tuple[SafetyConfig | None, Site | None, str]


def _span(seconds: float) -> str:
    return humanfriendly.format_timespan(round(seconds))


def _iso(when: datetime.datetime) -> str:
    return when.astimezone(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class SettingsFile:
    """The `safety` document from a JSON file; callable as `SafetyMonitor`'s `settings`.

    The same objects are returned until the file changes, which is how the monitor knows nothing
    moved. A file that cannot be read or is invalid raises `ConfigError`, and the monitor keeps
    the settings it already had.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._mtime: int | None = None
        self._value: Settings | None = None

    def __call__(self) -> Settings:
        try:
            mtime = self.path.stat().st_mtime_ns
        except OSError as e:
            raise ConfigError(f"cannot read settings file {self.path}: {e}") from e
        if self._value is None or mtime != self._mtime:
            self._value = self._load()
            self._mtime = mtime
        return self._value

    def _load(self) -> Settings:
        try:
            doc = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(doc, dict):
                raise ConfigError(f"settings file {self.path} is not a JSON object")
            location = doc.pop("location", None)  # not part of the DB's document
            config = SafetyConfig(**doc)
            site = None
            if location is not None:
                site = Site(
                    name=config.site,
                    project=PROGRAM,
                    controller_host="-",
                    spec_host="-",
                    unit_ids=[],
                    location=Location(**location),
                )
        except (OSError, ValueError) as e:  # pydantic's ValidationError is a ValueError
            raise ConfigError(f"settings file {self.path} is invalid: {e}") from e
        if config.sun_elevation is not None and site is None:
            raise ConfigError(f"settings file {self.path}: 'sun_elevation' needs a 'location' (latitude, longitude)")
        return config, site, PROGRAM


class NightSummary:
    """What one observing night looked like, from the checks made during it."""

    def __init__(self, night: str):
        self.night = night
        self.safe_s = 0.0
        self.unsafe_s = 0.0
        self.warming_s = 0.0
        self.flips = 0
        self.longest_unsafe_s = 0.0
        self.blocked_checks: Counter[str] = Counter()

    def render(self) -> str:
        measured = self.safe_s + self.unsafe_s
        pct = f" ({100 * self.safe_s / measured:.0f}% safe)" if measured else ""
        blockers = ", ".join(f"{name} x{n}" for name, n in self.blocked_checks.most_common()) or "none"
        return (
            f"night {self.night}: safe {_span(self.safe_s)}, not safe {_span(self.unsafe_s)}{pct}, "
            f"warming up {_span(self.warming_s)} (not counted); {self.flips} change(s), "
            f"longest not-safe stretch {_span(self.longest_unsafe_s)}; blocking checks: {blockers}"
        )


class Watch:
    def __init__(self, monitor: SafetyMonitor, data_dir: str | Path, *, started: datetime.datetime | None = None):
        self.monitor = monitor
        self.data_dir = Path(data_dir)
        self.state: bool | None = None
        self.since: datetime.datetime | None = None
        self.blockers: frozenset[str] = frozenset()
        self.summary: NightSummary | None = None
        self._last_check: datetime.datetime | None = None
        self._latest: tuple[dict[str, dict] | None, list[str]] = (None, [])
        self._started = started or datetime.datetime.now(datetime.UTC)
        self._warm_until: datetime.datetime | None = None
        self._was_warming: bool | None = None
        self._stretch_start: datetime.datetime | None = None

    # ---- what the monitor's poll sees, kept for the JSONL ----

    def fetch(self) -> tuple[dict[str, dict] | None, list[str]]:
        """Stands in for `fetch_station_sensors`: asks the service and remembers what it said."""
        self._latest = fetch_station_sensors()
        return self._latest

    # ---- the check ----

    def _warming(self, now: datetime.datetime) -> bool:
        if self._warm_until is None:
            windows = [s.window for s in self.monitor.safety.sensors.values() if s.settings.enabled]
            self._warm_until = self._started + max(windows, default=datetime.timedelta(0)) + MAX_GAP
        return now < self._warm_until

    def check(self, now: datetime.datetime | None = None) -> None:
        now = now or datetime.datetime.now(datetime.UTC)
        verdict = self.monitor.safety.verdict(now)
        reasons = verdict.reasons or []
        blockers = frozenset(r.split(":", 1)[0] for r in reasons)
        warming = self._warming(now)

        self._roll_night(now)
        assert self.summary is not None
        if self._last_check is not None:
            elapsed = (now - self._last_check).total_seconds()
            if warming:
                self.summary.warming_s += elapsed
            elif self.state:
                self.summary.safe_s += elapsed
            elif self.state is False:
                self.summary.unsafe_s += elapsed
        if not warming:
            self.summary.blocked_checks.update(blockers)
        self._last_check = now

        said = self._log_change(now, verdict.is_safe, reasons, blockers, warming)
        if self._was_warming and not warming and not said:
            # One line for the end of the warm-up instead of one per window that filled during it --
            # unless a change of state was just reported, which says as much.
            listing = "".join(f"\n  - {r}" for r in reasons)
            logger.log(
                20 if verdict.is_safe else 30,
                f"safety: warm-up over, the windows are full; {'SAFE' if verdict.is_safe else 'NOT SAFE'}{listing}",
            )
        self._was_warming = warming
        self._append(now, verdict.is_safe, blockers)

    def _log_change(
        self, now: datetime.datetime, is_safe: bool, reasons: list[str], blockers: frozenset[str], warming: bool
    ) -> bool:
        """Log what changed; True if a line was written."""
        said = True
        note = " [warming up: windows are still filling]" if warming else ""
        listing = "".join(f"\n  - {r}" for r in reasons)
        assert self.summary is not None

        if self.state is None:
            logger.log(20 if is_safe else 30, f"safety: {'SAFE' if is_safe else 'NOT SAFE'} (first check){note}{listing}")
        elif is_safe != self.state:
            held = _span((now - self.since).total_seconds()) if self.since else "?"
            if not warming:
                self.summary.flips += 1
                if not is_safe:
                    self._stretch_start = now
                elif self._stretch_start is not None:
                    self.summary.longest_unsafe_s = max(
                        self.summary.longest_unsafe_s, (now - self._stretch_start).total_seconds()
                    )
                    self._stretch_start = None
            if is_safe:
                logger.info(f"safety: SAFE (was not safe for {held}){note}")
            else:
                logger.warning(f"safety: NOT SAFE (was safe for {held}){note}{listing}")
        elif not is_safe and blockers != self.blockers and not warming:
            # Not while warming up: every window that fills would be a line, and none says anything
            # about the weather. The end of the warm-up is reported once instead.
            logger.warning(
                f"safety: still not safe, now blocked by {sorted(blockers)} (was {sorted(self.blockers)}){note}{listing}"
            )
        else:
            said = False

        if self.state is None or is_safe != self.state:
            self.since = now
        self.state = is_safe
        self.blockers = blockers
        return said

    def _roll_night(self, now: datetime.datetime) -> None:
        night = observing_night(now)
        if self.summary is None:
            self.summary = NightSummary(night)
        elif self.summary.night != night:
            self.close_night(now)
            self.summary = NightSummary(night)

    def close_night(self, now: datetime.datetime | None = None) -> None:
        """Log the summary of the night in progress (the night turned, or we are exiting)."""
        if self.summary is None:
            return
        now = now or datetime.datetime.now(datetime.UTC)
        if self.state is False and self._stretch_start is not None:
            self.summary.longest_unsafe_s = max(self.summary.longest_unsafe_s, (now - self._stretch_start).total_seconds())
        logger.info(f"safety summary, {self.summary.render()}")

    # ---- the raw data ----

    def _append(self, now: datetime.datetime, is_safe: bool, blockers: frozenset[str]) -> None:
        sensors, errors = self._latest
        line: dict[str, Any] = {
            "time": _iso(now),
            "is_safe": is_safe,
            "blockers": sorted(blockers),
            "service_ok": sensors is not None,
        }
        if sensors is not None:
            line["sensors"] = {name: _raw(entry) for name, entry in sorted(sensors.items())}
        elif errors:
            line["errors"] = errors
        assert self.summary is not None
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            with (self.data_dir / f"safety-monitor-{self.summary.night}.jsonl").open("a", encoding="utf-8") as fp:
                fp.write(json.dumps(line, separators=(",", ":")) + "\n")
        except OSError as e:
            logger.error(f"cannot write the data file in {self.data_dir}: {e}")


def _raw(entry: dict) -> dict[str, Any]:
    """One sensor as the service gave it: the number, its own timestamp, its unit."""
    out: dict[str, Any] = {}
    try:
        out["value"] = float(entry["value"])
    except (KeyError, ValueError, TypeError):
        out["value"] = entry.get("value")
    try:
        out["ts"] = float(entry["timestamp"])
    except (KeyError, ValueError, TypeError):
        out["ts"] = entry.get("timestamp")
    if entry.get("sensor_unit"):
        out["unit"] = entry["sensor_unit"]
    return out


def run(
    settings: Callable[[], Settings] | None,
    data_dir: str | Path,
    interval: float = DEFAULT_INTERVAL,
    stop: threading.Event | None = None,
) -> None:
    """Poll and check until `stop` is set; then log the night's summary."""
    stop = stop or threading.Event()
    if settings is not None:
        settings()  # fail now, with the reason, rather than run for a night on nothing
    watch = Watch(SafetyMonitor(settings=settings, fetch=lambda: watch.fetch()), data_dir)
    watch.monitor.start()
    logger.info(f"safety monitor started: checking every {_span(interval)}, data in {Path(data_dir).resolve()}")
    try:
        stop.wait(FIRST_CHECK_DELAY)
        while not stop.is_set():
            try:
                watch.check()
            except Exception:
                logger.exception("safety check failed")
            stop.wait(interval)
    finally:
        watch.monitor.stop()
        watch.close_night()
        logger.info("safety monitor stopped")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Watch is_safe over time and log what it does.")
    parser.add_argument("--settings", help="a JSON file in the format of the 'safety' document (default: the config DB's)")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="seconds between checks (default: 20)")
    parser.add_argument("--data-dir", default="safety-monitor-data", help="where the per-night JSONL files go")
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args(argv)

    configure_logging(args.log_level, program=PROGRAM)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        run(SettingsFile(args.settings) if args.settings else None, args.data_dir, args.interval, stop)
    except ConfigError as e:
        logger.error(f"cannot start: {e}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
