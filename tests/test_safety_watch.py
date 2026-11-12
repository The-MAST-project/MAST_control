"""The standalone safety monitor: what it logs, what it writes, how it reads its settings file."""

from __future__ import annotations

import datetime
import json
import logging
import os
import threading

import pytest
from fakes import make_site

from common.config.local import ConfigError
from common.config.safety import SafetyConfig
from control import safety_watch
from control.safety import SafetyMonitor
from control.safety_watch import SettingsFile, Watch

T0 = datetime.datetime(2026, 11, 11, 20, 0, 0, tzinfo=datetime.UTC)
S = datetime.timedelta(seconds=1)
SITE = make_site(location={"latitude": 30.0, "longitude": 35.0})
DOC = SafetyConfig(site="ns", sensors={"wind_speed": {"max": 25.0}, "rain_rate": {"max": 0.0}})


def reading(at: datetime.datetime, **values: float) -> dict[str, dict]:
    return {
        name: {"timestamp": str(at.timestamp()), "value": str(v), "sensor_unit": "u", "sensor_type": name}
        for name, v in values.items()
    }


class Rig:
    """A Watch over a monitor whose weather service is `self.answer` and whose clock is `self.now`."""

    def __init__(self, tmp_path, document=DOC):
        self.now = T0
        self.answer: dict | None = None
        self.monitor = SafetyMonitor(settings=lambda: (document, SITE, "mast"), fetch=self._fetch)
        self.watch = Watch(self.monitor, tmp_path, started=T0)
        self.data = tmp_path

    def _fetch(self):
        self.watch._latest = (self.answer, [] if self.answer is not None else ["down"])
        return self.watch._latest

    def step(self, seconds: float = 20, **values: float | None) -> None:
        """Advance the clock; the service says `values` (or `down=True`: it does not answer); poll; check."""
        self.now += datetime.timedelta(seconds=seconds)
        down = bool(values.pop("down", False))
        self.answer = None if down else reading(self.now - S, **values)
        self.watch._latest = (self.answer, ["down"] if down else [])
        # The monitor stamps readings with the real clock, so drive its Safety with ours instead.
        self.monitor._refresh_settings()
        for name, entry in (self.answer or {}).items():
            if name in self.monitor.safety.sensors:
                self.monitor.safety.ingest(name, (float(entry["value"]), S), now=self.now)
        self.watch.check(self.now)

    def lines(self) -> list[dict]:
        files = sorted(self.data.glob("safety-monitor-*.jsonl"))
        return [json.loads(line) for f in files for line in f.read_text().splitlines()]


@pytest.fixture
def rig(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    return Rig(tmp_path)


def messages(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name.startswith("mast.")]


def test_first_check_is_logged_with_the_reasons_when_not_safe(rig, caplog):
    rig.step(wind_speed=31.0, rain_rate=0.0)
    (msg,) = [m for m in messages(caplog) if m.startswith("safety:")]
    assert "NOT SAFE (first check)" in msg
    assert "wind_speed: 31 > 25" in msg


def test_a_change_is_logged_once_with_how_long_the_state_lasted(rig, caplog):
    for _ in range(3):
        rig.step(wind_speed=10.0, rain_rate=0.0)
    rig.step(wind_speed=31.0, rain_rate=0.0)
    for _ in range(3):
        rig.step(wind_speed=31.0, rain_rate=0.0)  # unchanged: nothing more to say
    rig.step(wind_speed=10.0, rain_rate=0.0)

    said = [m for m in messages(caplog) if m.startswith("safety:")]
    assert len(said) == 3
    assert "SAFE (first check)" in said[0]
    assert "NOT SAFE (was safe for 1 minute" in said[1] and "wind_speed" in said[1]
    assert said[2].startswith("safety: SAFE (was not safe for 1 minute")


def test_a_different_blocker_while_not_safe_is_logged(rig, caplog):
    rig.step(wind_speed=31.0, rain_rate=0.0)
    rig.step(wind_speed=31.0, rain_rate=0.0)
    rig.step(wind_speed=31.0, rain_rate=1.0)
    said = [m for m in messages(caplog) if m.startswith("safety:")]
    assert any("still not safe, now blocked by ['rain_rate', 'wind_speed']" in m for m in said)


def test_one_line_of_raw_data_per_check_with_every_sensor(rig):
    rig.step(wind_speed=10.0, rain_rate=0.0)
    rig.step(wind_speed=11.0, rain_rate=0.0, barometer=977.0)  # barometer: a sensor nobody guards
    first, second = rig.lines()
    assert first["is_safe"] is True and first["service_ok"] is True
    assert set(second["sensors"]) == {"wind_speed", "rain_rate", "barometer"}
    assert second["sensors"]["barometer"]["value"] == 977.0


def test_a_silent_service_is_recorded(rig):
    rig.step(down=True)
    (line,) = rig.lines()
    assert line["service_ok"] is False and line["errors"] == ["down"]
    assert line["is_safe"] is False


def test_warming_up_is_labelled_and_left_out_of_the_summary(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    doc = SafetyConfig(site="ns", sensors={"wind_speed": {"max": 25.0, "settling": 60}})
    rig = Rig(tmp_path, document=doc)
    rig.step(wind_speed=10.0)
    assert any("warming up" in m for m in messages(caplog))
    for _ in range(6):
        rig.step(wind_speed=10.0)
    assert rig.watch.summary.warming_s > 0
    assert rig.watch.summary.safe_s + rig.watch.summary.unsafe_s >= 0


def test_the_summary_counts_time_flips_and_blockers(rig, caplog):
    rig.step(wind_speed=10.0, rain_rate=0.0)
    rig.step(wind_speed=10.0, rain_rate=0.0)
    rig.step(wind_speed=31.0, rain_rate=0.0)
    rig.step(wind_speed=31.0, rain_rate=0.0)
    rig.step(wind_speed=10.0, rain_rate=0.0)
    rig.watch.close_night(rig.now)
    s = rig.watch.summary
    assert s.flips == 2
    assert s.blocked_checks["wind_speed"] == 2
    assert s.longest_unsafe_s == 40
    assert any(m.startswith("safety summary, night 2026-11-11") for m in messages(caplog))


def test_the_summary_is_logged_when_the_night_turns(rig, caplog):
    rig.step(wind_speed=10.0, rain_rate=0.0)
    rig.step(seconds=17 * 3600, wind_speed=10.0, rain_rate=0.0)  # 13:00 UTC next day: the next night
    assert rig.watch.summary.night == "2026-11-12"
    assert any("night 2026-11-11" in m for m in messages(caplog) if m.startswith("safety summary"))
    assert len(list(rig.data.glob("safety-monitor-*.jsonl"))) == 2


# ---- the settings file ----


def write(path, **doc):
    path.write_text(json.dumps(doc))
    return path


def test_settings_file_loads_and_is_the_same_object_until_it_changes(tmp_path):
    f = write(tmp_path / "s.json", site="ns", sensors={"wind_speed": {"max": 25}})
    settings = SettingsFile(f)
    first = settings()
    assert settings() is first
    assert first[0].sensors["wind_speed"].max == 25 and first[1] is None

    before = f.stat().st_mtime_ns
    write(f, site="ns", sensors={"wind_speed": {"max": 30}})
    os.utime(f, ns=(before + 1_000_000_000, before + 1_000_000_000))  # a coarse clock may not tell
    assert settings() is not first
    assert settings()[0].sensors["wind_speed"].max == 30


def test_settings_file_location_builds_the_site_for_the_sun(tmp_path):
    f = write(
        tmp_path / "s.json",
        site="ns",
        location={"latitude": 30.0, "longitude": 35.0},
        sun_elevation={"dawn": -9, "dusk": -12},
    )
    config, site, _ = SettingsFile(f)()
    assert config.sun_elevation.dusk == -12
    assert site.location.latitude == 30.0


@pytest.mark.parametrize(
    "doc",
    [
        {"site": "ns", "sun_elevation": {"dawn": -9, "dusk": -12}},  # sun without a location
        {"site": "ns", "sensors": {"wind_speed": {"setling": 60}}},  # a typo
        {"sensors": {}},  # no site
    ],
)
def test_settings_file_that_is_invalid_is_refused_with_the_reason(tmp_path, doc):
    with pytest.raises(ConfigError):
        SettingsFile(write(tmp_path / "s.json", **doc))()


def test_settings_file_missing_or_not_json(tmp_path):
    with pytest.raises(ConfigError, match="cannot read"):
        SettingsFile(tmp_path / "nope.json")()
    bad = tmp_path / "bad.json"
    bad.write_text("{ nope")
    with pytest.raises(ConfigError, match="invalid"):
        SettingsFile(bad)()


def test_run_stops_on_the_event_and_summarises(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(safety_watch, "FIRST_CHECK_DELAY", 0.0)
    monkeypatch.setattr(safety_watch, "fetch_station_sensors", lambda: (None, ["offline"]))
    f = write(tmp_path / "s.json", site="ns", sensors={"wind_speed": {"max": 25}})
    stop = threading.Event()
    threading.Timer(0.5, stop.set).start()
    safety_watch.run(SettingsFile(f), tmp_path / "data", interval=0.1, stop=stop)
    said = messages(caplog)
    assert any(m.startswith("safety monitor started") for m in said)
    assert any(m.startswith("safety summary") for m in said)
    assert any(m == "safety monitor stopped" for m in said)


def test_run_refuses_to_start_on_a_bad_settings_file(tmp_path):
    with pytest.raises(ConfigError):
        safety_watch.run(SettingsFile(tmp_path / "nope.json"), tmp_path, interval=0.1)
