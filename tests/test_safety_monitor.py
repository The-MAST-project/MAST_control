"""The safety monitor: polling the weather service into `Safety`, and the `is_safe` answer."""

from __future__ import annotations

import datetime
import time
from types import SimpleNamespace

import pytest
from fakes import bare_controller, make_site

from common.config.local import ConfigError
from common.config.safety import SafetyConfig
from control.safety import STATION, SafetyMonitor

SITE = make_site(location={"latitude": 30.0, "longitude": 35.0, "sun_limits": {"dusk": -12, "dawn": -9}})
DOC = SafetyConfig(site="ns", sensors={"wind_speed": {"max": 25.0}, "rain_rate": {"max": 0.0}})


def service(age_s: float = 0.0, **values: float | None):
    """What the weather service answers: every sensor timestamped `age_s` seconds ago.

    A later reading must be later than 0.5 s after an earlier one, or it is the same reading polled
    twice; tests that poll twice give the first answer an age of 8 and the second of 1.
    """
    now = datetime.datetime.now(datetime.UTC).timestamp() - age_s
    return {
        name: {"timestamp": str(now), "value": str(v), "sensor_unit": "kph", "sensor_type": name}
        for name, v in values.items()
        if v is not None
    }


class Harness:
    """A monitor whose settings and weather service are plain attributes the test can change."""

    def __init__(self, document=DOC, site=SITE):
        self.document, self.site = document, site
        self.answer: dict | None = service(age_s=8, wind_speed=10.0, rain_rate=0.0)
        self.errors: list[str] = []
        self.fetches = 0
        self.config_error: ConfigError | None = None
        self.monitor = SafetyMonitor(settings=self._settings, fetch=self._fetch)

    def _settings(self):
        if self.config_error:
            raise self.config_error
        return self.document, self.site, "mast"

    def _fetch(self):
        self.fetches += 1
        return self.answer, self.errors


@pytest.fixture
def h() -> Harness:
    return Harness()


def test_a_poll_feeds_every_enabled_sensor_and_answers(h):
    h.monitor.poll()
    assert set(h.monitor.safety.sensors) == {"wind_speed", "rain_rate"}
    assert all(len(s._readings) == 1 for s in h.monitor.safety.sensors.values())
    # no windows configured, so the newest reading decides; the sun sensor is not in this document
    assert h.monitor.verdict().is_safe


def test_unsafe_value_is_reported_with_the_reason(h):
    h.answer = service(wind_speed=31.0, rain_rate=0.0)
    h.monitor.poll()
    verdict = h.monitor.verdict()
    assert not verdict.is_safe
    assert verdict.reasons and verdict.reasons[0].startswith("wind_speed: 31 > 25")


def test_the_endpoint_carries_reasons_only_when_unsafe(h):
    h.monitor.poll()
    assert h.monitor.endpoint_is_safe().value == {"is_safe": True}

    h.answer = service(age_s=1, wind_speed=31.0, rain_rate=0.0)
    h.monitor.poll()
    value = h.monitor.endpoint_is_safe().value
    assert value["is_safe"] is False
    assert value["reasons"]


def test_unreachable_service_is_not_safe_and_says_why(h):
    h.answer, h.errors = None, ["timeout after 3.0 seconds"]
    h.monitor.poll()
    verdict = h.monitor.verdict()
    assert not verdict.is_safe
    assert any("timeout" in r for r in verdict.reasons or [])


def test_service_recovering_restores_the_answer(h):
    h.answer, h.errors = None, ["down"]
    h.monitor.poll()
    assert not h.monitor.verdict().is_safe
    h.answer, h.errors = service(wind_speed=10.0, rain_rate=0.0), []
    h.monitor.poll()
    assert h.monitor.verdict().is_safe


def test_a_sensor_the_service_does_not_have_is_not_safe(h):
    h.answer = service(wind_speed=10.0)  # no rain_rate
    h.monitor.poll()
    reasons = h.monitor.verdict().reasons or []
    assert any(r.startswith("rain_rate:") and STATION in r for r in reasons)


def test_a_malformed_reading_is_not_safe(h):
    h.answer = service(wind_speed=10.0, rain_rate=0.0)
    h.answer["wind_speed"]["value"] = "garbage"
    h.monitor.poll()
    assert any("malformed" in r for r in h.monitor.verdict().reasons or [])


def test_a_stale_reading_is_refused(h):
    old = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=60)).timestamp()
    h.answer = service(wind_speed=10.0, rain_rate=0.0)
    h.answer["wind_speed"]["timestamp"] = str(old)
    h.monitor.poll()
    assert any("old" in r for r in h.monitor.verdict().reasons or [])


def test_a_repeated_reading_is_counted_once(h):
    h.monitor.poll()
    h.monitor.poll()  # the service gave the very same readings
    assert all(len(s._readings) == 1 for s in h.monitor.safety.sensors.values())


def test_no_enabled_sensors_is_safe_and_the_service_is_not_asked(h):
    h.document = None
    h.monitor.poll()
    assert h.monitor.verdict().is_safe
    assert h.fetches == 0


def test_settings_change_is_picked_up_and_history_rejudged(h):
    h.monitor.poll()
    assert h.monitor.verdict().is_safe
    h.document = SafetyConfig(site="ns", sensors={"wind_speed": {"max": 5.0}})
    h.monitor.poll()
    assert set(h.monitor.safety.sensors) == {"wind_speed"}
    assert not h.monitor.verdict().is_safe  # 10 > 5, judged from the readings already held


def test_unchanged_settings_are_not_reapplied(h, monkeypatch):
    h.monitor.poll()
    calls = []
    monkeypatch.setattr(h.monitor.safety, "apply_config", lambda *a, **k: calls.append(1))
    h.monitor.poll()
    assert calls == []


def test_invalid_settings_keep_the_previous_ones(h):
    h.monitor.poll()
    h.config_error = ConfigError("the 'safety' document for site 'ns' is invalid: boom")
    h.answer = service(age_s=1, wind_speed=31.0, rain_rate=0.0)
    h.monitor.poll()
    assert set(h.monitor.safety.sensors) == {"wind_speed", "rain_rate"}  # still guarding
    assert not h.monitor.verdict().is_safe


def test_the_sun_sensor_joins_when_configured():
    h = Harness(document=SafetyConfig(site="ns", sun_elevation={"dawn": -9, "dusk": -12}))
    h.monitor.poll()
    assert h.monitor.safety.sun is not None
    # a settled elevation is either safe or not; what matters is that it was computed, not skipped
    assert h.fetches == 0
    assert h.monitor.verdict().reasons is None or "sun_elevation" in h.monitor.verdict().reasons[0]


def test_poll_never_raises(h):
    def boom():
        raise RuntimeError("service exploded")

    h.monitor._fetch = boom
    h.monitor.poll()  # must not propagate


def test_start_and_stop(h):
    h.monitor._interval = datetime.timedelta(milliseconds=20)
    h.monitor.start()
    try:
        deadline = time.monotonic() + 2
        while h.fetches < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        h.monitor.stop()
    assert h.fetches >= 2


def test_controller_route_is_registered_and_answers(monkeypatch):
    controller = bare_controller(monkeypatch, sites=[SITE])
    controller.planner = SimpleNamespace(add_routes=lambda router: None)
    h = Harness()
    controller.safety_monitor = h.monitor
    paths = {r.path for r in controller.api_router.routes}
    assert "/mast/api/v1/control/is_safe" in paths
    assert controller.endpoint_is_safe().value == {"is_safe": True}  # nothing polled yet and no sun: nothing says otherwise
