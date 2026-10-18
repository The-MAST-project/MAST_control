"""`Controller.status()`: the sites' statuses, as served from the cache."""

from __future__ import annotations

from fakes import FakePowerSwitch, bare_controller, make_site

from common.models.statuses import FullUnitStatus, SpecStatus
from control.controller import CachedValue


def _cached(machine_name: str, value=None) -> CachedValue:
    cached = CachedValue(interval_seconds=30, api=None, site_name="ns", machine_name=machine_name)
    cached.value = value
    return cached


def _site_cache(units: dict[str, CachedValue], spec: CachedValue | None = None) -> dict:
    return {"units": units, "spec": spec, "controller": None}


def test_units_with_a_status_are_served_with_their_power_state(monkeypatch):
    controller = bare_controller(monkeypatch, [make_site()])
    controller.status_cache["ns"] = _site_cache(
        {
            "mast01": _cached("mast01", FullUnitStatus(id=1, operational=True)),
            "mast02": _cached("mast02", FullUnitStatus(id=2, operational=False)),
        }
    )
    controller.power_switches["ns"] = {
        "mast01": FakePowerSwitch({"Computer": True}),
        "mast02": FakePowerSwitch({"Computer": False}),
    }

    sites = controller.status().value["sites"]

    units = sites["ns"]["units"]
    assert units["mast01"]["operational"] is True
    assert units["mast01"]["powered"] is True
    assert units["mast02"]["powered"] is False


def test_a_unit_not_yet_fetched_is_left_out(monkeypatch):
    controller = bare_controller(monkeypatch, [make_site()])
    controller.status_cache["ns"] = _site_cache(
        {
            "mast01": _cached("mast01", FullUnitStatus(id=1, operational=True)),
            "mast02": _cached("mast02"),  # no fetch has completed yet
        }
    )
    controller.power_switches["ns"] = {"mast01": FakePowerSwitch(), "mast02": FakePowerSwitch()}

    units = controller.status().value["sites"]["ns"]["units"]

    assert list(units) == ["mast01"]


def test_a_spec_without_a_status_is_reported_as_not_detected(monkeypatch):
    controller = bare_controller(monkeypatch, [make_site()])
    controller.status_cache["ns"] = _site_cache({})

    spec = controller.status().value["sites"]["ns"]["spec"]

    assert spec["detected"] is False
    assert spec["why_not_operational"] == ["No status available"]


def test_a_cached_spec_status_is_served_as_powered(monkeypatch):
    controller = bare_controller(monkeypatch, [make_site()])
    controller.status_cache["ns"] = _site_cache({}, spec=_cached("mast-ns-spec", SpecStatus(detected=True)))

    spec = controller.status().value["sites"]["ns"]["spec"]

    assert spec["detected"] is True
    assert spec["powered"] is True


def test_a_managed_site_with_no_cache_yet_is_skipped(monkeypatch):
    # A site assigned to this host appears in managed_sites() before refresh() has given it
    # a cache entry. That window used to raise KeyError out of the status endpoint.
    controller = bare_controller(monkeypatch, [make_site("ns"), make_site("wis")])
    controller.status_cache["ns"] = _site_cache({})

    sites = controller.status().value["sites"]

    assert list(sites) == ["ns"]
