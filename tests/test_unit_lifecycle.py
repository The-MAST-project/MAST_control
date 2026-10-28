"""Telling a unit to start up or shut down, through control."""

from __future__ import annotations

import asyncio
from typing import ClassVar

import pytest
from fakes import bare_controller, make_site

import control.controller as controller_module
from common.canonical import CanonicalResponse, CanonicalResponse_Ok
from common.models.statuses import BasicUnitStatus, FullUnitStatus
from control.controller import CachedValue


def _cached(value) -> CachedValue:
    cached = CachedValue(interval_seconds=30, api=None, site_name="ns", machine_name="mast01")
    cached.value = value
    return cached


class FakeUnitApi:
    """Records what it is asked, and answers like a unit would."""

    calls: ClassVar[list[tuple[str, str]]] = []

    def __init__(self, hostname: str):
        self.hostname = hostname

    async def put(self, method: str, **kwargs) -> CanonicalResponse:
        FakeUnitApi.calls.append((self.hostname, method))
        return CanonicalResponse_Ok


@pytest.fixture(autouse=True)
def _fake_unit_api(monkeypatch):
    FakeUnitApi.calls = []
    monkeypatch.setattr(controller_module, "UnitApi", FakeUnitApi)


def _controller(monkeypatch, status=None, **site_fields):
    controller = bare_controller(monkeypatch, [make_site(deployed_units=["mast01", "mast02"], **site_fields)])
    controller.status_cache["ns"] = {"units": {"mast01": _cached(status)}, "spec": None, "controller": None}
    return controller


def _startup(controller, unit="mast01", site="ns"):
    return asyncio.run(controller.endpoint_unit_startup(site, unit))


def _shutdown(controller, unit="mast01", site="ns"):
    return asyncio.run(controller.endpoint_unit_shutdown(site, unit))


class TestStartup:
    def test_a_deployed_unit_is_told_to_start_up(self, monkeypatch):
        response = _startup(_controller(monkeypatch))

        assert response.succeeded
        assert FakeUnitApi.calls == [("mast01", "startup")]

    def test_the_unit_need_not_have_a_status(self, monkeypatch):
        # A unit that has not started up (or is not reachable yet) is what startup is for.
        response = _startup(
            _controller(monkeypatch, status=BasicUnitStatus(detected=False, powered=True, operational=False))
        )

        assert response.succeeded

    @pytest.mark.parametrize("unit", ["mast09", "mast00"])
    def test_a_unit_that_is_not_deployed_is_refused(self, monkeypatch, unit):
        response = _startup(_controller(monkeypatch), unit=unit)

        assert response.failed
        assert "not deployed" in response.errors[0]
        assert FakeUnitApi.calls == []

    def test_a_unit_in_maintenance_is_refused(self, monkeypatch):
        response = _startup(_controller(monkeypatch, units_in_maintenance=["mast01"]))

        assert response.failed
        assert "maintenance" in response.errors[0]
        assert FakeUnitApi.calls == []

    def test_an_unmanaged_site_is_refused(self, monkeypatch):
        response = _startup(_controller(monkeypatch), site="elsewhere")

        assert response.failed
        assert "not managed" in response.errors[0]
        assert FakeUnitApi.calls == []


class TestShutdown:
    def test_an_idle_unit_is_told_to_shut_down(self, monkeypatch):
        response = _shutdown(_controller(monkeypatch, status=FullUnitStatus(id=1, why_busy={})))

        assert response.succeeded
        assert FakeUnitApi.calls == [("mast01", "shutdown")]

    def test_a_busy_unit_is_refused_and_says_why(self, monkeypatch):
        busy = FullUnitStatus(id=1, why_busy={"unit": ["ExecutingPlan"], "mount": ["Slewing"]})

        response = _shutdown(_controller(monkeypatch, status=busy))

        assert response.failed
        assert "busy" in response.errors[0]
        assert "ExecutingPlan" in response.errors[0]
        assert "Slewing" in response.errors[0]
        assert FakeUnitApi.calls == []

    def test_an_unknown_busy_status_is_not_taken_for_idle(self, monkeypatch):
        response = _shutdown(_controller(monkeypatch, status=FullUnitStatus(id=1, why_busy=None)))

        assert response.failed
        assert "unknown" in response.errors[0]
        assert FakeUnitApi.calls == []

    @pytest.mark.parametrize("status", [None, BasicUnitStatus(detected=False, powered=True, operational=False)])
    def test_a_unit_without_a_full_status_is_refused(self, monkeypatch, status):
        response = _shutdown(_controller(monkeypatch, status=status))

        assert response.failed
        assert "no full status" in response.errors[0]
        assert FakeUnitApi.calls == []

    def test_a_unit_that_is_not_deployed_is_refused(self, monkeypatch):
        response = _shutdown(_controller(monkeypatch, status=FullUnitStatus(id=1, why_busy={})), unit="mast09")

        assert response.failed
        assert "not deployed" in response.errors[0]
        assert FakeUnitApi.calls == []

    def test_the_units_own_refusal_is_passed_back(self, monkeypatch):
        class Refusing(FakeUnitApi):
            async def put(self, method, **kwargs):
                return CanonicalResponse(errors=["unit says no"])

        monkeypatch.setattr(controller_module, "UnitApi", Refusing)

        response = _shutdown(_controller(monkeypatch, status=FullUnitStatus(id=1, why_busy={})))

        assert response.errors == ["unit says no"]
