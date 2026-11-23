"""Telling a list of units to dance together, through control."""

from __future__ import annotations

import asyncio
from typing import ClassVar

import pytest
from fakes import bare_controller, make_site

import control.controller as controller_module
from common.canonical import CanonicalResponse, CanonicalResponse_Ok


class FakeUnitApi:
    """Answers like units: `states` is the opstate each reports, advanced by `up_after` polls."""

    calls: ClassVar[list[tuple[str, str, dict | None]]] = []
    states: ClassVar[dict[str, str]] = {}
    polls: ClassVar[dict[str, int]] = {}
    up_after: ClassVar[dict[str, int]] = {}
    refuse_dance: ClassVar[set[str]] = set()

    def __init__(self, hostname: str):
        self.hostname = hostname

    async def get(self, method: str, **kwargs) -> CanonicalResponse:
        n = FakeUnitApi.polls[self.hostname] = FakeUnitApi.polls.get(self.hostname, 0) + 1
        started = FakeUnitApi.states.get(self.hostname) == "startedup" or (
            ("startup" in [c[1] for c in FakeUnitApi.calls if c[0] == self.hostname])
            and n > FakeUnitApi.up_after.get(self.hostname, 0)
        )
        return CanonicalResponse(value={"opstate": "startedup" if started else "shutdown"})

    async def put(self, method: str, params: dict | None = None, **kwargs) -> CanonicalResponse:
        FakeUnitApi.calls.append((self.hostname, method, params))
        if method == "start_dancing" and self.hostname in FakeUnitApi.refuse_dance:
            return CanonicalResponse(errors=["busy"])
        return CanonicalResponse_Ok


@pytest.fixture(autouse=True)
def _fake(monkeypatch):
    FakeUnitApi.calls, FakeUnitApi.states, FakeUnitApi.polls = [], {}, {}
    FakeUnitApi.up_after, FakeUnitApi.refuse_dance = {}, set()
    monkeypatch.setattr(controller_module, "UnitApi", FakeUnitApi)
    monkeypatch.setattr(controller_module.Controller, "DANCE_POLL_SECONDS", 0.001)


def _controller(monkeypatch, **site_fields):
    return bare_controller(monkeypatch, [make_site(deployed_units=["mast01", "mast02", "mast03"], **site_fields)])


def _dance(controller, units, **kwargs):
    return asyncio.run(controller.endpoint_units_dance("ns", units, **kwargs))


def _dances():
    return [c for c in FakeUnitApi.calls if c[1] == "start_dancing"]


def test_units_already_up_dance_at_once_with_the_default_foxtrot(monkeypatch):
    FakeUnitApi.states = {"mast01": "startedup", "mast02": "startedup"}
    response = _dance(_controller(monkeypatch), ["mast01", "mast02"])

    assert response.succeeded
    assert response.value == {"mast01": "dancing", "mast02": "dancing"}
    assert _dances() == [
        ("mast01", "start_dancing", {"style": "foxtrot"}),
        ("mast02", "start_dancing", {"style": "foxtrot"}),
    ]
    assert not [c for c in FakeUnitApi.calls if c[1] == "startup"]


def test_no_unit_dances_until_all_are_started_up(monkeypatch):
    FakeUnitApi.states = {"mast01": "startedup"}
    FakeUnitApi.up_after = {"mast02": 5}
    response = _dance(_controller(monkeypatch), ["mast01", "mast02"], dance="alto")

    assert response.succeeded
    startups = [i for i, c in enumerate(FakeUnitApi.calls) if c[1] == "startup"]
    dances = [i for i, c in enumerate(FakeUnitApi.calls) if c[1] == "start_dancing"]
    assert [FakeUnitApi.calls[i][0] for i in startups] == ["mast02"]
    assert min(dances) > max(startups)
    assert {c[2]["style"] for c in _dances()} == {"alto"}


def test_a_unit_that_does_not_come_up_is_reported_and_the_rest_dance(monkeypatch):
    FakeUnitApi.states = {"mast01": "startedup"}
    FakeUnitApi.up_after = {"mast02": 10**9}
    response = _dance(_controller(monkeypatch), ["mast01", "mast02"], timeout=0.05)

    assert response.failed
    assert response.value["mast01"] == "dancing"
    assert "not STARTED_UP" in response.value["mast02"]
    assert [c[0] for c in _dances()] == ["mast01"]


def test_a_unit_that_refuses_the_dance_is_reported(monkeypatch):
    FakeUnitApi.states = {"mast01": "startedup", "mast02": "startedup"}
    FakeUnitApi.refuse_dance = {"mast02"}
    response = _dance(_controller(monkeypatch), ["mast01", "mast02"])

    assert response.failed and response.value["mast02"] == "refused: busy"
    assert response.value["mast01"] == "dancing"


def test_a_wrong_name_touches_no_unit(monkeypatch):
    response = _dance(_controller(monkeypatch, units_in_maintenance=["mast03"]), ["mast01", "mast03", "mast99"])

    assert response.failed
    assert "maintenance" in response.value["mast03"] and "not deployed" in response.value["mast99"]
    assert FakeUnitApi.calls == []


def test_no_units_is_an_error(monkeypatch):
    assert _dance(_controller(monkeypatch), []).failed


def test_stop_dancing_goes_to_every_unit(monkeypatch):
    response = asyncio.run(_controller(monkeypatch).endpoint_units_stop_dancing("ns", ["mast01", "mast02"]))

    assert response.succeeded
    assert [(c[0], c[1]) for c in FakeUnitApi.calls] == [("mast01", "stop_dancing"), ("mast02", "stop_dancing")]


def test_the_dance_names_are_the_units(monkeypatch):
    """control's Literal must list exactly the styles MAST_unit's `dance.STYLES` has."""
    import typing

    assert set(typing.get_args(controller_module.DanceName)) == {"foxtrot", "waltz", "alto"}
