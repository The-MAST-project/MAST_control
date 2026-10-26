"""Control's single `abort`, and the check that no unit works for another plan (6.6)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fakes import bare_controller

import control.controller as controller_module
from common.config.site import Site
from common.models.plans import Plan

SITE = Site(name="ns", project="mast", controller_host="c", spec_host="s", unit_ids="1-20", deployed_units=["1", "2", "3"])


@pytest.fixture
def controller(monkeypatch):
    controller = bare_controller(monkeypatch, sites=[SITE])
    controller.in_progress = None
    monkeypatch.setattr(controller_module, "Config", lambda: SimpleNamespace(local_site=SITE))
    return controller


class TestAbort:
    def test_nothing_in_progress(self, controller):
        assert asyncio.run(controller.endpoint_abort()).errors == ["nothing in progress"]

    def test_the_workload_in_progress_is_aborted(self, controller):
        aborted = []
        controller.in_progress = SimpleNamespace(plan=SimpleNamespace(ulid="01NOW"), abort=lambda: aborted.append(1) or [])
        assert asyncio.run(controller.endpoint_abort()).value == "01NOW"
        assert aborted == [1]

    def test_failed_participant_aborts_are_reported(self, controller):
        controller.in_progress = SimpleNamespace(plan=SimpleNamespace(ulid="01NOW"), abort=lambda: ["mast02: abort failed"])
        assert asyncio.run(controller.endpoint_abort()).errors == ["mast02: abort failed"]


class TestUnitsHeldByOtherPlans:
    def _plan(self, units):
        return Plan(ulid="01NEW", target={"ra_hours": 1, "dec_degrees": 2}, requested_units=units)

    def test_each_unit_working_for_another_plan_is_named(self, controller, monkeypatch):
        owners = {"mast01": "01OLD", "mast02": None, "mast03": "01NEW"}

        class Api:
            def __init__(self, hostname):
                self.name = hostname

            async def get(self, method, timeout=None):
                return {"succeeded": True, "value": {"plan_id": owners[self.name]}}

        monkeypatch.setattr(controller_module, "UnitApi", Api)
        problems = controller.units_held_by_other_plans(self._plan(["mast01", "mast02", "mast03"]))
        assert problems == ["mast01: working on plan 01OLD; abort it first"]

    def test_an_unreachable_unit_is_not_this_checks_to_report(self, controller, monkeypatch):
        class Api:
            def __init__(self, hostname):
                pass

            async def get(self, method, timeout=None):
                raise OSError("unreachable")

        monkeypatch.setattr(controller_module, "UnitApi", Api)
        assert controller.units_held_by_other_plans(self._plan(["mast01"])) == []
