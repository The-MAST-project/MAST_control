"""The acquisition endpoints and `submit_plan` (plans/control_acquisition_design.md 7, 8).

No network: the name resolver is stubbed, and so are the units and the controller.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import tomlkit

import common.object_resolver as resolver_module
import control.planning as planning_module
from common.config.site import Site
from common.object_resolver import ObjectNameError, ResolvedObject
from control.acquisition import Acquisitions
from control.execution import UnitRun, UnitRunState
from control.planning import Planner

M101 = ResolvedObject(name="M101", ra_j2000_hours=14.0535, dec_j2000_degs=54.34875, resolver="sesame")


@pytest.fixture(autouse=True)
def resolver(monkeypatch):
    def resolve(name, total_timeout):
        if name == "M101":
            return M101
        raise ObjectNameError(f"'{name}' is unknown")

    monkeypatch.setattr(resolver_module, "resolve_object_name", resolve)


@pytest.fixture
def site() -> Site:
    return Site(
        name="ns", project="mast", controller_host="c", spec_host="s", unit_ids="1-20", deployed_units=["1", "2", "3"]
    )


class FakeUnit:
    def __init__(self, status):
        self.status = status

    async def get(self, method, params=None, timeout=None):
        if self.status is None:
            return {"succeeded": False, "errors": ["timeout"]}
        return {"succeeded": True, "value": self.status}


class FakeExecution:
    def __init__(self, spec=None):
        self.plan = SimpleNamespace(ulid="01OLD", spec=spec)
        self.aborted = False

    def abort(self):
        self.aborted = True
        return []

    def wait(self, timeout=None):
        return True


@pytest.fixture
def controller(tmp_path, site, monkeypatch):
    started = []
    in_progress = tmp_path / "in-progress"
    in_progress.mkdir()
    return SimpleNamespace(
        in_progress=None,
        last_execution=None,
        managed_sites=lambda: [site],
        planner=SimpleNamespace(in_progress_folder=SimpleNamespace(folder_path=in_progress, refresh=lambda: None)),
        start_execution=lambda plan, path: started.append((plan, Path(path))) or SimpleNamespace(value=plan.ulid),
        started=started,
        held={},
        units_held_by_other_plans=lambda plan: [],
    )


def acquisitions(controller, statuses: dict | None = None) -> Acquisitions:
    statuses = statuses or {}
    return Acquisitions(controller, unit_api=lambda name: FakeUnit(statuses.get(name, {"activities_verbal": []})))


class TestGenerate:
    def test_a_plan_with_no_spectrograph(self, controller):
        plan = acquisitions(controller).generate(["mast01", "mast03"], "12:30:45", "+20:15:00", None, True, None)
        assert plan.requested_units == ["mast01", "mast03"] and plan.quorum == 2
        assert plan.autofocus and plan.approved and not plan.production and plan.owner == "mast"
        assert plan.spec is None
        assert (plan.target.ra_hours, plan.target.dec_degrees) == (pytest.approx(12.5125), pytest.approx(20.25))
        assert [e.what for e in plan.events] == ["generated"]
        assert plan.events[0].details == ["start_acquisition", "ra=12:30:45", "dec=+20:15:00", "object=None"]

    def test_any_needs_a_quorum(self, controller):
        assert acquisitions(controller).generate(["any"], "1", "2", None, False, None)[0].startswith("quorum is required")
        assert acquisitions(controller).generate(["any"], "1", "2", None, False, 2).requested_units == ["any"]

    def test_a_name_is_resolved_and_recorded(self, controller):
        plan = acquisitions(controller).generate(["mast01"], None, None, "M101", False, None)
        assert (plan.target.ra_hours, plan.target.name) == (14.0535, "M101")
        assert [e.what for e in plan.events] == ["generated", "resolved"]

    def test_a_name_that_does_not_resolve_refuses(self, controller):
        assert acquisitions(controller).generate(["mast01"], None, None, "M1O1", False, None) == [
            "could not resolve 'M1O1' -- 'M1O1' is unknown"
        ]

    def test_neither_coordinates_nor_a_name_refuses(self, controller):
        assert acquisitions(controller).generate(["mast01"], None, None, None, False, None) == [
            "a target needs both coordinates, or a name to resolve"
        ]


class TestStart:
    def _plan(self, controller):
        return acquisitions(controller).generate(["mast01"], "12:30:45", "+20:15:00", None, False, None)

    def test_written_into_in_progress_loaded_back_and_executed(self, controller):
        plan = self._plan(controller)
        response = acquisitions(controller).start(plan)

        assert response.value == plan.ulid
        ((started, path),) = controller.started
        assert path == controller.planner.in_progress_folder.folder_path / f"PLAN_{plan.ulid}.toml"
        assert tomlkit.parse(path.read_text())["requested_units"] == ["mast01"]
        assert started.full_path == path

    def test_a_workload_in_progress_refuses_nothing_is_aborted(self, controller):
        controller.in_progress = previous = FakeExecution()
        response = acquisitions(controller).start(self._plan(controller))
        assert response.errors == ["plan 01OLD is in progress; abort it first"]
        assert not previous.aborted and controller.started == []

    def test_units_working_for_another_plan_refuse_before_anything_is_written(self, controller):
        controller.units_held_by_other_plans = lambda plan: ["mast01: working on plan 01OLD; abort it first"]
        response = acquisitions(controller).start(self._plan(controller))
        assert response.errors == ["mast01: working on plan 01OLD; abort it first"]
        assert controller.started == []
        assert not list(controller.planner.in_progress_folder.folder_path.iterdir()), "no plan file left behind"


class TestTheTable:
    def test_no_acquisition(self, controller):
        assert acquisitions(controller).table() == "no acquisition\n"

    def test_columns_and_cells(self, controller):
        execution = SimpleNamespace(
            plan=SimpleNamespace(ulid="01NEW"),
            outcome="completed",
            quorum=2,
            errors=[],
            units={
                "mast01": UnitRun("mast01", UnitRunState.GUIDING),
                "mast03": UnitRun("mast03", UnitRunState.FOCUSING),
                "mast05": UnitRun("mast05", UnitRunState.FAILED, ["operational: mount: not connected"]),
                "mast07": UnitRun("mast07", UnitRunState.ACQUIRING),
            },
            count=lambda state: 1 if state is UnitRunState.GUIDING else 0,
        )
        controller.last_execution = execution
        table = acquisitions(
            controller,
            {
                "mast01": {"opstate": "startedup", "operational": True, "why_busy": {"unit": ["Guiding"]}, "guiding": True},
                "mast03": {
                    "opstate": "startedup",
                    "operational": False,
                    "why_not_operational": ["imager: cooling down"],
                    "why_busy": {},
                    "activities_verbal": ["FocusingOnStar"],
                },
                "mast05": {"opstate": "startedup", "operational": False, "why_busy": None},
                "mast07": None,
            },
        ).table()

        lines = table.splitlines()
        assert lines[0] == "plan 01NEW  completed (quorum 1 of 2)"
        assert lines[2].split() == ["unit", "state", "opstate", "operational", "autofocusing", "why_busy", "guiding"]
        rows = {line.split()[0]: line for line in lines[4:8]}
        assert "unit: Guiding" in rows["mast01"] and rows["mast01"].rstrip().endswith("True")
        assert "True (cooling)" in rows["mast03"] and " True " in rows["mast03"].replace("True (cooling)", "")
        assert " - " in rows["mast03"] and " ? " in rows["mast05"]
        assert rows["mast07"].count("unreachable") == 5
        assert lines[-1] == "  mast05: operational: mount: not connected"


class TestSubmitPlan:
    @pytest.fixture
    def planner(self, tmp_path, monkeypatch):
        monkeypatch.setattr(planning_module, "_do_scrape", lambda: None)
        planner = object.__new__(Planner)
        planner.submitted_folder = SimpleNamespace(folder_path=tmp_path, refresh=lambda: None)
        return planner

    def _stored(self, tmp_path, ulid="01JAD8Q4ZK7V3M2XG9T6R5N0HB") -> dict:
        return tomlkit.parse((tmp_path / f"PLAN_{ulid}.toml").read_text()).unwrap()

    def test_a_plan_without_a_spectrograph_is_accepted(self, planner, tmp_path):
        body = {
            "ulid": "01JAD8Q4ZK7V3M2XG9T6R5N0HB",
            "requested_units": ["mast01"],
            "target": {"ra_hours": "1", "dec_degrees": "2"},
        }
        assert planner.submit_plan(body).succeeded
        assert self._stored(tmp_path)["requested_units"] == ["mast01"]

    def test_a_name_alone_is_stored_with_its_coordinates(self, planner, tmp_path):
        body = {"ulid": "01JAD8Q4ZK7V3M2XG9T6R5N0HB", "requested_units": "any", "target": {"name": "M101"}}
        assert planner.submit_plan(body).succeeded
        stored = self._stored(tmp_path)
        assert stored["target"]["ra_hours"] == 14.0535
        assert [e["what"] for e in stored["events"]] == ["submitted", "resolved"]

    def test_a_name_that_does_not_resolve_is_refused(self, planner, tmp_path):
        body = {"ulid": "01JAD8Q4ZK7V3M2XG9T6R5N0HB", "requested_units": "any", "target": {"name": "nothing"}}
        response = planner.submit_plan(body)
        assert response.failed and "could not resolve" in response.errors[0]
        assert not list(tmp_path.iterdir())
