"""`PlanExecution`: one worker per unit, a quorum, and the plan file as the log
(plans/control_acquisition_design.md 5, 8).

A fake `UnitApi` scripted per unit, timings in milliseconds, no network and no sleeps beyond
the polls. A unit's script is a list of statuses read in turn, the last one repeating.
"""

from __future__ import annotations

import platform
import threading
from pathlib import Path

import pytest
import tomlkit

from common.config.site import Site
from common.models.plans import Plan, plan_to_toml
from control.execution import PlanExecution, Timing, UnitRunState, resolve_units, validate_units

ULID = "01JAD8Q4ZK7V3M2XG9T6R5N0HB"
FAST = Timing(poll_seconds=0.005, startup_seconds=0.3, focus_seconds=0.3, unreachable_seconds=0.1, spec_poll_seconds=0.005)


def status(
    opstate="startedup",
    operational=True,
    why_not=None,
    why_busy=None,
    activities=None,
    acquisition="absent",
    guiding=False,
    **extra,
) -> dict:
    s = {
        "opstate": opstate,
        "operational": operational,
        "why_not_operational": why_not or [],
        "why_busy": {} if why_busy is None else why_busy,
        "activities_verbal": activities or [],
        "guiding": guiding,
        **extra,
    }
    if acquisition != "absent":
        s["acquisition"] = acquisition
    return s


def outcome(state, errors=None) -> dict:
    return {"state": state, "errors": errors or [], "started_at": "t"}


READY = status()
RUNNING = status(acquisition=outcome("running"))
LOCKED = status(acquisition=outcome("guiding"), activities=["Guiding"], guiding=True)


class FakeUnit:
    """A unit: `statuses` read in turn (the last repeats), PUTs recorded and answered."""

    def __init__(self, statuses, focus=None, refuse: dict | None = None, abort_fails=False, unreachable=0):
        self.statuses = list(statuses)
        self.focus = list(focus or [{"active": False, "outcome": "solved", "errors": []}])
        self.refuse = refuse or {}
        self.abort_fails = abort_fails
        self.unreachable = unreachable  # this many status reads fail first
        self.puts: list[tuple[str, dict | None]] = []
        self.lock = threading.Lock()

    def _next(self, script):
        with self.lock:
            return script.pop(0) if len(script) > 1 else script[0]

    async def get(self, method, params=None, timeout=None):
        if method == "status" and self.unreachable:
            with self.lock:
                self.unreachable -= 1
            return {"succeeded": False, "errors": ["timeout"]}
        value = self._next(self.statuses if method == "status" else self.focus)
        if value is None:
            return {"succeeded": False, "errors": ["timeout"]}
        return {"succeeded": True, "value": value}

    async def put(self, method, params=None, json=None, timeout=None):
        with self.lock:
            self.puts.append((method, params))
        if method == "abort" and self.abort_fails:
            return {"succeeded": False, "errors": ["abort refused"]}
        if method in self.refuse:
            return {"succeeded": False, "errors": [self.refuse[method]]}
        return {"succeeded": True, "value": "ok"}

    def put_methods(self) -> list[str]:
        return [m for m, _ in self.puts]


@pytest.fixture
def site() -> Site:
    return Site(
        name="ns", project="mast", controller_host="c", spec_host="s", unit_ids="1-20",
        deployed_units=["1", "2", "3", "4", "5"], units_in_maintenance=["4"],
    )  # fmt: skip


@pytest.fixture
def plans(tmp_path) -> Path:
    for folder in ("in-progress", "completed", "failed", "canceled"):
        (tmp_path / "plans" / folder).mkdir(parents=True)
    return tmp_path / "plans"


def make(plans: Path, site: Site, units: dict[str, FakeUnit], spec_api=None, **fields) -> PlanExecution:
    plan = Plan(
        **{
            "ulid": ULID,
            "target": {"ra_hours": "12:30:45", "dec_degrees": "+20:15:00"},
            "requested_units": list(units),
            "quorum": len(units),
            "timeout_to_guiding": 0.3,
            **fields,
        }
    )
    path = plans / "in-progress" / f"PLAN_{ULID}.toml"
    path.write_text(plan_to_toml(plan))
    return PlanExecution(
        Plan.from_toml_file(str(path)),
        path,
        [site],
        site,
        unit_api=lambda name: units[name],
        spec_api=spec_api,
        products_base=str(plans / "products"),
        timing=FAST,
    )


def events(execution: PlanExecution) -> list[tuple[str, list[str]]]:
    doc = tomlkit.parse(execution.toml_path.read_text()).unwrap()
    return [(e["what"], e.get("details", [])) for e in doc.get("events", [])]


def what(execution) -> list[str]:
    return [w for w, _ in events(execution)]


class TestUnitsAndValidation:
    def test_scheduled_units_win(self, site):
        plan = Plan(target={"ra_hours": 1, "dec_degrees": 2}, requested_units=["mast01"], scheduled_units=["mast02"])
        assert resolve_units(plan, [site], site) == ["mast02"]

    def test_requested_specifiers_are_expanded_and_duplicates_dropped(self, site):
        plan = Plan(target={"ra_hours": 1, "dec_degrees": 2}, requested_units=["ns:1,3", "mast01"])
        assert resolve_units(plan, [site], site) == ["mast01", "mast03"]

    def test_any_is_every_deployed_unit_not_in_maintenance(self, site):
        plan = Plan(target={"ra_hours": 1, "dec_degrees": 2}, requested_units="any")
        assert resolve_units(plan, [site], site) == ["mast01", "mast02", "mast03", "mast05"]

    def test_undeployed_and_in_maintenance(self, site):
        assert validate_units(["mast01", "mast09", "mast04"], 1, [site]) == [
            "mast09: not deployed at any managed site",
            "mast04: in maintenance",
        ]

    def test_any_with_fewer_eligible_than_quorum_fails_at_once(self, site, plans):
        execution = make(plans, site, {}, requested_units="any", quorum=5)
        assert execution.run() == "failed"
        assert ("failed", ["quorum 5, only 4 units eligible"]) in events(execution)
        assert execution.toml_path.parent.name == "failed"


class TestOneUnit:
    def test_it_reaches_guiding_and_the_plan_completes(self, site, plans):
        unit = FakeUnit([READY, RUNNING, RUNNING, LOCKED])
        execution = make(plans, site, {"mast01": unit})

        assert execution.run() == "completed"
        assert execution.units["mast01"].state is UnitRunState.GUIDING
        assert "start_acquisition_and_guiding" in unit.put_methods()
        params = dict(unit.puts[unit.put_methods().index("start_acquisition_and_guiding")][1])
        assert params["plan_ulid"] == ULID and params["skip_sky"] == "true"

    def test_the_file_moves_and_its_path_follows(self, site, plans):
        execution = make(plans, site, {"mast01": FakeUnit([READY, LOCKED])})
        execution.run()
        assert execution.toml_path == plans / "completed" / f"PLAN_{ULID}.toml"
        assert not (plans / "in-progress" / f"PLAN_{ULID}.toml").exists()
        assert what(execution) == ["executing", "unit-acquiring", "unit-guiding", "completed"]

    def test_the_products_folder_is_made_recorded_and_given_the_plan(self, site, plans):
        execution = make(plans, site, {"mast01": FakeUnit([READY, LOCKED])})
        execution.run()
        root = Path(execution.products.root)
        assert root.parent == plans / "products" / f"PLAN_{ULID}"
        assert events(execution)[0] == ("executing", [f"products: {root.as_posix()}", "units: mast01", "quorum: 1"])
        assert (root / f"PLAN_{ULID}.toml").exists()

    def test_an_initialized_unit_is_started_up_first(self, site, plans):
        unit = FakeUnit([status(opstate="initialized"), status(opstate="startingup"), READY, LOCKED])
        execution = make(plans, site, {"mast01": unit})
        assert execution.run() == "completed"
        assert unit.put_methods()[0] == "startup"
        assert what(execution)[1] == "unit-starting-up"

    @pytest.mark.parametrize(
        ("script", "error"),
        [
            ([status(opstate=None)], "status: opstate not reported"),
            ([status(opstate="poweringdown")], "startup: opstate is poweringdown"),
            ([status(operational=False, why_not=["mount: not connected"])], "operational: mount: not connected"),
            ([status(operational=False)], "operational: not operational, no reason given"),
            ([status(why_busy={"unit": ["Autofocusing"]})], "busy: unit: Autofocusing"),
            ([{**READY, "why_busy": None}], "status: busy state not reported"),
            ([READY, status()], "status: unit does not report acquisition outcome"),
            ([READY, status(acquisition=outcome("failed", ["sky: no solution"]))], "acquisition: sky: no solution"),
        ],
    )
    def test_it_fails_in_its_own_words(self, site, plans, script, error):
        execution = make(plans, site, {"mast01": FakeUnit(script)})
        assert execution.run() == "failed"
        assert execution.units["mast01"].errors == [error]
        assert ("unit-failed", [f"mast01: {error}"]) in events(execution)

    def test_only_cooling_passes_the_operational_check(self, site, plans):
        cooling = status(operational=False, why_not=["imager: cooling down (-12.0 C, set point -20)"])
        execution = make(plans, site, {"mast01": FakeUnit([cooling, LOCKED])})
        assert execution.run() == "completed"


class TestFocusOnStar:
    def test_solved_goes_on_to_acquire(self, site, plans):
        unit = FakeUnit([READY, LOCKED], focus=[{"active": True}, {"active": False, "outcome": "solved"}])
        execution = make(plans, site, {"mast01": unit}, autofocus=True)
        assert execution.run() == "completed"
        assert unit.put_methods()[:2] == ["focus_on_star", "start_acquisition_and_guiding"]
        assert dict(unit.puts[0][1])["zenith"] == "false"

    def test_not_solved_fails_with_its_errors(self, site, plans):
        unit = FakeUnit([READY], focus=[{"active": False, "outcome": "failed", "errors": ["no isolated star"]}])
        execution = make(plans, site, {"mast01": unit}, autofocus=True)
        assert execution.run() == "failed"
        assert execution.units["mast01"].errors == ["focus_on_star: no isolated star"]


class TestTheStarLock:
    def test_guiding_without_a_lock_keeps_waiting_then_times_out(self, site, plans):
        unlocked = status(acquisition=outcome("guiding"), activities=["Guiding"], guiding=False)
        unit = FakeUnit([READY, unlocked])
        execution = make(plans, site, {"mast01": unit})
        assert execution.run() == "failed"
        assert execution.units["mast01"].errors == ["guiding: no star lock within 0.3 s, unit aborted"]
        assert unit.put_methods()[-1] == "abort"

    def test_a_timeout_before_the_handover_is_the_acquisitions(self, site, plans):
        unit = FakeUnit([READY, RUNNING])
        execution = make(plans, site, {"mast01": unit})
        execution.run()
        assert execution.units["mast01"].errors == ["acquisition: timed out after 0.3 s, unit aborted"]
        assert "abort" in unit.put_methods()

    def test_a_failure_after_the_handover_is_the_guidings(self, site, plans):
        handed = status(acquisition=outcome("running"), activities=["Guiding"])
        failed = status(acquisition=outcome("failed", ["guiding stopped before locking on a star"]))
        execution = make(plans, site, {"mast01": FakeUnit([READY, handed, failed])})
        execution.run()
        assert execution.units["mast01"].errors == ["guiding: guiding stopped before locking on a star"]

    def test_ended_fails(self, site, plans):
        execution = make(plans, site, {"mast01": FakeUnit([READY, status(acquisition=outcome("ended"))])})
        execution.run()
        assert execution.units["mast01"].errors == ["guiding: guiding ended"]


class TestReachability:
    def test_unreachable_for_long_enough_fails(self, site, plans):
        execution = make(plans, site, {"mast01": FakeUnit([None])})
        execution.run()
        assert execution.units["mast01"].errors == ["status: unreachable for 0.1 s"]

    def test_unreachable_for_less_then_answering_does_not(self, site, plans):
        execution = make(plans, site, {"mast01": FakeUnit([READY, LOCKED], unreachable=3)})
        assert execution.run() == "completed"


class TestQuorum:
    def test_two_of_three_succeeds_while_the_third_still_runs(self, site, plans):
        slow = FakeUnit([READY, RUNNING])
        units = {"mast01": FakeUnit([READY, LOCKED]), "mast02": FakeUnit([READY, LOCKED]), "mast03": slow}
        execution = make(plans, site, units, quorum=2)
        assert execution.run() == "completed"
        assert execution.count(UnitRunState.GUIDING) == 2
        assert "abort" not in slow.put_methods(), "a late unit is not aborted at success"
        assert ("completed", ["quorum reached: 2 of 3 guiding (mast01, mast02)"]) in events(execution)

    def test_too_many_failures_fail_at_once_and_abort_the_rest(self, site, plans):
        broken = status(operational=False, why_not=["mount: not connected"])
        slow = FakeUnit([READY, RUNNING])
        units = {"mast01": FakeUnit([broken]), "mast02": FakeUnit([broken]), "mast03": slow}
        execution = make(plans, site, units, quorum=2)
        assert execution.run() == "failed"
        assert "abort" in slow.put_methods()
        assert execution.units["mast03"].state is UnitRunState.ABORTED

    def test_every_transition_is_one_event_and_none_per_poll(self, site, plans):
        units = {name: FakeUnit([READY, RUNNING, RUNNING, RUNNING, LOCKED]) for name in ("mast01", "mast02", "mast03")}
        execution = make(plans, site, units)
        execution.run()
        assert what(execution).count("unit-acquiring") == 3
        assert what(execution).count("unit-guiding") == 3


class TestAbort:
    def test_every_unit_is_aborted_and_failures_recorded(self, site, plans):
        units = {"mast01": FakeUnit([READY, RUNNING]), "mast02": FakeUnit([READY, RUNNING], abort_fails=True)}
        execution = make(plans, site, units)
        execution.timing = Timing(**{**FAST.__dict__, "startup_seconds": 5})
        execution.plan.timeout_to_guiding = 60
        runner = threading.Thread(target=execution.run)
        runner.start()
        while execution.count(UnitRunState.ACQUIRING) < 2:
            threading.Event().wait(0.005)

        failures = execution.abort()
        runner.join(5)

        assert failures == ["mast02: abort failed: ['abort refused']"]
        assert all("abort" in u.put_methods() for u in units.values())
        assert execution.outcome == "aborted"
        assert execution.toml_path.parent.name == "canceled"
        assert ("abort-failures", failures) in events(execution)
        assert {r.state for r in execution.units.values()} == {UnitRunState.ABORTED}


class TestTheSpectrograph:
    def test_no_spectrograph_is_never_called(self, site, plans):
        called = []
        execution = make(plans, site, {"mast01": FakeUnit([READY, LOCKED])}, spec_api=lambda: called.append(1))
        assert execution.run() == "completed"
        assert called == []


@pytest.mark.skipif(platform.system() == "Windows", reason="control runs on Linux")
def test_the_products_folder_links_relatively(site, plans):
    execution = make(plans, site, {"mast01": FakeUnit([READY, LOCKED])})
    execution.run()
    assert Path(execution.products.root).is_dir()


class FakeSpec:
    """The spectrograph: busy for `exposing` status reads after its assignment, then idle."""

    def __init__(self, exposing=3):
        self.exposing = exposing
        self.puts: list[str] = []
        self.started = False

    async def get(self, method, params=None, timeout=None):
        if self.started and self.exposing > 0:
            self.exposing -= 1
            return {"succeeded": True, "value": {"operational": True, "activities": 4}}
        return {"succeeded": True, "value": {"operational": True, "activities": 0}}

    async def put(self, method, params=None, json=None, timeout=None):
        self.puts.append(method)
        self.started = self.started or method == "execute_assignment"
        return {"succeeded": True, "value": "ok"}


ENGAGED_UNLOCKED = status(acquisition=outcome("guiding"), activities=["Guiding"], guiding=False)  # LostLock
STOPPED = status(acquisition=outcome("ended"), activities=[], guiding=False)


class TestTheQuorumHolds:
    """`quorum` is the fewest units to continue: below it, the plan stops (5.5)."""

    @pytest.fixture(autouse=True)
    def initiator(self, monkeypatch):
        from common.models.assignments import Initiator

        monkeypatch.setattr(
            Initiator,
            "local_machine",
            classmethod(lambda cls: cls.model_construct(hostname="ctl", fqdn="ctl.x", ipaddr=None)),
        )

    def spectrograph(self):
        return {"instrument": "highspec", "requested_exposure_duration": 60}

    def test_losing_units_before_guiding_says_so(self, site, plans):
        broken = status(operational=False, why_not=["mount: not connected"])
        units = {"mast01": FakeUnit([broken]), "mast02": FakeUnit([broken]), "mast03": FakeUnit([READY, RUNNING])}
        execution = make(plans, site, units, quorum=2)
        execution.run()
        assert events(execution)[-1][1][0] == "quorum 2 lost: 1 of 3 units left"

    def test_the_exposure_completes_while_the_quorum_holds(self, site, plans):
        spec = FakeSpec()
        units = {"mast01": FakeUnit([READY, LOCKED]), "mast02": FakeUnit([READY, LOCKED])}
        execution = make(plans, site, units, spec_api=lambda: spec, spec=self.spectrograph())
        assert execution.run() == "completed"
        assert spec.puts == ["execute_assignment"]
        assert "exposing" in what(execution)

    def test_a_lost_star_still_being_sought_does_not_drop_a_unit(self, site, plans):
        spec = FakeSpec()
        units = {"mast01": FakeUnit([READY, LOCKED, ENGAGED_UNLOCKED]), "mast02": FakeUnit([READY, LOCKED])}
        execution = make(plans, site, units, spec_api=lambda: spec, spec=self.spectrograph())
        assert execution.run() == "completed"

    def test_a_unit_that_stops_guiding_below_the_quorum_aborts_the_plan(self, site, plans):
        spec = FakeSpec(exposing=100)
        dropping = FakeUnit([READY, LOCKED, LOCKED, STOPPED])
        steady = FakeUnit([READY, LOCKED])
        execution = make(
            plans, site, {"mast01": dropping, "mast02": steady}, spec_api=lambda: spec, spec=self.spectrograph()
        )

        assert execution.run() == "failed"
        assert spec.puts == ["execute_assignment", "abort"], "the exposure is aborted with the plan"
        assert "abort" in steady.put_methods(), "the units still guiding are stopped too"
        assert ("unit-stopped-guiding", ["mast01"]) in events(execution)
        assert execution.units["mast01"].errors == ["guiding: stopped during the exposure"]
        assert events(execution)[-1][1][0] == "quorum 2 lost during the exposure: 1 of 2 guiding"

    def test_above_the_quorum_one_unit_may_drop(self, site, plans):
        # mast01's status is read by the exposure loop alone once it guides: it drops only
        # after 20 polls, by when mast03, which may join after the quorum, guides too.
        spec = FakeSpec(exposing=40)
        units = {
            "mast01": FakeUnit([READY, LOCKED, *[LOCKED] * 20, STOPPED]),
            "mast02": FakeUnit([READY, LOCKED]),
            "mast03": FakeUnit([READY, LOCKED]),
        }
        execution = make(plans, site, units, quorum=2, spec_api=lambda: spec, spec=self.spectrograph())
        assert execution.run() == "completed"
        assert spec.puts == ["execute_assignment"]
        assert execution.units["mast01"].state is UnitRunState.FAILED
