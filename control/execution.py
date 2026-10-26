"""Executing a plan: one worker thread per unit, and a quorum (plans/control_acquisition_design.md 5).

Replaces the stepped `probe -> dispatch -> wait_for_guiding` phases that lived on `Plan`. Each
unit is taken, by its own worker, through start-up, the operational and busy checks, an
optional `focus_on_star`, and `start_acquisition_and_guiding`, until it is guiding -- locked on
a star -- or fails in its own words. The plan succeeds when `quorum` units are guiding, and
fails as soon as too many have failed for that to be possible. A plan with a spectrograph then
exposes it (5.7).

Every unit transition is an event in the plan's file, so the file says which units made the
quorum and why the others did not. Products are linked into the execution's products folder
by the controller, from the producers' notifications (5.10).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from common.api import UnitApi
from common.mast_logging import get_logger
from common.models.events import EventModel
from common.models.plans import Plan, PlanProducts
from common.models.statuses import COOLING_DOWN_REASON, AcquisitionState

logger = get_logger(__name__)

Outcome = Literal["running", "completed", "failed", "aborted"]

#: The plan folder each outcome moves the file to (5.8).
OUTCOME_FOLDER: dict[str, str] = {"completed": "completed", "failed": "failed", "aborted": "canceled"}


class UnitRunState(StrEnum):
    PENDING = "pending"
    STARTING_UP = "starting-up"
    FOCUSING = "focusing"
    ACQUIRING = "acquiring"
    GUIDING = "guiding"
    FAILED = "failed"
    ABORTED = "aborted"


TERMINAL = (UnitRunState.GUIDING, UnitRunState.FAILED, UnitRunState.ABORTED)

#: The event written when a unit enters a state (5.9). One per change, never per poll.
STATE_EVENT = {
    UnitRunState.STARTING_UP: "unit-starting-up",
    UnitRunState.FOCUSING: "unit-focusing",
    UnitRunState.ACQUIRING: "unit-acquiring",
    UnitRunState.GUIDING: "unit-guiding",
    UnitRunState.FAILED: "unit-failed",
    UnitRunState.ABORTED: "unit-aborted",
}


@dataclass
class UnitRun:
    name: str
    state: UnitRunState = UnitRunState.PENDING
    errors: list[str] = field(default_factory=list)


@dataclass
class Timing:
    """Every wait, injectable, so a test runs in milliseconds and never sleeps for real (8)."""

    poll_seconds: float = 3.0
    startup_seconds: float = 600.0
    focus_seconds: float = 600.0
    unreachable_seconds: float = 30.0
    spec_poll_seconds: float = 20.0
    #: The spectrograph's deadline: its exposure series, plus this, for overheads and readout.
    spec_margin_seconds: float = 600.0


class _UnitFailedError(Exception):
    """A unit's worker stops: ``step: message``, as the unit said it."""

    def __init__(self, step: str, message: str):
        super().__init__(f"{step}: {message}")


class _StoppedError(Exception):
    """The worker was told to stop: the plan ended, or was aborted."""


def _get(value: Any, name: str, default: Any = None) -> Any:
    """A field of a status, whether it arrived as a dict or as an attribute-access object."""
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def deployed_not_in_maintenance(sites) -> list[str]:
    names: list[str] = []
    for site in sites:
        for unit in site.deployed_units:
            if unit not in site.units_in_maintenance and unit not in names:
                names.append(unit)
    return names


def resolve_units(plan: Plan, sites, local_site) -> list[str]:
    """The unit names a plan runs on (3.1): the scheduler's choice, else what it requested,
    expanded; `"any"` is every deployed unit not in maintenance, whatever the instrument.
    Duplicates dropped. Raises ValueError for a specifier that names no unit."""
    if plan.scheduled_units:
        return list(dict.fromkeys(plan.scheduled_units))
    names = plan.requested_unit_names(sites, local_site)
    if names is not None:
        return list(dict.fromkeys(names))
    return deployed_not_in_maintenance(sites)


def validate_units(names: list[str], quorum: int, sites) -> list[str]:
    """Why the whole plan cannot run on ``names`` (5.6), or []. On expanded names only."""
    if not names:
        return ["no units: none deployed outside maintenance"]
    problems = []
    deployed = {u for s in sites for u in s.deployed_units}
    in_maintenance = {u for s in sites for u in s.units_in_maintenance}
    for name in names:
        if name not in deployed:
            problems.append(f"{name}: not deployed at any managed site")
        elif name in in_maintenance:
            problems.append(f"{name}: in maintenance")
    if not problems and len(names) < quorum:
        problems.append(f"quorum {quorum}, only {len(names)} units eligible")
    return problems


class PlanExecution:
    """One run of one plan. `run()` blocks until the plan terminates; `abort()` from any thread."""

    def __init__(
        self,
        plan: Plan,
        toml_path: str | os.PathLike,
        sites: list,
        local_site,
        *,
        unit_api: Callable[[str], Any] = lambda name: UnitApi(hostname=name),
        spec_api: Callable[[], Any] | None = None,
        products_base: str | None = None,
        timing: Timing | None = None,
        on_terminated: Callable[[PlanExecution], None] | None = None,
    ):
        self.plan = plan
        self.toml_path = Path(toml_path)
        self.sites = sites
        self.local_site = local_site
        self._unit_api = unit_api
        self._spec_api = spec_api
        self._products_base = products_base
        self.timing = timing or Timing()
        self._on_terminated = on_terminated

        self.file_lock = threading.Lock()
        self._changed = threading.Condition()
        self.cancelled = threading.Event()  # an operator's abort
        self._abort_done = threading.Event()  # ... and every participant told
        self._stop = threading.Event()  # every worker: the plan is over, one way or another
        self.units: dict[str, UnitRun] = {}
        self.outcome: Outcome = "running"
        self.errors: list[str] = []
        self.products: PlanProducts | None = None
        self.started_at = time.time()
        self.spec_started = False
        self.quorum_reached = False
        #: Every unit that reached guiding, in order: the plan's end aborts them all (6.6).
        self.guided: list[str] = []
        #: Set by the controller when the spectrograph says it is done with the plan (6.6).
        self.spec_finished = threading.Event()
        self.spec_errors: list[str] = []
        #: Why the plan stopped for want of units, when it did (5.5).
        self.quorum_lost: str | None = None
        self._terminated = threading.Event()

    # --- the plan's file ------------------------------------------------------------------

    def event(self, what: str, details: list[str] | None = None) -> None:
        with self.file_lock:
            try:
                self.plan.add_event(EventModel(what=what, details=details or []), toml_path=self.toml_path)
            except OSError as e:
                logger.error(f"plan {self.plan.ulid}: could not write event '{what}' to '{self.toml_path}': {e}")

    # --- running --------------------------------------------------------------------------

    @property
    def quorum(self) -> int:
        return self.plan.quorum

    def count(self, state: UnitRunState) -> int:
        return sum(1 for run in self.units.values() if run.state is state)

    def run(self) -> Outcome:
        try:
            names = resolve_units(self.plan, self.sites, self.local_site)
        except ValueError as e:
            return self._terminate("failed", [f"units: {e}"])
        problems = validate_units(names, self.quorum, self.sites)
        if problems:
            return self._terminate("failed", problems)

        self.units = {name: UnitRun(name) for name in names}
        try:
            self.products = PlanProducts.start(str(self.plan.ulid), base=self._products_base)
            where = self.products.root
        except OSError as e:
            where = f"not created ({e})"
            logger.error(f"plan {self.plan.ulid}: products folder {where}")
        self.event("executing", [f"products: {where}", f"units: {', '.join(names)}", f"quorum: {self.quorum}"])

        with ThreadPoolExecutor(max_workers=max(1, min(20, len(names))), thread_name_prefix="plan-unit") as workers:
            for name in names:
                workers.submit(self._worker, self.units[name])
            verdict = self._await_verdict()
            if verdict == "completed":
                self.quorum_reached = True
                self.event(
                    "quorum-reached", [", ".join(r.name for r in self.units.values() if r.state is UnitRunState.GUIDING)]
                )
                # The plan goes on: a science plan until the spectrograph has made its
                # products, an acquisition until it is aborted -- its units keep working for
                # it -- and either one fails as soon as fewer than `quorum` units remain (6.6).
                held = self._spectrograph_phase() if self._has_spectrograph() else self._hold()
                if held is not None:
                    verdict = held
                elif self.cancelled.is_set():
                    # Aborted: a science plan cut short, an acquisition ended. An acquisition
                    # that reached its quorum did what it was for.
                    verdict = "aborted" if self._has_spectrograph() else "completed"
            if verdict == "failed" or (verdict == "completed" and not self.cancelled.is_set()):
                # The plan is over, and control ends it everywhere: every unit, guiding ones
                # included -- they were working for it -- and the spectrograph if it was
                # still at it. A science plan's spectrograph has already ended its own part.
                self._end_everywhere("plan failed" if verdict == "failed" else "plan completed")
            # Aborted: the plan ends once every participant has been told, not before.
            if self.cancelled.is_set():
                self._abort_done.wait(60)
            # Workers still running until now (5.5): the plan ends here.
            self._stop.set()
            with self._changed:
                self._changed.notify_all()
        return self._terminate(verdict, self._summary())

    def _await_verdict(self) -> Outcome:
        n = len(self.units)
        with self._changed:
            while True:
                if self.cancelled.is_set():
                    return "aborted"
                if self.count(UnitRunState.GUIDING) >= self.quorum:
                    return "completed"
                failed = self.count(UnitRunState.FAILED) + self.count(UnitRunState.ABORTED)
                if failed > n - self.quorum:
                    # Fewer units left in the plan than it needs to continue (5.5).
                    self.quorum_lost = f"quorum {self.quorum} lost: {n - failed} of {n} units left"
                    return "failed"
                self._changed.wait(timeout=self.timing.poll_seconds)

    def _has_spectrograph(self) -> bool:
        spec = self.plan.spec
        return spec is not None and spec.instrument is not None and self._spec_api is not None

    def _summary(self) -> list[str]:
        guiding = list(self.guided)
        failed = [r.name for r in self.units.values() if r.state is UnitRunState.FAILED]
        verb = "quorum reached" if self.quorum_reached else "quorum not reached"
        details = [self.quorum_lost] if self.quorum_lost else []
        details.append(f"{verb}: {len(guiding)} of {len(self.units)} guiding ({', '.join(guiding) or 'none'})")
        if failed:
            details.append(f"failed: {', '.join(failed)}")
        return details

    def _terminate(self, outcome: Outcome, details: list[str]) -> Outcome:
        self.outcome = outcome
        if outcome == "failed" and not self.units:
            self.errors.extend(details)
        self.event(outcome, details)
        folder = OUTCOME_FOLDER.get(outcome)
        if folder is not None:
            destination = self.toml_path.parent.parent / folder / self.toml_path.name
            try:
                with self.file_lock:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(self.toml_path), str(destination))
                    self.toml_path = destination
            except OSError as e:
                logger.error(f"plan {self.plan.ulid}: could not move '{self.toml_path}' to '{destination}': {e}")
        if self.products is not None:
            try:
                shutil.copy2(self.toml_path, Path(self.products.root) / self.toml_path.name)
            except OSError as e:
                logger.warning(f"plan {self.plan.ulid}: could not copy the plan into its products folder: {e}")
        logger.info(f"plan {self.plan.ulid}: {outcome}: {'; '.join(details)}")
        self._terminated.set()
        if self._on_terminated is not None:
            self._on_terminated(self)
        return outcome

    def wait(self, timeout: float | None = None) -> bool:
        return self._terminated.wait(timeout)

    # --- abort ----------------------------------------------------------------------------

    def abort(self) -> list[str]:
        """Abort the execution (5.8): every unit and, once exposing, the spectrograph, each
        awaited. Returns the abort calls that failed, which are also recorded as events."""
        self.cancelled.set()
        self._stop.set()
        with self._changed:
            self._changed.notify_all()
        failures = self._abort_units(list(self.units.values()), "aborted by operator", every=True)
        if self.spec_started and not self.spec_finished.is_set() and self._spec_api is not None:
            response = self._call(self._spec_api().put("abort"))
            if not _get(response, "succeeded", False):
                failures.append(f"spec: abort failed: {_get(response, 'errors')}")
        if failures:
            self.event("abort-failures", failures)
        self._abort_done.set()
        return failures

    def _abort_units(self, runs: list[UnitRun], reason: str, every: bool = False) -> list[str]:
        """`PUT unit/abort` on ``runs`` in parallel, awaited; those not already terminal are
        marked ABORTED with ``reason``. With ``every``, guiding units are aborted too."""
        targets = [r for r in runs if every or r.state not in TERMINAL]
        failures: list[str] = []

        def abort_one(run: UnitRun) -> None:
            response = self._call(self._unit_api(run.name).put("abort"))
            if not _get(response, "succeeded", False):
                failures.append(f"{run.name}: abort failed: {_get(response, 'errors')}")

        if targets:
            with ThreadPoolExecutor(max_workers=len(targets), thread_name_prefix="plan-abort") as pool:
                list(pool.map(abort_one, targets))
        for run in targets:
            if run.state is not UnitRunState.FAILED:
                self._set(run, UnitRunState.ABORTED, [reason])
        return failures

    # --- one unit -------------------------------------------------------------------------

    def _set(self, run: UnitRun, state: UnitRunState, errors: list[str] | None = None) -> None:
        with self._changed:
            if run.state is state or run.state in (UnitRunState.FAILED, UnitRunState.ABORTED):
                return
            run.state = state
            if errors:
                run.errors.extend(errors)
            if state is UnitRunState.GUIDING:
                self.guided.append(run.name)
            self._changed.notify_all()
        if state is UnitRunState.FAILED:
            self.event(STATE_EVENT[state], [f"{run.name}: {e}" for e in run.errors] or [run.name])
        elif state is UnitRunState.ABORTED:
            self.event(STATE_EVENT[state], [f"{run.name}: {'; '.join(errors or [])}"])
        else:
            self.event(STATE_EVENT[state], [run.name])

    @staticmethod
    def _call(coroutine):
        """A unit API call from a worker thread, as control's status cache makes them."""
        try:
            return asyncio.run(coroutine)
        except Exception as e:  # noqa: BLE001 -- a remote call's failure is a value to the worker, never a crash
            return {"succeeded": False, "errors": [str(e)]}

    def _pause(self, seconds: float) -> None:
        if self._stop.wait(seconds):
            raise _StoppedError

    def _worker(self, run: UnitRun) -> None:
        try:
            self._take_unit_to_guiding(run)
        except _StoppedError:
            pass
        except _UnitFailedError as e:
            self._set(run, UnitRunState.FAILED, [str(e)])
        except Exception as e:
            logger.exception(f"plan {self.plan.ulid}: {run.name}: worker failed")
            self._set(run, UnitRunState.FAILED, [f"status: {e}"])

    def _status(self, run: UnitRun, step: str, method: str = "status") -> Any:
        """The unit's status (or `focus_on_star_status`), retried while unreachable; fails
        "status: unreachable for 30 s" once it has been for that long in a row (5.4)."""
        first_failure: float | None = None
        while True:
            response = self._call(self._unit_api(run.name).get(method))
            if _get(response, "succeeded", False):
                return _get(response, "value")
            now = time.monotonic()
            first_failure = now if first_failure is None else first_failure
            if now - first_failure >= self.timing.unreachable_seconds:
                raise _UnitFailedError("status", f"unreachable for {self.timing.unreachable_seconds:g} s")
            self._pause(self.timing.poll_seconds)

    def _put(self, run: UnitRun, step: str, method: str, params: dict | None = None) -> None:
        response = self._call(self._unit_api(run.name).put(method, params=params))
        if not _get(response, "succeeded", False):
            raise _UnitFailedError(step, "; ".join(_get(response, "errors") or [f"{method} refused, no reason given"]))

    def _timed_out(self, run: UnitRun, step: str, seconds: float) -> _UnitFailedError:
        self._call(self._unit_api(run.name).put("abort"))
        return _UnitFailedError(step, f"timed out after {seconds:g} s, unit aborted")

    def _take_unit_to_guiding(self, run: UnitRun) -> None:
        """5.4, steps 1-5; any step that fails raises, with its prefix."""
        self._ready(run)
        ra, dec = float(self.plan.target.ra_hours), float(self.plan.target.dec_degrees)
        coordinates = {"ra_j2000_hours": f"{ra:.6f}", "dec_j2000_degs": f"{dec:+.6f}"}
        if self.plan.autofocus:
            self._focus_on_star(run, coordinates)
        self._acquire(run, coordinates)

    def _ready(self, run: UnitRun) -> None:
        """Steps 1-3: started up, operational (or only cooling), and not busy."""
        status = self._status(run, "startup")

        # 1. Opstate
        opstate = _get(status, "opstate")
        if opstate is None:
            raise _UnitFailedError("status", "opstate not reported")
        if opstate in ("initialized", "shutdown"):
            self._set(run, UnitRunState.STARTING_UP)
            self._put(run, "startup", "startup")
            deadline = time.monotonic() + self.timing.startup_seconds
            while _get(status, "opstate") != "startedup":
                if time.monotonic() >= deadline:
                    raise self._timed_out(run, "startup", self.timing.startup_seconds)
                self._pause(self.timing.poll_seconds)
                status = self._status(run, "startup")
        elif opstate != "startedup":
            raise _UnitFailedError("startup", f"opstate is {opstate}")

        # 2. Operational, or only cooling
        if not _get(status, "operational", False):
            reasons = _get(status, "why_not_operational") or []
            if not reasons:
                raise _UnitFailedError("operational", "not operational, no reason given")
            if not all(str(r).startswith(COOLING_DOWN_REASON) for r in reasons):
                raise _UnitFailedError("operational", "; ".join(str(r) for r in reasons))

        # 3. Busy -- working for this plan already is not busy: a revived plan runs again
        # under the same token, and its units may still hold it (6.6).
        why_busy = _get(status, "why_busy")
        if why_busy is None:
            raise _UnitFailedError("status", "busy state not reported")
        owner = _get(status, "plan_id")
        if owner is not None and owner != self.plan.ulid:
            raise _UnitFailedError("busy", f"working on plan {owner}")
        if owner == self.plan.ulid:
            why_busy = {k: [f for f in v if f != "ExecutingPlan"] for k, v in why_busy.items()}
            why_busy = {k: v for k, v in why_busy.items() if v}
        if why_busy:
            raise _UnitFailedError("busy", "; ".join(f"{owner}: {', '.join(flags)}" for owner, flags in why_busy.items()))

    def _focus_on_star(self, run: UnitRun, coordinates: dict) -> None:
        """Step 4."""
        self._set(run, UnitRunState.FOCUSING)
        self._put(run, "focus_on_star", "focus_on_star", {"zenith": "false", **coordinates, "plan_id": self.plan.ulid})
        deadline = time.monotonic() + self.timing.focus_seconds
        while True:
            focus = self._status(run, "focus_on_star", "focus_on_star_status")
            if not _get(focus, "active", False):
                break
            if time.monotonic() >= deadline:
                raise self._timed_out(run, "focus_on_star", self.timing.focus_seconds)
            self._pause(self.timing.poll_seconds)
        if _get(focus, "outcome") != "solved":
            errors = _get(focus, "errors") or []
            raise _UnitFailedError("focus_on_star", "; ".join(errors) or str(_get(focus, "outcome")))

    def _acquire(self, run: UnitRun, coordinates: dict) -> None:
        """Step 5: acquire, and wait for the star lock."""
        self._set(run, UnitRunState.ACQUIRING)
        self._put(
            run,
            "acquisition",
            "start_acquisition_and_guiding",
            {**coordinates, "skip_sky": "true", "handover_automatically_to_guider": "true", "plan_id": self.plan.ulid},
        )
        timeout = float(self.plan.timeout_to_guiding or 600)
        deadline = time.monotonic() + timeout
        handed_over = False
        while True:
            status = self._status(run, "acquisition")
            handed_over = handed_over or "Guiding" in (_get(status, "activities_verbal") or [])
            step = "guiding" if handed_over else "acquisition"
            outcome = _get(status, "acquisition")
            if outcome is None:
                raise _UnitFailedError("status", "unit does not report acquisition outcome")
            state = _get(outcome, "state")
            errors = list(_get(outcome, "errors") or [])
            if state == AcquisitionState.GUIDING and handed_over and _get(status, "guiding", False):
                self._set(run, UnitRunState.GUIDING)
                return
            if state in (AcquisitionState.FAILED, AcquisitionState.ABORTED):
                raise _UnitFailedError(step, "; ".join(errors) or str(state))
            if state == AcquisitionState.ENDED:
                raise _UnitFailedError("guiding", "; ".join(["guiding ended", *errors]))
            if time.monotonic() >= deadline:
                if handed_over:
                    self._call(self._unit_api(run.name).put("abort"))
                    raise _UnitFailedError("guiding", f"no star lock within {timeout:g} s, unit aborted")
                raise self._timed_out(run, "acquisition", timeout)
            self._pause(self.timing.poll_seconds)

    # --- the spectrograph (5.7) -----------------------------------------------------------

    def spectrograph_finished(self, errors: list[str] | None = None) -> None:
        """The spectrograph says it is done with this plan: its products are made, or it
        failed. Idempotent: a notification sent again changes nothing (6.6)."""
        if not self.spec_finished.is_set():
            self.spec_errors = list(errors or [])
            self.spec_finished.set()

    def _end_everywhere(self, reason: str) -> None:
        """Abort every unit of the plan, and the spectrograph if it has not ended its own part."""
        failures = self._abort_units(list(self.units.values()), reason, every=True)
        if self.spec_started and not self.spec_finished.is_set() and self._spec_api is not None:
            response = self._call(self._spec_api().put("abort"))
            if not _get(response, "succeeded", False):
                failures.append(f"spec: abort failed: {_get(response, 'errors')}")
        if failures:
            self.event("abort-failures", failures)

    def _quorum_holds(self, unreachable_since: dict[str, float]) -> bool:
        """Drop the guiding units that stopped -- guider no longer engaged (a lost star still
        being sought does not count), or unreachable for `unreachable_seconds` -- and say
        whether `quorum` units are still guiding (5.5)."""
        for name, run in self.units.items():
            if run.state is not UnitRunState.GUIDING:
                continue
            response = self._call(self._unit_api(name).get("status"))
            if _get(response, "succeeded", False):
                unreachable_since.pop(name, None)
                if "Guiding" in (_get(_get(response, "value"), "activities_verbal") or []):
                    continue
                why = "guiding: stopped"
            else:
                first = unreachable_since.setdefault(name, time.monotonic())
                if time.monotonic() - first < self.timing.unreachable_seconds:
                    continue
                why = f"status: unreachable for {self.timing.unreachable_seconds:g} s"
            self.event("unit-stopped-guiding", [name])
            self._set(run, UnitRunState.FAILED, [why])
        guiding = self.count(UnitRunState.GUIDING)
        if guiding >= self.quorum:
            return True
        self.quorum_lost = f"quorum {self.quorum} lost: {guiding} of {len(self.units)} guiding"
        return False

    def _hold(self) -> Outcome | None:
        """An acquisition, its quorum reached: its units keep working for it until it is
        aborted. Returns "failed" if fewer than `quorum` remain first, None when aborted."""
        unreachable_since: dict[str, float] = {}
        while not self.cancelled.wait(self.timing.poll_seconds):
            if not self._quorum_holds(unreachable_since):
                return "failed"
        return None

    def _spectrograph_phase(self) -> Outcome | None:
        """Hand the spectrograph its assignment, under the plan's token, and wait for it to say
        it is done (`spectrograph_finished`, from its notification) while the quorum holds.
        Returns None when it is done, "failed" when it failed, the quorum was lost, or it went
        silent past its deadline. Aborted: None, with `cancelled` set.

        Backstops for a notification that never arrives (a restart loses the sender's queue):
        a spectrograph whose status no longer carries this plan's token is done; and one
        silent beyond its exposure series plus `spec_margin_seconds` has failed."""
        from common.models.assignments import Initiator, SpectrographAssignment

        assert self._spec_api is not None and self.plan.spec is not None
        spec = self._spec_api()
        status = _get(self._call(spec.get("status")), "value")
        if not _get(status, "operational", False) or _get(status, "activities", 0) != 0:
            self.errors.append("spectrograph: not operational, or not idle")
            self.event("spec-unavailable", self.errors[-1:])
            return "failed"
        assignment = SpectrographAssignment(
            instrument=self.plan.spec.instrument,
            initiator=Initiator.local_machine(),
            plan=self.plan,
            spec=self.plan.spec,
        )
        response = self._call(
            spec.put("execute_assignment", params={"plan_id": self.plan.ulid}, json=assignment.model_dump(mode="json"))
        )
        if not _get(response, "succeeded", False):
            self.errors.append(f"spectrograph refused: {_get(response, 'errors')}")
            self.event("spec-refused", self.errors[-1:])
            return "failed"
        self.spec_started = True
        self.event("exposing", [str(self.plan.spec.instrument)])
        series = (self.plan.spec.requested_exposure_duration or 0) * (self.plan.spec.requested_number_of_exposures or 1)
        deadline = time.monotonic() + series + self.timing.spec_margin_seconds
        unreachable_since: dict[str, float] = {}
        while not self.cancelled.is_set():
            if self.spec_finished.wait(self.timing.spec_poll_seconds):
                break
            if self.cancelled.is_set():
                return None
            if not self._quorum_holds(unreachable_since):
                return "failed"
            status = _get(self._call(spec.get("status")), "value")
            if status is not None and _get(status, "plan_id") != self.plan.ulid:
                self.event("spec-done", ["the spectrograph no longer works for the plan; no notification came"])
                self.spectrograph_finished()
                break
            if time.monotonic() >= deadline:
                self.errors.append(f"spectrograph: no word within {series + self.timing.spec_margin_seconds:g} s")
                self.event("spec-silent", self.errors[-1:])
                return "failed"
        if self.cancelled.is_set():
            return None
        if self.spec_errors:
            self.errors.extend(self.spec_errors)
            self.event("spec-failed", self.spec_errors)
            return "failed"
        self.event("spec-completed", [str(self.plan.spec.instrument)])
        return None
