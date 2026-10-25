"""Acquisition: take units to one sky position and into guiding, as a generated plan
(plans/control_acquisition_design.md 7).

`start_acquisition` writes a plan -- the target, the units, optionally `focus_on_star`, no
spectrograph -- straight into `in-progress/`, and executes it like any other: the same
engine, events and folder moves. `acquisition_status` shows the current execution (or the
last) with every unit's live status; `abort_acquisition` aborts it.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import ulid
from fastapi import Query
from fastapi.responses import PlainTextResponse
from pydantic import ValidationError

from common.api import UnitApi
from common.canonical import CanonicalResponse
from common.config import Config
from common.const import Const
from common.mast_logging import get_logger
from common.models.events import EventModel
from common.models.plans import Plan, plan_to_toml
from common.models.statuses import COOLING_DOWN_REASON
from common.models.targets import resolve_target
from common.object_resolver import ObjectNameError
from common.parsers import ANY_UNITS, DEC_PATTERN, RA_PATTERN
from control.execution import PlanExecution, UnitRunState, _get, resolve_units

if TYPE_CHECKING:
    from control.controller import Controller

logger = get_logger(__name__)

#: How long a new acquisition waits for the previous one's units to clear `Aborting` (7.1).
ABORT_WAIT_SECONDS = 30.0
#: Each unit's fresh status for the table (7.2).
STATUS_TIMEOUT_SECONDS = 5.0


def _has_spectrograph(execution: PlanExecution) -> bool:
    spec = execution.plan.spec
    return spec is not None and spec.instrument is not None


class Acquisitions:
    def __init__(self, controller: Controller, unit_api=lambda name: UnitApi(hostname=name)):
        self.controller = controller
        self._unit_api = unit_api

    # --- start ----------------------------------------------------------------------------

    def generate(
        self,
        units: list[str],
        ra_j2000_hours: str | None,
        dec_j2000_degs: str | None,
        object_name: str | None,
        autofocus_on_star: bool,
        quorum: int | None,
    ) -> Plan | list[str]:
        """The generated plan, validated -- or why there is none (7.1, step 1)."""
        any_units = units == [ANY_UNITS]
        if quorum is None:
            if any_units:
                return [f"quorum is required with units={ANY_UNITS}: the count is not known until they are resolved"]
            quorum = len(units)
        typed = {"ra_hours": ra_j2000_hours, "dec_degrees": dec_j2000_degs}
        target = {k: v for k, v in typed.items() if v not in (None, "")}
        if object_name:
            target["name"] = object_name
        try:
            target, resolved = resolve_target(target)
        except ObjectNameError as e:
            return [f"could not resolve '{object_name}' -- {e}"]
        except ValueError as e:
            return [str(e)]
        generated = EventModel(
            what="generated",
            details=["start_acquisition", f"ra={ra_j2000_hours}", f"dec={dec_j2000_degs}", f"object={object_name}"],
        )
        events = [generated] + ([resolved] if resolved is not None else [])
        try:
            return Plan(
                ulid=str(ulid.ULID()),
                owner="mast",
                approved=True,
                production=False,
                requested_units=ANY_UNITS if any_units else units,
                autofocus=autofocus_on_star,
                quorum=quorum,
                target=target,
                events=events,
            )
        except ValidationError as e:
            return [f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()]

    def _make_room(self, plan: Plan) -> list[str]:
        """Step 2: abort an acquisition in progress, never a science plan; then wait for the
        requested units to clear `Aborting`."""
        current = self.controller.in_progress
        if current is not None:
            if _has_spectrograph(current):
                return [f"plan {current.plan.ulid} is in progress"]
            current.abort()
            current.wait(ABORT_WAIT_SECONDS)
        try:
            names = resolve_units(plan, self.controller.managed_sites(), Config().local_site)
        except ValueError as e:
            return [f"units: {e}"]
        deadline = time.monotonic() + ABORT_WAIT_SECONDS
        while True:
            aborting = [n for n in names if "Aborting" in (_get(self._status(n), "activities_verbal") or [])]
            if not aborting or time.monotonic() >= deadline:
                return [f"{n}: still aborting" for n in aborting]
            time.sleep(1.0)

    def _status(self, name: str) -> Any:
        try:
            response = asyncio.run(self._unit_api(name).get("status", timeout=STATUS_TIMEOUT_SECONDS))
        except Exception:  # noqa: BLE001 -- an unreachable unit is a table cell, not an error
            return None
        return _get(response, "value") if _get(response, "succeeded", False) else None

    def start(self, plan: Plan) -> CanonicalResponse:
        """Steps 2-4: make room, write the plan into `in-progress/`, load it back, execute."""
        problems = self._make_room(plan)
        if problems:
            return CanonicalResponse(errors=problems)
        planner = self.controller.planner
        path = Path(planner.in_progress_folder.folder_path) / f"PLAN_{plan.ulid}.toml"
        path.write_text(plan_to_toml(plan))
        loaded = Plan.from_toml_file(str(path))
        loaded.full_path = path
        planner.in_progress_folder.refresh()
        return self.controller.start_execution(loaded, path)

    async def endpoint_start_acquisition(
        self,
        units: Annotated[
            list[str],
            Query(min_length=1, description="Unit specifiers (`mast01`, `ns:1,3,5`), or `any`."),
        ],
        ra_j2000_hours: Annotated[str | None, Query(pattern=RA_PATTERN)] = None,
        dec_j2000_degs: Annotated[str | None, Query(pattern=DEC_PATTERN)] = None,
        object_name: Annotated[str | None, Query(description="Resolved when the coordinates are not both usable.")] = None,
        autofocus_on_star: bool = False,
        quorum: Annotated[
            int | None,
            Query(ge=1, description="Defaults to the number of units; required with `units=any`."),
        ] = None,
    ) -> CanonicalResponse:
        """Take ``units`` to one sky position and into guiding: a generated plan, executed
        at once. Answers with the plan's ULID; watch `acquisition_status`."""
        plan = self.generate(units, ra_j2000_hours, dec_j2000_degs, object_name, autofocus_on_star, quorum)
        if isinstance(plan, list):
            return CanonicalResponse(errors=plan)
        return await asyncio.to_thread(self.start, plan)

    # --- status ---------------------------------------------------------------------------

    def table(self) -> str:
        execution = self.controller.in_progress or self.controller.last_execution
        if execution is None:
            return "no acquisition\n"
        names = list(execution.units)
        statuses: dict[str, Any] = {}
        if names:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=len(names)) as pool:
                statuses = dict(zip(names, pool.map(self._status, names), strict=True))
        guiding = execution.count(UnitRunState.GUIDING)
        header = f"plan {execution.plan.ulid}  {execution.outcome} (quorum {guiding} of {execution.quorum})"
        columns = ["unit", "state", "opstate", "operational", "autofocusing", "why_busy", "guiding"]
        rows = [[name, str(execution.units[name].state), *_status_cells(statuses.get(name))] for name in names]
        widths = [max(len(c), *(len(r[i]) for r in rows)) if rows else len(c) for i, c in enumerate(columns)]
        lines = [header, ""]
        lines.append("  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)).rstrip())
        lines.append("  ".join("-" * w for w in widths))
        lines += ["  ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)).rstrip() for r in rows]
        errors = [f"  {run.name}: {e}" for run in execution.units.values() for e in run.errors] + [
            f"  {e}" for e in execution.errors
        ]
        if errors:
            lines += ["", "errors:", *errors]
        return "\n".join(lines) + "\n"

    async def endpoint_acquisition_status(self) -> PlainTextResponse:
        """The current acquisition, or the last one: each unit's place in it and its live status."""
        return PlainTextResponse(await asyncio.to_thread(self.table))

    # --- abort ----------------------------------------------------------------------------

    async def endpoint_abort_acquisition(self) -> CanonicalResponse:
        execution = self.controller.in_progress
        if execution is None:
            return CanonicalResponse(errors=["no acquisition"])
        failures = await asyncio.to_thread(execution.abort)
        return CanonicalResponse(errors=failures) if failures else CanonicalResponse(value=execution.plan.ulid)

    def add_routes(self, router) -> None:
        base = Const.BASE_CONTROL_PATH
        tag = "acquisition"
        router.add_api_route(
            base + "/start_acquisition", tags=[tag], endpoint=self.endpoint_start_acquisition, methods=["PUT"]
        )
        router.add_api_route(
            base + "/acquisition_status", tags=[tag], endpoint=self.endpoint_acquisition_status, methods=["GET"]
        )
        router.add_api_route(
            base + "/abort_acquisition", tags=[tag], endpoint=self.endpoint_abort_acquisition, methods=["PUT"]
        )


def _status_cells(status: Any) -> list[str]:
    """opstate, operational, autofocusing, why_busy, guiding -- or `unreachable` across them."""
    if status is None:
        return ["unreachable"] * 5
    operational = _get(status, "operational", False)
    reasons = _get(status, "why_not_operational") or []
    if not operational and reasons and all(str(r).startswith(COOLING_DOWN_REASON) for r in reasons):
        operational_cell = "True (cooling)"
    else:
        operational_cell = str(bool(operational))
    autofocusing = bool(_get(status, "autofocusing", False)) or "FocusingOnStar" in (_get(status, "activities_verbal") or [])
    why_busy = _get(status, "why_busy")
    if why_busy is None:
        busy_cell = "?"
    elif not why_busy:
        busy_cell = "-"
    else:
        busy_cell = "; ".join(f"{owner}: {', '.join(flags)}" for owner, flags in why_busy.items())
    return [str(_get(status, "opstate")), operational_cell, str(autofocusing), busy_cell, str(bool(_get(status, "guiding")))]
