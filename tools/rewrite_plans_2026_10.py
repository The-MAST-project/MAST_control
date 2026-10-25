#!/usr/bin/env python
"""One-off rewrite of the plan files to the step-2 plan model (plans/control_acquisition_design.md 3.5).

Run once on mast-ns-control, before control starts on the new model:

    python tools/rewrite_plans_2026_10.py --dry-run        # list what would change
    python tools/rewrite_plans_2026_10.py                  # rewrite
    python tools/rewrite_plans_2026_10.py some/folder ...  # other folders than the plans'

For every `PLAN_*.toml` under the given folders (default: every state folder of the plans
folder):

- `required_units` is dropped; `allocated_units` is renamed `scheduled_units`, value kept;
- an empty or missing `requested_units` becomes `"any"`; a `quorum` above an explicit list
  is reported and the file left alone;
- a target with a `name` but not both coordinates usable is resolved, its coordinates written
  in and a `resolved` event appended (needs the network); a name that does not resolve is
  reported and the file left alone;
- the exposure series -- `requested_exposure_duration`, `requested_number_of_exposures`,
  `max_exposure_duration`, `[target.repeats]` -- moves into `[spec_assignment]`; a plan with
  none and any of them set is reported and left alone;
- the run-time keys the old model wrote (`run_folder`, `timings`, ...) are dropped.

Every rewritten file must load with the new `Plan`; a file that would not is reported and
left exactly as it was. Exit status: 0 if every file was rewritten or needed nothing, 1 if any
was reported.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path

import tomlkit
from pydantic import ValidationError

from common.models.plans import Plan
from common.models.targets import resolve_target
from common.object_resolver import ObjectNameError
from common.parsers import ANY_UNITS, parse_target

EXPOSURE_FIELDS = ("requested_exposure_duration", "requested_number_of_exposures", "max_exposure_duration", "repeats")
RUN_TIME_KEYS = ("run_folder", "timings", "committed_unit_apis", "unit_apis", "spec_api", "terminated")


@dataclass
class Result:
    path: Path
    changes: list[str] = field(default_factory=list)
    problem: str | None = None


def _usable_coordinates(target: dict) -> bool:
    try:
        ra, dec = parse_target(target.get("ra_hours"), target.get("dec_degrees"))
    except ValueError:
        return False
    return ra is not None and dec is not None


def rewrite(doc: dict, resolve=resolve_target) -> tuple[dict, list[str]]:
    """The rewritten document and what changed. Raises ValueError for a file to leave alone."""
    changes: list[str] = []
    doc = dict(doc)

    if "required_units" in doc:
        doc.pop("required_units")
        changes.append("dropped required_units")
    if "allocated_units" in doc:
        doc["scheduled_units"] = doc.pop("allocated_units")
        changes.append("allocated_units -> scheduled_units")
    for key in RUN_TIME_KEYS:
        if key in doc:
            doc.pop(key)
            changes.append(f"dropped {key}")

    requested = doc.get("requested_units")
    if not requested:
        doc["requested_units"] = ANY_UNITS
        changes.append(f"requested_units -> '{ANY_UNITS}'")
    elif isinstance(requested, list) and requested != [ANY_UNITS]:
        quorum = int(doc.get("quorum", 1))
        one_each = all("," not in u and "-" not in str(u).split(":")[-1] for u in requested)
        if one_each and quorum > len(requested):
            raise ValueError(f"quorum {quorum} is above the {len(requested)} requested units {requested}")

    target = dict(doc.get("target") or {})
    moving = {k: target.pop(k) for k in EXPOSURE_FIELDS if k in target}
    if moving:
        spec = doc.get("spec_assignment")
        if spec is None:
            raise ValueError(f"exposure fields {sorted(moving)} but no [spec_assignment] for them to describe")
        doc["spec_assignment"] = {**dict(spec), **moving}
        changes.append(f"moved {', '.join(sorted(moving))} into spec_assignment")

    if target.get("name") and not _usable_coordinates(target):
        try:
            resolved_target, event = resolve(target)
        except ObjectNameError as e:
            raise ValueError(f"target name {target.get('name')!r} did not resolve: {e}") from e
        except ValueError:
            resolved_target, event = target, None  # no name to resolve: validation below says what is wrong
        if event is not None:
            target = resolved_target
            events = list(doc.get("events") or [])
            events.append(event.model_dump(mode="json", exclude_none=True))
            doc["events"] = events
            changes.append(f"resolved target {target['name']!r}")
    doc["target"] = target
    return doc, changes


def rewrite_file(path: Path, dry_run: bool, resolve=resolve_target) -> Result:
    result = Result(path)
    try:
        original = tomlkit.parse(path.read_text())
    except Exception as e:  # noqa: BLE001 -- a file that does not parse is reported, never fatal
        result.problem = f"does not parse: {e}"
        return result
    try:
        doc, result.changes = rewrite(original.unwrap(), resolve)
    except ValueError as e:
        result.problem = str(e)
        return result
    if not result.changes:
        return result
    try:
        Plan(**doc)
    except ValidationError as e:
        result.problem = "would not load: " + "; ".join(
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in e.errors()
        )
        result.changes = []
        return result
    except Exception as e:  # noqa: BLE001 -- a validator that cannot run (no config DB) is reported, not fatal
        result.problem = f"could not be validated: {e}"
        result.changes = []
        return result
    if not dry_run:
        path.write_text(tomlkit.dumps(doc))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("folders", nargs="*", help="folders to search (default: the plans folder)")
    parser.add_argument("--dry-run", action="store_true", help="list the changes, write nothing")
    args = parser.parse_args(argv)

    if args.folders:
        folders = [Path(f) for f in args.folders]
    else:
        from common.paths import PathMaker

        folders = [Path(PathMaker.make_plans_folder())]

    paths = sorted(p for folder in folders for p in folder.rglob("PLAN_*.toml") if "products" not in p.parts)
    reported = 0
    for path in paths:
        result = rewrite_file(path, args.dry_run)
        if result.problem:
            reported += 1
            print(f"LEFT ALONE  {path}: {result.problem}")
        elif result.changes:
            print(f"{'WOULD WRITE' if args.dry_run else 'REWROTE'}  {path}: {'; '.join(result.changes)}")
    print(f"{len(paths)} plan file(s), {reported} left alone")
    return 1 if reported else 0


if __name__ == "__main__":
    sys.exit(main())
