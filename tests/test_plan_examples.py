"""The example plans in docs/ say what the model accepts (plans/control_acquisition_design.md 3.4).

Both load as plans and carry no empty table. And the full example, with every commented-out
field and table uncommented, is a valid plan too: a default or an allowed value the model
does not accept, or a field it no longer has, fails here instead of misleading a reader.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomlkit

import common.models.calibration as calibration_module
from common.models.plans import Plan

DOCS = Path(__file__).resolve().parent.parent / "docs"
COMMENTED_OUT = re.compile(r"^# (?=\[|\w+ = )")


@pytest.fixture(autouse=True)
def thar_filters(monkeypatch):
    monkeypatch.setattr(
        calibration_module, "Config", lambda: type("C", (), {"get_thar_filters": lambda self: ["ND1000", "ND3000"]})()
    )


def empty_tables(value, path="") -> list[str]:
    found = []
    if isinstance(value, dict):
        if not value and path:
            found.append(path)
        for key, item in value.items():
            found += empty_tables(item, f"{path}.{key}" if path else key)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            found += empty_tables(item, f"{path}[{i}]")
    return found


@pytest.mark.parametrize("name", ["minimal-plan-example.toml", "full-plan-example.toml"])
def test_loads_with_no_empty_tables(name):
    doc = tomlkit.parse((DOCS / name).read_text()).unwrap()
    assert empty_tables(doc) == []
    assert Plan(**doc).requested_units


def test_the_full_example_with_everything_uncommented_is_valid():
    text = "\n".join(COMMENTED_OUT.sub("", line) for line in (DOCS / "full-plan-example.toml").read_text().splitlines())
    doc = tomlkit.parse(text).unwrap()
    plan = Plan(**doc)

    assert plan.spec.requested_exposure_duration == 1800.0
    assert plan.spec.settings.disperser == "Halpha"
    assert plan.constraints.time_window.end_after_nights == 7
    assert set(doc) - {"events"} <= set(Plan.model_fields), "every field it shows is the model's"


def test_every_field_with_a_default_is_commented_out():
    active = [
        line.split(" = ")[0]
        for line in (DOCS / "full-plan-example.toml").read_text().splitlines()
        if re.match(r"^\w+ = ", line)
    ]
    assert active == ["requested_units", "ra_hours", "dec_degrees"], "only what has no default is set"
