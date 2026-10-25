"""The one-off rewrite of the plan files (plans/control_acquisition_design.md 3.5, 8)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import tomlkit

from common.models.events import EventModel
from common.models.plans import Plan

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import rewrite_plans_2026_10 as tool

TARGET = {"ra_hours": 7.233, "dec_degrees": 74.05, "name": "WD 0710+741"}


def no_network(target):
    raise AssertionError("resolved a target that had coordinates")


def write(tmp_path, doc: dict) -> Path:
    path = tmp_path / "PLAN_01JAD8Q4ZK7V3M2XG9T6R5N0HB.toml"
    path.write_text(tomlkit.dumps(doc))
    return path


def test_the_units_fields(tmp_path):
    path = write(tmp_path, {"required_units": 2, "allocated_units": ["mast02"], "requested_units": [], "target": TARGET})
    result = tool.rewrite_file(path, dry_run=False, resolve=no_network)
    doc = tomlkit.parse(path.read_text()).unwrap()
    assert "required_units" not in doc and "allocated_units" not in doc
    assert doc["scheduled_units"] == ["mast02"] and doc["requested_units"] == "any"
    assert result.problem is None and Plan(**doc)


def test_a_quorum_above_an_explicit_list_is_left_alone(tmp_path):
    original = {"requested_units": ["mast01"], "quorum": 3, "required_units": 1, "target": TARGET}
    path = write(tmp_path, original)
    result = tool.rewrite_file(path, dry_run=False, resolve=no_network)
    assert "quorum 3" in result.problem
    assert tomlkit.parse(path.read_text()).unwrap() == original


def test_the_exposure_series_moves_repeats_included(tmp_path):
    target = {
        **TARGET,
        "requested_exposure_duration": 2700.0,
        "max_exposure_duration": 900.0,
        "repeats": {"every": "Once per night", "nights": 3},
    }
    path = write(tmp_path, {"requested_units": "any", "target": target, "spec": {"instrument": "highspec"}})
    tool.rewrite_file(path, dry_run=False, resolve=no_network)
    doc = tomlkit.parse(path.read_text()).unwrap()
    assert doc["spec"]["requested_exposure_duration"] == 2700.0
    assert doc["spec"]["repeats"] == {"every": "Once per night", "nights": 3}
    assert "repeats" not in doc["target"] and "max_exposure_duration" not in doc["target"]


def test_exposure_fields_without_a_spectrograph_are_left_alone(tmp_path):
    path = write(tmp_path, {"requested_units": "any", "target": {**TARGET, "requested_exposure_duration": 60.0}})
    before = path.read_text()
    result = tool.rewrite_file(path, dry_run=False, resolve=no_network)
    assert "no [spec]" in result.problem and path.read_text() == before


def test_a_name_without_coordinates_is_resolved_and_recorded(tmp_path):
    def resolve(target):
        return {**target, "ra_hours": 14.0535, "dec_degrees": 54.34875}, EventModel(what="resolved", details=["M101 -> ..."])

    path = write(tmp_path, {"requested_units": "any", "target": {"name": "M101"}})
    tool.rewrite_file(path, dry_run=False, resolve=resolve)
    doc = tomlkit.parse(path.read_text()).unwrap()
    assert doc["target"]["ra_hours"] == 14.0535
    assert doc["events"][-1]["what"] == "resolved"


def test_dry_run_writes_nothing(tmp_path):
    path = write(tmp_path, {"required_units": 1, "target": TARGET})
    before = path.read_text()
    result = tool.rewrite_file(path, dry_run=True, resolve=no_network)
    assert result.changes and path.read_text() == before


def test_a_file_that_would_not_load_is_left_alone(tmp_path):
    path = write(tmp_path, {"required_units": 1, "target": {"name": "x"}})
    before = path.read_text()

    def unresolvable(target):
        raise ValueError("neither")

    result = tool.rewrite_file(path, dry_run=False, resolve=unresolvable)
    assert result.problem.startswith("would not load") and path.read_text() == before


@pytest.mark.parametrize("ok", [True])
def test_main_reports_and_exits(tmp_path, ok, capsys):
    write(tmp_path, {"required_units": 1, "target": TARGET})
    assert tool.main([str(tmp_path), "--dry-run"]) == 0
    assert "WOULD WRITE" in capsys.readouterr().out


def test_spec_assignment_is_spec(tmp_path):
    path = write(
        tmp_path,
        {
            "requested_units": "any",
            "target": TARGET,
            "spec_assignment": {"instrument": "highspec", "requested_exposure_duration": 60.0},
        },
    )
    tool.rewrite_file(path, dry_run=False, resolve=no_network)
    doc = tomlkit.parse(path.read_text()).unwrap()
    assert "spec_assignment" not in doc and doc["spec"]["instrument"] == "highspec"
