"""`Controller._handle_assignment_notification`: a unit's products, linked into the run folder.

A unit reports its products relative to its own root on the share (`<share>/<unit>/`), so
the link's source is `<share_root>/<unit>/<shared_top>`. It used to be joined under
`Filer().shared`, which on Linux was the share root by accident -- and stops being it once
`shared` is per-machine everywhere (plans/filing-areas-design.md 3.1), when every link would
point under `.../MAST/mast-ns-control/<unit>/` and dangle.
"""

from __future__ import annotations

import asyncio
import os
import platform
from pathlib import Path
from types import SimpleNamespace

import pytest
from fakes import bare_controller

from common.filer import Filer

pytestmark = pytest.mark.skipif(platform.system() == "Windows", reason="control runs on Linux; symlinks")


def _notify(controller, shared_top: str, unit: str = "mast03", subpath: str = "acquisition"):
    notification = SimpleNamespace(
        assignment_id="01JAD8Q4ZK7V3M2XG9T6R5N0HB",
        shared_top=shared_top,
        shared_subpath=subpath,
        initiator=SimpleNamespace(hostname=unit),
    )
    asyncio.run(controller._handle_assignment_notification(notification))


@pytest.fixture
def controller(monkeypatch, tmp_path):
    controller = bare_controller(monkeypatch, sites=[])
    controller.in_progress = SimpleNamespace(ulid="01JAD8Q4ZK7V3M2XG9T6R5N0HB", run_folder=str(tmp_path / "run-0007"))
    return controller


def test_the_link_points_at_the_units_folder_under_the_share_root(controller, tmp_path):
    product = Path(Filer().share_root.root) / "mast03" / "2026-10-21" / "Acquisitions" / "seq=0001"
    product.mkdir(parents=True)

    _notify(controller, "2026-10-21/Acquisitions/seq=0001")

    link = tmp_path / "run-0007" / "mast03" / "acquisition"
    assert link.is_symlink()
    assert os.path.realpath(link) == os.path.realpath(product)


def test_not_under_this_machines_folder(controller, tmp_path):
    # The isolated layout is Windows-like: `shared` is this machine's folder inside
    # `share_root`, as it is on control once `shared` is per-machine.
    filer = Filer()
    assert filer.shared.root != filer.share_root.root

    _notify(controller, "2026-10-21/Acquisitions/seq=0002")

    link = tmp_path / "run-0007" / "mast03" / "acquisition"
    assert not os.readlink(link).startswith(filer.shared.root.rstrip("/"))
