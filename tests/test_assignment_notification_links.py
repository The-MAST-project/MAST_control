"""`Controller._handle_assignment_notification`: a producer's products, linked into the plan
execution's products folder (plans/control_acquisition_design.md 5.10).

A producer reports its folder relative to its own folder on the share (`<share>/<host>/`),
so the link's target is `<share_root>/<host>/<shared_top>` -- not under `Filer().shared`,
which is control's own folder. Links go under `<Stage>/<unit, or instrument for Spec>/`,
named after their target, and are relative.
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
from common.models.plans import PlanProducts, PlanStage

pytestmark = pytest.mark.skipif(platform.system() == "Windows", reason="control runs on Linux; symlinks")

ULID = "01JAD8Q4ZK7V3M2XG9T6R5N0HB"


def _notify(controller, shared_top: str | None, unit: str = "mast03", subpath=None, stage=None, ulid=ULID):
    notification = SimpleNamespace(
        assignment_id=ulid,
        shared_top=shared_top,
        shared_subpath=subpath,
        stage=stage,
        initiator=SimpleNamespace(hostname=unit),
    )
    asyncio.run(controller._handle_assignment_notification(notification))


@pytest.fixture
def products(tmp_path) -> PlanProducts:
    return PlanProducts.start(ULID, base=str(tmp_path / "products"))


@pytest.fixture
def controller(monkeypatch, products):
    controller = bare_controller(monkeypatch, sites=[])
    controller.in_progress = SimpleNamespace(plan=SimpleNamespace(ulid=ULID), products=products)
    return controller


def _on_share(*parts: str) -> Path:
    return Path(Filer().share_root.root, *parts)


def test_a_stage_links_under_it_by_unit(controller, products):
    product = _on_share("mast03", "2026-10-21", "Acquisitions", "seq=0001")
    product.mkdir(parents=True)

    _notify(controller, "2026-10-21/Acquisitions/seq=0001", stage=PlanStage.Acquisition, subpath="acquisition")

    link = products.folder_for(PlanStage.Acquisition, "mast03") / "seq=0001"
    assert link.is_symlink() and not os.path.isabs(os.readlink(link))
    assert os.path.realpath(link) == os.path.realpath(product)


def test_not_under_this_machines_folder(controller, products):
    filer = Filer()
    assert filer.shared.root != filer.share_root.root

    _notify(controller, "2026-10-21/FocusOnStar/0003", stage=PlanStage.Autofocus)

    link = products.folder_for(PlanStage.Autofocus, "mast03") / "0003"
    assert os.path.realpath(link) == os.path.realpath(_on_share("mast03", "2026-10-21", "FocusOnStar", "0003"))


def test_a_target_not_on_the_share_yet_is_still_linked(controller, products):
    _notify(controller, "2026-10-21/Acquisitions/seq=0009", stage=PlanStage.Acquisition)
    link = products.folder_for(PlanStage.Acquisition, "mast03") / "seq=0009"
    assert link.is_symlink() and not link.exists()


@pytest.mark.parametrize(
    ("subpath", "stage", "producer"),
    [
        ("acquisition", PlanStage.Acquisition, "mast03"),
        ("focus_on_star", PlanStage.Autofocus, "mast03"),
        ("highspec", PlanStage.Spec, "highspec"),
        ("deepspec", PlanStage.Spec, "deepspec"),
    ],
)
def test_without_a_stage_the_subpath_places_it(controller, products, subpath, stage, producer):
    _notify(
        controller,
        "2026-10-21/x/acquisition-0001",
        unit="mast-ns-spec" if stage is PlanStage.Spec else "mast03",
        subpath=subpath,
    )
    assert (products.folder_for(stage, producer) / "acquisition-0001").is_symlink()


def test_an_unknown_subpath_is_not_linked(controller, products):
    _notify(controller, "2026-10-21/x/y", subpath="mystery")
    assert not any(Path(products.root).rglob("y"))


def test_a_plan_not_in_progress_is_not_linked(controller, products):
    _notify(controller, "2026-10-21/Acquisitions/seq=0001", stage=PlanStage.Acquisition, ulid="01OTHER")
    assert not any(Path(products.root).iterdir())


def test_an_absolute_shared_top_is_not_linked(controller, products):
    _notify(controller, "D:/MAST/2026-10-21/Acquisitions/seq=0001", stage=PlanStage.Acquisition)
    assert not any(Path(products.root).rglob("seq=0001"))


class TestTheHandshake:
    """The endpoint answers `ok` once it has acted on a notification, or deliberately ignored
    it; errors only for what a resend might fix. The sender keeps a notification until `ok`."""

    @pytest.fixture
    def endpoint(self, controller, monkeypatch):
        async def no_relay(data):
            return None

        monkeypatch.setattr(controller, "_relay_to_django", no_relay)

        def call(**fields):
            from common.models.assignments import AssignmentNotification
            from common.notifications import NotificationInitiator

            notification = AssignmentNotification(
                assignment_id=fields.pop("ulid", ULID),
                state="in-progress",
                initiator=NotificationInitiator(site="ns", type="unit", hostname="mast03"),
                **fields,
            )
            return asyncio.run(controller.notifications_endpoint(notification))

        return call

    def test_linked_is_ok(self, endpoint):
        assert endpoint(shared_top="2026-10-21/Acquisitions/seq=0001", stage=PlanStage.Acquisition).value == "ok"

    @pytest.mark.parametrize(
        "fields",
        [
            {"ulid": "01OTHER", "shared_top": "x/y", "stage": PlanStage.Acquisition},
            {"shared_top": "x/y", "shared_subpath": "mystery"},
            {"shared_top": "D:/MAST/x/y", "stage": PlanStage.Acquisition},
        ],
        ids=["not-the-plan-in-progress", "unknown-kind", "absolute-path"],
    )
    def test_ignored_on_purpose_is_ok_too(self, endpoint, fields):
        assert endpoint(**fields).value == "ok", "no resend could change it"

    def test_a_link_that_cannot_be_made_is_an_error_to_resend(self, endpoint, products, monkeypatch):
        def fail(*args, **kwargs):
            raise OSError("No space left on device")

        monkeypatch.setattr(type(products), "link", fail)
        response = endpoint(shared_top="2026-10-21/Acquisitions/seq=0001", stage=PlanStage.Acquisition)
        assert response.failed and "No space left on device" in response.errors[0]
