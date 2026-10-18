"""`Controller.status_from_dict`: a fetched status, validated into the model its source sends."""

from __future__ import annotations

import pytest

from common.api import ControllerApi, SpecApi, UnitApi
from common.models.statuses import BaseStatus, ControllerStatus, FullUnitStatus, SpecStatus
from control.controller import Controller


def _api(cls):
    """An instance of `cls` without its constructor, which resolves hostnames.

    `status_from_dict` dispatches on the API's type name only, so nothing else is needed.
    """
    return object.__new__(cls)


@pytest.fixture
def controller() -> Controller:
    return object.__new__(Controller)


def test_a_unit_status_becomes_a_full_unit_status(controller):
    data = FullUnitStatus(id=7, operational=True, opstate="startedup", why_busy={}).model_dump(mode="json")

    status = controller.status_from_dict(_api(UnitApi), data)

    assert isinstance(status, FullUnitStatus)
    assert status.opstate == "startedup"
    assert status.why_busy == {}


def test_a_spec_status_becomes_a_spec_status(controller):
    data = SpecStatus(detected=True, operational=True).model_dump(mode="json")

    assert isinstance(controller.status_from_dict(_api(SpecApi), data), SpecStatus)


def test_a_controller_status_becomes_a_controller_status(controller):
    data = ControllerStatus(detected=True, operational=True).model_dump(mode="json")

    assert isinstance(controller.status_from_dict(_api(ControllerApi), data), ControllerStatus)


def test_a_unit_status_missing_required_fields_falls_back_to_the_base_status(controller):
    # FullUnitStatus requires `id`; what is left is still worth showing.
    status = controller.status_from_dict(_api(UnitApi), {"detected": True, "operational": False})

    assert type(status) is BaseStatus
    assert status.detected is True
    assert status.operational is False


def test_an_unreadable_status_becomes_not_detected(controller):
    status = controller.status_from_dict(_api(UnitApi), None)

    assert type(status) is BaseStatus
    assert status.detected is False
    assert status.operational is False


def test_an_unknown_api_type_is_refused(controller):
    class SomeOtherApi:
        pass

    with pytest.raises(ValueError, match="SomeOtherApi"):
        controller.status_from_dict(SomeOtherApi(), {})
