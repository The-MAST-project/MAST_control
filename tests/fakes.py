"""Stand-ins for what a real `Controller` gets from the configuration and the network."""

from __future__ import annotations

from threading import Lock

from common.config.site import Site
from control.controller import Controller


def make_site(name: str = "ns", deployed_units: list[str] | None = None, **fields) -> Site:
    """A `Site` as the config DB would hold it, with only what a test cares about varied."""
    values = {
        "name": name,
        "project": "mast",
        "deployed_units": deployed_units if deployed_units is not None else ["mast01", "mast02"],
        "controller_host": "mast-ns-control",
        "spec_host": "mast-ns-spec",
        "unit_ids": "1-20",
    } | fields
    return Site(**values)


class FakePowerSwitch:
    """Answers `get_outlet_state` from a dict instead of over HTTP."""

    def __init__(self, outlets: dict[str, bool] | None = None):
        self.outlets = outlets if outlets is not None else {"Computer": True}

    def get_outlet_state(self, outlet: str) -> bool | None:
        return self.outlets.get(outlet)


def bare_controller(monkeypatch, sites: list[Site]) -> Controller:
    """A `Controller` with its state but none of `__init__`'s side effects.

    The real constructor reads the config DB, builds a `Planner` on disk, resolves and
    contacts power switches, and starts a 2-second fetch timer. This one has just the
    attributes the status path reads -- `lock`, `status_cache`, `power_switches` -- and
    `managed_sites()` answers `sites`. It is also not the singleton: `Controller()` would
    hand every test the same instance.
    """
    controller = object.__new__(Controller)
    controller.lock = Lock()
    controller.status_cache = {}
    controller.power_switches = {}
    monkeypatch.setattr(controller, "managed_sites", lambda: sites)
    return controller
