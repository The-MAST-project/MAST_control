"""Every control module imports with no configuration and no network.

Runs first in collection order on purpose: if the environment is incomplete -- the sibling
``common`` missing, a dependency not installed -- this fails with the real ImportError,
rather than every other module failing to collect with a less direct one.

``app.py`` is deliberately not here: it builds ``Config()`` at import, so it needs a real
configuration database.
"""

from __future__ import annotations

import importlib

import pytest

from common.config import Config, ConfigError

MODULES = [
    "control.controller",
    "control.planning",
    "control.scheduling",
    "control.data_server",
    "control.safety",
]


@pytest.mark.parametrize("module", MODULES)
def test_module_imports(module):
    importlib.import_module(module)


def test_the_suite_cannot_reach_a_real_configuration():
    # conftest points MAST_CONFIG at a missing file. If this ever constructs, the suite is
    # one stray call away from the production config DB on mast-ns-control.
    with pytest.raises(ConfigError, match="does not exist"):
        Config()
