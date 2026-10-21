"""Test bootstrap for the MAST_control suite.

``common`` is not under this repo: it is a sibling clone (``<top>/common/``) reached
through the ``mast.pth`` the provisioning writes into the venv (in CI, ``PYTHONPATH``
stands in for it). The repo root goes on ``sys.path`` here, so ``import control`` works
however pytest is started.

Three guards, all installed at conftest IMPORT time, so they cover collection as well as
the tests:

1. **No configuration database.** This suite is run on mast-ns-control, which holds the
   site's live config DB *on loopback* -- so the network guard below would not stop a
   stray ``Config()`` from reading, or ``set_unit`` from writing, the production
   configuration. ``MAST_CONFIG`` is pointed at a file that does not exist, so any
   ``Config()`` a test did not fake raises ``ConfigError`` instead. Tests that need a
   configuration patch one in (see ``fakes.py``).

2. **No network**, except for tests marked ``network`` (``common.testing.network``):
   a unit, the spectrograph or a power switch is never contacted from here.

3. **No real share.** Every ``Filer`` points at temporary folders, one per role
   (``common.testing.filer``). On mast-ns-control a bare ``Filer()`` is the live
   ``/Storage/mast-share/MAST`` -- every unit's products, the plans and the runs.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Before anything imports common.config: a directory that exists, holding no config file.
os.environ["MAST_CONFIG"] = str(Path(tempfile.mkdtemp(prefix="mast-control-tests-")) / "no-such-config.toml")

from common.testing import network as _network  # noqa: E402 -- must follow the MAST_CONFIG override

_network.install()

from common.testing import filer as _filer_testing  # noqa: E402 -- before anything builds a Filer

_filer_testing.isolate_filer(prefix="mast-control-tests-")
pytest_configure = _network.pytest_configure
pytest_collection_modifyitems = _network.pytest_collection_modifyitems
_allow_network_for_marked_tests = _network.allow_network_for_marked_tests
