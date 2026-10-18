@../common/CLAUDE.md

# MAST_control — Claude Guidance

Central backend orchestrator. Runs on `mast-ns-control` (the active site is `ns`). Imports `MAST_common` as `common`, which is cloned as a **sibling** of this repo in the flat layout (`<top>/common/`, `<top>/control/`) and put on `sys.path` by the `mast.pth` the provisioning writes into the venv. It is no longer a submodule.

## Running

```bash
python app.py   # role + identity come from the bootstrap config file
                # (/etc/wis/config.toml; set MAST_CONFIG to override for dev)
```

## Tests

```bash
python -m pytest tests/ -q -ra   # from the repo root, in the venv that has mast.pth
```

- **No configuration database, ever.** `tests/conftest.py` points `MAST_CONFIG` at a missing
  file, so an unfaked `Config()` raises `ConfigError`. This matters most on mast-ns-control
  itself: its config DB is on loopback, which the network guard deliberately allows, so
  without this a stray `Config()` would read, and `set_unit` would write, production.
- **No network** off this machine except in tests marked `network` (`common.testing.network`).
- **Don't construct `Controller()`.** Its `__init__` reads the config DB, builds a `Planner`
  on disk, contacts power switches and starts the fetch timer, and it is a singleton.
  `tests/fakes.py: bare_controller()` gives a `Controller` with just its state.
- **Don't import `app.py`**: it builds `Config()` at import. Every module under `control/`
  imports cleanly, and `test_imports.py` keeps it that way.

## Plan Execution (`control/planning.py`)

Phases tracked as `AssignmentActivities` bitflags:
1. **Probing** — checks units and spec are detected and operational (quorum enforced; relaxed in non-production/debug mode)
2. **Dispatching** — sends assignments to all operational units concurrently via `asyncio.gather`
3. **WaitingForGuiding** — polls unit status every 20 s until all committed units report `UnitActivities.Guiding`, or `timeout_to_guiding` expires
4. **ExposingSpec** — sends assignment to spectrograph, polls every 20 s until spec returns to `Idle`

Any phase failure calls `Plan.abort()` which sends abort to all committed units and the spec.

## Plan API routes (under `/mast/api/v1/control/plans/`)

`GET /get`, `POST /execute`, `POST /postpone`, `POST /revive`, `POST /cancel`, `DELETE /delete`

Utility: `python ../common/models/plans.py <plan-file.toml>` to parse and dump a plan as JSON.

## Project-wide LLM guidance

Cross-repo LLM guidance for MAST lives in the **`mast-claude-config`** repo (`github.com/The-MAST-project/mast-claude-config`) — the overarching home for project-wide instructions (shared coding standards, team working-style, global environment facts), deployed into `~/.claude/` by its `setup.sh`. Keep repo-specific guidance in this file; put genuinely cross-repo guidance there. See `mast-claude-config/CLAUDE.md` for what belongs where.
