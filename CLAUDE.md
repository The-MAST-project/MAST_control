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

## Plan execution (`control/execution.py`)

`PlanExecution` runs one plan: **one worker thread per unit**, each taking its unit through
start-up, the operational and busy checks, an optional `focus_on_star`, and
`start_acquisition_and_guiding`, until it is guiding -- PHD2 locked on a star -- or fails in its
own words (`startup:`, `operational:`, `busy:`, `focus_on_star:`, `acquisition:`, `guiding:`,
or `status:` when control could not learn the unit's state). The plan **succeeds when `quorum`
units are guiding**, and fails as soon as too many have failed for that. A plan with a
spectrograph then exposes it. Every unit transition is an event in the plan file.

- `Controller.start_execution(plan, path)` starts one; there is one `in_progress` at a time, and
  `last_execution` keeps the last for `acquisition_status`. A `Batch` is refused.
- Each execution has a products folder, `plans/products/PLAN_<ulid>/<UTC start>/`; producers
  notify control of their folders and `_handle_assignment_notification` links them in, by
  stage (`Autofocus`, `Acquisition`, `Spec`).
- **Acquisition** (`control/acquisition.py`, tag `acquisition`): `PUT start_acquisition`
  generates a plan -- target, units, optional `focus_on_star`, no spectrograph -- writes it
  straight into `in-progress/` and executes it; `GET acquisition_status` is a live table;
  `PUT abort_acquisition`.
- Every wait is in `Timing`, and the unit API is injected: tests run the engine in
  milliseconds against scripted fake units (`tests/test_plan_execution.py`).

Design: `plans/control_acquisition_design.md` in mast-claude-config.

## Plan API routes (under `/mast/api/v1/control/plans/`)

`GET /get`, `POST /execute`, `POST /postpone`, `POST /revive`, `POST /cancel`, `DELETE /delete`

Utility: `python ../common/models/plans.py <plan-file.toml>` to parse and dump a plan as JSON.

## Project-wide LLM guidance

Cross-repo LLM guidance for MAST lives in the **`mast-claude-config`** repo (`github.com/The-MAST-project/mast-claude-config`) — the overarching home for project-wide instructions (shared coding standards, team working-style, global environment facts), deployed into `~/.claude/` by its `setup.sh`. Keep repo-specific guidance in this file; put genuinely cross-repo guidance there. See `mast-claude-config/CLAUDE.md` for what belongs where.
