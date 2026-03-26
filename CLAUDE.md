@common/CLAUDE.md

# MAST_control — Claude Guidance

## Running

```bash
MAST_PROJECT=control python app.py
```

## Plan Execution (`control/planning.py`)

Phases tracked as `AssignmentActivities` bitflags:
1. **Probing** — checks units and spec are detected and operational (quorum enforced; relaxed in non-production/debug mode)
2. **Dispatching** — sends assignments to all operational units concurrently via `asyncio.gather`
3. **WaitingForGuiding** — polls unit status every 20 s until all committed units report `UnitActivities.Guiding`, or `timeout_to_guiding` expires
4. **ExposingSpec** — sends assignment to spectrograph, polls every 20 s until spec returns to `Idle`

Any phase failure calls `Plan.abort()` which sends abort to all committed units and the spec.

## Plan API routes (under `/mast/api/v1/control/plans/`)

`GET /get`, `POST /execute`, `POST /postpone`, `POST /revive`, `POST /cancel`, `DELETE /delete`

Utility: `python common/models/plans.py <plan-file.toml>` to parse and dump a plan as JSON.
