@common/CLAUDE.md

# MAST_control — Claude Guidance

## Running

```bash
python app.py   # role + identity come from the bootstrap config file
                # (/etc/wis/config.toml; set MAST_CONFIG to override for dev)
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

## Project-wide LLM guidance

Cross-repo LLM guidance for MAST lives in the **`mast-claude-config`** repo (`github.com/The-MAST-project/mast-claude-config`) — the overarching home for project-wide instructions (shared coding standards, team working-style, global environment facts), deployed into `~/.claude/` by its `setup.sh`. Keep repo-specific guidance in this file; put genuinely cross-repo guidance there. See `mast-claude-config/CLAUDE.md` for what belongs where.
