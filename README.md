# MAST_control

Central backend orchestrator for the MAST telescope control system. Runs on
`mast-wis-control` and coordinates units, the spectrograph, the scheduler, and
the GUI. FastAPI app entry point is `app.py`.

## Running

```bash
MAST_PROJECT=control python app.py
```

## Endpoints

- `/mast/api/v1/control/...` — controller and plan-execution routes (see
  `control/` package).
- `/build-report` — cross-module build / version report (schema and rationale
  in `common/README.md`).

## Tooling

CLI scripts live under `tools/`:

- `tools/mast-plan-find` — search / scrape MAST observation plans.
- `tools/generate_mockup_plans.py` — emit ~50 realistic mockup plans for
  scheduler tests.
- `tools/mast-release` — coordinated cross-repo release tagging. See below.

### Cross-repo release tagging (`tools/mast-release`)

Tags are the single source of truth for a release identity in the MAST
ecosystem. `mast-release` applies the same annotated tag at HEAD of every
`MAST_*` repo in the workspace, runs the build report to verify everything is
coherent, and then offers an explicit push prompt only if it is.

```bash
# Tag every MAST_* repo locally, run the report, prompt to push to origin.
tools/mast-release tag v2.0 "Operational baseline May 2026"

# Skip the pre-tag confirm (still prompts for push).
tools/mast-release tag v2.0 "..." --yes

# Push an already-applied tag (re-runs the report + push gate).
tools/mast-release push v2.0

# Audit which tags exist in each repo.
tools/mast-release list
tools/mast-release list 'v*'
```

Preflight refuses to tag if any repo has a dirty working tree, a detached HEAD,
or already has the requested tag pointing at a different SHA. The push gate
requires the post-tag build report to show a single MAST_common SHA, no
per-repo errors, and every repo's tag resolving to its current HEAD. The push
target is always `origin`, per-repo; per-repo push failures are listed but do
not abort the batch.

Exit codes: `0` success, `1` preflight failure, `2` partial tag failure, `3`
push-gate failure, `4` partial push failure.
