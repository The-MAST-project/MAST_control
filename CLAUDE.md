# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is MAST?

**MAsters of Spectra** — a distributed telescope control system for the Multiple Aperture Spectroscopic Telescope. It consists of several Python services communicating over HTTP (FastAPI), coordinated by a central controller.

## Project Structure

All projects live under `/home/mast/PycharmProjects/`:

| Project | Role | Runs on |
|---|---|---|
| `MAST_common` | Shared library (git submodule in all others) | — |
| `MAST_control` | Central backend orchestrator | `mast-wis-control` |
| `MAST_spec` | Spectrograph control backend | `mast-wis-spec` |
| `MAST_unit.2024-12-12` | Per-unit backend (telescope hardware) | Each unit machine (`mast01`…`mast20`) |
| `MAST_gui` | Django web frontend | `mast-wis-control` |

### MAST_common submodule placement
- In `MAST_control` and `MAST_spec`: submoduled as `./common/`
- In `MAST_unit.*`: submoduled as `./src/common/`
- In `MAST_gui`: submoduled as `./common/`

## Running the Services

Each FastAPI backend requires the `MAST_PROJECT` environment variable before starting:

```bash
# MAST_control
MAST_PROJECT=control python app.py

# MAST_spec
MAST_PROJECT=spec python app.py

# MAST_unit (run on the unit machine, Windows)
set MAST_PROJECT=unit
python src/app.py
```

Django GUI:
```bash
cd MAST_gui
python manage.py migrate
python manage.py runserver 0.0.0.0:8010
```

## Configuration System (`MAST_common/config/`)

`Config` is a singleton. It reads from two sources (merged):
1. **MongoDB** at `mongodb://mast-wis-control:27017` (primary, cached with TTL)
2. **TOML files** in `<project>/config/<project>.toml` and `<hostname>.toml` (fallback/override)

The `MAST_PROJECT` env var (`unit`, `control`, or `spec`) tells `Config` which TOML file to load and how to locate itself.

Key `Config` methods: `get_unit()`, `get_sites()`, `get_service()`, `get_specs()`, `get_users()`.

## API Conventions

### URL paths (defined in `common/const.py`)
- Units: `/mast/api/v1/unit/...`
- Control: `/mast/api/v1/control/...`
- Spec: `/mast/api/v1/spec/...`

### `CanonicalResponse` (`common/canonical.py`)
All API endpoints return a `CanonicalResponse`:
```python
class CanonicalResponse(BaseModel):
    api_version: str = "1.0"
    value: Any | None = None   # present on success
    errors: list[str] | None = None  # present on failure
```
Use `response.succeeded` / `response.failed` / `response.is_error`. `CanonicalResponse_Ok` is a convenience constant for `value="ok"`.

### `ApiClient` (`common/api.py`)
Wraps `httpx` for inter-service HTTP calls. `UnitApi`, `SpecApi`, `ControllerApi` are typed wrappers around `ApiClient`. `ApiResponse` converts JSON dicts to attribute-access objects.

## Component Architecture (`common/interfaces/components.py`)

All hardware components (Mount, Focuser, Camera, Covers, Stage, Spectrographs) implement the `Component` ABC which combines:
- `ABC` — requires `startup()`, `shutdown()`, `is_shutting_down`, `status`, `is_operational`
- `Activities` — bitflag-based activity tracking (`IntFlag`) with timing

`ComponentStatus` is the Pydantic status model: `detected`, `connected`, `operational`, `activities`, `why_not_operational`.

Each component exposes a `FastAPI` `APIRouter` (`api_router`) that is included in the main app.

## Logging (`common/mast_logging.py`)

Use `init_log(logger)` after getting a logger. Logs rotate daily under:
- Linux: `/var/log/mast/<date>/`
- Windows: `%LOCALAPPDATA%/mast/<date>/`

Rich console output is enabled by default.

## MAST_unit Hardware Stack

Each unit controls (via `unit.py`):
- **PlaneWave L550 mount** — via `PWI4` client (must be running as a process)
- **PlaneWave Hedrik focuser**
- **PlaneWave covers**
- **Standa translating stage**
- **ZWO ASI294MM camera**
- **DLI managed power switch** — `dlipowerswitch.py`

The unit startup (`app.py`) ensures `PWI4.exe`, `PWShutter.exe`, and `ps3cli.exe` are running before FastAPI starts.

## Notifications (`common/notifications.py`)

`Notifier` / `UiUpdateNotifications` push WebSocket events to the Django GUI. The `NotificationInitiator` is auto-detected from hostname convention: `mast-<site>-control`, `mast-<site>-spec`, or `mastXX` (unit).

## Linting

`MAST_unit` uses `ruff` (configured in `pyproject.toml`): line length 125, Python 3.12 target, Black-compatible formatter. Run with:
```bash
ruff check src/
ruff format src/
```

Other projects do not have a `pyproject.toml` — check `required.txt` / `requirements.txt` for dependencies.

## MAST_gui (Django)

- Uses **SQLite** (`db.sqlite3`) for app data, **Django Q2** for background tasks
- Auth: custom `accounts.User` model + `django-allauth` (Google, GitHub, Facebook, Apple)
- Real-time: Django Channels with in-memory channel layer
- Frontend: HTMX for partial updates, JS9 for FITS image viewing
- Requires `MAST_COMMON_PATH` env var (or defaults to `../MAST_common`) so it can import shared models
- The `MAST_gui/settings.py` is under `MAST_gui/MAST_gui/`; `wsgi.py` lives at the project root
