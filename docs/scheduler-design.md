# MAST Scheduler Design

## Overview

The scheduler selects batches of approved observation plans and executes them nightly between dusk and dawn. It runs on the controller host alongside the existing FastAPI server.

---

## Filesystem Layout

Files are moved between folders to reflect state transitions. File naming: `PLAN_<ulid>.toml`, `ASSIGNMENT_<ulid>.toml`.

---

## Data Models

### Plan (additions to existing model)

| Field                    | Type          | Default | Notes                                                     |
| ------------------------ | ------------- | ------- | --------------------------------------------------------- |
| `approved`               | `bool`        | `False` | Toggled by authorized users in the GUI                    |
| `too`                    | `bool`        | `False` | Target-of-opportunity; preempts current batch immediately |
| `calibration_lamp`       | `bool`        | `False` | Request calibration lamp on during exposure               |
| `calibration_filter`     | `str \| None` | `None`  | ND filter value (numeric string)                          |
| `observations_requested` | `int`         | `1`     | Total observations desired                                |
| `observations_completed` | `int`         | `0`     | Incremented on each successful batch                      |

Existing fields: `autofocus: bool`, `merit: int`, `quorum: int`, `timeout_to_guiding: int`, `spec`, `constraints`.

### Batch

---

## Scheduler Lifecycle

---

## Batch Formation

1. Take all plans with `approved=True` from `pending/`.
2. Apply the **elimination pipeline** (see below).
3. Group survivors by spectrograph type (`highspec` / `deepspec`).
4. For each group, greedily assign available operational units up to capacity.
5. Choose the group whose batch fills the **most units**. Ties broken by total `merit` sum.
6. Derive batch fields as the **max** of respective values across member plans.
7. `calibration_filter`: highest numeric ND value among plans that request the lamp.

Spectrograph type may freely alternate between consecutive batches.

---

## Elimination Pipeline

Applied in order before each batch selection:

| Stage           | Rule                                                                                               |
| --------------- | -------------------------------------------------------------------------------------------------- |
| **Approval**    | `approved=True` only                                                                               |
| **Time window** | `constraints.time_window` must overlap the scheduling window                                       |
| **Visibility**  | Target above horizon throughout the window (`astropy` + site lat/lon/elevation)                    |
| **Airmass**     | `constraints.airmass.max` satisfied at window start and end                                        |
| **Moon**        | `moon.max_phase` and `moon.min_distance` satisfied at window start and end                         |
| **Weather**     | _Next-in-line_: live sensor check via `safety_get_sensor()`. _Prediction_: assume within tolerance |

---

## Merit Sorting

After elimination, survivors are sorted in two tiers:

- **Tier 1** — `too=True` plans: `merit` descending, then westernmost target (smallest hour angle).
- **Tier 2** — `too=False` plans: same sub-sort.

TOO preemption: a newly submitted TOO immediately aborts the current assignment and executes without waiting for the next batch boundary.

---

## Time Estimation

`autofocus_timeout` is stored in the config DB (site-level). Units whose assigned plan has `autofocus=False` skip autofocus and proceed directly to acquisition and guiding.

---

## predict(count=None)

- Predicts next-in-line batches starting from now.
- Optional `count` limits the number of batches returned.
- Simulates consecutive batches with 1-minute gaps between them.
- Uses `deployed_units` minus `units_in_maintenance` as the available pool.
- Assumes weather within tolerance throughout.
- Returns `list[Batch]` with `estimated_duration` populated.

---

## recover()

Called at scheduler startup. If any `ASSIGNMENT_<ulid>.toml` exists in `assignments/in-progress/`:

- Move all its member plans back to `plans/pending/`.
- Move the assignment file to `assignments/failed/` with `reason_aborted = "crash_recovery"`.

---

## FastAPI Endpoints

| Method | Path                                  | Description                                                                  |
| ------ | ------------------------------------- | ---------------------------------------------------------------------------- |
| `POST` | `/scheduler/assignment`               | Form next batch; write to `assignments/in-progress/`                         |
| `PUT`  | `/scheduler/assignment/execute`       | Execute the current in-progress assignment                                   |
| `PUT`  | `/scheduler/assignment/abort`         | Abort current assignment. Body: `{"reason": "user\|safety\|too_preemption"}` |
| `GET`  | `/scheduler/predict?count=N`          | Return predicted batch list                                                  |
| `PUT`  | `/scheduler/plans/{ulid}/revive`      | Move one `failed` plan back to `pending`                                     |
| `PUT`  | `/scheduler/assignment/{ulid}/revive` | Bulk-revive all `failed` plans of an aborted assignment                      |

The safety monitor calls `PUT /scheduler/assignment/abort` directly over HTTP. The scheduler holds the active assignment reference — no ULID is required in the abort call.

---

## Prerequisites

| What                                                                                                            | Where                                     | Status       |
| --------------------------------------------------------------------------------------------------------------- | ----------------------------------------- | ------------ |
| `latitude`, `longitude`, `elevation`                                                                            | Config DB `sites` collection              | ❌ Missing   |
| `autofocus_timeout`                                                                                             | Config DB `sites` collection              | ❌ Missing   |
| `too`, `approved`, `calibration_lamp`, `calibration_filter`, `observations_requested`, `observations_completed` | `Plan` model                              | ❌ Missing   |
| `Batch`, `SchedulerAssignment` models                                                                           | `common/models/batch.py`                  | ❌ New file  |
| Wire `do_execute_plan` → `Plan.execute()`                                                                       | `control/planning.py`                     | ❌ Stub only |
| ULID generation                                                                                                 | `python-ulid` (already in `required.txt`) | ✅ Available |
