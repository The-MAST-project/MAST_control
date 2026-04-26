# MAST Observatory — Observation Scheduler
System Design Specification
Weizmann Institute of Science — March 2026

## Table of Contents
1. [Overview](#1-overview)
2. [Plan Model](#2-plan-model)
3. [Batch Construction](#3-batch-construction)
4. [Priority and Scheduling Logic](#4-priority-and-scheduling-logic)
5. [Setup Costs Between Batches](#5-setup-costs-between-batches)
6. [Runtime Control Loop](#6-runtime-control-loop)
7. [Predictive Mode](#7-predictive-mode)
8. [Configurable Constants](#8-configurable-constants)
9. [Open Items](#9-open-items)

---

## 1. Overview

The MAST scheduler is a continuous reactive control loop that manages observations across up to 20 robotic telescope units feeding one of two spectrographs (DeepSpec or HighSpec). Only one spectrograph is active at a time. The scheduler builds batches of plans, each batch assigning groups of units to individual targets for simultaneous observation.

The scheduler operates in two modes:

- **Immediate mode** — uses real-time unit status, weather, and current time; builds one batch and executes it; drives the nightly operations control loop
- **Predictive mode** — given a start datetime, assumes all deployed non-maintenance units are operational; simulates the full sequence of batches through the night; outputs a JSON list of predicted batches with timing estimates; advisory only, not binding on operations

---

## 2. Plan Model

### 2.1 Plan Attributes

**Plan** (top-level fields):

| Field | Type | Description |
|-------|------|-------------|
| `ulid` | `str` | Auto-generated ULID; also encoded in the filename |
| `owner` | `str` | Submitting user |
| `merit` | `int` | Scientific priority score, 1 (lowest) to 10 (highest) |
| `autofocus` | `bool` | If true, autofocus is performed before the exposure series |
| `too` | `bool` | Target of Opportunity flag; triggers preemption of running batch |
| `approved` | `bool` | Set by capability owner before plan enters pending pool |
| `production` | `bool` | If false, availability checks are relaxed (testing only) |
| `quorum` | `int` | Minimum operational units required to proceed (default: 1) |
| `timeout_to_guiding` | `float` | Seconds to wait for units to reach guiding (default: 600) |
| `requested_units` | `list[str]` | Unit names explicitly requested by the scientist |
| `allocated_units` | `list[str]` | Unit names allocated by the scheduler |
| `target` | `Target` | Target coordinates and exposure series parameters (see below) |
| `spec_assignment` | `SpectrographModel` | Spectrograph configuration (see below) |
| `constraints` | `ConstraintsModel` | Scheduling constraints (see below) |
| `events` | `list[EventModel]` | Append-only history: `what`, `details`, `when` |

Plan state is not stored as a field — it is tracked by which filesystem subfolder the plan file resides in.

---

**Target** (`plan.target`):

| Field | Type | Description |
|-------|------|-------------|++
| `ra_hours` | `float` | Right ascension in decimal hours |
| `dec_degrees` | `float` | Declination in decimal degrees |
| `requested_exposure_duration` | `float` | Preferred exposure duration (seconds, ≤ 3600) |
| `max_exposure_duration` | `float` | Maximum acceptable exposure duration (seconds, ≤ 3600) |
| `requested_number_of_exposures` | `int` | Number of consecutive exposures per batch (default: 1) |
| `repeats` | `RepeatsModel` | Repetition schedule (see below) |

**RepeatsModel** (`plan.target.repeats`):

| Field | Type | Description |
|-------|------|-------------|
| `every` | `str` | One of: `"Only once"`, `"Once per night"`, `"Twice per night"`, `"As much as possible"` |
| `nights` | `int` | Number of nights this plan must be executed (1–100, default: 1) |

---

**SpectrographModel** (`plan.spec_assignment`):

| Field | Type | Description |
|-------|------|-------------|
| `instrument` | `str` | `"deepspec"` or `"highspec"` |
| `exposure_duration` | `float` | Scheduled exposure duration (seconds); set by scheduler |
| `number_of_exposures` | `int` | Scheduled number of exposures; set by scheduler |
| `calibration` | `CalibrationSettings` | ThAr lamp and filter (see below) |
| `settings` | `HighspecSettings \| DeepspecSettings` | Instrument-specific settings |

**CalibrationSettings** (`plan.spec_assignment.calibration`):

| Field | Type | Description |
|-------|------|-------------|
| `lamp_on` | `bool` | Whether the ThAr calibration lamp should be on (default: false) |
| `filter` | `str` | ThAr filter ID; required when `lamp_on=true`; defaults to `"Empty"` |

**HighspecSettings** (`plan.spec_assignment.settings`, HighSpec only):

| Field | Type | Description |
|-------|------|-------------|
| `disperser` | `Disperser` | Grating/disperser ID; all plans in a batch must share the same disperser |

---

**ConstraintsModel** (`plan.constraints`):

| Field | Type | Description |
|-------|------|-------------|
| `airmass.max` | `float` | Hard upper limit on airmass (1.0–3.0) |
| `moon.max_phase` | `float` | Maximum moon illumination percentage (0–100%) |
| `moon.min_distance` | `float` | Minimum angular distance from the moon (degrees, 0–180) |
| `seeing.max` | `float` | Maximum seeing (arcsec) |
| `time_window` | `TimeWindow` | Observation time window: `start`, `end`, `start_mode`, `end_mode`, `end_after_nights` |

### 2.2 Plan Lifecycle

```
pending → in-progress → completed
                      → failed
                      → canceled
        → postponed
        → deleted
failed | completed | canceled | postponed | expired → pending  (revive)
```

Plan state is represented by the filesystem subfolder the plan file resides in — there is no `state` field on the model. The `approved` field gates entry into the pending pool: plans are submitted and approved before becoming pending.

Only operator or system action can move a plan to `failed`, `canceled`, `postponed`, or `deleted`.

### 2.3 Repeatability and Series

- **Runs per night** (`target.repeats.every`): controls how many times per night a plan is scheduled; values are `"Only once"`, `"Once per night"`, `"Twice per night"`, `"As much as possible"`; the scheduler checks the `events` list to count completions tonight
- **Multi-night plans** (`target.repeats.nights`): number of nights the plan must be executed (1–100); no minimum spacing between nights is enforced
- **Exposure series** (`target.requested_number_of_exposures`): all exposures occur within one batch; batch duration = `num_exposures × exposure_time + (num_exposures − 1) × readout_time`; `readout_time` is a configurable constant (default: 0, pending hardware characterization)

---

## 3. Batch Construction

### 3.1 Homogeneity Constraints

All plans in a batch must satisfy:

- Same spectrograph (`spec_assignment.instrument`: `deepspec` or `highspec`)
- HighSpec only: same disperser (`spec_assignment.settings.disperser`) — candidate plans with a different disperser cannot join the batch

### 3.2 Exposure Time Negotiation

- Batch exposure time = `max(target.requested_exposure_duration)` across all plans in the batch
- Capped at `min(target.max_exposure_duration)` across all plans in the batch
- A candidate plan may only join if its `target.max_exposure_duration ≥` current batch exposure time
- Plans that cannot fit are skipped and remain pending

### 3.3 Calibration Lamp and Filter

- `lamp_on` (batch) = `any(plan.spec_assignment.calibration.lamp_on)` — if any plan requests the lamp, it is on for all
- `filter` (batch) = densest filter among lamp-on plans (`spec_assignment.calibration.filter`)
- Plans that requested a lighter filter receive denser attenuation — acceptable for pipeline calibration
- ThAr lamp warmup/cooldown overhead: configurable constant (default: 0, pending characterization)

### 3.4 Unit Allocation

The scheduler queries operational unit status before each batch. Units are candidates if they are deployed, not in maintenance mode, and responding.

**Explicit unit list** (`requested_units` is non-empty):
- Intersect `requested_units` with currently operational units
- If intersection size ≥ `quorum` → schedule, assign the intersection
- If intersection size < `quorum` → plan is infeasible, skip

**Scheduler-chosen units** (`requested_units` is empty):
- Pool = operational units feeding the required spectrograph
- Assign units from pool up to what is available; proceed if pool size ≥ `quorum`
- If pool size < `quorum` → plan is infeasible, skip

### 3.5 Batch Duration Formula

```
setup_overhead
+ autofocus_time              (if any plan.autofocus is true)
+ num_exposures × exposure_time
+ (num_exposures − 1) × readout_time
```

Setup overhead is computed from the physical moves required between the previous batch and this one (see Section 5).

---

## 4. Priority and Scheduling Logic

### 4.1 Priority Ordering

1. ToO plans take absolute priority over all normal plans
2. Within ToO plans: highest merit wins
3. ToO tie on merit: operator is alerted and given 30 seconds to decide; if no decision, first submitted ToO is batched and others remain pending
4. Normal plans: ranked by merit (1–10, higher is better)
5. HighSpec disperser group tie: longer negotiated batch exposure time wins

### 4.2 Feasibility Filters (evaluated at current time)

Filters are applied as a chain; each step reduces `self.plans` and returns `Self`:

```python
feasible = (
    PlanFilter(pending_plans)
    .astronomical_night()          # current time between twilight limits
    .within_time_window()          # plan.constraints.time_window contains now
    .airmass(max=plan.constraints.airmass.max)
    .moon_phase(max=plan.constraints.moon.max_phase)        # percent
    .moon_separation(min=plan.constraints.moon.min_distance) # degrees
    .quorum_available()            # operational units ≥ plan.quorum (see 3.4)
    .repeats_not_exhausted()       # tonight's count < quota per plan.target.repeats.every
    .plans
)
```

### 4.3 Observing Condition Score (soft ranking within merit tier)

score = w_airmass × (1 / airmass)
      + w_moon    × moon_separation_score
      + w_urgency × time_remaining_score

`time_remaining_score` increases as the observable window closes, preventing targets from being missed before they set.

---

## 5. Setup Costs Between Batches

Each batch transition may incur hardware movement overhead. All durations are computed from known current and target positions:

| Hardware | Notes |
|----------|-------|
| Spectrograph switch | Post move from current to new spectrograph input |
| Filter wheel rotation | ThAr calibration beam filter wheel; time depends on angular delta between positions |
| Grating stage move | HighSpec only; linear stage; time depends on distance between grating positions |
| Lamp warmup/cooldown | Configurable constant (default: 0, pending hardware characterization) |

In predictive mode, the initial stage state is taken from the current actual hardware state. All subsequent transitions are computed from the simulated state after each batch.

---

## 6. Runtime Control Loop

### 6.1 Poll Cycle (every 30 seconds)

1. Query all unit operational states
2. Query spectrograph machine states
3. Query weather conditions
4. If currently exposing: check abort conditions
5. If idle: attempt to build and start next batch

### 6.2 Abort Conditions During Exposure

- Weather unsafe → abort batch, close enclosures, all plans return to pending
- All plans in batch simultaneously below quorum → abort batch, plans return to pending
- Individual unit dropouts during exposure do not abort — the pipeline processes incomplete trace sets gracefully

### 6.3 ToO Preemption

1. ToO plan submitted → detected on next poll cycle (within 30 seconds)
2. Scheduler sends abort signal to current batch
3. Waits for all units and spectrograph to become idle
4. Operator alerted for tie-breaking if multiple ToOs share equal merit
5. 30-second operator decision window; if no response, first submitted ToO is batched
6. Preempted plans return to pending (not failed)

### 6.4 After Batch Completion or Abort

1. Update plan states and append to `events` lists
2. Re-poll conditions
3. If safe → immediately evaluate next batch
4. If unsafe → wait, continue polling

---

## 7. Predictive Mode

### 7.1 Inputs

- `start_datetime`: the datetime from which to begin simulation
- Assumed unit pool: all deployed, non-maintenance units (regardless of actual operational state)
- Assumed conditions: ideal (no weather interruptions)
- Plan queue: current pending pool

### 7.2 Simulation Logic

The predictive scheduler runs the same batch-building logic as immediate mode, advancing a simulated clock:

1. Build next feasible batch from simulated time
2. Compute total batch duration (setup + autofocus + exposures + readout)
3. Advance simulated clock by total batch duration
4. Mark simulated plans as used per their `runs_per_night` quota
5. Repeat until queue exhausted or morning twilight reached

### 7.3 Output Format

JSON array of batch objects:

```json
[
  {
    "batch_id": "string",
    "spectrograph": "deepspec | highspec",
    "grating": "string | null",
    "lamp": true,
    "filter": "string | null",
    "exposure_time": 1800,
    "num_exposures": 1,
    "estimated_start": "2026-03-26T21:14:00Z",
    "estimated_duration_s": 1920,
    "setup_overhead_s": 45,
    "autofocus_overhead_s": 120,
    "plans": [
      {
        "plan_id": "string",
        "target": "string",
        "units": [1, 3, 5, 7],
        "merit": 8,
        "too": false
      }
    ]
  }
]
```

---

## 8. Configurable Constants

| Constant | Description / Default |
|----------|-----------------------|
| `autofocus_time` | Fixed autofocus duration in seconds (default: TBD) |
| `readout_time` | Detector readout time between series exposures in seconds (default: 0) |
| `lamp_warmup_time` | ThAr lamp warmup duration in seconds (default: 0) |
| `lamp_cooldown_time` | ThAr lamp cooldown duration in seconds (default: 0) |
| `too_operator_timeout` | Seconds to wait for operator ToO tie-break decision (default: 30) |
| `poll_interval` | Scheduler poll cycle interval in seconds (default: 30) |
| `twilight_type` | Twilight definition for night boundary (default: astronomical) |

---

## 9. Open Items

- Lamp warmup and cooldown durations — pending hardware characterization
- Autofocus estimated time — to be measured and set as constant
- Readout/save time between series exposures — pending detector characterization
- Filter wheel position-to-rotation-time lookup table — to be provided
- Grating stage position-to-move-time model — to be provided
- Spectrograph post move time — to be measured
- Definition of failure conditions that move a plan from `in-progress` to `failed`
- Minimum S/N threshold or completion fraction for a plan run to count as successful
