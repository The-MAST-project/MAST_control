# Scheduler design

Summary

- The scheduler accepts a datetime for which to schedule. If None is provided it schedules for "now".
- The scheduler produces a Batch. A Batch is a list of Plans that will run at the scheduled datetime.

Batch creation procedure

- The scheduler examines all pending Plans and reduces them by elimination into a Batch.
- Available resources are the deployed units that are not currently in maintenance.
- A Batch's time window is the longest time window among all Plans included in the Batch.
- Plans that specify required units are considered first; those units are reserved/allocated accordingly.

Batch composition constraints

- All Plans in a Batch must specify the same spectrograph (i.e. a Batch binds to one spectrograph).
- A Batch's exposure length is equal to the maximal exposure length required by any Plan in the Batch.
- To be decided: behavior when some Plans require calibration exposures and others do not (e.g., whether calibrations are run per-Plan, once per-Batch, or disallow mixing).

Plan elimination rules

A Plan is eliminated (excluded from the Batch) if any of the following apply:

- The plan specifies a required number of units but there are not enough units available to allocate.
- The plan does not specify a required number of units and there is not at least one unit available to allocate.
- The Target (RA/Dec) is not visible during the Batch time window.
- The airmass exceeds constraint.airmass.max at any point during the Batch time window.
- The Moon separation/distance violates constraint.moon.max_distance during the Batch time window.
- The Plan's time window starts earlier than constraint.time_window.start.

Prediction mode

- In prediction mode the scheduler generates a list of consecutive Batches starting at the predicted datetime.
- Batches follow one another with a 1-minute gap between them.
- Prediction mode assumes all deployed (and not in maintenance) units are available (i.e., ignores transient assignments).

Notes / Clarifications

- Visibility, airmass and moon-distance are evaluated at the Batch's time window start and end and may cause elimination if they're not in the constraint ranges.
- Plans that bind specific units must be allocated first, and those allocations must reduce the available units pool for subsequently considered Plans.
- The scheduler should be deterministic given the same input set, time and resource state.
- Prediction mode should not modify persistent state (it is read-only/simulation).

Execution semantics

- A Batch may be promoted to an Assignment for execution. When executed, all Plans in the Assignment are worked on simultaneously.
- Execution order within an Assignment:
  1. Allocated units start and achieve 'guiding'.
  2. After the units have started guiding, the selected spectrograph begins exposures for the Assignment.
- The Batch's exposure length (maximal Plan exposure) and shared spectrograph binding apply to the simultaneous execution — the spectrograph runs according to the Batch-level exposure plan while units continue guiding.
- Behavior for mixed calibration requirements (some Plans require calibration, others do not) remains to be decided.

System context & configuration

- Deployment topology:
  - Up to ~20 MAST units (Windows IoT machines) control telescopes; 1 MAST-spec (spectrograph controller); 1 MAST-controller (Linux, e.g. mast-wis-control) hosts scheduler and config DB.
  - Example sites: WIS (development) and NS (production). Unit naming supports short (e.g. '0','w') and long ('mast00','mastw') formats; short names are resolved to the local site's namespace.
  - Units are grouped by buildings/rows (affects UI grouping and physical placement but also can influence allocation policies).
- Configuration DB:
  - MongoDB is the canonical configuration and event store on the controller (collections: groups, services, sites, specs, units, users, events).
  - Units/specs use a common → specific merge pattern (read: merge common config with per-unit overrides; write: store only delta).
  - Scheduler reads unit deployment/maintenance state from this DB; "available resources" = deployed AND not in maintenance.

Control architecture (implications for scheduler)

- Centralized ControlApi model: GUI and external clients interact with units/specs via the controller (Controller → UnitApi / SpecApi). Scheduler uses the same controller APIs for allocation and dispatch.
- All API calls return CanonicalResponse; scheduler must interpret succeeded/failed and log errors consistently.
- Centralization implies scheduler can enforce safety and transaction semantics (atomic multi-unit operations) and rely on consistent state from the controller DB.

Plans, TOML fields and scheduler inputs

- The scheduler consumes Plans (TOML + MongoDB events). Important plan fields the scheduler requires:
  - ulid, status (Approved), merit, quorum, timeout_to_guiding, autofocus, spec.instrument, spec.exposure, constraints (airmass, moon.min_distance/max_phase, seeing), constraints.time_window, observations_requested, observations_completed, run_folder, toml_file
- Only Plans in Approved (eligible) state are considered for batch formation. Draft/Submitted/Rejected are ignored by scheduler.
- The scheduler must update plan TOML and also emit MongoDB events (hybrid logging) when state changes occur (scheduled, succeeded, failed).

File organization & event logging (reference)

- File layout (plans/assignments/observations directories) is the authoritative file-based snapshot; scheduler reads from and moves TOML files between status folders when scheduling/assignment state changes.
- Event logging: hybrid approach
  - TOML files store current state and key lifecycle timestamps for fast snapshot loading.
  - MongoDB stores granular event streams (unit timeouts, spec failures, assignment events) for auditing and UI timelines.
  - Scheduler commits state by updating TOML (state snapshot) and writing corresponding MongoDB events (consistency mechanism).

Scheduler runtime notes

- Planning (prediction) mode must be read-only and use the config DB to assume all deployed/non-maintenance units are available. It should not move TOML files or emit persistent events.
- Real-time scheduling uses current unit/spec operational status (from controller/status APIs) when deciding allocation and when forming Batches → Assignments.
- Merit/priority logic from the GUI design applies: merit is a tiebreaker; failures may increase merit for retries (scheduler will implement a defined policy).
