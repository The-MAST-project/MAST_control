# Addendum: Multi-Site Scheduling
Weizmann Institute of Science — March 2026

## A.1 Scope and Motivation

This addendum extends the MAST Scheduler design to cover operation across multiple physical observatory sites. The scheduler has two multi-site aims:

- **Throughput** — keep spectrographs at all sites busy; a unified scheduler manages the full cross-site plan queue rather than independent per-site queues
- **Interferometry (future)** — coordinate simultaneous observation of the same target from two or more sites; the scientific and time-coordination implications of this mode are deferred to Phase 2

## A.2 Known Sites

| Site ID | Location | Buildings | Units (max) | Status |
|---------|----------|-----------|-------------|--------|
| ***wis*** | Weizmann Institute, IL | 1 | 1 | Operational |
| ***ns*** | Neot Smadar, IL | north, south | 20 (10 each) | Planned |
| ***nam*** | Namibia | TBD | TBD | Aspirational |

Sites are stored in the MongoDB `sites` collection. Each site document contains a list of buildings; each building contains a list of its units.

## A.3 Site Hierarchy and Unit Naming

Units are addressed within a three-level hierarchy: **site → building → unit**.

The unit name parser supports the scheme:

```
[<site>:][<building>:]<units-list>
```

where `<units-list>` is a comma-separated list of unit names or ranges using `-` notation.

**Examples:**

| Expression | Meaning |
|------------|---------|
| `ns:north:2-4` | Units 2, 3, 4 in building north at site ns |
| `ns:south:11` | Unit 11 in building south at site ns |
| `ns:south:14-16` | Units 14, 15, 16 in building south at ns |
| `ns:south:11,ns:south:14-16` | Units 11, 14, 15, 16 in building south at ns |

When site and building prefixes are omitted, the local control machine's site and building context is assumed.

## A.4 Infrastructure Assumptions

- Reliable network connectivity between all sites; the two Israeli sites (***wis***, ***ns***) share a VPN
- Each site has its own spectrograph machine, named `mast-<site>-spec`
- One control machine may manage more than one site, as determined by its configuration
- The scheduler is logically centralized but operationally aware of per-site hardware state

## A.5 Plan Model Extensions

A Plan remains site-scoped: all units in a plan belong to the same site. No changes to the core Plan model are required for Phase 1.

A `site_id` field is added to the Plan:

| Field | Description |
|-------|-------------|
| `site_id` | The site this plan's units belong to |

In Phase 1, the scheduler builds batches per site independently, using the same logic defined in Sections 3–5, and coordinates across sites only at the queue level.

## A.6 Phase 1 — Israeli Sites (Throughput Scheduling)

**Scope:** ***wis*** and ***ns***

**Scheduler behavior:**

- Maintains a single unified pending plan queue across both sites
- On each poll cycle, evaluates feasibility per site independently (unit availability, weather, visibility)
- Builds and dispatches batches at each site in parallel, independently
- A site with no feasible plans idles; the other site continues unaffected

**Visibility checking:**

***wis*** and ***ns*** are approximately 80 km apart at similar latitude. For Phase 1, a target above the airmass limit at one Israeli site is assumed to be above it at the other. No per-site visibility computation is required beyond what is already defined in Section 4.2.

**Spectrograph coordination:**

Each site operates its own spectrograph (`mast-wis-spec`, `mast-ns-spec`) independently. No cross-site spectrograph constraint applies.

**Open items for Phase 1:**

- Per-site weather feed integration
- Per-site unit status polling (the poll cycle in Section 6.1 extends to cover all sites)
- Scheduler configuration: which control machine is responsible for which sites

## A.7 Phase 2 — Multi-Site Observation (Interferometry)

**Scope:** 2+ sites including ***nam***; deferred to a future development stage

### A.7.1 MultiSiteObservation Entity

A **MultiSiteObservation (MSO)** groups two or more site-scoped Plans for coordinated simultaneous observation of the same target. It is an optional layer — standalone Plans continue to operate as defined in the base specification.

| Field | Description |
|-------|-------------|
| `id` | Unique identifier |
| `target` | Sky coordinates shared by all constituent plans |
| `spectrograph_type` | Must be identical across all constituent plans (`deepspec` or `highspec`) |
| `plans` | List of Plan references, one per participating site |
| `time_window` | UTC start/end within which all sites must begin observing |
| `coherence_tolerance` | Maximum permitted start-time spread across sites |
| `quorum_sites` | Minimum number of sites that must succeed for the MSO to be valid (TBD) |
| `status` | `pending` \| `scheduled` \| `executing` \| `completed` \| `failed` |

Each constituent Plan carries a back-reference:

| Field | Description |
|-------|-------------|
| `mso_id` | Reference to parent MSO, or `null` if standalone |

### A.7.2 Scheduler Responsibilities for MSOs

- **Spectrograph constraint:** enforced at MSO creation — all constituent plans must specify the same spectrograph type
- **Visibility validation:** before scheduling an MSO, the scheduler verifies the target is above the airmass limit at all participating sites within the requested time window, using per-site horizon models; this is a scheduler responsibility, not a pre-submission check
- **Time coordination:** the scheduler must start all constituent plans within `coherence_tolerance` of each other; the mechanism for cross-site start synchronization is TBD
- **MSO-level quorum:** TBD — definition of how many sites must succeed for the MSO outcome to be counted as a valid observation

### A.7.3 Visibility Considerations for ***nam***

Namibia (~23°S) has substantially different sky coverage from the Israeli sites (~31°N). When ***nam*** is a participant:

- Declination overlap is partial — the scheduler must compute per-site altitude profiles independently
- LST offset between Israel and Namibia is significant — the shared observable window may be narrow
- Some targets observable from Israel may be below the horizon or at unacceptable airmass from Namibia, and vice versa

Phase 2 requires a per-site horizon model as a new configurable input.

## A.8 Additions to Configurable Constants

| Constant | Description / Default |
|----------|-----------------------|
| `sites` | List of active site IDs managed by this control instance |
| `per_site_weather_feeds` | Mapping of site ID to weather data source |
| `mso_coherence_tolerance` | Default max start-time spread for MSOs in seconds (Phase 2, TBD) |

## A.9 Additions to Open Items

- Per-site weather feed integration for ***ns*** and ***nam***
- Poll cycle extension to cover multi-site unit and spectrograph state
- Per-site horizon models (required for Phase 2 visibility checking)
- MSO-level quorum definition — minimum sites required for a valid interferometric observation
- Cross-site start synchronization mechanism for MSOs (time coordination protocol)
- MSO lifecycle: how constituent Plan failures propagate to MSO status
