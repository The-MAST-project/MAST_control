#!/usr/bin/env python3
"""
Generate ~50 realistic WD mockup plans for MAST planning/scheduling tests.

Output:
  tools/mockup_plans/submitted/PLAN_<ULID>.toml   (25 plans)
  tools/mockup_plans/pending/PLAN_<ULID>.toml     (25 plans, with approved event)

A few plans carry intentional errors to test error handling.
"""

# ── venv auto-loader ──────────────────────────────────────────────────────────
import sys
import os
from pathlib import Path

_venv_python = Path(__file__).resolve().parent.parent / ".venv" / "bin" / "python"
if _venv_python.exists() and Path(sys.executable).resolve() != _venv_python.resolve():
    os.execv(str(_venv_python), [str(_venv_python)] + sys.argv)

# ── imports ───────────────────────────────────────────────────────────────────
import datetime
import tomlkit
import ulid as ulid_lib

# ── target catalogue ──────────────────────────────────────────────────────────
# (name, ra_hours, dec_degrees, magnitude, classification)
WD_TARGETS = [
    # Polluted WDs — DeepSpec broad-band for multi-element absorption
    ("G29-38",           23.480,   5.248,  13.0, "WD Pollution"),
    ("GD 394",           21.221,  50.069,  13.4, "WD Pollution"),
    ("SDSS J1228+1040",  12.475,  10.669,  16.5, "WD Pollution"),
    ("WD 1257+278",      13.000,  27.533,  15.0, "WD Pollution"),
    ("GD 16",             0.571, -27.144,  14.0, "WD Pollution"),
    ("WD 1350-090",      13.883,  -9.267,  15.8, "WD Pollution"),
    ("WD 1840+042",      18.717,   4.250,  14.8, "WD Pollution"),
    ("WD 0038-226",       0.683, -22.367,  15.0, "WD Pollution"),
    ("GD 61",             5.917,  45.217,  13.7, "WD Pollution"),
    ("LHS 1734",          3.860,  34.059,  14.1, "WD Pollution"),
    # Atmospheric composition / standard hot DAs
    ("G191-B2B",          5.092,  52.831,  11.8, "WD Atmospheric Composition"),
    ("GD 153",           12.950,  22.031,  13.4, "WD Atmospheric Composition"),
    ("GD 71",             5.874,  15.887,  13.0, "WD Atmospheric Composition"),
    ("HZ 43",            13.273,  29.099,  12.9, "WD Atmospheric Composition"),
    ("Van Maanen's Star", 0.819,   5.389,  12.4, "WD Atmospheric Composition"),
    ("Feige 24",          2.585,   3.733,  12.4, "WD Atmospheric Composition"),
    ("WD 2359-434",       0.036, -43.163,  13.8, "WD Atmospheric Composition"),
    ("WD 0553+053",       5.940,   5.400,  13.9, "WD Atmospheric Composition"),
    ("WD 1620-391",      16.393, -39.230,  11.2, "WD Atmospheric Composition"),
    ("EG 21",             3.739,  18.367,  13.5, "WD Atmospheric Composition"),
    # WD binary RV — HighSpec at Ca II H&K / Hα
    ("WD 1455+298",      14.960,  29.583,  15.5, "WD Binary RV"),
    ("WD 0141-675",       1.717, -67.267,  13.8, "WD Binary RV"),
    ("WD 1631+781",      16.526,  78.100,  15.1, "WD Binary RV"),
    ("WD 1917+386",      19.323,  38.733,  14.5, "WD Binary RV"),
    ("WD 0136+251",       1.657,  25.367,  15.9, "WD Binary RV"),
    ("WD 0710+741",       7.233,  74.050,  15.4, "WD Binary RV"),
    ("WD 1304+227",      13.098,  22.459,  14.7, "WD Binary RV"),
    ("Stein 2051B",       4.520,  58.977,  12.4, "WD Binary RV"),
    # WD exoplanet transits
    ("WD 1145+017",      11.813,   1.467,  17.3, "WD Exoplanet Transit"),
    ("WD 0806-661",       8.150, -66.300,  13.8, "WD Exoplanet Transit"),
    ("WD 1257+278 B",    13.010,  27.600,  16.2, "WD Exoplanet Transit"),
    # Flash spectroscopy — HighSpec rapid response to LAST/ULTRASAT alerts
    ("SN 2024abf",       10.503,  15.234,  16.0, "Flash Spectroscopy"),
    ("AT 2024gfo",       22.294, -10.512,  17.1, "Flash Spectroscopy"),
    ("SN 2024ape",        3.841,  41.512,  16.8, "Flash Spectroscopy"),
    # Variable / pulsating WDs
    ("ZZ Ceti",           1.604,  11.342,  14.5, "Variable Star"),
    ("KIC 8626021",      19.625,  44.171,  17.5, "Variable Star"),
    # Cataclysmic variable / accreting WD
    ("GW Lib",           15.141,  23.728,  18.0, "Cataclysmic Variable"),
    # ULTRASAT follow-up
    ("ULTRASAT-J1058+32", 10.975, 32.417,  18.5, "ULTRASAT Follow-Up"),
    # Additional atmospheric / misc to reach 50
    ("GJ 440",           11.762, -64.841,  11.5, "WD Atmospheric Composition"),
    ("Feige 55",         10.661,  43.067,  14.2, "WD Atmospheric Composition"),
    ("WD 0752-676",       7.867, -67.733,  14.2, "WD Atmospheric Composition"),
    ("WD 2115-560",      21.317, -55.817,  15.3, "WD Atmospheric Composition"),
    ("HS 2253+8023",     22.917,  80.650,  15.0, "WD Atmospheric Composition"),
    ("WD 0322-019",       3.368,  -1.867,  13.2, "WD Atmospheric Composition"),
    ("WD 0348+339",       3.800,  34.100,  15.6, "WD Binary RV"),
    ("WD 1145+074",      11.813,   7.250,  16.1, "WD Pollution"),
    ("WD 1257+272",      13.015,  27.200,  15.5, "WD Pollution"),
    ("Procyon B",         7.655,   5.225,  10.7, "WD Atmospheric Composition"),
    ("WD 0136+768",       1.700,  77.100,  16.3, "WD Binary RV"),
    ("WD 1840+098",      18.720,   9.870,  15.7, "WD Pollution"),
]

assert len(WD_TARGETS) == 50, f"Expected 50 targets, got {len(WD_TARGETS)}"

# ── science case descriptions ─────────────────────────────────────────────────
SCIENCE_CASES = {
    "WD Pollution": (
        "Spectroscopic monitoring to characterize accreted planetary material. "
        "DeepSpec's 380-850nm bandpass enables simultaneous detection of Ca, Mg, Fe and Si "
        "absorption features from tidally disrupted planetesimals."
    ),
    "WD Atmospheric Composition": (
        "Atmospheric parameter determination (Teff, log g) via Balmer line profile fitting. "
        "DeepSpec broad-band coverage provides clean continuum for reliable normalization."
    ),
    "WD Binary RV": (
        "Radial velocity monitoring campaign to determine orbital parameters. "
        "HighSpec R~20,000 at Ca II H&K lines provides ~15 km/s precision, "
        "sufficient to characterize compact binary orbits."
    ),
    "WD Exoplanet Transit": (
        "Photometric and spectroscopic monitoring for transiting debris and planetesimals. "
        "Target shows irregular transit-like dimming events consistent with "
        "disintegrating rocky bodies in a close-in orbit."
    ),
    "Flash Spectroscopy": (
        "Rapid spectroscopic follow-up triggered by LAST/ULTRASAT alert within hours of explosion. "
        "HighSpec classification of circumstellar material via flash ionization lines "
        "before the early CSM signatures fade."
    ),
    "Variable Star": (
        "Time-series spectroscopy of pulsating WD to characterize g-mode oscillation spectrum. "
        "Repeated short exposures to build phase-resolved spectral time series."
    ),
    "Cataclysmic Variable": (
        "Spectroscopic monitoring of accreting WD system. "
        "HighSpec resolves the double-peaked Hα emission from the accretion disk "
        "and tracks radial velocity variations through the orbital cycle."
    ),
    "ULTRASAT Follow-Up": (
        "Ground-based spectroscopic follow-up of ULTRASAT UV transient alert. "
        "DeepSpec classification to determine transient type and redshift."
    ),
}

# ── instrument assignment ─────────────────────────────────────────────────────
HIGHSPEC_CLASSES = {"WD Binary RV", "Flash Spectroscopy", "Cataclysmic Variable", "Variable Star"}


def instrument_for(classification: str) -> str:
    return "highspec" if classification in HIGHSPEC_CLASSES else "deepspec"


# ── exposure time logic ───────────────────────────────────────────────────────
def exposure_params(magnitude: float, instrument: str) -> tuple[float, int]:
    """Returns (exposure_duration_s, number_of_exposures)."""
    if instrument == "deepspec":
        if magnitude < 13:
            return 600.0, 3
        elif magnitude < 15:
            return 1200.0, 3
        elif magnitude < 17:
            return 1800.0, 3
        else:
            return 2400.0, 5
    else:  # highspec
        if magnitude < 13:
            return 900.0, 2
        elif magnitude < 15:
            return 1800.0, 2
        elif magnitude < 17:
            return 2700.0, 3
        else:
            return 3600.0, 3


# ── TOML helpers ──────────────────────────────────────────────────────────────

def now_zulu() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def make_plan_toml(
    ulid_str: str,
    name: str,
    ra_hours: float,
    dec_degrees: float,
    magnitude: float,
    classification: str,
    instrument: str,
    exp_dur: float,
    n_exp: int,
    merit: int,
    pool: str,
    lamp_on: bool = False,
    error_type: str | None = None,
) -> str:
    doc = tomlkit.document()

    doc.add("ulid", ulid_str)
    doc.add("owner", "mockup.scientist")
    doc.add("mockup", True)
    doc.add("merit", merit)
    doc.add("requested_units", ["mast00"])

    if pool == "pending":
        doc.add("approved", True)

    # ── target ────────────────────────────────────────────────────────────────
    if error_type != "missing_target":
        target = tomlkit.table()
        if error_type == "bad_ra":
            target.add("ra_hours", 25.0)          # out of range — hard error
        else:
            target.add("ra_hours", ra_hours)
        target.add("dec_degrees", dec_degrees)
        target.add("name", name)
        target.add("magnitude", magnitude)
        target.add("requested_exposure_duration", exp_dur)
        target.add("requested_number_of_exposures", n_exp)

        science = tomlkit.table()
        science.add("classification", classification)
        science.add("case", SCIENCE_CASES[classification])
        target.add("science", science)

        doc.add("target", target)

    # ── spec_assignment ───────────────────────────────────────────────────────
    if error_type != "missing_spec":
        spec = tomlkit.table()
        if error_type == "null_instrument":
            pass                                   # omit instrument — soft error
        else:
            spec.add("instrument", instrument)

        if lamp_on:
            cal = tomlkit.table()
            cal.add("lamp_on", True)
            cal.add("filter", "ND3000")
            spec.add("calibration", cal)

        doc.add("spec_assignment", spec)

    # ── events (pending: approved) ────────────────────────────────────────────
    if pool == "pending":
        events = tomlkit.aot()
        event = tomlkit.table()
        event.add("what", "approved")
        event.add("detail", "mockup")
        event.add("when", now_zulu())
        events.append(event)
        doc.add("events", events)

    return tomlkit.dumps(doc)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    out_root = Path(__file__).resolve().parent / "mockup_plans"
    submitted_dir = out_root / "submitted"
    pending_dir   = out_root / "pending"
    submitted_dir.mkdir(parents=True, exist_ok=True)
    pending_dir.mkdir(parents=True, exist_ok=True)

    # Intentional errors — indices into WD_TARGETS
    # hard errors (fail to load): indices 3 (bad_ra), 27 (missing_target)
    # soft errors (load OK, fail -valid): indices 10 (missing_spec), 35 (null_instrument)
    ERROR_MAP = {
        3:  "bad_ra",
        10: "missing_spec",
        27: "missing_target",
        35: "null_instrument",
    }

    generated = {"submitted": 0, "pending": 0, "errors": 0}

    for i, (name, ra, dec, mag, classification) in enumerate(WD_TARGETS):
        pool        = "submitted" if i < 25 else "pending"
        instrument  = instrument_for(classification)
        exp_dur, n_exp = exposure_params(mag, instrument)
        merit       = (i % 10) + 1
        lamp_on     = (i % 4 == 0)                # every 4th plan has ThAr lamp
        error_type  = ERROR_MAP.get(i)
        ulid_str    = str(ulid_lib.ULID())
        filename    = f"PLAN_{ulid_str}.toml"
        folder      = submitted_dir if pool == "submitted" else pending_dir

        toml_str = make_plan_toml(
            ulid_str=ulid_str,
            name=name,
            ra_hours=ra,
            dec_degrees=dec,
            magnitude=mag,
            classification=classification,
            instrument=instrument,
            exp_dur=exp_dur,
            n_exp=n_exp,
            merit=merit,
            pool=pool,
            lamp_on=lamp_on,
            error_type=error_type,
        )

        (folder / filename).write_text(toml_str)
        generated[pool] += 1
        if error_type:
            generated["errors"] += 1
            print(f"  [ERROR:{error_type}] {pool}/{filename}  {name}")
        else:
            print(f"  [{pool}] {filename}  {name}  {instrument}  merit={merit}")

    print(f"\nGenerated {generated['submitted']} submitted, "
          f"{generated['pending']} pending, "
          f"{generated['errors']} with intentional errors.")


if __name__ == "__main__":
    main()
