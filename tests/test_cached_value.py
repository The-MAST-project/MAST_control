"""`CachedValue`: when a status is due for another fetch."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from control.controller import CachedValue


def _cached(interval_seconds: float = 30) -> CachedValue:
    return CachedValue(interval_seconds=interval_seconds, api=None, site_name="ns", machine_name="mast01")


def test_a_value_never_fetched_needs_a_refresh():
    assert _cached().needs_refresh()


def test_a_recent_attempt_does_not_need_a_refresh():
    cached = _cached(30)
    cached.last_attempt = datetime.now(UTC) - timedelta(seconds=5)

    assert not cached.needs_refresh()


def test_an_attempt_older_than_the_interval_needs_a_refresh():
    cached = _cached(30)
    cached.last_attempt = datetime.now(UTC) - timedelta(seconds=31)

    assert cached.needs_refresh()


def test_a_failed_attempt_still_counts_as_an_attempt():
    # needs_refresh() goes by last_attempt, not last_success: an unreachable unit is retried
    # on the interval, not on every 2-second tick.
    cached = _cached(30)
    cached.last_attempt = datetime.now(UTC)
    cached.last_success = None

    assert not cached.needs_refresh()


def test_time_since_last_success():
    cached = _cached()
    assert cached.time_since_last_success() is None

    cached.last_success = datetime.now(UTC) - timedelta(seconds=10)
    assert timedelta(seconds=10) <= cached.time_since_last_success() < timedelta(seconds=11)
