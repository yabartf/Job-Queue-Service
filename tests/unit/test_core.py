"""L1-40 .. L1-41 — clock, state transitions, and log redaction."""

from datetime import UTC, datetime, timedelta

import pytest

from app.core.clock import SystemClock
from app.core.enums import (
    ALLOWED_TRANSITIONS,
    CANCELLABLE_STATUSES,
    TERMINAL_STATUSES,
    JobStatus,
    can_transition,
)
from app.core.logging import payload_fingerprint
from tests.doubles import FrozenClock


def test_l1_40_frozen_clock_only_moves_when_told():
    clock = FrozenClock(datetime(2026, 1, 1, tzinfo=UTC))
    first = clock.now()
    assert clock.now() == first

    clock.advance(timedelta(hours=2))
    assert clock.now() == first + timedelta(hours=2)


def test_l1_40b_system_clock_is_timezone_aware_utc():
    now = SystemClock().now()
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (JobStatus.SCHEDULED, JobStatus.PENDING),
        (JobStatus.SCHEDULED, JobStatus.CANCELLED),
        (JobStatus.PENDING, JobStatus.PROCESSING),
        (JobStatus.PENDING, JobStatus.CANCELLED),
        (JobStatus.PROCESSING, JobStatus.COMPLETED),
        (JobStatus.PROCESSING, JobStatus.FAILED),
        (JobStatus.PROCESSING, JobStatus.PENDING),
        (JobStatus.FAILED, JobStatus.PENDING),
    ],
)
def test_l1_41_allowed_transitions(source, target):
    assert can_transition(source, target)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        # Cancelling work already in flight would need cooperative interruption
        # of a running handler, which is out of scope.
        (JobStatus.PROCESSING, JobStatus.CANCELLED),
        (JobStatus.COMPLETED, JobStatus.PENDING),
        (JobStatus.CANCELLED, JobStatus.PENDING),
        (JobStatus.PENDING, JobStatus.COMPLETED),
        (JobStatus.SCHEDULED, JobStatus.PROCESSING),
    ],
)
def test_l1_41b_disallowed_transitions(source, target):
    assert not can_transition(source, target)


def test_l1_41c_every_status_has_a_transition_entry():
    assert set(ALLOWED_TRANSITIONS) == set(JobStatus)


def test_l1_41d_terminal_statuses_lead_nowhere():
    for status in TERMINAL_STATUSES:
        assert ALLOWED_TRANSITIONS[status] == frozenset()


def test_l1_41e_failed_is_not_terminal():
    """A failed job can be manually retried, so it is not a dead end."""
    assert JobStatus.FAILED not in TERMINAL_STATUSES
    assert can_transition(JobStatus.FAILED, JobStatus.PENDING)


def test_l1_41f_cancellable_statuses_can_reach_cancelled():
    for status in CANCELLABLE_STATUSES:
        assert can_transition(status, JobStatus.CANCELLED)


def test_payload_fingerprint_hides_contents():
    payload = {"to": "user@example.com", "body": "sensitive contents"}
    fingerprint = payload_fingerprint(payload)

    rendered = str(fingerprint)
    assert "user@example.com" not in rendered
    assert "sensitive" not in rendered
    assert fingerprint["payload_bytes"] > 0
    assert len(fingerprint["payload_sha256"]) == 16


def test_payload_fingerprint_is_order_independent():
    assert payload_fingerprint({"a": 1, "b": 2}) == payload_fingerprint({"b": 2, "a": 1})
