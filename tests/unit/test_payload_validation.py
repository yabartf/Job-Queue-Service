"""L1-10 .. L1-18 — payload schemas, run across every job type."""

import pytest

from app.core.enums import JobType
from app.core.errors import PayloadValidationError
from app.jobs.registry import get_job_class

VALID_PAYLOADS: dict[JobType, dict] = {
    JobType.EMAIL: {"to": "user@example.com", "subject": "Hi", "body": "Hello"},
    JobType.WEBHOOK: {"url": "https://example.com/hook"},
    JobType.REPORT: {
        "report_type": "sales",
        "date_from": "2026-01-01",
        "date_to": "2026-02-01",
    },
    JobType.BATCH: {"items": ["a", "b"], "operation": "index"},
}

REQUIRED_FIELDS: dict[JobType, list[str]] = {
    JobType.EMAIL: ["to", "subject", "body"],
    JobType.WEBHOOK: ["url"],
    JobType.REPORT: ["report_type", "date_from", "date_to"],
    JobType.BATCH: ["items", "operation"],
}


def parse(job_type: JobType, payload: dict):
    return get_job_class(job_type).parse_payload(payload)


@pytest.mark.parametrize("job_type", list(JobType))
def test_l1_10_valid_payload_parses(job_type):
    parsed = parse(job_type, VALID_PAYLOADS[job_type])
    assert isinstance(parsed, get_job_class(job_type).Payload)


@pytest.mark.parametrize(
    ("job_type", "field"),
    [(t, f) for t, fields in REQUIRED_FIELDS.items() for f in fields],
)
def test_l1_11_missing_required_field_is_named(job_type, field):
    payload = {k: v for k, v in VALID_PAYLOADS[job_type].items() if k != field}
    with pytest.raises(PayloadValidationError) as excinfo:
        parse(job_type, payload)
    assert any(d["field"] == f"payload.{field}" for d in excinfo.value.details)


@pytest.mark.parametrize("job_type", list(JobType))
def test_l1_12_unknown_extra_field_is_rejected(job_type):
    payload = VALID_PAYLOADS[job_type] | {"surprise": "value"}
    with pytest.raises(PayloadValidationError) as excinfo:
        parse(job_type, payload)
    assert any("surprise" in d["field"] for d in excinfo.value.details)


@pytest.mark.parametrize(
    ("field", "length"),
    [("subject", 200), ("body", 10_000)],
)
def test_l1_13_email_fields_accept_their_maximum(field, length):
    payload = VALID_PAYLOADS[JobType.EMAIL] | {field: "x" * length}
    assert parse(JobType.EMAIL, payload)


@pytest.mark.parametrize(
    ("field", "length"),
    [("subject", 201), ("body", 10_001), ("subject", 0), ("body", 0)],
)
def test_l1_14_email_fields_reject_one_past_the_bound(field, length):
    payload = VALID_PAYLOADS[JobType.EMAIL] | {field: "x" * length}
    with pytest.raises(PayloadValidationError):
        parse(JobType.EMAIL, payload)


def test_l1_14b_cc_list_is_bounded():
    addresses = [f"user{i}@example.com" for i in range(10)]
    assert parse(JobType.EMAIL, VALID_PAYLOADS[JobType.EMAIL] | {"cc": addresses})

    with pytest.raises(PayloadValidationError):
        parse(
            JobType.EMAIL,
            VALID_PAYLOADS[JobType.EMAIL] | {"cc": [*addresses, "one@too.many"]},
        )


def test_l1_15_values_are_not_coerced_across_types():
    with pytest.raises(PayloadValidationError):
        parse(JobType.BATCH, {"items": "not-a-list", "operation": "index"})
    with pytest.raises(PayloadValidationError):
        parse(JobType.EMAIL, VALID_PAYLOADS[JobType.EMAIL] | {"to": 12345})


def test_l1_16_report_rejects_reversed_date_range():
    payload = VALID_PAYLOADS[JobType.REPORT] | {
        "date_from": "2026-03-01",
        "date_to": "2026-01-01",
    }
    with pytest.raises(PayloadValidationError) as excinfo:
        parse(JobType.REPORT, payload)
    assert "date_from" in str(excinfo.value.details)


def test_l1_16b_report_accepts_a_single_day_range():
    payload = VALID_PAYLOADS[JobType.REPORT] | {
        "date_from": "2026-01-01",
        "date_to": "2026-01-01",
    }
    assert parse(JobType.REPORT, payload)


@pytest.mark.parametrize("count", [1, 1000])
def test_l1_17_batch_accepts_its_bounds(count):
    payload = {"items": ["x"] * count, "operation": "index"}
    assert parse(JobType.BATCH, payload)


@pytest.mark.parametrize("count", [0, 1001])
def test_l1_17b_batch_rejects_outside_its_bounds(count):
    payload = {"items": ["x"] * count, "operation": "index"}
    with pytest.raises(PayloadValidationError):
        parse(JobType.BATCH, payload)


def test_l1_17c_batch_item_length_is_bounded():
    with pytest.raises(PayloadValidationError):
        parse(JobType.BATCH, {"items": ["x" * 501], "operation": "index"})


@pytest.mark.parametrize("address", ["not-an-email", "@example.com", "user@", ""])
def test_l1_18_malformed_email_is_rejected(address):
    with pytest.raises(PayloadValidationError):
        parse(JobType.EMAIL, VALID_PAYLOADS[JobType.EMAIL] | {"to": address})


def test_l1_18b_literal_fields_reject_unlisted_values():
    with pytest.raises(PayloadValidationError):
        parse(JobType.BATCH, {"items": ["a"], "operation": "delete-everything"})
    with pytest.raises(PayloadValidationError):
        parse(JobType.WEBHOOK, {"url": "https://example.com", "method": "DELETE"})


def test_l1_18c_whitespace_is_stripped():
    parsed = parse(JobType.EMAIL, VALID_PAYLOADS[JobType.EMAIL] | {"subject": "  Hi  "})
    assert parsed.subject == "Hi"
