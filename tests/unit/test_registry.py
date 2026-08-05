"""L1-01 .. L1-05 — the job type registry."""

import pytest

from app.core.enums import JobType
from app.core.errors import UnknownJobTypeError
from app.jobs.base import BaseJob, JobPayload, JobResult
from app.jobs.registry import JOB_REGISTRY, get_job_class, register


class _Payload(JobPayload):
    value: str


class _Result(JobResult):
    value: str


@pytest.fixture
def clean_registry():
    """Undo any registration a test performs."""
    before = dict(JOB_REGISTRY)
    yield
    JOB_REGISTRY.clear()
    JOB_REGISTRY.update(before)


def test_l1_01_register_adds_the_class(clean_registry):
    JOB_REGISTRY.pop(JobType.EMAIL)

    @register
    class Replacement(BaseJob):
        job_type = JobType.EMAIL
        Payload = _Payload
        Result = _Result

        async def run(self) -> _Result:
            return _Result(value="x")

    assert JOB_REGISTRY[JobType.EMAIL] is Replacement


def test_l1_02_duplicate_job_type_is_rejected(clean_registry):
    with pytest.raises(TypeError, match="duplicates job_type"):

        @register
        class Duplicate(BaseJob):
            job_type = JobType.EMAIL
            Payload = _Payload
            Result = _Result

            async def run(self) -> _Result:
                return _Result(value="x")


def test_l1_03a_missing_payload_model_is_rejected(clean_registry):
    JOB_REGISTRY.pop(JobType.REPORT)

    with pytest.raises(TypeError, match="Payload must be"):

        @register
        class NoPayload(BaseJob):
            job_type = JobType.REPORT
            Result = _Result

            async def run(self) -> _Result:
                return _Result(value="x")


def test_l1_03b_missing_result_model_is_rejected(clean_registry):
    JOB_REGISTRY.pop(JobType.REPORT)

    with pytest.raises(TypeError, match="Result must be"):

        @register
        class NoResult(BaseJob):
            job_type = JobType.REPORT
            Payload = _Payload

            async def run(self) -> _Result:
                return _Result(value="x")


def test_l1_03c_unimplemented_run_is_rejected(clean_registry):
    JOB_REGISTRY.pop(JobType.REPORT)

    with pytest.raises(TypeError, match="does not implement run"):

        @register
        class Abstract(BaseJob):
            job_type = JobType.REPORT
            Payload = _Payload
            Result = _Result


def test_l1_03d_non_enum_job_type_is_rejected(clean_registry):
    with pytest.raises(TypeError, match="must set job_type"):

        @register
        class Bogus(BaseJob):
            job_type = "not-an-enum"  # type: ignore[assignment]
            Payload = _Payload
            Result = _Result

            async def run(self) -> _Result:
                return _Result(value="x")


@pytest.mark.parametrize("unknown", ["nope", "", "EMAIL"])
def test_l1_04_unknown_type_raises(unknown):
    with pytest.raises(UnknownJobTypeError):
        get_job_class(unknown)


def test_l1_04b_unregistered_but_valid_enum_raises(clean_registry):
    JOB_REGISTRY.pop(JobType.BATCH)
    with pytest.raises(UnknownJobTypeError, match="No handler registered"):
        get_job_class("batch")


def test_l1_05_every_job_type_has_a_handler():
    assert set(JOB_REGISTRY) == set(JobType)
    for job_type, cls in JOB_REGISTRY.items():
        assert cls.job_type is job_type
