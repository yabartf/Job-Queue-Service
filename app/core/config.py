"""Runtime configuration, read from the environment."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://jobs:jobs@localhost:5432/jobs"
    test_database_url: str = "postgresql+asyncpg://jobs:jobs@localhost:5433/jobs_test"
    redis_url: str = "redis://localhost:6379/0"
    #: A separate logical database on the same server, so the suite never
    #: disturbs development data and needs no second container.
    test_redis_url: str = "redis://localhost:6379/15"

    service_name: str = "job-queue"
    log_level: str = "INFO"

    #: Independent claim loops per worker process (spec 03 section 2).
    worker_concurrency: int = 2
    #: How long a claim holds a job before the reaper may take it back.
    worker_lease_seconds: int = 60
    #: Lease extension interval — a third of the lease, so a single missed
    #: extension still leaves a further one before it could expire.
    worker_heartbeat_seconds: int = 20
    #: Blocking wait when the queue is empty — not a poll interval in the hot path.
    worker_poll_interval_seconds: float = 5.0
    #: Reaper and promoter sweep interval.
    maintenance_interval_seconds: float = 5.0
    maintenance_batch_size: int = 100
    #: Time allowed for in-flight jobs to finish before leases are force-released.
    shutdown_grace_seconds: float = 30.0

    #: Bodies larger than this are rejected before parsing (spec 02 section 5).
    max_request_body_bytes: int = 64 * 1024

    #: A job may not be scheduled further ahead than this.
    max_schedule_horizon_days: int = 90

    default_page_size: int = 20
    max_page_size: int = 100


@lru_cache
def get_settings() -> Settings:
    return Settings()
