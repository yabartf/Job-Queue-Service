"""L2-17 — the migration builds and removes the schema cleanly."""

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text


@pytest.fixture
def alembic_config(test_db_url: str) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", test_db_url)
    return config


def test_l2_17_downgrade_then_upgrade_is_clean(alembic_config):
    """Both directions must work on a real database: a migration that only goes
    forward cannot be rehearsed before it is run for real."""
    command.downgrade(alembic_config, "base")
    command.upgrade(alembic_config, "head")


async def test_schema_contains_the_expected_objects(engine):
    async with engine.connect() as connection:
        tables = (
            (
                await connection.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()")
                )
            )
            .scalars()
            .all()
        )
        assert {"jobs", "job_logs"}.issubset(set(tables))

        indexes = (
            (
                await connection.execute(
                    text("SELECT indexname FROM pg_indexes WHERE tablename = 'jobs'")
                )
            )
            .scalars()
            .all()
        )
        assert {
            "ux_jobs_idempotency",
            "ix_jobs_claim",
            "ix_jobs_scheduled",
            "ix_jobs_lease",
            "ix_jobs_status_created",
            "ix_jobs_created",
        }.issubset(set(indexes))


async def test_idempotency_index_is_partial(engine):
    """If it were not partial, every job submitted without a key would collide
    with every other on NULL."""
    async with engine.connect() as connection:
        predicate = (
            await connection.execute(
                text(
                    "SELECT pg_get_expr(indpred, indrelid) FROM pg_index "
                    "WHERE indexrelid = 'ux_jobs_idempotency'::regclass"
                )
            )
        ).scalar_one()
    assert predicate is not None
    assert "idempotency_key" in predicate


async def test_claim_index_is_partial_to_pending(engine):
    async with engine.connect() as connection:
        predicate = (
            await connection.execute(
                text(
                    "SELECT pg_get_expr(indpred, indrelid) FROM pg_index "
                    "WHERE indexrelid = 'ix_jobs_claim'::regclass"
                )
            )
        ).scalar_one()
    assert predicate is not None
    assert "pending" in predicate


async def test_updated_at_trigger_exists(engine):
    async with engine.connect() as connection:
        count = (
            await connection.execute(
                text(
                    "SELECT count(*) FROM pg_trigger "
                    "WHERE tgname = 'trg_jobs_updated_at' AND NOT tgisinternal"
                )
            )
        ).scalar_one()
    assert count == 1
