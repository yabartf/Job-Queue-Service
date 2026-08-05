"""Dead-letter classification on failed jobs.

A column rather than a seventh status: being dead-lettered describes *why* a job
failed, not a state it moves through, so the state machine stays as it was. See
specs/09-hardening.md section 4.

No index. The set is small by construction and queries narrow through
ix_jobs_status_created first; another index would cost write throughput on the
claim path for nothing.

Revision ID: 0002_dead_letter
Revises: 0001_initial
Create Date: 2026-08-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_dead_letter"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("dead_letter_reason", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_jobs_dead_letter_is_failed",
        "jobs",
        "dead_letter_reason IS NULL OR status = 'failed'",
    )
    op.create_check_constraint(
        "ck_jobs_dead_letter_reason",
        "jobs",
        "dead_letter_reason IS NULL OR dead_letter_reason IN "
        "('unprocessable_payload','timeout_loop','worker_crash_loop')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_jobs_dead_letter_reason", "jobs", type_="check")
    op.drop_constraint("ck_jobs_dead_letter_is_failed", "jobs", type_="check")
    op.drop_column("jobs", "dead_letter_reason")
