"""add watchlist backrun fields

Revision ID: 20260330_add_watchlist_backrun_fields
Revises:
Create Date: 2026-03-30
"""

from alembic import op
import sqlalchemy as sa


revision = "20260330_add_watchlist_backrun_fields"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "watchlist_items",
        sa.Column("backrun_status", sa.String(length=20), nullable=False, server_default="IDLE"),
    )
    op.add_column(
        "watchlist_items",
        sa.Column("backrun_error", sa.Text(), nullable=False, server_default=""),
    )


def downgrade() -> None:
    op.drop_column("watchlist_items", "backrun_error")
    op.drop_column("watchlist_items", "backrun_status")
