"""additional user info

Revision ID: f2f2c5e36d04
Revises: 38badc441e8b
Create Date: 2024-03-20 10:17:05.543504

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = 'f2f2c5e36d04'
down_revision = '38badc441e8b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE `users` ADD COLUMN `first_name` varchar(50) null after oauth_provider"))
    conn.execute(text("ALTER TABLE `users` ADD COLUMN `last_name` varchar(50) null after first_name"))
    conn.execute(text("ALTER TABLE `users` ADD COLUMN `email` varchar(100) null after last_name"))
    conn.execute(text("ALTER TABLE `users` ADD COLUMN `avatar` text null after email"))
    conn.execute(text("ALTER TABLE `users` ADD COLUMN `is_active` tinyint(1) not null default 1 after avatar"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE `users` DROP COLUMN `first_name`"))
    conn.execute(text("ALTER TABLE `users` DROP COLUMN `last_name`"))
    conn.execute(text("ALTER TABLE `users` DROP COLUMN `email`"))
    conn.execute(text("ALTER TABLE `users` DROP COLUMN `avatar`"))
    conn.execute(text("ALTER TABLE `users` DROP COLUMN `is_active`"))
