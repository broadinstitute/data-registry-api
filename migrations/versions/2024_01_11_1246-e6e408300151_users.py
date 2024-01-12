"""users

Revision ID: e6e408300151
Revises: 4f57c9e3f095
Create Date: 2024-01-11 12:46:30.227792

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'e6e408300151'
down_revision = '4f57c9e3f095'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `users` (
        `id` int NOT NULL AUTO_INCREMENT,
        `user_name` varchar(50) NOT NULL UNIQUE,
        `password` varchar(255),
        `roles` JSON NOT NULL,
        `oauth_provider` varchar(50),
        `created_at` datetime NOT NULL,
        `last_login` datetime,   
        PRIMARY KEY (`id`)
        )
        """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `users`"))
