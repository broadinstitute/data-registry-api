"""groups

Revision ID: 603e009de736
Revises: f2f2c5e36d04
Create Date: 2024-04-11 13:00:20.547614

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = '603e009de736'
down_revision = 'f2f2c5e36d04'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `groups` (
        `id` int NOT NULL AUTO_INCREMENT,
        `group_name` varchar(100) NOT NULL,
        PRIMARY KEY (`id`)
        )
        """
    conn.execute(text(query))
    query = """
        CREATE TABLE `user_groups` (
        `user_id` int NOT NULL,
        `group_id` int NOT NULL,
        PRIMARY KEY (`user_id`, `group_id`),
        CONSTRAINT `user_groups_user_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) on delete cascade,
        CONSTRAINT `user_groups_group_fk` FOREIGN KEY (`group_id`) REFERENCES `groups`(`id`) on delete cascade
        )
    """
    conn.execute(text(query))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `user_groups`"))
    conn.execute(text("DROP TABLE `groups`"))
