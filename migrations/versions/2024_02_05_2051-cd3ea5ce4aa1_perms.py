"""perms

Revision ID: cd3ea5ce4aa1
Revises: e6a081f21335
Create Date: 2024-02-05 20:51:46.662133

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision = 'cd3ea5ce4aa1'
down_revision = 'e6a081f21335'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    query = """
        CREATE TABLE `roles` (
        `id` int NOT NULL AUTO_INCREMENT,
        `role` varchar(100) NOT NULL UNIQUE,
        PRIMARY KEY (`id`)
        )
        """
    conn.execute(text(query))
    query = """
        CREATE TABLE `permissions` (
        `id` int NOT NULL AUTO_INCREMENT,
        `permission` varchar(100) NOT NULL UNIQUE,
        PRIMARY KEY (`id`)
        )
    """
    conn.execute(text(query))
    query = """
        CREATE TABLE `user_roles` (
        `user_id` int NOT NULL,
        `role_id` int NOT NULL,
        PRIMARY KEY (`user_id`, `role_id`),
        CONSTRAINT `user_role_user_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) on delete cascade,
        CONSTRAINT `user_role_role_fk` FOREIGN KEY (`role_id`) REFERENCES `roles`(`id`) on delete cascade
        )
    """
    conn.execute(text(query))
    query = """
        CREATE TABLE `role_permissions` (
        `permission_id` int NOT NULL,
        `role_id` int NOT NULL,
        PRIMARY KEY (`permission_id`, `role_id`),
        CONSTRAINT `role_perms_perm_fk` FOREIGN KEY (`permission_id`) REFERENCES `permissions` (`id`) on delete cascade,
        CONSTRAINT `role_perms_role_fk` FOREIGN KEY (`role_id`) REFERENCES `roles`(`id`) on delete cascade
        )
    """
    conn.execute(text(query))

def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE `role_permissions`"))
    conn.execute(text("DROP TABLE `user_roles`"))
    conn.execute(text("DROP TABLE `roles`"))
    conn.execute(text("DROP TABLE `permissions`"))
