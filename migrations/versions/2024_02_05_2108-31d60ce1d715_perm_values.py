"""perm-values

Revision ID: 31d60ce1d715
Revises: cd3ea5ce4aa1
Create Date: 2024-02-05 21:08:01.810772

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text
import json

# revision identifiers, used by Alembic.
revision = '31d60ce1d715'
down_revision = 'cd3ea5ce4aa1'
branch_labels = None
depends_on = None



def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("INSERT INTO roles(role) VALUES ('admin')"))
    migrate_existing_roles(conn)
    conn.execute(text("ALTER TABLE users DROP COLUMN roles"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("ALTER TABLE users add column roles JSON NOT NULL"))
    conn.execute(text("TRUNCATE TABLE user_roles"))
    conn.execute(text("DELETE from roles where name = 'admin'"))


def migrate_existing_roles(conn):
    role_result = conn.execute(text("select id, role from roles"))
    roles_dict = {row[1]: row[0] for row in role_result}
    result = conn.execute(text("SELECT id, roles FROM users"))
    for row in result:
        roles = json.loads(row[1])
        for role in roles:
            conn.execute(text("INSERT INTO user_roles (user_id, role_id) VALUES (:user_id, :role_id)"),
                              {"user_id": row[0], "role_id": roles_dict[role]})
