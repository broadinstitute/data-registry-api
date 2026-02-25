"""add_sgc_phenotypes_feb2026

Revision ID: add_sgc_phenotypes_feb2026
Revises: 3ee8662aa028
Create Date: 2026-02-25 10:00:00.000000

"""
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'add_sgc_phenotypes_feb2026'
down_revision = '3ee8662aa028'
branch_labels = None
depends_on = None

NEW_PHENOTYPES = [
    ("INGROWN_NAIL", "Ingrown nail"),
    ("ALOPECIA_AREATA", "Alopecia areata"),
    ("ANDROGENIC_ALOPECIA", "Androgenic alopecia"),
    ("TELOGEN_EFFLUVIUM", "Telogen effluvium"),
]


def upgrade() -> None:
    conn = op.get_bind()
    insert_query = "INSERT INTO `sgc_phenotypes` (phenotype_code, description) VALUES (:phenotype_code, :description)"
    for phenotype_code, description in NEW_PHENOTYPES:
        conn.execute(text(insert_query), {'phenotype_code': phenotype_code, 'description': description})
    conn.commit()


def downgrade() -> None:
    conn = op.get_bind()
    for phenotype_code, _ in NEW_PHENOTYPES:
        conn.execute(text("DELETE FROM `sgc_phenotypes` WHERE phenotype_code = :code"), {'code': phenotype_code})
    conn.commit()
