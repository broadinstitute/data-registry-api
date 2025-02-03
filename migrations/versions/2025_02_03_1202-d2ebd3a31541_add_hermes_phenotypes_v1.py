"""add hermes phenotypes V1

Revision ID: d2ebd3a31541
Revises: 09c1d30b0efe
Create Date: 2025-02-03 12:02:42.677885

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = 'd2ebd3a31541'
down_revision = '09c1d30b0efe'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    phenotypes = [
        ('ALL_HF', 'All heart failure', 1),
        ('I_HF', 'Ischaemic heart failure', 1),
        ('NI_HF', 'Non-ischaemic heart failure', 1),
        ('DCM_Broad', 'Dilated cardiomyopathy', 1),
        ('NI_CM', 'Non-ischaemic cardiomyopathy', 1)
    ]
    query = """
        insert into `hermes_phenotype` (name, description, dichotomous) 
        values (:name, :description, :dichotomous)
    """
    for name, description, dichotomous in phenotypes:
        conn.execute(text(query), {'name': name, 'description': description, 'dichotomous': dichotomous})


def downgrade() -> None:
    conn = op.get_bind()
    phenotypes = ['ALL_HF', 'I_HF', 'NI_HF', 'DCM_Broad', 'NI_CM']
    query = "delete from `hermes_phenotype` where name = :name"
    for name in phenotypes:
        conn.execute(text(query), {'name': name})
