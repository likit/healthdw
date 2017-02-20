"""moved compamy_id from customer to the fact table

Revision ID: 694e15987c87
Revises: 224dcd69d53e
Create Date: 2017-02-21 02:34:26.983883

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '694e15987c87'
down_revision = '224dcd69d53e'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('customers_company_id_fkey', 'customers')
    op.drop_column('customers', 'company_id')
    op.add_column('facts', sa.Column('company_id',
                    sa.Integer, sa.ForeignKey('companies.company_id')))

def downgrade():
    op.add_column('customers', sa.Column('company_id',
                    sa.Integer, sa.ForeignKey('companies.id')))
