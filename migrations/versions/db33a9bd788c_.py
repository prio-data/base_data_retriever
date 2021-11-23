"""empty message

Revision ID: db33a9bd788c
Revises: 
Create Date: 2021-11-23 10:18:59.175908

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'db33a9bd788c'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('level_of_analysis',
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('description', sa.String(), nullable=True),
    sa.Column('time_index', sa.String(), nullable=True),
    sa.Column('unit_index', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('name')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('level_of_analysis')
    # ### end Alembic commands ###
