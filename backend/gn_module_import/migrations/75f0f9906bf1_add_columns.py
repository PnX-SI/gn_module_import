"""add_columns

Revision ID: 75f0f9906bf1
Revises: 2ed6a7ee5250
Create Date: 2021-04-27 10:02:53.798753

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '75f0f9906bf1'
down_revision = '2ed6a7ee5250'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        ALTER TABLE gn_imports.t_imports
        ADD COLUMN columns HSTORE
    """)
    op.execute("""
        ALTER TABLE gn_imports.t_user_error_list
        ALTER COLUMN id_rows TYPE integer[] USING id_rows::integer[]
    """)
    op.execute("""
        ALTER TABLE gn_imports.t_mappings_fields
        DROP COLUMN is_added
    """)
    op.execute("""
        ALTER TABLE gn_imports.t_mappings_fields
        DROP COLUMN is_selected
    """)


def downgrade():
    op.execute("""
        ALTER TABLE gn_imports.t_mappings_fields
        ADD COLUMN is_selected BOOLEAN NOT NULL DEFAULT TRUE
    """)
    op.execute("""
        ALTER TABLE gn_imports.t_mappings_fields
        DROP COLUMN is_added BOOLEAN NOT NULL DEFAULT FALSE
    """)
    op.execute("""
        ALTER TABLE gn_imports.t_user_error_list
        ALTER COLUMN id_rows TYPE text[] USING id_rows::text[]
    """)
    op.execute("""
        ALTER TABLE gn_imports.t_imports
        DROP COLUMN columns
    """)
