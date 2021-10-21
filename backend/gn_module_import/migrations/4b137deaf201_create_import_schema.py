"""Create import schema

Revision ID: 4b137deaf201
Revises: 
Create Date: 2021-03-29 18:38:24.512562

"""
import importlib

from alembic import op, context
import sqlalchemy as sa
from distutils.util import strtobool


# revision identifiers, used by Alembic.
revision = '4b137deaf201'
down_revision = None
branch_labels = ('import',)
depends_on = (
    'f06cc80cc8ba',  # GeoNature 2.7.5
)


schema = 'gn_imports'
archive_schema = 'gn_import_archives'


def upgrade():
    path = "gn_module_import.migrations.data"
    sql_files = ['schema.sql', 'data.sql']
    if strtobool(context.get_x_argument(as_dictionary=True).get('default-mappings', "true")):
        sql_files += ['default_mappings_data.sql']
    for sql_file in sql_files:
        op.execute(importlib.resources.read_text(path, sql_file))


def downgrade():
    op.execute(f'DROP TABLE {archive_schema}.cor_import_archives')
    op.execute(f'DROP SCHEMA {archive_schema}')
    op.execute(f'DROP VIEW IF EXISTS {schema}.v_imports_errors')
    for table in [
            'cor_role_import',
            'cor_role_mapping',
            'cor_synthese_nomenclature',
            't_mappings_fields',
            't_mappings_values',
            't_user_error_list',
            't_imports',
            't_mappings',
            't_user_errors',
            'dict_fields',
            'dict_themes',
            ]:
        op.execute(f'DROP TABLE IF EXISTS {schema}.{table}')
