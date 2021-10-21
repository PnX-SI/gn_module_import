"""Declare available permissions

Revision ID: ff0d68285cd8
Revises: 4b137deaf201
Create Date: 2021-10-21 15:28:07.014444

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ff0d68285cd8'
down_revision = '4b137deaf201'
branch_labels = None
depends_on = (
    '3b2f3de760dc',  # geonature with permissions managment
)


def upgrade():
    for perm in [
            {
                'action': 'C',
                'object': 'ALL',
                'label': 'Créer des imports',
                'description': 'Créer des imports et les modifier en étant limité par leur appartenance.',
            }, {
                'action': 'R',
                'object': 'ALL',
                'label': 'Voir les imports',
                'description': 'Voir les imports en étant limité par leur appartenance.',
            }, {
                'action': 'D',
                'object': 'ALL',
                'label': 'Supprimer des imports',
                'description': 'Supprimer des imports en étant limité par leur appartenance.',
            }, {
                'action': 'R',
                'object': 'MAPPING',
                'label': 'Voir les mappings',
                'description': 'Voir les mappings en étant limité par leur appartenance.',
            }]:
        op.execute(sa.text("""
        INSERT INTO gn_permissions.cor_module_action_object_filter (
            id_module, id_action, id_object, id_filter_type, code, label, description
        ) VALUES (
            gn_commons.get_id_module_bycode('IMPORT'),
            gn_permissions.get_id_action(:action),
            gn_permissions.get_id_object(:object),
            gn_permissions.get_id_filter_type('SCOPE'),
            'IMPORT-' || :action || '-' || :object || '-SCOPE',
            :label,
            :description
        )
        """).bindparams(**perm))


def downgrade():
    op.execute("""
    DELETE FROM gn_permissions.cor_module_action_object_filter
        WHERE id_module = gn_commons.get_id_module_bycode('IMPORT')
    """)
