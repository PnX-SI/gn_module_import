"""add mapping to admin

Revision ID: 391ae11bedd2
Revises: 5158afe602d2
Create Date: 2023-04-14 18:52:18.571423

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "391ae11bedd2"
down_revision = "5158afe602d2"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        INSERT INTO
            gn_permissions.cor_object_module (id_module, id_object)
        VALUES (
            (SELECT id_module FROM gn_commons.t_modules WHERE module_code = 'ADMIN'),
            (SELECT id_object FROM gn_permissions.t_objects WHERE code_object = 'MAPPING')
        )
        """
    )


def downgrade():
    op.execute(
        """
        DELETE FROM
            gn_permissions.cor_object_module
        WHERE
            id_module = (SELECT id_module FROM gn_commons.t_modules WHERE module_code = 'ADMIN')
        AND
            id_object = (SELECT id_object FROM gn_permissions.t_objects WHERE code_object = 'MAPPING')
        """
    )
