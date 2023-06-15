"""declare available permissions

Revision ID: a89a99f68203
Revises: 5158afe602d2
Create Date: 2023-06-14 11:40:29.580680

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a89a99f68203"
down_revision = "5158afe602d2"
branch_labels = None
depends_on = ("f051b88a57fd",)


def upgrade():
    op.execute(
        """
        INSERT INTO
            gn_permissions.t_permissions_available (
                id_module,
                id_object,
                id_action,
                label,
                scope_filter
            )
        SELECT
            m.id_module,
            o.id_object,
            a.id_action,
            v.label,
            v.scope_filter
        FROM
            (
                VALUES
                     ('IMPORT', 'ALL', 'C', True, 'Créer des imports')
                    ,('IMPORT', 'ALL', 'R', True, 'Voir les imports')
                    ,('IMPORT', 'ALL', 'D', True, 'Supprimer un import')
                    ,('IMPORT', 'IMPORT', 'C', True, 'Créer des imports')
                    ,('IMPORT', 'IMPORT', 'R', True, 'Voir les imports')
                    ,('IMPORT', 'IMPORT', 'D', True, 'Supprimer un import')
                    ,('IMPORT', 'MAPPING', 'C', True, 'Créer un mapping')
                    ,('IMPORT', 'MAPPING', 'R', True, 'Voir les mapping')
                    ,('IMPORT', 'MAPPING', 'U', True, 'Modifier un mapping')
                    ,('IMPORT', 'MAPPING', 'D', True, 'Supprimer des mapping')
            ) AS v (module_code, object_code, action_code, scope_filter, label)
        JOIN
            gn_commons.t_modules m ON m.module_code = v.module_code
        JOIN
            gn_permissions.t_objects o ON o.code_object = v.object_code
        JOIN
            gn_permissions.bib_actions a ON a.code_action = v.action_code
        """
    )
    op.execute(
        """
        WITH bad_permissions AS (
            SELECT
                p.id_permission
            FROM
                gn_permissions.t_permissions p
            JOIN gn_commons.t_modules m
                    USING (id_module)
            WHERE
                m.module_code = 'IMPORT'
            EXCEPT
            SELECT
                p.id_permission
            FROM
                gn_permissions.t_permissions p
            JOIN gn_permissions.t_permissions_available pa ON
                (p.id_module = pa.id_module
                    AND p.id_object = pa.id_object
                    AND p.id_action = pa.id_action)
        )
        DELETE
        FROM
            gn_permissions.t_permissions p
                USING bad_permissions bp
        WHERE
            bp.id_permission = p.id_permission;
        """
    )


def downgrade():
    op.execute(
        """
        DELETE FROM
            gn_permissions.t_permissions_available pa
        USING
            gn_commons.t_modules m
        WHERE
            pa.id_module = m.id_module
            AND
            module_code = 'IMPORT'
        """
    )
