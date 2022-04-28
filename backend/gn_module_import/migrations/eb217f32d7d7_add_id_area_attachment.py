"""add id_area_attachment

Revision ID: eb217f32d7d7
Revises: 74058f69828a
Create Date: 2022-04-28 16:48:46.664645

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'eb217f32d7d7'
down_revision = '74058f69828a'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        schema="gn_imports",
        table_name="t_imports_synthese",
        column=sa.Column(
            "id_area_attachment",
            sa.Integer,
            sa.ForeignKey("ref_geo.l_areas.id_area"),
        ),
    )
    op.execute(
        """
    INSERT INTO gn_imports.dict_fields (
        name_field,
        synthese_field,
        fr_label,
        mandatory,
        autogenerated,
        id_theme,
        order_field,
        display
    )
    VALUES (
        'id_area_attachment',
        'id_area_attachment',
        'Rattachement géographique de l’observation',
        FALSE,
        FALSE,
        (SELECT id_theme FROM gn_imports.dict_fields WHERE name_field = 'WKT'),
        (SELECT order_field FROM gn_imports.dict_fields WHERE name_field = 'WKT'),
        FALSE
    )
    """
    )
    op.execute(
        """
    UPDATE
        gn_imports.dict_fields
    SET
        mandatory = FALSE
    WHERE
        name_field IN ('WKT', 'codecommune', 'codemaille', 'codedepartement', 'longitude', 'latitude')
    """
    )


def downgrade():
    op.execute(
        """
    UPDATE
        gn_imports.dict_fields
    SET
        mandatory = TRUE
    WHERE
        name_field IN ('WKT', 'codecommune', 'codemaille', 'codedepartement', 'longitude', 'latitude')
    """
    )
    op.execute(
        """
    DELETE FROM
        gn_imports.dict_fields
    WHERE
        name_field = 'id_area_attachment'
    """
    )
    op.drop_column(
        schema="gn_imports",
        table_name="t_imports_synthese",
        column_name="id_area_attachment",
    )
