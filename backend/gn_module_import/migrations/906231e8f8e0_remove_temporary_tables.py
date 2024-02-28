"""remove temporary tables

Revision ID: 906231e8f8e0
Revises: 6470a2141c83
Create Date: 2022-03-31 12:38:11.170056

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.types import ARRAY
from sqlalchemy.dialects.postgresql import HSTORE, JSONB, UUID
from geoalchemy2 import Geometry


# revision identifiers, used by Alembic.
revision = "906231e8f8e0"
down_revision = "6470a2141c83"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column(table_name="dict_fields", column_name="nomenclature", schema="gn_imports")

    op.alter_column(
        table_name="dict_fields",
        column_name="synthese_field",
        new_column_name="is_synthese_field",
        schema="gn_imports",
    )
    op.add_column(
        table_name="dict_fields",
        column=sa.Column(
            "source_field",
            sa.Unicode,
        ),
        schema="gn_imports",
    )
    op.add_column(
        table_name="dict_fields",
        column=sa.Column(
            "synthese_field",
            sa.Unicode,
        ),
        schema="gn_imports",
    )
    op.execute(
        """
    WITH cte as (
        SELECT
            column_name,
            data_type
        FROM
            information_schema.columns
        WHERE
            table_schema = 'gn_synthese'
            AND
            table_name = 'synthese'
    )
    UPDATE
        gn_imports.dict_fields f
    SET
        source_field = 'src_' || f.name_field
    FROM
        cte
    WHERE
        (
            f.is_synthese_field IS FALSE
            AND
            autogenerated IS FALSE
        )
        OR
        f.mnemonique IS NOT NULL
        OR
        (
            cte.column_name = f.name_field
            AND
            cte.data_type NOT IN ('text', 'character', 'character varying', 'jsonb')
            AND
            f.name_field NOT IN ('the_geom_4326', 'the_geom_point', 'the_geom_local')
        )
    """
    )
    op.drop_constraint(
        constraint_name="chk_mandatory",
        table_name="dict_fields",
        schema="gn_imports",
    )
    op.execute(
        """
    UPDATE
        gn_imports.dict_fields
    SET
        synthese_field = name_field
    WHERE
        is_synthese_field IS TRUE
    """
    )
    op.execute(
        """
    INSERT INTO
        gn_imports.dict_fields
    (
        name_field,
        fr_label,
        "comment",
        source_field,
        synthese_field,
        is_synthese_field,
        mandatory,
        autogenerated,
        display,
        id_theme,
        order_field
    )
    VALUES
    (
        'datetime_min',
        'Date début',
        'Date de début de l’observation.',
        'date_min',
        'date_min',
        TRUE,
        FALSE,
        FALSE,
        FALSE,
        (SELECT id_theme FROM gn_imports.dict_fields WHERE name_field = 'date_min'),
        (SELECT order_field FROM gn_imports.dict_fields WHERE name_field = 'date_min')
    ),
    (
        'datetime_max',
        'Date fin',
        'Date de fin de l’observation.',
        'date_max',
        'date_max',
        TRUE,
        FALSE,
        FALSE,
        FALSE,
        (SELECT id_theme FROM gn_imports.dict_fields WHERE name_field = 'date_max'),
        (SELECT order_field FROM gn_imports.dict_fields WHERE name_field = 'date_max')
    )
    """
    )
    op.execute(
        """
    UPDATE
        gn_imports.dict_fields
    SET
        is_synthese_field = FALSE,
        synthese_field = NULL
    WHERE
        name_field IN ('date_min', 'date_max')
    """
    )
    op.drop_column(table_name="dict_fields", column_name="is_synthese_field", schema="gn_imports")

    op.create_table(
        "t_imports_synthese",
        sa.Column(
            "id_import",
            sa.Integer,
            sa.ForeignKey("gn_imports.t_imports.id_import", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("line_no", sa.Integer, primary_key=True),
        sa.Column("valid", sa.Boolean, nullable=False, server_default=sa.false()),
        ### source fields
        # non-synthese fields: used to populate synthese fields
        sa.Column("src_WKT", sa.Unicode),
        sa.Column("src_codecommune", sa.Unicode),
        sa.Column("src_codedepartement", sa.Unicode),
        sa.Column("src_codemaille", sa.Unicode),
        sa.Column("src_hour_max", sa.Unicode),
        sa.Column("src_hour_min", sa.Unicode),
        sa.Column("src_latitude", sa.Unicode),
        sa.Column("src_longitude", sa.Unicode),
        # synthese fields
        sa.Column("src_unique_id_sinp", sa.Unicode),
        sa.Column("src_unique_id_sinp_grp", sa.Unicode),
        # nomenclature fields
        sa.Column("src_id_nomenclature_geo_object_nature", sa.Unicode),
        sa.Column("src_id_nomenclature_grp_typ", sa.Unicode),
        sa.Column("src_id_nomenclature_obs_technique", sa.Unicode),
        sa.Column("src_id_nomenclature_bio_status", sa.Unicode),
        sa.Column("src_id_nomenclature_bio_condition", sa.Unicode),
        sa.Column("src_id_nomenclature_naturalness", sa.Unicode),
        sa.Column("src_id_nomenclature_valid_status", sa.Unicode),
        sa.Column("src_id_nomenclature_exist_proof", sa.Unicode),
        sa.Column("src_id_nomenclature_diffusion_level", sa.Unicode),
        sa.Column("src_id_nomenclature_life_stage", sa.Unicode),
        sa.Column("src_id_nomenclature_sex", sa.Unicode),
        sa.Column("src_id_nomenclature_obj_count", sa.Unicode),
        sa.Column("src_id_nomenclature_type_count", sa.Unicode),
        sa.Column("src_id_nomenclature_sensitivity", sa.Unicode),
        sa.Column("src_id_nomenclature_observation_status", sa.Unicode),
        sa.Column("src_id_nomenclature_blurring", sa.Unicode),
        sa.Column("src_id_nomenclature_source_status", sa.Unicode),
        sa.Column("src_id_nomenclature_info_geo_type", sa.Unicode),
        sa.Column("src_id_nomenclature_behaviour", sa.Unicode),
        sa.Column("src_id_nomenclature_biogeo_status", sa.Unicode),
        sa.Column("src_id_nomenclature_determination_method", sa.Unicode),
        sa.Column("src_count_min", sa.Unicode),
        sa.Column("src_count_max", sa.Unicode),
        sa.Column("src_cd_nom", sa.Unicode),
        sa.Column("src_cd_hab", sa.Unicode),
        sa.Column("src_altitude_min", sa.Unicode),
        sa.Column("src_altitude_max", sa.Unicode),
        sa.Column("src_depth_min", sa.Unicode),
        sa.Column("src_depth_max", sa.Unicode),
        sa.Column("src_precision", sa.Unicode),
        sa.Column("src_id_area_attachment", sa.Unicode),
        sa.Column("src_date_min", sa.Unicode),
        sa.Column("src_date_max", sa.Unicode),
        sa.Column("src_id_digitiser", sa.Unicode),
        sa.Column("src_meta_validation_date", sa.Unicode),
        sa.Column("src_meta_create_date", sa.Unicode),
        sa.Column("src_meta_update_date", sa.Unicode),
        # un-mapped fields
        sa.Column("extra_fields", HSTORE),
        ### synthese fields
        sa.Column("unique_id_sinp", UUID(as_uuid=True)),  #
        sa.Column("unique_id_sinp_grp", UUID(as_uuid=True)),  #
        sa.Column("entity_source_pk_value", sa.Unicode),  #
        sa.Column("grp_method", sa.Unicode(length=255)),  #
        # nomenclature fields
        sa.Column(
            "id_nomenclature_geo_object_nature",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_grp_typ",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_obs_technique",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_bio_status",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_bio_condition",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_naturalness",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_valid_status",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_exist_proof",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_diffusion_level",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_life_stage",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_sex",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_obj_count",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_type_count",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_sensitivity",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_observation_status",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_blurring",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_source_status",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_info_geo_type",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_behaviour",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_biogeo_status",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        sa.Column(
            "id_nomenclature_determination_method",
            sa.Integer,
            sa.ForeignKey("ref_nomenclatures.t_nomenclatures.id_nomenclature"),
        ),  #
        # others fields
        sa.Column("reference_biblio", sa.Unicode),  #
        sa.Column("count_min", sa.Integer),  #
        sa.Column("count_max", sa.Integer),  #
        sa.Column("cd_nom", sa.Integer, sa.ForeignKey("taxonomie.taxref.cd_nom")),  #
        sa.Column("cd_hab", sa.Integer, sa.ForeignKey("ref_habitats.habref.cd_hab")),  #
        sa.Column("nom_cite", sa.Unicode),  #
        sa.Column("meta_v_taxref", sa.Unicode),  #
        # sa.Column("sample_number_proof", sa.UnicodeText),  #######################
        sa.Column("digital_proof", sa.UnicodeText),  #
        sa.Column("non_digital_proof", sa.UnicodeText),  #
        sa.Column("altitude_min", sa.Integer),  #
        sa.Column("altitude_max", sa.Integer),  #
        sa.Column("depth_min", sa.Integer),  #
        sa.Column("depth_max", sa.Integer),  #
        sa.Column("place_name", sa.Unicode),  #
        sa.Column("the_geom_4326", Geometry("GEOMETRY", 4326)),  #
        sa.Column("the_geom_point", Geometry("GEOMETRY", 4326)),  # FIXME useful?
        sa.Column("the_geom_local", Geometry("GEOMETRY")),  # FIXME useful?
        sa.Column("precision", sa.Integer),  #
        # sa.Column("id_area_attachment", sa.Integer),  #######################
        sa.Column("date_min", sa.DateTime),  #
        sa.Column("date_max", sa.DateTime),  #
        sa.Column("validator", sa.Unicode),  #
        sa.Column("validation_comment", sa.Unicode),  #
        sa.Column("observers", sa.Unicode),  #
        sa.Column("determiner", sa.Unicode),  #
        sa.Column("id_digitiser", sa.Integer, sa.ForeignKey("utilisateurs.t_roles.id_role")),  #
        sa.Column("comment_context", sa.UnicodeText),  #
        sa.Column("comment_description", sa.UnicodeText),  #
        sa.Column("additional_data", JSONB),  #
        sa.Column("meta_validation_date", sa.DateTime),
        sa.Column("meta_create_date", sa.DateTime),
        sa.Column("meta_update_date", sa.DateTime),
        schema="gn_imports",
    )

    op.rename_table(
        old_table_name="t_user_errors",
        new_table_name="dict_errors",
        schema="gn_imports",
    )
    op.rename_table(
        old_table_name="t_user_error_list",
        new_table_name="t_user_errors",
        schema="gn_imports",
    )


def downgrade():
    op.rename_table(
        old_table_name="t_user_errors",
        new_table_name="t_user_error_list",
        schema="gn_imports",
    )
    op.rename_table(
        old_table_name="dict_errors",
        new_table_name="t_user_errors",
        schema="gn_imports",
    )
    op.drop_table(
        table_name="t_imports_synthese",
        schema="gn_imports",
    )

    op.add_column(
        table_name="dict_fields",
        column=sa.Column(
            "is_synthese_field",
            sa.Boolean,
        ),
        schema="gn_imports",
    )
    op.execute(
        """
    UPDATE
        gn_imports.dict_fields
    SET
        is_synthese_field = (synthese_field IS NOT NULL)
    """
    )
    op.execute(
        """
    DELETE FROM
        gn_imports.dict_fields
    WHERE
        name_field IN ('datetime_min', 'datetime_max')
    """
    )
    op.execute(
        """
    UPDATE
        gn_imports.dict_fields
    SET
        is_synthese_field = TRUE
    WHERE
        name_field IN ('date_min', 'date_max')
    """
    )
    op.execute(
        """
    ALTER TABLE gn_imports.dict_fields ADD CONSTRAINT chk_mandatory CHECK (
    CASE
        WHEN ((name_field)::text = ANY ((ARRAY['date_min', 'longitude', 'latitude', 'nom_cite', 'cd_nom', 'wkt'])::text[])) THEN (mandatory = true)
        ELSE NULL::boolean
    END)
    """
    )
    op.drop_column(
        table_name="dict_fields",
        column_name="source_field",
        schema="gn_imports",
    )
    op.drop_column(
        table_name="dict_fields",
        column_name="synthese_field",
        schema="gn_imports",
    )
    op.alter_column(
        table_name="dict_fields",
        column_name="is_synthese_field",
        new_column_name="synthese_field",
        schema="gn_imports",
    )

    op.add_column(
        table_name="dict_fields",
        column=sa.Column("nomenclature", sa.Boolean, nullable=False, server_default=sa.false()),
        schema="gn_imports",
    )
    op.execute(
        """
        WITH cte AS (
                SELECT
                    id_field,
                    mnemonique IS NOT NULL AS nomenclature
                FROM
                    gn_imports.dict_fields
            )
        UPDATE
            gn_imports.dict_fields f
        SET
            nomenclature = cte.nomenclature
        FROM
            cte
        WHERE
            f.id_field = cte.id_field
        """
    )
