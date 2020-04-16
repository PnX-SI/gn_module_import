import datetime

from utils_flask_sqla.response import json_resp
from geonature.utils.env import DB
from geonature.core.gn_permissions import decorators as permissions

from ..db.models import TImports

from ..db.queries.save_mapping import get_selected_columns
from ..db.queries.load_to_synthese import insert_into_t_sources, check_id_source
from ..db.queries.user_table_queries import (
    set_imports_table_name,
    get_table_name,
    get_date_ext,
    get_n_valid_rows,
    get_n_taxa,
)
from ..db.queries.metadata import get_id_field_mapping

from ..logs import logger
from ..api_error import GeonatureImportApiError
from ..data_preview.preview import set_total_columns
from ..load.into_synthese.import_data import load_data_to_synthese

from ..blueprint import blueprint


@blueprint.route("/importData/<import_id>", methods=["GET", "POST"])
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def import_data(info_role, import_id):
    """"Import data in synthese"""
    try:

        logger.info("Importing data in gn_synthese.synthese table")

        IMPORTS_SCHEMA_NAME = blueprint.config["IMPORTS_SCHEMA_NAME"]
        MODULE_CODE = blueprint.config["MODULE_CODE"]

        # get table name
        table_name = set_imports_table_name(get_table_name(import_id))
        # set total user columns
        id_mapping = get_id_field_mapping(import_id)
        selected_cols = get_selected_columns(id_mapping)
        added_cols = {
            "the_geom_4326": "gn_the_geom_4326",
            "the_geom_local": "gn_the_geom_local",
            "the_geom_point": "gn_the_geom_point",
        }
        total_columns = set_total_columns(
            selected_cols, added_cols, import_id, MODULE_CODE
        )

        # IMPORT DATA IN SYNTHESE

        # check if id_source already exists in synthese table
        is_id_source = check_id_source(import_id)

        if is_id_source:
            return (
                {
                    "message": "échec : déjà importé",
                    "details": "(vérification basée sur l'id_source)",
                },
                400,
            )

        # insert into t_sources
        insert_into_t_sources(IMPORTS_SCHEMA_NAME, table_name, import_id, total_columns)

        # insert into synthese
        load_data_to_synthese(IMPORTS_SCHEMA_NAME, table_name, total_columns, import_id)

        logger.info("-> Data imported in gn_synthese.synthese table")

        # UPDATE TIMPORTS

        logger.info("update t_imports on final step")

        date_ext = get_date_ext(
            IMPORTS_SCHEMA_NAME,
            table_name,
            total_columns["date_min"],
            total_columns["date_max"],
        )

        DB.session.query(TImports).filter(TImports.id_import == int(import_id)).update(
            {
                TImports.import_count: get_n_valid_rows(
                    IMPORTS_SCHEMA_NAME, table_name
                ),
                TImports.taxa_count: get_n_taxa(
                    IMPORTS_SCHEMA_NAME, table_name, total_columns["cd_nom"]
                ),
                TImports.date_min_data: date_ext["date_min"],
                TImports.date_max_data: date_ext["date_max"],
                TImports.date_end_import: datetime.datetime.now(),
                TImports.is_finished: True,
            }
        )

        logger.info("-> t_imports updated on final step")

        DB.session.commit()

        return (
            {
                "status": "imported successfully"
                #'total_columns': total_columns
            },
            200,
        )

    except Exception as e:
        DB.session.rollback()
        logger.error("*** SERVER ERROR WHEN IMPORTING DATA IN GN_SYNTHESE.SYNTHESE")
        logger.exception(e)
        raise GeonatureImportApiError(
            message="INTERNAL SERVER ERROR when importing data in gn_synthese.synthese",
            details=str(e),
        )
    finally:
        DB.session.close()
