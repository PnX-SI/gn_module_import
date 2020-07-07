import datetime
import threading

from flask import copy_current_request_context, current_app

from utils_flask_sqla.response import json_resp
from geonature.utils.env import DB
from geonature.core.gn_permissions import decorators as permissions

from ..db.models import TImports, TMappings

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

from ..send_mail import import_send_mail, import_send_mail_error


@blueprint.route("/importData/<import_id>", methods=["GET", "POST"])
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def import_data(info_role, import_id):
    """
    Run import data in synthese
    The route must return an import object with its mapping (use in frontend)
    """
    import_obj = DB.session.query(TImports).get(import_id)
    import_as_dict = import_obj.as_dict(True)

    if import_obj.source_count > current_app.config["IMPORT"]["MAX_LINE_LIMIT"]:

        import_obj.processing = True

        DB.session.commit()

        import_data = {"import_as_dict": import_as_dict}

        @copy_current_request_context
        def data_import_task(import_as_dict):
            recipients = list((map(lambda a: a["email"], import_as_dict.get("author"))))
            try:
                res = import_in_synthese(import_id)
                import_send_mail(
                    id_import=import_as_dict["id_import"],
                    mail_to=recipients,
                    file_name=import_as_dict["full_file_name"],
                    step="import",
                )
                return res
            except Exception as e:
                DB.session.query(TImports).filter(
                    TImports.id_import == import_as_dict["id_import"]
                ).update({"in_error": True})
                DB.session.commit()
                import_send_mail_error(
                    mail_to=recipients,
                    file_name=import_as_dict["full_file_name"],
                    error=e,
                )
                return "Error", 500

        a = threading.Thread(
            name="data_import_task", target=data_import_task, kwargs=import_data
        )
        a.start()

        import_obj = TImports.query.get(import_id)
        mappings = (
            DB.session.query(TMappings)
            .filter(
                TMappings.id_mapping.in_(
                    [import_obj.id_content_mapping, import_obj.id_field_mapping]
                )
            )
            .all()
        )
        import_as_dict = import_obj.as_dict()
        import_as_dict["mappings"] = [m.as_dict() for m in mappings]

        return import_as_dict
    else:
        try:
            return import_in_synthese(import_id)
        except Exception as e:
            DB.session.query(TImports).filter(TImports.id_import == import_id).update(
                {"in_error": True}
            )
            DB.session.commit()

            raise GeonatureImportApiError(message=str(e), details="", status_code=500)


def import_in_synthese(import_id):
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
            "id_area_attachment": "id_area_attachment",
        }
        total_columns = set_total_columns(
            selected_cols, added_cols, import_id, MODULE_CODE
        )

        # check if id_source already exists in synthese table
        is_id_source = check_id_source(import_id)
        print(import_id)
        if is_id_source:
            raise GeonatureImportApiError(
                message="échec : déjà importé (vérification basée sur l'id_source)",
                details="",
                status_code=400,
            )
        logger.info("INSERT IN t_sources")
        # insert into t_sources
        insert_into_t_sources(IMPORTS_SCHEMA_NAME, table_name, import_id, total_columns)

        logger.info("#### Start insert in Synthese")
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
        import_obj = DB.session.query(TImports).get(int(import_id))
        import_obj.import_count = get_n_valid_rows(IMPORTS_SCHEMA_NAME, table_name)
        import_obj.taxa_count = get_n_taxa(
            IMPORTS_SCHEMA_NAME, table_name, total_columns["cd_nom"]
        )
        import_obj.date_min = date_ext["date_min"]
        import_obj.date_max = date_ext["date_max"]
        import_obj.date_end_import = datetime.datetime.now()
        import_obj.is_finished = True
        import_obj.processing = False

        logger.info("-> t_imports updated on final step")

        DB.session.commit()

        mappings = (
            DB.session.query(TMappings)
            .filter(
                TMappings.id_mapping.in_(
                    [import_obj.id_content_mapping, import_obj.id_field_mapping]
                )
            )
            .all()
        )
        import_as_dict = import_obj.as_dict()
        import_as_dict["mappings"] = [m.as_dict() for m in mappings]

        return import_as_dict

    except Exception as e:
        DB.session.rollback()
        logger.error("*** SERVER ERROR WHEN IMPORTING DATA IN GN_SYNTHESE.SYNTHESE")
        logger.exception(e)
        raise GeonatureImportApiError(
            message=str(e), details="",
        )
    finally:
        DB.session.close()
