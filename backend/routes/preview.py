from geonature.core.gn_permissions import decorators as permissions
from utils_flask_sqla.response import json_resp

from ..api_error import GeonatureImportApiError
from ..db.queries.user_table_queries import (
    set_imports_table_name,
    get_table_names,
    get_table_name,
    get_n_valid_rows,
    get_n_invalid_rows,
    get_valid_bbox,
)

from ..db.queries.metadata import get_id_field_mapping, get_id_mapping
from ..db.queries.nomenclatures import get_saved_content_mapping
from ..db.queries.save_mapping import get_selected_columns
from ..data_preview.preview import set_total_columns, get_preview
from ..logs import logger

from ..blueprint import blueprint


@blueprint.route("/getValidData/<import_id>", methods=["GET", "POST"])
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def get_valid_data(info_role, import_id):

    logger.info("Get valid data for preview")

    ARCHIVES_SCHEMA_NAME = blueprint.config["ARCHIVES_SCHEMA_NAME"]
    IMPORTS_SCHEMA_NAME = blueprint.config["IMPORTS_SCHEMA_NAME"]
    MODULE_CODE = blueprint.config["MODULE_CODE"]

    # get table name
    table_name = set_imports_table_name(get_table_name(import_id))

    # set total user columns
    # form_data = request.form.to_dict(flat=False)
    id_mapping = get_id_field_mapping(import_id)
    selected_cols = get_selected_columns(table_name, id_mapping)

    # added_cols = get_added_columns(id_mapping)
    added_cols = {
        "the_geom_4326": "gn_the_geom_4326",
        "the_geom_local": "gn_the_geom_local",
        "the_geom_point": "gn_the_geom_point",
        "id_area_attachment": "id_area_attachment",
    }

    total_columns = set_total_columns(selected_cols, added_cols, import_id, MODULE_CODE)

    # get content mapping data
    id_content_mapping = get_id_mapping(import_id)
    selected_content = get_saved_content_mapping(id_content_mapping)

    # get valid data preview
    valid_data_list = get_preview(
        import_id,
        MODULE_CODE,
        IMPORTS_SCHEMA_NAME,
        table_name,
        total_columns,
        selected_content,
        selected_cols,
    )

    # get valid gejson
    valid_bbox = get_valid_bbox(IMPORTS_SCHEMA_NAME, table_name)

    # get n valid data
    n_valid = get_n_valid_rows(IMPORTS_SCHEMA_NAME, table_name)

    # get n invalid data
    table_names = get_table_names(
        ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME, int(import_id)
    )
    n_invalid = get_n_invalid_rows(table_names["imports_full_table_name"])
    logger.info("-> got valid data for preview")
    return (
        {
            # 'total_columns': total_columns,
            "valid_data": valid_data_list,
            "n_valid_data": n_valid,
            "n_invalid_data": n_invalid,
            "valid_bbox": valid_bbox,
        },
        200,
    )

    # except Exception as e:
    #     logger.error("*** SERVER ERROR WHEN GETTING VALID DATA")
    #     logger.exception(e)
    #     raise GeonatureImportApiError(
    #         message="INTERNAL SERVER ERROR when getting valid data", details=str(e)
    #     )
