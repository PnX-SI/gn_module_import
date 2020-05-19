from flask import request, current_app

from utils_flask_sqla.response import json_resp
from geonature.utils.env import DB
from geonature.core.gn_permissions import decorators as permissions

from ..transform.transform import (
    field_mapping_data_checking,
    content_mapping_data_checking,
)
from ..api_error import GeonatureImportApiError

from ..blueprint import blueprint


@blueprint.route(
    "/data_checker/<import_id>/field_mapping/<int:id_field_mapping>/content_mapping/<int:id_content_mapping>",
    methods=["GET", "POST"],
)
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def data_checker(info_role, import_id, id_field_mapping, id_content_mapping):
    """
    Check and transform the data for field and content mapping
    """
    try:
        field_mapping_data_checking(import_id, id_field_mapping)
        content_mapping_data_checking(import_id, id_content_mapping)
        return "Done"
    except Exception as e:
        raise GeonatureImportApiError(
            message="INTERNAL SERVER ERROR : Erreur pendant le mapping de correspondance - contacter l'administrateur",
            details=str(e),
        )

