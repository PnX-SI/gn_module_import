from utils_flask_sqla.response import json_resp
from geonature.core.gn_permissions import decorators as permissions

from ..db.queries.rights import get_user_rights
from ..blueprint import blueprint

@blueprint.route("/permissions/U/<int:id_import>", methods=["GET"])
@permissions.check_cruved_scope("U", True, module_code="IMPORT")
@json_resp
def can_user_update_import(info_role, id_import):
    return {"is_authorized": get_user_rights(info_role, id_import)}
