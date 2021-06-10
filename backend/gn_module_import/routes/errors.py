from flask import jsonify
from sqlalchemy.orm import joinedload

from geonature.core.gn_permissions import decorators as permissions

from gn_module_import.blueprint import blueprint
from gn_module_import.db.models import TImports


@blueprint.route("/imports/<int:import_id>/errors", methods=["GET"])
@permissions.check_cruved_scope("R", True, module_code="IMPORT")
def get_import_errors(info_role, import_id):
    """
    .. :quickref: Import; Get errors of an import.

    Get errors of an import.
    """
    imprt = TImports.query.options(joinedload('errors')).get_or_404(import_id)
    imprt.check_instance_permission(info_role)
    return jsonify([ error.as_dict(fields=['type'])
                     for error in imprt.errors ])
