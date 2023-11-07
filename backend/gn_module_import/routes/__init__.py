from geonature.core.gn_permissions.decorators import login_required

from gn_module_import.models import Destination
from gn_module_import.schemas import DestinationSchema
from gn_module_import.blueprint import blueprint


@blueprint.route("/destinations/", methods=["GET"])
@login_required
def list_destinations():
    schema = DestinationSchema()
    destinations = Destination.query.all()
    # FIXME: filter with C permissions?
    return schema.dump(destinations, many=True)
