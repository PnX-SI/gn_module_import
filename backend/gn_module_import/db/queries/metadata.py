from geonature.utils.env import DB

from geonature.core.gn_synthese.models import Synthese, CorObserverSynthese

from geonature.core.gn_meta.models import TDatasets

from ..models import TImports


def get_id_roles():
    ids = DB.session.execute(
        """
        SELECT id_role
        FROM utilisateurs.t_roles
        """
    )
    id_roles = [str(id[0]) for id in ids]
    return id_roles
