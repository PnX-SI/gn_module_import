from sqlalchemy import or_

from geonature.utils.env import DB

from ..models import TImports


def get_user_rights(info_role, id_import):
    """
    Get the import if the user has rights
    """
    ors = []
    q = DB.session.query(TImports).filter(TImports.id_import == id_import)
    if info_role.value_filter == "1":
        ors.append(TImports.author.any(id_role=info_role.id_role))
    if info_role.value_filter == "2":
        ors.append(TImports.author.any(id_organisme=info_role.id_organisme))
    q = q.filter(or_(*ors))
    results = q.all()

    return results != []
