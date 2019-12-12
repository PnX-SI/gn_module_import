from geonature.utils.env import DB

from geonature.core.gn_synthese.models import (
    Synthese,
    CorObserverSynthese
)

from geonature.core.gn_meta.models import TDatasets

from ..models import (
    CorRoleImport,
    CorImportArchives,
    TImports
)


def delete_import_CorImportArchives(id_import):
    """ Delete an import from cor_import_archives table.

        Args:
            id_import (int) : import id to delete
        Returns:
            None

    """
    DB.session.query(CorImportArchives) \
        .filter(CorImportArchives.id_import == id_import) \
        .delete()


def delete_import_CorRoleImport(id_import):
    """ Delete an import from cor_role_import table.

        Args:
            id_import (int) : import id to delete
        Returns:
            None

    """
    DB.session.query(CorRoleImport) \
        .filter(CorRoleImport.id_import == id_import) \
        .delete()


def delete_import_TImports(id_import):
    """ Delete an import from t_imports table.

    Args:
        id_import (int) : import id to delete
    Returns:
        None

    """
    DB.session.query(TImports) \
        .filter(TImports.id_import == id_import) \
        .delete()


def test_user_dataset(id_role, current_dataset_id):
    """ Test if the dataset_id provided in the url path ("url/process/dataset_id") is allowed
        (allowed = in the list of dataset_ids previously created by the user)

        Args:
            id_role (int) : id_role of the user
            current_dataset_id (str?) : dataset_id provided in the url path

        Returns:
            Boolean : True if allowed, False if not allowed
    """

    results = DB.session.query(TDatasets) \
        .filter(TDatasets.id_dataset == Synthese.id_dataset) \
        .filter(CorObserverSynthese.id_synthese == Synthese.id_synthese) \
        .filter(CorObserverSynthese.id_role == id_role) \
        .distinct(Synthese.id_dataset) \
        .all()

    dataset_ids = []

    for r in results:
        dataset_ids.append(r.id_dataset)

    if int(current_dataset_id) not in dataset_ids:
        return False

    return True


def get_id_roles():
    ids = DB.session.execute("""
        SELECT id_role
        FROM utilisateurs.t_roles
        """)
    id_roles = [str(id[0]) for id in ids]
    return id_roles


def get_id_mapping(import_id):
    try:
        t_import = DB.session \
            .query(TImports.id_content_mapping) \
            .filter(TImports.id_import == int(import_id)) \
            .one()
        return t_import.id_content_mapping
    except Exception:
        raise


def get_id_field_mapping(import_id):
    try:
        t_import = DB.session \
            .query(TImports.id_field_mapping) \
            .filter(TImports.id_import == int(import_id)) \
            .one()
        return t_import.id_field_mapping
    except Exception:
        raise