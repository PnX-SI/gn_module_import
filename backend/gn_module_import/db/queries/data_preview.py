from geonature.utils.env import DB
from ..models import TImports


def get_synthese_fields():
    synthese_fields = DB.session.execute("""
        SELECT column_name, ordinal_position
        FROM information_schema.columns
        WHERE table_name = 'synthese'
        ORDER BY ordinal_position ASC;
        """).fetchall()
    return synthese_fields


def get_id_module(module_name):
    # set 'IMPORT' as variable initialized in configuration parameter ?
    id_module = DB.session.execute("""
        SELECT id_module
        FROM gn_commons.t_modules
        WHERE module_code = '{module_name}';
        """.format(
            module_name=module_name
        )).fetchone()[0]
    return id_module
