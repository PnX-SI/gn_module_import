from geonature.utils.env import DB
from ..models import TImports


def get_valid_user_data(schema_name, table_name, limit):
    try:
        preview = DB.session.execute("""
                SELECT *
                FROM {schema_name}.{table_name}
                WHERE gn_is_valid = 'True'
                LIMIT {limit};        
                """.format(
                    schema_name = schema_name,
                    table_name = table_name,
                    limit = limit
                )).fetchall()
        return preview
    except Exception:
        raise


def get_synthese_fields():
    try:
        synthese_fields = DB.session.execute("""
            SELECT column_name, ordinal_position
            FROM information_schema.columns
            WHERE table_name = 'synthese'
            ORDER BY ordinal_position ASC;
            """).fetchall()
        return synthese_fields
    except Exception:
        raise


def get_id_module(module_name):
    # set 'IMPORT' as variable initialized in configuration parameter ?
    try:
        id_module = DB.session.execute("""
            SELECT id_module
            FROM gn_commons.t_modules
            WHERE module_code = '{module_name}';
            """.format(
                module_name=module_name
            )).fetchone()[0]
        return id_module
    except Exception:
        raise


def get_id_dataset(import_id):
    try:
        id_dataset = DB.session.query(TImports.id_dataset)\
            .filter(TImports.id_import == import_id)\
            .one()[0]
        return id_dataset
    except Exception:
        raise


def get_synthese_dict_fields(schema_name):
    try:
        fields = DB.session.execute("""
            SELECT name_field
            FROM {schema_name}.dict_fields
            WHERE synthese_field = TRUE;
        """.format(schema_name = schema_name)
        ).fetchall()
        fields_list = [field[0] for field in fields]
        return fields_list
    except Exception:
        raise
        