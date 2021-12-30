from geonature.utils.env import DB
from ..models import TImports


def get_valid_user_data(schema_name, table_name, selected_cols, limit):
    cols = [str(target) for source, target in selected_cols.items()]
    preview = DB.session.execute("""
            SELECT {cols}
            FROM {schema_name}.{table_name}
            WHERE gn_is_valid = 'True'
            LIMIT {limit};        
            """.format(
                cols=",".join(cols),
                schema_name = schema_name,
                table_name = table_name,
                limit = limit
            )).fetchall()
    transformed_preview = []
    # HACK : the nomenclature column could be too long for postgreSQL
    # we can select it with the long column name, but the response give the truncated column name
    # which cause a bug on preview.py when we want to access to the dict key
    # here we recreate the original dict with the correct long column name/ley
    for p in preview:
        temp = {}
        for i in range(len(cols)):
            temp[cols[i]] = p[i]
        transformed_preview.append(temp)
    return transformed_preview


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
        