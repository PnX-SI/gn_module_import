from geonature.utils.env import DB


def is_cd_nom_required(schema_name):
    try:
        is_nullable = DB.session.execute(
            """
            SELECT mandatory
            FROM {schema_name}.dict_fields
            WHERE name_field = 'cd_nom';
            """.format(
                schema_name=schema_name
            )
        ).fetchone()[0]
        return is_nullable
    except Exception:
        raise


def get_types(schema_name, selected_columns):
    try:
        data_types = DB.session.execute("""
            SELECT DISTINCT type_field
            FROM {schema_name}.dict_fields
            WHERE name_field IN ({col_list});
            """.format(
                schema_name=schema_name,
                col_list=",".join(map(repr, selected_columns.keys()))
            )).fetchall()
        types = [data.type_field for data in data_types]
        return types
    except Exception:
        raise
