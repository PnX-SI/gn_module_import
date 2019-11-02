from geonature.utils.env import DB


def get_data_type(column_name):
    try:
        key_type = DB.session.execute("""
            SELECT data_type 
            FROM information_schema.columns
            WHERE table_name = 'synthese'
            AND column_name = '{column_name}';
            """.format(column_name = column_name)).fetchone()[0]
        return key_type
    except Exception:
        raise


def insert_into_synthese(schema_name, table_name, select_part, total_columns):
    try:    
        DB.session.execute("""
            INSERT INTO gn_synthese.synthese ({into_part})
            SELECT {select_part}
            FROM {schema_name}.{table_name}
            WHERE gn_is_valid='True';
            """.format(
                into_part = ','.join(total_columns.keys()),
                select_part = ','.join(select_part),
                schema_name = schema_name,
                table_name = table_name
            ))
    except Exception:
        raise