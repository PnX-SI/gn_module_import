from geonature.utils.env import DB

"""
correct error : the_geom_4326 doit pas etre fix
"""


def set_geom_4326(schema_name, table_name, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, 4326);
            """.format(
                schema_name = schema_name, 
                table_name = table_name,
                col_name = col_name))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def set_geom_point(schema_name, table_name, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, 4326);
            """.format(
                schema_name = schema_name, 
                table_name = table_name,
                col_name = col_name))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def set_geom_local(schema_name, table_name, local_srid, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, {local_srid});
            """.format(
                schema_name = schema_name, 
                table_name = table_name,
                col_name = col_name,
                local_srid = local_srid))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise