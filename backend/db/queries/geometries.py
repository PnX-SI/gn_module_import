from geonature.utils.env import DB


def set_geom_4326(schema_name, table_name, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, 4326);
            """.format(
                schema_name=schema_name, 
                table_name=table_name,
                col_name=col_name))
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
                schema_name=schema_name, 
                table_name=table_name,
                col_name=col_name))
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
                schema_name=schema_name, 
                table_name=table_name,
                col_name=col_name,
                local_srid=local_srid))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_local_srid():
    local_srid = DB.session.execute("""
        SELECT parameter_value 
        FROM gn_commons.t_parameters 
        WHERE parameter_name = 'local_srid';
        """).fetchone()[0]
    DB.session.close()
    return int(local_srid)
