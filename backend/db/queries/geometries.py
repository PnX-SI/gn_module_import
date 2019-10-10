from geonature.utils.env import DB

"""
correct error : the_geom_4326 doit pas etre fix
"""


def set_geom_4326(schema_name, table_name):
    DB.session.execute("""
        UPDATE {schema_name}.{table_name} 
        SET the_geom_4326 = ST_SetSRID(the_geom_4326, 4326);"""\
            .format(
                schema_name = schema_name, 
                table_name = table_name
                ))
    DB.session.flush()


def set_geom_point(schema_name, table_name):
    DB.session.execute("""
        UPDATE {schema_name}.{table_name} SET the_geom_point = ST_SetSRID(the_geom_point, 4326);"""\
            .format(
                schema_name = schema_name, 
                table_name = table_name))
    DB.session.flush()


def set_geom_local(schema_name, table_name, local_srid):
    DB.session.execute("""
        UPDATE {schema_name}.{table_name} 
        SET the_geom_local = ST_SetSRID(the_geom_local, {local_srid});"""\
            .format(
                schema_name = schema_name, 
                table_name = table_name, 
                local_srid = local_srid))
    DB.session.flush()