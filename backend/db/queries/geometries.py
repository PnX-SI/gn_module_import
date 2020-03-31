from sqlalchemy.sql import text

from geonature.utils.env import DB


def set_geom_4326(schema_name, table_name, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, 4326);
            """.format(
                schema_name=schema_name, table_name=table_name, col_name=col_name
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def add_geom_column(schema_name, table_name):
    DB.session.begin(subtransactions=True)
    # delete = """
    # ALTER TABLE {schema_name}.{table_name}
    # DROP COLUMN IF EXISTS the_geom_4326
    # """
    DB.session.execute(
        """
        SELECT public.AddGeometryColumn({schema_name}, {table_name}, the_geom_4326, 4326, 'Geometry', 2 );
        SELECT public.AddGeometryColumn({schema_name}, {table_name}, the_geom_local, {local_srid} 'Geometry', 2 );
        SELECT public.AddGeometryColumn({schema_name}, {table_name}, the_geom_4326, 4326, 'POINT', 2 );
        """.format(
            schema_name=schema_name, table_name=table_name, local_srid=local_srid,
        )
    )


def set_given_geom(schema_name, table_name, source_column, target_colmun):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            UPDATE {schema_name}.{table_name} 
            SET {target_colmun}={source_column}
            )
            """.format(
                schema_name=schema_name,
                table_name=table_name,
                target_colmun=target_colmun,
                source_column=source_column,
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def transform_geom(schema_name, table_name, target_geom_col, origin_srid, target_srid):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            UPDATE {schema_name}.{table_name} 
            SET {target_geom_col} = ST_transform(
                ST_SetSRID(the_given_geom, {origin_srid}), 
                {target_srid}
            )
            """.format(
                schema_name=schema_name,
                table_name=table_name,
                col_name=col_name,
                origin_srid=origin_srid,
                target_srid=target_srid,
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def calculate_geom_point(
    schema_name, table_name, source_geom_column, target_geom_column
):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            UPDATE {schema_name}.{table_name} 
            SET {target_geom_column} = ST_centroid({source_geom_column});
            """.format(
                schema_name=schema_name,
                table_name=table_name,
                target_geom_column=target_geom_column,
                source_geom_column=target_geom_column,
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def set_geom_point(schema_name, table_name, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, 4326);
            """.format(
                schema_name=schema_name, table_name=table_name, col_name=col_name
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def set_geom_local(schema_name, table_name, local_srid, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, {local_srid});
            """.format(
                schema_name=schema_name,
                table_name=table_name,
                col_name=col_name,
                local_srid=local_srid,
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_local_srid():
    local_srid = DB.session.execute(
        """
        SELECT parameter_value 
        FROM gn_commons.t_parameters 
        WHERE parameter_name = 'local_srid';
        """
    ).fetchone()[0]
    DB.session.close()
    return int(local_srid)


def calculate_geom_attachement(id_area_type, code):
    """
    Return an id_area and its geom (in local_srid)
    from a geom column

    :param int id_area_type: the id_area type (ref_geo.bib_areas)
    :param str code: the attachment code (could be in insee code, a dep code..)
    """
    query = text(
        """
        SELECT id_area, geom
        FROM ref_geo.l_areas
        WHERE id_type = :id_area_type and area_code=:code
        """
    )
    result = DB.session.execute(
        query, {"id_area_type": id_area_type, "area_code": code}
    ).fetchone()
    if len(result) > 0:
        return result[0]
    return None


def update_geoms(schema_name, table_name, col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            UPDATE {schema_name}.{table_name} 
            SET {col_name} = ST_SetSRID({col_name}, 4326);
            """.format(
                schema_name=schema_name, table_name=table_name, col_name=col_name
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_id_area_type():
    query = """
    SELECT id_type 
    FROM ref_geo.l_areas
    WHERE type_code IN ('COM', 'DEP', 'M1', 'M5', 'M10')
    """
    return DB.session.execute(query).fetchall()


def calculate_geom_from_code(area_code, id_type_area):
    """
    Return the geom and the id_area from a code and its id_type
    id_type can be a list or an integer (in order to find any code_maille)
    """
    if type(id_type_area) is list:
        query = text(
            """
            SELECT id_area, geom
            FROM ref_geo.l_areas
            WHERE area_code = :area_code AND id_type IN :id_type_area
            """
        )
    else:
        query = text(
            """
            SELECT id_area, geom
            FROM ref_geo.l_areas
            WHERE area_code = :area_code AND id_type=:id_type_area
            """
        )

    results = DB.session.execute(
        query, {"area_code": area_code, "id_type_area": id_type_area}
    ).fetchone()
    if len(results) > 0:
        return results[0]
    return None
