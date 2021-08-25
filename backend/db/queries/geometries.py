from sqlalchemy.sql import text

from geonature.utils.env import DB


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


def get_id_area_type():
    query = """
    SELECT id_type 
    FROM ref_geo.bib_areas_types
    WHERE type_code IN ('COM', 'DEP', 'M1', 'M5', 'M10')
    """
    data = DB.session.execute(query).fetchall()
    try:
        return [d[0] for d in data]
    except Exception:
        raise


def check_inside_area_id(id_area: int, wkt: str):
    """
    Checks if the provided wkt is inside the area defined
    by id_area

    Args:
        id_area(int): id to get the area in ref_geo.l_areas
        wkt(str): geometry to check if inside the area
    """
    query = """SELECT ST_CONTAINS(
				(SELECT geom 
                  FROM ref_geo.l_areas
                  WHERE id_area={id}),
                st_transform(st_setsrid(ST_GeomFromText('{wkt}'), 4326), 2154)
                );
            """.format(id=id_area, wkt=wkt)
    try:
        data = DB.session.execute(query).fetchall()
    except Exception:
        # No logs here since it can be called a million times...
        return False

    # Ugly but ST_CONTAINS returns [(False,)] or [(True,)]
    return data[0][0]
