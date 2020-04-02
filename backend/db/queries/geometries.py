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

