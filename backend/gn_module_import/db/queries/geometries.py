from sqlalchemy.sql import text

from geonature.utils.env import DB


def get_id_area_type():
    query = """
    SELECT id_type 
    FROM ref_geo.bib_areas_types
    WHERE type_code IN ('COM', 'DEP', 'M1', 'M5', 'M10')
    """
    data = DB.session.execute(query).fetchall()
    return [d[0] for d in data]
