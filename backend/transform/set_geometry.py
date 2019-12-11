from ..wrappers import checker
from ..logs import logger
from ..db.queries.geometries import*


@checker('Data cleaning : geometries created')
def set_geometry(schema_name, table_name, local_srid, col_4326, col_point, col_local):

    try:

        logger.info('creating geometries (postgis from wkt):')

        set_geom_4326(schema_name, table_name, col_4326)
        set_geom_point(schema_name, table_name, col_point)
        set_geom_local(schema_name, table_name, local_srid, col_local)

    except Exception:
        raise
