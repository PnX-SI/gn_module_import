from ..wrappers import checker
from ..logs import logger
from ..db.queries.geometries import*


@checker('Data cleaning : geometries created')
def set_geometry(schema_name, table_name, local_srid):

    try:

        logger.info('creating geometries (postgis from wkt):')

        set_geom_4326(schema_name, table_name)
        set_geom_point(schema_name, table_name)
        set_geom_local(schema_name, table_name, local_srid)

    except Exception:
        raise