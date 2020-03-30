from ..wrappers import checker
from ..logs import logger
from ..db.queries.geometries import *


@checker("Data cleaning : geometries created")
def set_geometry(
    schema_name, table_name, given_geometry, local_srid, col_4326, col_point, col_local
):

    try:

        logger.info("creating geometries (postgis from wkt):")

        if given_geometry == "4326":
            transform_geom(
                schema_name=schema_name,
                table_name=table_name,
                target_geom_col="the_geom_local",
                origin_srid="4326",
                target_srid=local_srid,
            )
        else:
            transform_geom(
                schema_name=schema_name,
                table_name=table_name,
                target_geom_col="the_geom_local",
                origin_srid=local_srid,
                target_srid="4326",
            )

        calculate_geom_point(
            schema_name=schema_name,
            table_name=table_name,
            source_geom_column="the_geom_local",
            target_geom_column="the_geom_point",
        )

    except Exception:
        raise
