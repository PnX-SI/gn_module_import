from ..wrappers import checker
from ..logs import logger
from ..db.queries.geometries import (
    set_given_geom,
    transform_geom,
    calculate_geom_point,
    add_geom_column,
    set_text,
    calculate_geom_attachement,
    get_id_area_type,
)


@checker("Data cleaning : geometries created")
def set_geometry(
    schema_name,
    table_name,
    given_srid,
    local_srid,
    code_commune_col,
    code_maille_col,
    code_dep_col,
):
    """
    Add 3 geometry columns and 1 column id_area_attachmet to the temp table and fill them from the "given_geom" col
    calculated in python in the geom_check step
    Also calculate the attachment geoms
    """
    try:

        logger.info("creating  geometry columns and transform them (srid and points):")
        add_geom_column(schema_name, table_name, local_srid)
        if given_srid == 4326:
            set_given_geom(schema_name, table_name, "the_geom_4326", 4326)
            transform_geom(
                schema_name=schema_name,
                table_name=table_name,
                target_geom_col="the_geom_local",
                origin_srid="4326",
                target_srid=local_srid,
            )
        else:
            set_given_geom(schema_name, table_name, "the_geom_4326", local_srid)
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
            source_geom_column="the_geom_4326",
            target_geom_column="the_geom_point",
        )
        (
            id_type_comm,
            id_type_dep,
            id_type_m1,
            id_type_m6,
            id_type_m10,
        ) = get_id_area_type()
        print(id_type_dep)
        #  retransform the geom col in text (otherwise dask not working)
        set_text(schema_name, table_name)
        # calculate the geom attachement for communes / maille et département
        if code_commune_col:
            calculate_geom_attachement(
                schema=schema_name,
                table=table_name,
                id_area_type=id_type_comm,
                code_col=code_commune_col,
                ref_geo_area_code_col="area_code",
            )
        if code_dep_col:
            calculate_geom_attachement(
                schema=schema_name,
                table=table_name,
                id_area_type=id_type_dep,
                code_col=code_dep_col,
                ref_geo_area_code_col="area_code",
            )
        if code_dep_col:
            calculate_geom_attachement(
                schema=schema_name,
                table=table_name,
                id_area_type=id_type_m10,
                code_col=code_maille_col,
                ref_geo_area_code_col="area_name",
            )

    except Exception:
        raise
