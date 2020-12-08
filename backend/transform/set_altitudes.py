from ..wrappers import checker
from ..logs import logger
from ..db.queries.altitudes import create_column, generate_altitudes
from ..utils.utils import create_col_name


@checker("Data cleaning : altitudes created")
def set_altitudes(
    df,
    selected_columns,
    import_id,
    schema_name,
    full_table_name,
    table_name,
    index_col,
    is_generate_alt,
    the_geom_local_col,
):
    try:

        logger.info("generating altitudes :")

        if is_generate_alt:
            added_cols = {}
            # altitude_min

            # create_col_name(df, added_cols, "altitude_min", import_id)

            generate_type = None
            if "altitude_min" not in selected_columns.keys():
                generate_type = "generate_all"
                df["gn_altitude_min"] = ""
                create_column(
                    full_table_name=full_table_name, alt_col="gn_altitude_min"
                )
            else:
                original_alt_col = selected_columns["altitude_min"]
                generate_type = "generate_missing"

            if "altitude_max" not in selected_columns.keys():
                df["gn_altitude_max"] = ""
                create_column(
                    full_table_name=full_table_name, alt_col="gn_altitude_max"
                )
            else:
                original_alt_col = selected_columns["altitude_max"]

            generate_altitudes(
                schema=schema_name,
                table=table_name,
                alti_min_col=selected_columns.get("altitude_min", "gn_altitude_min"),
                alti_max_col=selected_columns.get("altitude_max", "gn_altitude_max"),
                table_pk=index_col,
                geom_col=the_geom_local_col,
                generate_type=generate_type,
            )

    except Exception:
        raise
