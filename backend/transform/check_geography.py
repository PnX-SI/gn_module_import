import geopandas as gpd
import pandas as pd
from shapely import wkt, wkb
import numpy as np

from ..db.queries.user_errors import set_user_error
from ..logs import logger
from .utils import fill_map, set_is_valid, set_invalid_reason
from ..wrappers import checker
from ..utils.utils import create_col_name


def set_wkb(value):
    try:
        return wkb.dumps(wkt.loads(value)).hex()
    except Exception:
        return None


def check_multiple_code(val):
    if val:
        return len(val.split(",")) == 1
    return True


def check_wkt(value, min_x, max_x, min_y, max_y):
    try:
        if value.geom_type == "Point":
            if value.x > max_x:
                return False
            if value.x < min_x:
                return False
            if value.y > max_y:
                return False
            if value.y < min_y:
                return False
        if value.geom_type == "Polygon":
            for x_coord in value.exterior.coords.xy[0]:
                if x_coord > max_x:
                    return False
                if x_coord < min_x:
                    return False
            for y_coord in value.exterior.coords.xy[1]:
                if y_coord > max_y:
                    return False
                if y_coord < min_y:
                    return False
        return True
    except Exception:
        return True


def manage_erros_and_validity(
    df, import_id, schema_name, id_error, df_temp_col, column_invalid, nb_invalid
):
    """
            High level function to set column which are valid in the dataframe
            and to write in database the errors
        """
    set_is_valid(df, df_temp_col)
    if nb_invalid > 0:
        set_user_error(import_id, id_error, column_invalid, nb_invalid)
        set_invalid_reason(
            df, schema_name, df_temp_col, import_id, id_error, column_invalid,
        )


@checker("Data cleaning : geographic data checked")
def check_geography(
    df, import_id, added_cols, selected_columns, srid, local_srid, schema_name
):
    try:

        logger.info("CHECKING GEOGRAPHIC DATA:")
        line_with_codes = df.index[
            (df["codecommune"].notnull())
            | (df["codemaille"].notnull())
            | (df["codedepartement"].notnull())
        ]
        # index starting at 1 -> so -1
        line_with_codes = line_with_codes - 1
        if srid == 4326:
            max_x = 180
            min_x = -180
            max_y = 90
            min_y = -90
        if srid == 2154:
            max_x = 1300000
            min_x = 100000
            max_y = 7200000
            min_y = 6000000
        if "latitude" and "longitude" in selected_columns.keys():
            coordinates = ["longitude", "latitude"]

            for col in coordinates:
                logger.info("- converting %s in numeric values", selected_columns[col])

                col_name = "_".join(["temp", col])
                df[col_name] = pd.to_numeric(df[selected_columns[col]], "coerce")

                logger.info(
                    "- checking consistency of values in %s synthese column (= %s user column):",
                    col,
                    selected_columns[col],
                )

                if col == "longitude":
                    df["invalid_lat_long"] = df[col_name].le(min_x) | df[col_name].ge(
                        max_x
                    )
                if col == "latitude":
                    df["invalid_lat_long"] = df[col_name].le(min_y) | df[col_name].ge(
                        max_y
                    )
                df["valid_lat_long"] = ~df["invalid_lat_long"]
                # remove invalid where codecommune/maille or dep are fill
                df.iloc[line_with_codes, df.columns.get_loc("valid_lat_long")] = True
                set_is_valid(df, "valid_lat_long")
                n_bad_coo = (df["invalid_lat_long"]).sum()

                # setting eventual inconsistent values to pd.np.nan
                df[col_name] = df[col_name].where(df["temp"], pd.np.nan)

                logger.info(
                    "%s inconsistant values detected in %s synthese column (= %s user column)",
                    n_bad_coo,
                    col,
                    selected_columns[col],
                )

                if n_bad_coo > 0:
                    set_user_error(import_id, 13, selected_columns[col], n_bad_coo)
                    set_invalid_reason(
                        df,
                        schema_name,
                        "valid_lat_long",
                        import_id,
                        13,
                        selected_columns[col],
                    )
                df.drop("invalid_lat_long", axis=1)
                df.drop("valid_lat_long", axis=1)

        elif "WKT" in selected_columns.keys():
            # create wkt with crs provided by user
            crs = {"init": "epsg:{}".format(srid)}

            # load wkt
            df["given_geom"] = df[selected_columns["WKT"]].apply(lambda x: set_wkb(x))

            df["valid_wkt"] = df["given_geom"].notnull()

            # remove invalid where codecommune/maille or dep are fill
            df.iloc[line_with_codes, df.columns.get_loc("valid_wkt")] = True
            set_is_valid(df, "valid_wkt")
            n_bad_coo = (~df["valid_wkt"]).sum()
            logger.info(
                "%s inconsistant values detected in %s column",
                n_bad_coo,
                selected_columns["WKT"],
            )

            if n_bad_coo > 0:
                set_user_error(import_id, 13, selected_columns["WKT"], n_bad_coo)
                set_invalid_reason(
                    df, schema_name, "valid_wkt", import_id, 13, selected_columns["WKT"]
                )
            df.drop("valid_wkt", axis=1)

        if (
            "codemaille" in selected_columns.keys()
            or "codecommune" in selected_columns.keys()
            or "codedepartement" in selected_columns.keys()
        ):
            # check if multiple type code for one line (code commune and code maille for ex)
            df["is_multiple_type_code"] = (
                (df[selected_columns["codecommune"]].notnull())
                & (df[selected_columns["codemaille"]].notnull())
                | (
                    (df[selected_columns["codecommune"]].notnull())
                    & (df[selected_columns["codedepartement"]].notnull())
                )
                | (
                    (df[selected_columns["codemaille"]].notnull())
                    & (df[selected_columns["codedepartement"]].notnull())
                )
            )
            # set gn_is_valid where not is_multiple_type_code = true (~ invert a boolean)
            df["line_with_one_code"] = ~df["is_multiple_type_code"]
            set_is_valid(df, "line_with_one_code")
            nb_invalid = (df["is_multiple_type_code"]).sum()
            set_user_error(import_id, 19, selected_columns["codecommune"], nb_invalid)
            set_invalid_reason(
                df,
                schema_name,
                "line_with_one_code",
                import_id,
                19,
                selected_columns["codecommune"],
            )
            df.drop("line_with_one_code", axis=1)
            df.drop("is_multiple_type_code", axis=1)

            #  check if the value in the code are not multiple (ex code_commune = '77005, 77006')
            df["one_comm_code"] = df[selected_columns["codecommune"]].apply(
                lambda x: check_multiple_code(x)
            )
            manage_erros_and_validity(
                df,
                import_id,
                schema_name,
                20,
                "one_comm_code",
                selected_columns["codecommune"],
                (~df["one_comm_code"]).sum(),
            )
            df.drop("one_comm_code", axis=1)

            df["one_maille_code"] = df[selected_columns["codemaille"]].apply(
                lambda x: check_multiple_code(x)
            )
            manage_erros_and_validity(
                df,
                import_id,
                schema_name,
                20,
                "one_maille_code",
                selected_columns["codemaille"],
                (~df["one_maille_code"]).sum(),
            )
            df.drop("one_maille_code", axis=1)

            df["one_dep_code"] = df[selected_columns["codedepartement"]].apply(
                lambda x: check_multiple_code(x)
            )
            manage_erros_and_validity(
                df,
                import_id,
                schema_name,
                20,
                "one_dep_code",
                selected_columns["codedepartement"],
                (~df["one_dep_code"]).sum(),
            )
            df.drop("one_dep_code", axis=1)
    except Exception:
        raise
