import geopandas as gpd
import pandas as pd
from shapely import wkt, wkb
from shapely.geometry import Point, Polygon
from shapely.ops import transform
import numpy as np
from pyproj import CRS, Transformer, Proj


from ..logs import logger
from .utils import fill_map, set_is_valid, set_error_and_invalid_reason
from ..wrappers import checker
from ..utils.utils import create_col_name


def set_wkb(value):
    try:
        return wkb.dumps(wkt.loads(value)).hex()
    except Exception:
        return None


def x_y_to_wkb(x, y):
    try:
        assert not pd.isna(x)
        assert not pd.isna(y)
        wkt = Point(float(x), float(y))

        return Point(float(x), float(y)).wkb.hex()
    except Exception:
        return None


def check_bounds_x_y(x, y, file_srid_bounding_box: Polygon):
    #  if is Nan (float): don't check if fit bounds
    # x and y are str
    if type(x) is float or type(y) is float:
        return True
    else:
        cur_wkt = Point(float(x), float(y))
        return check_bound(cur_wkt, file_srid_bounding_box)


def check_bounds_wkt(value, file_srid_bounding_box: Polygon):
    try:
        cur_wkt = wkt.loads(value)
        if not cur_wkt:
            return True
        return check_bound(cur_wkt, file_srid_bounding_box)
    except Exception:
        return True


def check_multiple_code(val):
    if val:
        return len(val.split(",")) == 1
    return True


def check_bound(wkt, file_srid_bounding_box: Polygon):
    try:
        return wkt.within(file_srid_bounding_box)
    except Exception:
        return True

        # id_rows_invalid = df.index[df["temp"] == False].to_list()


def calculate_bounding_box(given_srid):
    """
    calculate the local bounding box and 
    return a shapely polygon of this BB with local coordq
    """
    xmin, ymin, xmax, ymax = CRS.from_epsg(given_srid).area_of_use.bounds
    bounding_polygon_4326 = Polygon([
        (xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)
    ])

    projection = Transformer.from_crs(
        CRS(4326),
        CRS(int(given_srid)),
        always_xy=True
    )
    return transform(projection.transform, bounding_polygon_4326)


def manage_erros_and_validity(
    df, import_id, schema_name, code_error, df_temp_col, column_invalid, id_rows_error
):
    """
        High level function to set column which are valid in the dataframe
        and to write in database the errors
    """
    set_is_valid(df, df_temp_col)
    if len(id_rows_error) > 0:
        set_error_and_invalid_reason(
            df=df,
            id_import=import_id,
            error_code=code_error,
            col_name_error=column_invalid,
            df_col_name_valid=df_temp_col,
            id_rows_error=id_rows_error,
        )


@checker("Data cleaning : geographic data checked")
def check_geography(
    df, import_id, added_cols, selected_columns, srid, local_srid, schema_name
):
    try:

        logger.info("CHECKING GEOGRAPHIC DATA:")
        file_srid_bounding_box = calculate_bounding_box(srid)
        line_with_codes = []

        try:
            mask_with_code = (
                (df[selected_columns["codecommune"]].notnull())
                | (df[selected_columns["codemaille"]].notnull())
                | (df[selected_columns["codedepartement"]].notnull())
            )

            line_with_codes = df[mask_with_code].index
        except KeyError:
            pass
        df["given_geom"] = None
        # index starting at 1 -> so -1
        if len(line_with_codes) > 0:
            line_with_codes = line_with_codes - 1
        if "latitude" and "longitude" in selected_columns.keys():
            df["given_geom"] = df.apply(
                lambda row: x_y_to_wkb(
                    row[selected_columns["longitude"]],
                    row[selected_columns["latitude"]],
                ),
                axis=1,
            )
            df["valid_x_y"] = df["given_geom"].notnull()
            df["no_geom"] = ~mask_with_code & ~df["valid_x_y"]
            df["geom_correct"] = ~df["no_geom"]
            no_geom_errors = df[df["no_geom"] == True]
            if len(no_geom_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="NO-GEOM",
                    col_name_error="Colonnes géometriques",
                    df_col_name_valid="geom_correct",
                    id_rows_error=no_geom_errors.index.to_list(),
                )

            df["in_bounds"] = df.apply(
                lambda row: check_bounds_x_y(
                    row[selected_columns["longitude"]],
                    row[selected_columns["latitude"]],
                    file_srid_bounding_box,
                ),
                axis=1,
            )

            out_bound_errors = df[df["in_bounds"] == False]
            if len(out_bound_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="PROJECTION_ERROR",
                    col_name_error="Colonnes géometriques",
                    df_col_name_valid="in_bounds",
                    id_rows_error=out_bound_errors.index.to_list(),
                )

        elif "WKT" in selected_columns:
            # load wkt
            df["given_geom"] = df[selected_columns["WKT"]].apply(lambda x: set_wkb(x))
            df["valid_wkt"] = df["given_geom"].notnull()
            #  row with not code and no valid wkt
            df["no_geom"] = ~mask_with_code & ~df["valid_wkt"]
            df["geom_correct"] = ~df["no_geom"]
            no_geom_errors = df[df["no_geom"] == True]
            if len(no_geom_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="NO-GEOM",
                    col_name_error="Colonnes géometriques",
                    df_col_name_valid="geom_correct",
                    id_rows_error=no_geom_errors.index.to_list(),
                )

                df["in_bounds"] = df.apply(
                    lambda row: check_bounds_wkt(
                        row[selected_columns["WKT"]], file_srid_bounding_box
                    ),
                    axis=1,
                )
                out_bound_errors = df[df["in_bounds"] == False]
                if len(out_bound_errors) > 0:
                    set_error_and_invalid_reason(
                        df=df,
                        id_import=import_id,
                        error_code="PROJECTION_ERROR",
                        col_name_error="Colonnes géometriques",
                        df_col_name_valid="in_bounds",
                        id_rows_error=out_bound_errors.index.to_list(),
                    )

            # remove invalid where codecommune/maille or dep are fill
            df["valid_wkt"] = df.iloc[
                line_with_codes, df.columns.get_loc("valid_wkt")
            ] = True
            set_is_valid(df, "valid_wkt")
            id_rows_errors = df.index[df["valid_wkt"] == False].to_list()

            logger.info(
                "%s inconsistant values detected in %s column",
                len(id_rows_errors),
                selected_columns["WKT"],
            )

            if len(id_rows_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="INVALID_WKT",
                    col_name_error=selected_columns["WKT"],
                    df_col_name_valid="valid_wkt",
                    id_rows_error=id_rows_errors,
                )
        # if no wkt and no x/y
        else:
            df["no_geom"] = ~mask_with_code & df["given_geom"].isnull()
            no_geom_errors = df[df["no_geom"] == True]
            df["geom_correct"] = ~df["no_geom"]
            if len(no_geom_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="NO-GEOM",
                    col_name_error="Colonnes géometriques",
                    df_col_name_valid="geom_correct",
                    id_rows_error=no_geom_errors.index.to_list(),
                )

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
            id_rows_errors = df.index[df["line_with_one_code"] == False].to_list()

            if len(id_rows_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="MULTIPLE_ATTACHMENT_TYPE_CODE",
                    col_name_error=selected_columns["codecommune"],
                    df_col_name_valid="line_with_one_code",
                    id_rows_error=id_rows_errors,
                )
            df.drop("line_with_one_code", axis=1)
            df.drop("is_multiple_type_code", axis=1)

            #  check if the value in the code are not multiple (ex code_commune = '77005, 77006')
            df["one_comm_code"] = df[selected_columns["codecommune"]].apply(
                lambda x: check_multiple_code(x)
            )
            id_rows_error = df.index[df["one_comm_code"] == False].to_list()
            manage_erros_and_validity(
                df,
                import_id,
                schema_name,
                "MULTIPLE_CODE_ATTACHMENT",
                "one_comm_code",
                selected_columns["codecommune"],
                id_rows_error,
            )
            df.drop("one_comm_code", axis=1)

            df["one_maille_code"] = df[selected_columns["codemaille"]].apply(
                lambda x: check_multiple_code(x)
            )
            id_rows_error = df.index[df["one_maille_code"] == False].to_list()

            manage_erros_and_validity(
                df,
                import_id,
                schema_name,
                "MULTIPLE_CODE_ATTACHMENT",
                "one_maille_code",
                selected_columns["codemaille"],
                id_rows_error,
            )
            df.drop("one_maille_code", axis=1)

            df["one_dep_code"] = df[selected_columns["codedepartement"]].apply(
                lambda x: check_multiple_code(x)
            )
            id_rows_error = df.index[df["one_dep_code"] == False].to_list()

            manage_erros_and_validity(
                df,
                import_id,
                schema_name,
                "MULTIPLE_CODE_ATTACHMENT",
                "one_dep_code",
                selected_columns["codedepartement"],
                id_rows_error,
            )

            df = df.drop("one_dep_code", axis=1)
            df = df.drop("one_maille_code", axis=1)
            df = df.drop("one_comm_code", axis=1)
            df = df.drop("is_multiple_type_code", axis=1)
            df = df.drop("line_with_one_code", axis=1)

    except Exception:
        raise
