import pandas as pd
import dask
from .utils import fill_col, set_error_and_invalid_reason
from ..wrappers import checker


def check_alt_min_max(min_val, max_val):
    try:
        if pd.isnull(min_val) or pd.isnull(max_val):
            return True
        else:
            if int(min_val) > int(max_val):
                return False
            else:
                return True
    except Exception:
        return True


@checker("Data cleaning : altitudes checked")
def check_altitudes(
    df, selected_columns, synthese_info, calcul, import_id, schema_name
):
    """
    - if user want to calculate altitudes:
        -> if only altitude max column provided, calculates altitude min column
        -> if only altitude min column provided, calculates altitude max column
        -> if both alt_min and max columns provided, calculate missing values
        -> if no altitude column provided, calculates altitude min and max

    - if user doesn't want to calculate altitudes:
        -> if only altitude min column provided, altitude max column = altitude min column
        -> if only altitude max column provided, altitude min column = 0
        -> if both alt_min and max columns provided :
            . does nothing except check if altitude min <= max if min != NA and max!= NA

    replace alt min = 0 if alt min = NA ?
    """

    try:

        altitudes = []

        for element in list(selected_columns.keys()):
            if element == "altitude_min" or element == "altitude_max":
                altitudes.append(element)

        if calcul is False:

            if len(altitudes) == 2:

                # check max >= min
                df["temp"] = ""
                df["temp"] = df.apply(
                    lambda x: check_alt_min_max(
                        x[selected_columns["altitude_min"]],
                        x[selected_columns["altitude_max"]],
                    ),
                    axis=1,
                )
                df["gn_is_valid"] = df["gn_is_valid"].where(
                    cond=df["temp"].apply(lambda x: fill_col(x)), other=False
                )
                # get invalid id rows
                id_rows_errors = df.index[df["temp"] == False].to_list()

                if len(id_rows_errors) > 0:
                    set_error_and_invalid_reason(
                        df=df,
                        id_import=import_id,
                        error_code="ALTI_MIN_SUP_ALTI_MAX",
                        col_name_error=selected_columns["altitude_min"],
                        df_col_name_valid="temp",
                        id_rows_error=id_rows_errors,
                    )

    except Exception:
        raise
