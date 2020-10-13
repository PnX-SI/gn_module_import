"""
Checker for column with min and max dependencies
Use for altitudes and depth
"""

import pandas as pd
import dask
from .utils import fill_col, set_error_and_invalid_reason
from ..wrappers import checker


def _check_min_max_cols(min_val, max_val):
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


@checker("Data cleaning : min_max checked")
def check_min_max(
    df, selected_columns, synthese_info, calcul, import_id, schema_name, min_col, max_col
):
    """
    - if user want to calculate altitudes:
        -> if only col max column provided, calculates col min column
        -> if only col min column provided, calculates col max column
        -> if both col_min and col_max columns provided, calculate missing values
        -> if no min_max column provided, calculates col min and max

    - if user doesn't want to calculate:
        -> if only col min column provided, col max = col min 
        -> if only col max column provided, col min = 0
        -> if both col_min and col_max columns provided :
            . does nothing except check if min <= max if min != NA and max!= NA
    """

    try:

        fields_min_max = []

        for element in list(selected_columns.keys()):
            if element == min_col or element == max_col:
                fields_min_max.append(element)

        if calcul is False:

            if len(fields_min_max) == 2:
                # check max >= min
                df["temp"] = ""
                df["temp"] = df.apply(
                    lambda x: _check_min_max_cols(
                        x[selected_columns[min_col]],
                        x[selected_columns[max_col]],
                    ),
                    axis=1,
                )
                df["gn_is_valid"] = df["gn_is_valid"].where(
                    cond=df["temp"].apply(lambda x: fill_col(x)), other=False
                )
                # get invalid id rows
                id_rows_errors = df.index[df["temp"] == False].to_list()
                error_code = "ALTI_MIN_SUP_ALTI_MAX" if min_col == "altitude_min" else "DEPTH_MIN_SUP_ALTI_MAX"
                if len(id_rows_errors) > 0:
                    set_error_and_invalid_reason(
                        df=df,
                        id_import=import_id,
                        error_code=error_code,
                        col_name_error=selected_columns[min_col],
                        df_col_name_valid="temp",
                        id_rows_error=id_rows_errors,
                    )

    except Exception:
        raise
