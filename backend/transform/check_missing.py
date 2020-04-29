import pandas as pd
import numpy as np

from .utils import fill_map, set_is_valid, set_error_and_invalid_reason
from ..wrappers import checker
from ..logs import logger


def format_missing(df, selected_columns, synthese_info, missing_values):
    try:
        fields = [field for field in synthese_info]

        for field in fields:
            logger.info(
                "- formatting eventual missing values in %s synthese column (= %s user column)",
                field,
                selected_columns[field],
            )
            df[selected_columns[field]] = (
                df[selected_columns[field]]
                .replace(missing_values, pd.np.nan)
                .fillna(value=pd.np.nan)
            )

    except Exception as e:
        raise


@checker("Data cleaning : missing values checked")
def check_missing(
    df, selected_columns, synthese_info, missing_values, import_id, schema_name
):
    try:
        logger.info("CHECKING MISSING VALUES : ")

        format_missing(df, selected_columns, synthese_info, missing_values)

        fields = [
            field
            for field in synthese_info
            if synthese_info[field]["is_nullable"] == "NO"
        ]
        for field in fields:

            logger.info(
                "- checking missing values in %s synthese column (= %s user column)",
                field,
                selected_columns[field],
            )

            if df[selected_columns[field]].isnull().any():
                df["temp"] = ""
                df["temp"] = (
                    df[selected_columns[field]]
                    .replace(missing_values, np.nan)
                    .notnull()
                    .map(fill_map)
                    .astype("bool")
                )
                set_is_valid(df, "temp")
                id_rows_errors = df.index[df["temp"] == False].to_list()

                logger.info(
                    "%s missing values detected for %s synthese column (= %s user column)",
                    len(id_rows_errors),
                    field,
                    selected_columns[field],
                )

                if len(id_rows_errors) > 0:
                    set_error_and_invalid_reason(
                        df=df,
                        id_import=import_id,
                        error_code="MISSING_VALUE",
                        col_name_error=selected_columns[field],
                        df_col_name_valid="temp",
                        id_rows_error=id_rows_errors,
                    )
                df.drop("temp", axis=1)
            else:
                logger.info(
                    "0 missing values detected for %s synthese column (= %s user column)",
                    field,
                    selected_columns[field],
                )

    except Exception:
        raise
