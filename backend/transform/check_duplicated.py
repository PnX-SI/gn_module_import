import pandas as pd
import numpy as np

from .utils import fill_map, set_is_valid, set_error_and_invalid_reason
from ..wrappers import checker
from ..logs import logger

import pdb


@checker("Data cleaning : row duplicates checked")
def check_row_duplicates(df, selected_columns, import_id, schema_name):
    try:
        logger.info("CHECKING FOR DUPLICATED ROWS : ")
        selected_cols = selected_columns
        generate_fields = ["unique_id_sinp_generate", "altitudes_generate"]
        for field in generate_fields:
            if field in selected_cols.keys():
                del selected_cols[field]
        df["duplicate"] = df.duplicated(subset=selected_cols.values(), keep=False)
        df["no_duplicate"] = ~df["duplicate"]
        set_is_valid(df, "no_duplicate")
        id_rows_errors = df.index[df["duplicate"] == True].to_list()

        logger.info("%s duplicated rows detected", len(id_rows_errors))

        if len(id_rows_errors) > 0:
            set_error_and_invalid_reason(
                df=df,
                id_import=import_id,
                error_code="DUPLICATE_ROWS",
                col_name_error="",
                df_col_name_valid="duplicate",
                id_rows_error=id_rows_errors,
            )

        df = df.drop("duplicate", axis=1)
        df = df.drop("no_duplicate", axis=1)

    except Exception:
        raise
