import dask
import numpy as np

from geonature.utils.env import DB

from .utils import fill_col, fill_map, set_is_valid, set_error_and_invalid_reason
from ..wrappers import checker
from ..logs import logger


@checker("Data cleaning : cd_nom checked")
def check_cd_nom(
    df, selected_columns, missing_values, cd_nom_list, schema_name, import_id
):

    try:

        logger.info("CHECKING CD_NOM VALIDITY in %s column", selected_columns["cd_nom"])

        # return False if invalid cd_nom, else (including missing values) return True
        df["temp"] = ""
        df["temp"] = (
            df["temp"]
            .where(cond=df[selected_columns["cd_nom"]].isin(cd_nom_list), other=False)
            .where(cond=df[selected_columns["cd_nom"]].notnull(), other="")
            .map(fill_map)
            .astype("bool")
        )

        # set gn_is_valid and invalid_reason
        set_is_valid(df, "temp")
        # get invalid id rows
        id_rows_invalid = df.index[df["temp"] == False].to_list()

        logger.info(
            "%s invalid cd_nom detected in %s column",
            len(id_rows_invalid),
            selected_columns["cd_nom"],
        )

        # set front interface error
        if len(id_rows_invalid) > 0:
            set_error_and_invalid_reason(
                df=df,
                id_import=import_id,
                error_code="CD_NOM_NOT_FOUND",
                col_name_error=selected_columns["cd_nom"],
                df_col_name_valid="temp",
                id_rows_error=id_rows_invalid,
            )
    except Exception:
        raise
