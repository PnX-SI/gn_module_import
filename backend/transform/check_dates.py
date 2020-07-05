import pandas as pd

from .utils import fill_col, fill_map, set_is_valid, set_error_and_invalid_reason
from ..wrappers import checker
from ..logs import logger


def is_negative_date(value):
    try:
        if type(value) != pd.Timedelta:
            return True
        else:
            if value.total_seconds() >= 0:
                return True
            else:
                return False
    except TypeError:
        return True


def check_date_min_inf_date_max(row, selected_columns):
    """set true in check_dates if date_min <= date_max"""
    interval = row[selected_columns["date_max"]] - \
        row[selected_columns["date_min"]]
    if interval.total_seconds() >= 0:
        row["check_dates"] = True
    else:
        row["check_dates"] = False


@checker("Data cleaning : dates checked")
def check_dates(df, selected_columns, synthese_info, import_id, schema_name):
    try:

        logger.info("CHECKING DATES :")

        # get user synthese fields having timestamp type
        date_fields = [
            field
            for field in synthese_info
            if synthese_info[field]["data_type"] == "timestamp without time zone"
        ]

        # date_min and date_max :

        # set date_max (=if data_max not existing, then set equal to date_min)
        if "date_max" not in date_fields:
            logger.info("- date_max not provided : set date_max = date_min")
            df["date_max"] = df[selected_columns["date_min"]]  # utile?
            synthese_info.update(
                {"date_max": synthese_info["date_min"]})  # utile?

        # check date min <= date max
        if "date_min" in date_fields and "date_max" in date_fields:

            logger.info(
                "- checking date_min (= %s user column) <= date_max (= %s user column)",
                selected_columns["date_min"],
                selected_columns["date_max"],
            )
            df["interval"] = ""
            df["temp"] = ""
            try:
                df["interval"] = (
                    df[selected_columns["date_max"]] -
                    df[selected_columns["date_min"]]
                )
            except Exception:
                logger.error("Error on date")
            df["temp"] = df["interval"].apply(lambda x: is_negative_date(x))

            id_rows_errors = df.index[df["temp"] == False].to_list()

            logger.info(
                "%s date_min (= %s user column) > date_max (= %s user column) errors detected",
                len(id_rows_errors),
                selected_columns["date_min"],
                selected_columns["date_max"],
            )

            if len(id_rows_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="DATE_MIN_SUP_DATE_MAX",
                    col_name_error=selected_columns["date_min"],
                    df_col_name_valid="temp",
                    id_rows_error=id_rows_errors,
                )

    except Exception:
        raise
