from datetime import datetime
import pandas as pd

from .utils import fill_col, fill_map, set_is_valid, set_error_and_invalid_reason
from ..wrappers import checker
from ..logs import logger





@checker("Data cleaning : dates checked")
def set_dates(df, selected_columns, synthese_info, import_id, schema_name):
    try:

        logger.info("CHECKING DATES :")

        # get user synthese fields having timestamp type
        date_fields = [
            field
            for field in synthese_info
            if synthese_info[field]["data_type"] == "timestamp without time zone"
        ]
        #  reset date_min and date_max if the controls are lauched twice
        df[selected_columns["date_min"]]
        #  set hours in date cols
        if "hour_min" in selected_columns and not "concatened_date_min" in df:
            # set 00:00:00 where hour empty
            hour_min_col = selected_columns["hour_min"]
            # remove possibe null values
            df[hour_min_col] = df[hour_min_col].where(df[hour_min_col].notnull(), other="")
            # calculate hour length
            df['hour_min_length'] = df[hour_min_col].apply(len)
            # replace unfill hour with 00:00:00
            df[hour_min_col] = df[hour_min_col].where(df['hour_min_length'] != 0, other='00:00:00')

            df[selected_columns["date_min"]] = (
                df[selected_columns["date_min"]]
                + " "
                + df[hour_min_col]
            )
            df["concatened_date_min"] = True
        if (
            "hour_max" in selected_columns
            and "date_max" in selected_columns
            and not "concatened_date_max" in df
        ):
            # set 00:00:00 where hour empty
            hour_max_col = selected_columns["hour_max"]
            # fill hour max where hour_max is null and hour_min is fill
            if "hour_min" in selected_columns:
                df[hour_max_col].fillna(df[selected_columns["hour_min"]], inplace=True)
            # calculate hour length
            df['hour_max_length'] = df[hour_max_col].apply(len)
            df[hour_max_col] = df[hour_max_col].where(df['hour_max_length'] != 0, other='00:00:00')
            df[selected_columns["date_max"]] = (
                df[selected_columns["date_max"]]
                + " "
                + df[selected_columns["hour_max"]]
            )
            df["concatened_date_max"] = True
        if (
            not "hour_max" in selected_columns
            and "hour_min" in selected_columns
            and "date_max" in selected_columns
            and not "concatened_date_max" in df
        ):
            df[selected_columns["date_max"]] = (
                df[selected_columns["date_max"]]
                + " "
                + df[selected_columns["hour_min"]]
            )
            df["concatened_date_max"] = True

        # set date_max (=if data_max not existing, then set equal to date_min)
        if "date_max" not in date_fields:
            logger.info("- date_max not provided : set date_max = date_min")
            df["date_max"] = df[selected_columns["date_min"]]  # utile?
            synthese_info.update({"date_max": synthese_info["date_min"]})  # utile?

        # check date min <= date max
        if "date_min" in date_fields and "date_max" in date_fields:

            logger.info(
                "- checking date_min (= %s user column) <= date_max (= %s user column)",
                selected_columns["date_min"],
                selected_columns["date_max"],
            )
            df["interval"] = ""
            df["temp"] = ""

    except Exception:
        raise
