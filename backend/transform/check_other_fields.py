import re
import numpy as np
import pandas as pd
from ..wrappers import checker
from .utils import set_is_valid, fill_map, set_error_and_invalid_reason
from ..logs import logger
from ..db.queries.metadata import get_id_roles


@checker("Data cleaning : entity source pk value checked")
def check_entity_source(
    df, added_cols, selected_columns, synthese_info, import_id, schema_name
):
    try:
        fields = [field for field in synthese_info]

        logger.info("CHECKING ENTITY SOURCE PK VALUE:")

        if "entity_source_pk_value" not in fields:
            logger.info(
                "- no entity source pk value provided : set gn_pk column as entity source pk column"
            )
            added_cols["entity_source_pk_value"] = "gn_pk"  # récupérer gn_pk en conf
        else:
            # check duplicates
            logger.info(
                "- checking duplicates in entity_source_pk_value column (= %s user column)",
                selected_columns["entity_source_pk_value"],
            )

            df["temp"] = df.duplicated(
                selected_columns["entity_source_pk_value"], keep=False
            )
            df["temp"] = ~df["temp"].astype("bool")

            set_is_valid(df, "temp")
            id_rows_errors = df.index[df["temp"] == False].to_list()

            logger.info(
                "%s duplicates errors in entity_source_pk_value column (= %s user column)",
                len(id_rows_errors),
                selected_columns["entity_source_pk_value"],
            )

            if len(id_rows_errors) > 0:
                set_error_and_invalid_reason(
                    df=df,
                    id_import=import_id,
                    error_code="DUPLICATE_ENTITY_SOURCE_PK",
                    col_name_error=selected_columns["entity_source_pk_value"],
                    df_col_name_valid="temp",
                    id_rows_error=id_rows_errors,
                )
    except Exception:
        raise


@checker("Data cleaning : id_digitizer checked")
def check_id_digitizer(df, selected_columns, synthese_info, import_id, schema_name):
    try:
        # check if id_digitizer exists in t_roles
        fields = [field for field in synthese_info]

        if "id_digitiser" in fields:
            logger.info("CHECKING ID DIGITIZER :")
            ids = df[selected_columns["id_digitiser"]].dropna().unique().tolist()
            if len(ids) > 0:
                id_roles = get_id_roles()
                is_invalid_id = any(id not in id_roles for id in ids)
                if is_invalid_id:
                    df["temp"] = (
                        df[selected_columns["id_digitiser"]]
                        .fillna(id_roles[0])
                        .isin(id_roles)
                    )
                    set_is_valid(df, "temp")

                    id_rows_errors = df.index[df["temp"] == False].to_list()

                    logger.info(
                        "%s invalid id_digitizer detected in %s column",
                        len(id_rows_errors),
                        selected_columns["id_digitiser"],
                    )

                    # set front interface error
                    if len(id_rows_errors) > 0:
                        set_error_and_invalid_reason(
                            df=df,
                            id_import=import_id,
                            error_code="ID_DIGITISER_NOT_EXISITING",
                            col_name_error=selected_columns["id_digitiser"],
                            df_col_name_valid="temp",
                            id_rows_error=id_rows_errors,
                        )

    except Exception:
        raise


def reg_match(element, regex):
    if pd.isnull(element):
        return True
    else:
        try:
            element = str(element)
            match = regex.match(element)
            return True if match else False
        except Exception as e:
            return False


@checker("Data cleaning : url checked")
def check_url(df, selected_columns, import_id):
    regex = re.compile(
        r"^(?:http|ftp)s?://"  # http:// or https://
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|"  # domain...
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
        r"(?::\d+)?"  # optional port
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )
    # ACCEPT NAN ?
    # accept emty str ?
    if "digital_proof" in selected_columns:
        digit_col = df[selected_columns["digital_proof"]]
        # better perf but don't work ?
        # df.loc[digit_proof_not_null, "valid_url"] = df.loc[
        #     digit_proof_not_null, digit_col
        # ].str.contains(regex)

        # set true where null and compare the regex on all other lines
        df["valid_url"] = digit_col.apply(lambda x: reg_match(x, regex))
        invalid_rows = df[df["valid_url"] == False]

        if len(invalid_rows) > 0:
            set_error_and_invalid_reason(
                df=df,
                id_import=import_id,
                error_code="INVALID_URL_PROOF",
                col_name_error=selected_columns["digital_proof"],
                df_col_name_valid="valid_url",
                id_rows_error=invalid_rows.index.to_list(),
            )

