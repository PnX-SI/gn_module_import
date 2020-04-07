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
