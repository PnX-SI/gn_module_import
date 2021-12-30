from ..db.queries.user_errors import (
    get_error_message,
    get_error_from_code,
    set_user_error,
)


def fill_col(value):
    if value is not False:
        return True
    else:
        return False


def set_is_valid(df, column_name):
    df["gn_is_valid"] = df["gn_is_valid"].where(cond=df[column_name], other=False)


fill_map = {"": True, False: False}


def set_invalid_reason(df, source_col_name, message):
    df["gn_invalid_reason"] = df["gn_invalid_reason"].where(
        cond=df[source_col_name], other=df["gn_invalid_reason"] + message
    )


def set_warning_reason(df, source_col_name, message, col_name):
    message = message.format(col_name)
    df["gn_invalid_reason"] = df["gn_invalid_reason"].where(
        cond=df[source_col_name], other=df["gn_invalid_reason"] + message + " *** "
    )


def set_error_and_invalid_reason(
    df,
    id_import,
    error_code,
    col_name_error,
    df_col_name_valid,
    id_rows_error,
    comment=None,
):
    """
    Add errors in gn_import.t_user_errors_list
    and set invalid reason on the dataframe
    """
    error_obj = get_error_from_code(error_code)
    set_user_error(
        id_import=id_import,
        step="FIELD_MAPPING",
        id_error=error_obj.id_error,
        col_name=col_name_error,
        id_rows=id_rows_error,
        comment=comment,
    )
    set_is_valid(df, df_col_name_valid)
    message = "{}: {}".format(error_obj.name, col_name_error)
    set_invalid_reason(df=df, source_col_name=df_col_name_valid, message=message)


def add_code_columns(selected_columns, df):
    if "codecommune" not in selected_columns:
        selected_columns["codecommune"] = "codecommune"
        df["codecommune"] = None
    if "codemaille" not in selected_columns:
        selected_columns["codemaille"] = "codemaille"
        df["codemaille"] = None
    if "codedepartement" not in selected_columns:
        selected_columns["codedepartement"] = "codedepartement"
        df["codedepartement"] = None


def remove_temp_columns(temp_cols: list, df):
    for col in temp_cols:
        try:
            df = df.drop(col, axis=1)
        except Exception:
            pass
    return df
