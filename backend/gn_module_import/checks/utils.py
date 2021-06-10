


def handle_invalid_rows(df, imprt, error_code, error_field_name, invalid_rows, comment=None):
    """
    Add errors in gn_import.t_user_errors_list,
    mark erroneous rows as invalid and set invalid reason.
    """
    if invalid_rows.empty:
        return
    try:
        error_type = ImportUserErrorType.query.filter_by(name=error_code).one()
    except NoResultFound:
        raise Exception(f"Error code '{error_code}' not found.")
    set_user_error(
        id_import=imprt.id_import,
        step="FIELD_MAPPING",
        id_error=error_type.pk,
        col_name=error_field_name,
        id_rows=invalid_rows.index.to_list(),
        comment=comment,
    )
    # #  FIXME update invalid reason only on newly invalid rows!!!!
    # df['gn_is_valid'][invalid_rows.index] = False
    # df['gn_invalid_reason'][invalid_rows.index] = f'{error_type.name}: {error_field_name}'
    # print("ERROR DF", df)













#from sqlalchemy.orm.exc import NoResultFound
#
#from ..db.queries.user_errors import (
#    set_user_error,
#)
#from ..db.models import ImportUserErrorType
#
#
#def fill_col(value):
#    if value is not False:
#        return True
#    else:
#        return False
#
#
#def set_is_valid(df, column_name):
#    df["gn_is_valid"] = df["gn_is_valid"].where(cond=df[column_name], other=False)
#
#
#fill_map = {"": True, False: False}
#
#
#def set_invalid_reason(df, source_col_name, message):
#    df["gn_invalid_reason"] = df["gn_invalid_reason"].where(
#        cond=df[source_col_name], other=df["gn_invalid_reason"] + message
#    )
#
#
#def set_warning_reason(df, source_col_name, message, col_name):
#    message = message.format(col_name)
#    df["gn_invalid_reason"] = df["gn_invalid_reason"].where(
#        cond=df[source_col_name], other=df["gn_invalid_reason"] + message + " *** "
#    )
#
#
#def set_error_and_invalid_reason(
#    df,
#    id_import,
#    error_code,
#    col_name_error,
#    df_col_name_valid,
#    id_rows_error,
#    comment=None,
#):
#    """
#    Add errors in gn_import.t_user_errors_list
#    and set invalid reason on the dataframe
#    """
#    try:
#        error_type = ImportUserErrorType.query.filter_by(name=error_code).one()
#    except NoResultFound:
#        raise Exception(f"Error code '{error_code}' not found.")
#    set_user_error(
#        id_import=id_import,
#        step="FIELD_MAPPING",
#        id_error=error_type.pk,
#        col_name=col_name_error,
#        id_rows=id_rows_error,
#        comment=comment,
#    )
#    set_is_valid(df, df_col_name_valid)
#    message = "{}: {}".format(error_type.name, col_name_error)
#    set_invalid_reason(df=df, source_col_name=df_col_name_valid, message=message)
#    print("DF_", df)
#
#
#def add_code_columns(selected_columns, df):
#    if "codecommune" not in selected_columns:
#        selected_columns["codecommune"] = "codecommune"
#        df["codecommune"] = None
#    if "codemaille" not in selected_columns:
#        selected_columns["codemaille"] = "codemaille"
#        df["codemaille"] = None
#    if "codedepartement" not in selected_columns:
#        selected_columns["codedepartement"] = "codedepartement"
#        df["codedepartement"] = None
#
#
#def remove_temp_columns(temp_cols: list, df):
#    for col in temp_cols:
#        try:
#            df = df.drop(col, axis=1)
#        except Exception:
#            pass
#    return df
