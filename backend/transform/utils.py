from ..db.queries.user_errors import get_error_message


def fill_col(value):
    if value is not False:
        return True
    else:
        return False


def set_is_valid(df, column_name):
    df['gn_is_valid'] = df['gn_is_valid'] \
        .where(
        cond=df[column_name],
        other=False
    )


fill_map = {'': True, False: False}


def set_invalid_reason(df, schema_name, source_col_name, import_id, id_error, col_name):
    message = get_error_message(schema_name, import_id, id_error, col_name)
    df['gn_invalid_reason'] = df['gn_invalid_reason'] \
        .where(
            cond=df[source_col_name],
            other=df['gn_invalid_reason'] + message
        )


def set_warning_reason(df, source_col_name, message, col_name):
    message = message.format(col_name)
    df['gn_invalid_reason'] = df['gn_invalid_reason'] \
        .where(
            cond=df[source_col_name],
            other=df['gn_invalid_reason'] + message + ' *** '
        )