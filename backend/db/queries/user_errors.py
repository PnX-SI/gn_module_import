from geonature.utils.env import DB


def set_user_error(id_import, id_error, col_name, n_errors):
    try:
        DB.session.execute(
            """
                INSERT INTO 
            """
        )
    except Exception:
        raise


def set_invalid_reason(df, source_col_name, message, col_name):
    df['gn_invalid_reason'] = df['gn_invalid_reason'] \
        .where(
        cond=df[source_col_name],
        other=df['gn_invalid_reason'] + message \
            .format(col_name)
              + ' *** '
    )
