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