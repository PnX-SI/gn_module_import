from ..db.query import get_synthese_types, get_user_error


def fill_col(value):
    if value is not False:
        return True
    else:
        return False


def set_is_valid(df, column_name):
    df['gn_is_valid'] = df['gn_is_valid']\
        .where(
            cond=df[column_name], 
            other=False
        )


def set_invalid_reason(df, source_col_name, message, col_name):
    df['gn_invalid_reason'] = df['gn_invalid_reason']\
        .where(
            cond=df[source_col_name], 
            other=df['gn_invalid_reason'] + message\
                .format(col_name)
                + ' *** '
        )


def get_types(synthese_info):
    return [synthese_info[field]['data_type'] for field in synthese_info]

    
fill_map = {'':True, False:False}


def set_user_error(dc_user_errors, id, col_name, n_errors):
    for error in dc_user_errors:
        if error['id'] == id and error['column'] == col_name:
            error['n_errors'] = error['n_errors'] + n_errors  


def create_col_name(col_dict, key, value, import_id):
    if value not in col_dict.values():
        col_dict[key] = value
    else:
        colname = '_'.join(['gn', import_id, value])
        while colname in col_dict.values():
            colname = '_'.join(['gn',colname])
        col_dict[key] = colname