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

def get_types(synthese_info):
    return [synthese_info[field]['data_type'] for field in synthese_info]

    
fill_map = {'':True, False:False}


def set_user_error(error_name, column_name, n_errors):
    error_details = get_user_error(error_name)
    return {
        'id' : str(error_details.id_error),
        'type' : error_details.error_type,
        'message' : error_details.description,
        'column' : column_name,
        'n_errors' : str(n_errors)
    }