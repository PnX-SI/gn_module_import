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

