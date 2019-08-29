import pdb
import pandas as pd
import dask
from .transform import fill_col


@dask.delayed
def check_min_max(min_val, max_val):
    try:
        if int(min_val) > int(max_val):
            return False
        else:
            return True
    except Exception:
        return True



def check_numeric(df, selected_columns):

    # send warnings for empty fields in nullable numeric columns ?
    # if provided, checks if count_min <= count_max and altitude_max <= altitude_min
    # if only count_min is provided, set count_max equal to count_min ? value by value or for all the column values ?
    # if only count_min is provided, set count_max equal to count_min ?

    #df = df.compute()

    # count :

    user_error = []


    #df = df.compute()
    df['temp'] = ''

    """
    # pandas version
    df['temp'] = pd.to_numeric(df[selected_columns['count_max']], errors='coerce') - pd.to_numeric(df[selected_columns['count_min']], errors='coerce') < 0
    df['temp'] = -df['temp']
    """

    # dask version
    df['temp'] = df.apply(lambda x: check_min_max(x['my_min'], x['my_max']), axis=1)

    df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
    df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
        cond=df['temp'].apply(lambda x: fill_col(x)),
        other=df['gn_invalid_reason'] + 'count_min ({}) > count_max ({}) -- '.format(selected_columns['count_min'],selected_columns['count_max']))

    n_count_min_sup = df['temp'].astype(str).str.contains('False').sum()

    if n_count_min_sup > 0:
        user_error.append({
            'code': 'count error',
            'message': 'Des count min sont supérieurs à count max',
            'message_data': 'nombre de lignes avec erreurs : {}'.format(n_count_min_sup)
        })

    if len(user_error) == 0:
        user_error = ''

    return user_error

    

