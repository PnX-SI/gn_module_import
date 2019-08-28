import pandas as pd
import pdb
import datetime
import dask
import dask.dataframe as dd

from .transform import fill_col


@dask.delayed
def check_dates(col_date_min, col_date_max):
    for i_index, i_value in col_date_min.iteritems():
        for j_index, j_value in col_date_max.iteritems():
            try:
                if pd.to_datetime(i_value) <= pd.to_datetime(j_value):
                    df['check_dates'][i_index] = True
                else:
                    df['check_dates'][i_index] = False
            except Exception:
                df['check_dates'][i_index] = False


"""
@dask.delayed
def count_false(column):
    n = 0
    for index,value in column.iteritems():
        if value is False:
            n = n+1
    return n
"""


def is_negative_date(value):
    try:
        if type(value) != pd.Timedelta:
            return True
        else:
            if value.total_seconds() >= 0:
                return True
            else:
                return False
    except TypeError:
        return True


def cleaning_dates(df, selected_columns, synthese_info):

    # get user synthese fields having timestamp type
    date_fields = [field for field in synthese_info if synthese_info[field]['data_type'] == 'timestamp without time zone']

    # transform and check of date type columns

    user_error = []

    # set date_max (=if data_max not existing, then set equal to date_min)
    if 'date_max' not in list(selected_columns.keys()):
        selected_columns['date_max'] = selected_columns['date_min']
        df['date_max'] = df[selected_columns['date_min']] # utile?
        synthese_info.update({'date_max': synthese_info['date_min']}) # utile?


    # ajouter verif que date min <= date max
    df['temp'] = ''
    df['check_dates'] = (dd.to_datetime(df[selected_columns['date_max']], errors='coerce') - dd.to_datetime(df[selected_columns['date_min']], errors='coerce'))
    df['temp'] = df['temp'].where(
        cond=df['check_dates'].apply(lambda x: is_negative_date(x)), 
        other=False)
    df['gn_is_valid'] = df['gn_is_valid'].where(
        cond=df['temp'].apply(lambda x: fill_col(x)), 
        other=False)
    df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
        cond=df['temp'].apply(lambda x: fill_col(x)), 
        other=df['gn_invalid_reason'] + 'date_min > date_max; ')

    n_date_min_sup = df['temp'].astype(str).str.contains('False').sum().compute()

    if n_date_min_sup > 0:
        user_error.append({
            'code': 'date error',
            'message': 'Des dates min sont supérieures à date max',
            'message_data': 'nombre de lignes avec erreurs : {}'.format(n_date_min_sup)
        })
        
    if len(user_error) == 0:
        user_error = ''

    return user_error