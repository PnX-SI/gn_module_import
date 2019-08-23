import pandas as pd
import pdb
import datetime
import dask
import dask.dataframe as dd


def convert_to_datetime(value):
    try:
        return pd.to_datetime(value)
    except ValueError:
        return value


def check_datetime_conversion(value):
    try:
        pd.to_datetime(value)
        return True
    except ValueError:
        return False


def fill_col(value):
    if value is not False:
        return True
    else:
        return False


@dask.delayed
def test(col_date_min, col_date_max):
    for i_index, i_value in col_date_min.iteritems():
        for j_index, j_value in col_date_max.iteritems():
            try:
                if pd.to_datetime(i_value) <= pd.to_datetime(j_value):
                    df['test'][i_index] = True
                else:
                    df['test'][i_index] = False
            except Exception:
                df['test'][i_index] = False


"""
@dask.delayed
def count_false(column):
    n = 0
    for index,value in column.iteritems():
        if value is False:
            n = n+1
    return n
"""


"""
@dask.delayed
def check_dates(column):
    n = 0
    for index,value in column.iteritems():
        if value is False:
            n = n+1
    return n
"""


def check_negative(value):
    try:
        if type(value) != pd.Timedelta:
            return True
        else:
            if value.days > 0:
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

    for field in date_fields:

        # datetime conversion
        df[selected_columns[field]] = df[selected_columns[field]].apply(lambda x: convert_to_datetime(x))

        # check invalid date type and set user errors in db and front interface
        df['temp'] = ''
        df['temp'] = df['temp'].where(cond=df[selected_columns[field]].apply(lambda x: check_datetime_conversion(x)), other=False)
        df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
        df['gn_invalid_reason'] = df['gn_invalid_reason'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=df['gn_invalid_reason']+'invalid date for {} column; '.format(selected_columns[field]))

        #n_invalid_date_error = df['temp'].count().compute()
        #n_invalid_date_error = count_false(df['temp']).compute()
        n_invalid_date_error = df['temp'].astype(str).str.contains('False').sum()
        #n_invalid_date_error = df['temp'].compute().value_counts()[False]

        if n_invalid_date_error > 0:
            user_error.append({
                'code': 'date error',
                'message': 'dates invalides pour la colonne {}'.format(selected_columns[field]),
                'message_data': 'nombre de lignes avec erreurs : {}'.format(n_invalid_date_error)
            })

        #pdb.set_trace()

        # check missing value
        if synthese_info[field]['is_nullable'] == 'NO':
            df['temp'] = ''
            df['temp'] = df['temp'].where(cond=df[selected_columns[field]].notnull(), other=False)
            df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=df['gn_invalid_reason']+'missing date for {} column; '.format(selected_columns[field]))

            n_missing_date_error = df['temp'].astype(str).str.contains('False').sum()

            if n_missing_date_error > 0:
                user_error.append({
                    'code': 'date error',
                    'message': 'dates manquantes pour la colonne {}'.format(selected_columns[field]),
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_missing_date_error)
                })

    # set date_max (=if data_max not existing, then set equal to date_min)
    if 'date_max' not in list(selected_columns.keys()):
        print('ok date max')
        selected_columns['date_max'] = selected_columns['date_min']
        df['date_max'] = df[selected_columns['date_min']]
        synthese_info.update({'date_max': synthese_info['date_min']})

    # ajouter verif que date min <= date max
    df['temp'] = ''
    df['test'] = dd.to_datetime(df['my_timestamp'], errors='coerce') - dd.to_datetime(df['my_date'], errors='coerce')
    df['temp'] = df['temp'].where(cond=df['test'].apply(lambda x: check_negative(x)), other=False)
    df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
    df['gn_invalid_reason'] = df['gn_invalid_reason'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=df['gn_invalid_reason']+'date_min > date_max; ')

    n_date_min_sup = df['temp'].astype(str).str.contains('False').sum()

    if n_date_min_sup > 0:
        user_error.append({
            'code': 'date error',
            'message': 'des dates min sont supérieures à date max',
            'message_data': 'nombre de lignes avec erreurs : {}'.format(n_date_min_sup)
        })


    if len(user_error) == 0:
        user_error = ''

    df = df.drop('temp', axis=1)
    #pdb.set_trace()
    return user_error