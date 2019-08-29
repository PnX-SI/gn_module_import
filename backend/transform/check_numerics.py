import pdb
import pandas as pd
import dask
from .transform import fill_col


def check_count_min_max(min_val, max_val):
    try:
        if int(min_val) > int(max_val):
            return False
        else:
            return True
    except Exception:
        return True


def check_missing_count_max(min_val, max_val):
    try:
        if pd.isnull(max_val):
            if int(min_val) > 0:
                return min_val
        else:
            return max_val
    except Exception:
        return max_val


def check_counts(df, selected_columns, synthese_info, def_count_val):

    # replace count_min empty value by 1
    # send error if count_min or count_max value equal to 0
    # (if provided,) checks if count_min <= count_max
    # if only count_min is provided, set count_max equal to count_min
    # if only count_max is provided, set count_min equal to parametered default count_min value
    # if count_min and max are provided, set count_max equal to count_min if count_max = NA and count_min > 0
    # !! send error if negative value

    user_error = []

    counts = []

    for element in list(selected_columns.keys()):
        if element == 'count_min' or element == 'count_max':
            counts.append(element)


    if len(counts) > 0:

        for count in counts:

            # replace na value by 1 if count_min
            if count == 'count_min':
                df[selected_columns[count]] = df[selected_columns[count]].replace(pd.np.nan,str(def_count_val))

            # error if value = 0
            df['temp'] = ''
            df['temp'] = df[selected_columns[count]].apply(lambda x: False if x == '0' else True)
            df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                cond=df['temp'].apply(lambda x: fill_col(x)),
                other=df['gn_invalid_reason'] + 'invalid {} in {} column : value = 0 -- '.format(count,selected_columns[count]))

            n_count_zero = df['temp'].astype(str).str.contains('False').sum()

            if n_count_zero > 0:
                user_error.append({
                    'code': 'count error',
                    'message': 'Des {} sont = à 0'.format(count),
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_count_zero)
                })


    if len(counts) == 1:

        if counts[0] == 'count_min':
            # if only count_min is indicated, then set count_max equal to count_min
            selected_columns['count_max'] = selected_columns['count_min']
            df['count_max'] = df[selected_columns['count_min']] # utile?
            synthese_info.update({'count_max': synthese_info['count_min']}) # utile?

        if counts[0] == 'count_max':
            # if only count_max is indicated, then set count_min to defaut count_min value
            df['count_min'] = str(def_count_val)
            synthese_info.update({'count_min': synthese_info['count_max']}) # utile?


    if len(counts) == 2:

        # set count_max equal to count_min if count_max missing
        df['temp'] = ''
        df['temp'] = df.apply(lambda x: check_missing_count_max(x[selected_columns['count_min']], x[selected_columns['count_max']]), axis=1)
        df[selected_columns['count_max']] = df['temp']

    # check count_min not > to count_max :

    df['temp'] = ''
    # dask version
    df['temp'] = df.apply(lambda x: check_count_min_max(x[selected_columns['count_min']], x[selected_columns['count_max']]), axis=1)
    """
    # pandas version
    df['temp'] = pd.to_numeric(df[selected_columns['count_max']], errors='coerce') - pd.to_numeric(df[selected_columns['count_min']], errors='coerce') < 0
    df['temp'] = -df['temp']
    """

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


def check_altitudes(df, selected_columns, synthese_info):
    
    """
    - check if altitude max not < to altitude min
    - check for negative values in altitude max and min ?
    - if user want to calculate altitudes :
        -> if only altitude max provided, calculates altitude min
        -> if only altitude min provided, calculates altitude max
    - if user doesn't want to calculate altitudes :
        -> if only altitude min provided, altitude max = altitude min
        -> if only altitude max provided, altitude min = 0
    """

    user_error = []

    counts = []

    for element in list(selected_columns.keys()):
        if element == 'altitude_min' or element == 'altitude_max':
            counts.append(element)


    if len(counts) > 0:

        for count in counts:

            # replace na value by 1
            df[selected_columns[count]] = df[selected_columns[count]].replace(pd.np.nan,str(def_count_val))

            # error if value = 0
            df['temp'] = ''
            df['temp'] = df[selected_columns[count]].apply(lambda x: False if x == '0' else True)
            df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                cond=df['temp'].apply(lambda x: fill_col(x)),
                other=df['gn_invalid_reason'] + 'invalid {} in {} column : value = 0 -- '.format(count,selected_columns[count]))

            n_count_zero = df['temp'].astype(str).str.contains('False').sum()

            if n_count_zero > 0:
                user_error.append({
                    'code': 'count error',
                    'message': 'Des {} sont = à 0'.format(count),
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_count_zero)
                })


    if len(counts) == 2:

        # check count_min not > to count_max:

        df['temp'] = ''
        # dask version
        df['temp'] = df.apply(lambda x: check_count_min_max(x[selected_columns['count_min']], x[selected_columns['count_max']]), axis=1)
        """
        # pandas version
        df['temp'] = pd.to_numeric(df[selected_columns['count_max']], errors='coerce') - pd.to_numeric(df[selected_columns['count_min']], errors='coerce') < 0
        df['temp'] = -df['temp']
        """

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


    if len(counts) == 1:

        if counts[0] == 'count_min':
            # if only count_min is indicated, then set count_max equal to count_min
            selected_columns['count_max'] = selected_columns['count_min']
            df['count_max'] = df[selected_columns['count_min']] # utile?
            synthese_info.update({'count_max': synthese_info['count_min']}) # utile?

        if counts[0] == 'count_max':
            # if only count_max is indicated, then set count_min to defaut count_min value
            df['count_min'] = str(def_count_val)
            synthese_info.update({'count_min': synthese_info['count_max']}) # utile?


    if len(user_error) == 0:
        user_error = ''

    return user_error