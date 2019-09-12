import pandas as pd
import dask

from .utils import fill_col, fill_map
from ..wrappers import checker
from ..logs import logger

import pdb


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


"""
def check_negative(val):
    try:
        if pd.to_numeric(val) < 0:
            return False
        else:
            return True
    except Exception:
        return True
"""


@checker('Data cleaning : counts checked')
def check_counts(df, selected_columns, synthese_info, def_count_val):

    """
    - every time :
        -> count_min = def_count_val if NA
        -> if count_min < 0 or count_max < 0 (previously check during integer type checks)
    - if only count_min column is provided : count_max column = count_min column
    - if only count_max column is provided : count_min column = default count_min value (defined in parameters)
    - if both count_min and max columns are provided :
        -> if count_max = NA : count_max = count_min if count_min > 0
    - if count_min not provided, or both count_min and max provided :
        -> checks if count_min <= count_max
    """

    try:
            
        # remark : negative values previously checked during check_types step

        logger.info('checking count_min and count_max : ')
        
        user_error = []

        # define combination of counts provided:
        if 'count_min' in list(selected_columns.keys())\
            and 'count_max' not in list(selected_columns.keys()):
            status = 'only count_min'
        elif 'count_min' not in list(selected_columns.keys())\
            and 'count_max' in list(selected_columns.keys()):
            status = 'only count_max'
        elif 'count_min' in list(selected_columns.keys())\
            and 'count_max' in list(selected_columns.keys()):
            status = 'min and max'
        else:
            status = 'no count'


        if status != 'no count':

            # set missing in count_min
            if 'count_min' in list(selected_columns.keys()):
                if df[selected_columns['count_min']].isnull().any():
                    logger.info('count_min provided by user: set default value in missing rows')
                    df[selected_columns['count_min']] = df[selected_columns['count_min']]\
                        .replace(pd.np.nan,str(def_count_val))


            # if only count_max is indicated, then set count_min to default count_min values
            if status == 'only count_max':
                logger.info('count_min not provided: filling count_min with default values')
                df['count_min'] = str(def_count_val)
                selected_columns['count_min'] = 'count_min'
                synthese_info.update({'count_min': synthese_info['count_max']}) # utile?


            # if only count_min is indicated, then set count_max equal to count_min
            if status == 'only count_min':
                logger.info('count_max not provided: setting count_max equal to count_min')
                selected_columns['count_max'] = selected_columns['count_min']
                df['count_max'] = df[selected_columns['count_min']] # utile?
                synthese_info.update({'count_max': synthese_info['count_min']}) # utile?

            
            if status == 'only count_max' or status == 'min and max':
                
                # Checking and filling missing count_max values
                if df[selected_columns['count_max']].isnull().any():
                    logger.info('setting count_max equal to count_min when count_max missing')
                    df['temp'] = ''
                    df['temp'] = df\
                        .apply(lambda x: x[selected_columns['count_min']] if pd.isnull(x[selected_columns['count_max']]) else x[selected_columns['count_max']], axis=1)
                    df[selected_columns['count_max']] = df['temp']

                # check if count_max >= count_min
                logger.info('checking if count_max >= count_min')

                df['temp'] = ''
                df['temp'] = df\
                    .apply(lambda x: check_count_min_max(x[selected_columns['count_min']], x[selected_columns['count_max']]), axis=1)\
                    .map(fill_map)\
                    .astype('bool')

                df['gn_is_valid'] = df['gn_is_valid']\
                    .where(
                        cond=df['temp'], 
                        other=False)
                
                df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                    cond=df['temp'],
                    other=df['gn_invalid_reason'] + 'count_min ({}) > count_max ({}) -- '\
                        .format(selected_columns['count_min'],selected_columns['count_max']))

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
    
    except Exception:
        raise
