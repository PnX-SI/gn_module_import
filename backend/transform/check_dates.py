import pandas as pd
import datetime
import dask
import dask.dataframe as dd

from .utils import fill_col
from ..wrappers import checker
from ..logs import logger

import pdb


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


@checker('Data cleaning : dates checked')
def check_dates(df, selected_columns, synthese_info, df_type):

    try:

        logger.info('checking date validity :')
        # get user synthese fields having timestamp type
        date_fields = [field for field in synthese_info if synthese_info[field]['data_type'] == 'timestamp without time zone']

        user_error = []


        ## date_min and date_max :

        # set date_max (=if data_max not existing, then set equal to date_min)
        if 'date_max' not in date_fields:
            logger.info('- checking if date_max column is missing')
            selected_columns['date_max'] = selected_columns['date_min']
            #df['date_max'] = df[selected_columns['date_min']] # utile?
            synthese_info.update({'date_max': synthese_info['date_min']}) # utile?

        # ajouter verif que date min <= date max
        if 'date_min' in date_fields and 'date_max' in date_fields:

            df['temp'] = ''
            df['check_dates'] = ''
            df['check_dates'] = dd.to_datetime(df[selected_columns['date_max']], errors='coerce') - dd.to_datetime(df[selected_columns['date_min']], errors='coerce')
            df['temp'] = df['temp']\
                .where(
                    cond=df['check_dates'].apply(lambda x: is_negative_date(x)), 
                    other=False)\
                .map(fill_map)\
                .astype('bool')
            
            df['gn_is_valid'] = df['gn_is_valid'].where(
                cond=df['temp'], 
                other=False)
            
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                cond=df['temp'], 
                other=df['gn_invalid_reason'] + 'date_min ({}) > date_max ({}) -- '\
                    .format(selected_columns['date_min'],selected_columns['date_max']))

            n_date_min_sup = df['temp'].astype(str).str.contains('False').sum()

            if df_type == 'dask':
                    n_date_min_sup = n_date_min_sup.compute()

            if n_date_min_sup > 0:
                user_error.append({
                    'code': 'date error',
                    'message': 'Des dates min sont supérieures à date max',
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_date_min_sup)
                })
        

        ## meta_create_date and meta_update_date :

        

        if len(user_error) == 0:
            user_error = ''

        if 'check_dates' in df.columns:
            df = df.drop('check_dates', axis=1)

        return user_error

    except Exception:
        raise