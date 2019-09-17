import pandas as pd 
import numpy as np
import datetime

from .utils import fill_map, set_is_valid
from ..wrappers import checker
from ..logs import logger

import pdb


def format_missing(df, selected_columns, synthese_info, missing_values):

    try :
        fields = [field for field in synthese_info]

        for field in fields:
            df[selected_columns[field]] = df[selected_columns[field]].replace(missing_values,pd.np.nan).fillna(value=pd.np.nan)

    except Exception as e:
        raise 


@checker('Data cleaning : missing values checked')
def check_missing(df, selected_columns, synthese_info, missing_values, df_type):

    try:
        logger.info('checking missing values : ')

        format_missing(df, selected_columns, synthese_info, missing_values)

        user_error = []

        fields = [field for field in synthese_info if synthese_info[field]['is_nullable'] == 'NO']

        #df[selected_columns[field]].isnull().any()

        for field in fields:

            logger.info('- checking missing values for %s column', field)
            
            if df[selected_columns[field]].isnull().any():

                df['temp'] = ''
                df['temp'] = df[selected_columns[field]]\
                    .replace(missing_values,np.nan)\
                    .notnull()\
                    .map(fill_map)\
                    .astype('bool')

                set_is_valid(df, 'temp')

                df['gn_invalid_reason'] = df['gn_invalid_reason']\
                    .where(
                        cond=df['temp'], 
                        other=df['gn_invalid_reason'] + 'missing value in {} column -- '\
                            .format(selected_columns[field]))
            
                df.drop('temp',axis=1)

                n_missing_value = df['temp'].astype(str).str.contains('False').sum()
                
                if df_type == 'dask':
                    n_missing_value = n_missing_value.compute()

                if n_missing_value > 0:
                    user_error.append({
                        'code': 'valeur manquante',
                        'message': 'Des valeurs manquantes dans la colonne {}'.format(selected_columns[field]),
                        'message_data': 'nombre de lignes avec erreurs : {}'.format(n_missing_value)
                    })
        
        if len(user_error) == 0:
            user_error = ''

        return user_error

    except Exception:
        raise