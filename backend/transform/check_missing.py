import pandas as pd 
import numpy as np
import datetime
from .utils import fill_col
from ..wrappers import checker

import pdb

import traceback

from ..logs import logger
from ..api_error import GeonatureImportApiError


def format_missing(df, selected_columns, synthese_info, missing_values):

    try :
        fields = [field for field in synthese_info]

        for field in fields:
            df[selected_columns[field]] = df[selected_columns[field]].replace(missing_values,pd.np.nan).fillna(value=pd.np.nan)

    except Exception as e:
        raise 


@checker('Data cleaning : missing values checked')
def check_missing(df, selected_columns, synthese_info, missing_values):

    try:

        format_missing(df, selected_columns, synthese_info, missing_values)

        user_error = []

        fields = [field for field in synthese_info if synthese_info[field]['is_nullable'] == 'NO']

        #df[selected_columns[field]].isnull().any()

        for field in fields:
            
            df['temp'] = ''
            df['temp'] = df[selected_columns[field]].replace(missing_values,np.nan).notnull()
            df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x), meta=False), other=False)
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                cond=df['temp'].apply(lambda x: fill_col(x), meta=False), 
                other=df['gn_invalid_reason'] + 'missing value in {} column -- '.format(selected_columns[field]))
        
            df.drop('temp',axis=1)

            n_missing_value = df['temp'].astype(str).str.contains('False').sum()

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
