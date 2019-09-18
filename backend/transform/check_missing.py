import pandas as pd 
import numpy as np
import datetime

from .utils import fill_map, set_is_valid, set_invalid_reason, set_user_error
from ..wrappers import checker
from ..logs import logger

import pdb


def format_missing(df, selected_columns, synthese_info, missing_values):

    try :
        fields = [field for field in synthese_info]

        for field in fields:
            df[selected_columns[field]] = df[selected_columns[field]].replace(missing_values,pd.np.nan).fillna(value=pd.np.nan)

    except Exception as e:
        pdb.set_trace()
        raise 


@checker('Data cleaning : missing values checked')
def check_missing(df, selected_columns, synthese_info, missing_values):

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
                set_invalid_reason(df, 'temp', 'missing value in {} column', selected_columns[field])
                n_missing_value = df['temp'].astype(str).str.contains('False').sum()

                df.drop('temp',axis=1)


                if n_missing_value > 0:
                    user_error.append(
                        set_user_error(
                            'missing value in required field',
                            selected_columns[field],
                            n_missing_value
                        )
                    )
        
        if len(user_error) == 0:
            user_error = ''

        return user_error

    except Exception:
        raise
