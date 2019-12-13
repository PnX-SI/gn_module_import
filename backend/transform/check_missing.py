import pandas as pd
import numpy as np

from ..db.queries.user_errors import set_user_error
from .utils import fill_map, set_is_valid, set_invalid_reason
from ..wrappers import checker
from ..logs import logger


def format_missing(df, selected_columns, synthese_info, missing_values):
    try:
        fields = [field for field in synthese_info]

        for field in fields:
            logger.info('- formatting eventual missing values in %s synthese column (= %s user column)', field,
                        selected_columns[field])
            df[selected_columns[field]] = df[selected_columns[field]].replace(missing_values, pd.np.nan).fillna(
                value=pd.np.nan)

    except Exception as e:
        raise


@checker('Data cleaning : missing values checked')
def check_missing(df, selected_columns, synthese_info, missing_values, import_id, schema_name):
    try:
        logger.info('CHECKING MISSING VALUES : ')

        format_missing(df, selected_columns, synthese_info, missing_values)

        fields = [field for field in synthese_info if synthese_info[field]['is_nullable'] == 'NO']

        for field in fields:

            logger.info('- checking missing values in %s synthese column (= %s user column)', field,
                        selected_columns[field])

            if df[selected_columns[field]].isnull().any():
                df['temp'] = ''
                df['temp'] = df[selected_columns[field]] \
                    .replace(missing_values, np.nan) \
                    .notnull() \
                    .map(fill_map) \
                    .astype('bool')
                set_is_valid(df, 'temp')
                n_missing_value = df['temp'].astype(str).str.contains('False').sum()
                
                logger.info('%s missing values detected for %s synthese column (= %s user column)', n_missing_value,
                            field, selected_columns[field])
                
                if n_missing_value > 0:
                    set_user_error(import_id, 5, selected_columns[field], n_missing_value)
                    set_invalid_reason(df, schema_name, 'temp', import_id, 5, selected_columns[field])

                df.drop('temp', axis=1)
            else:
                logger.info('0 missing values detected for %s synthese column (= %s user column)', field,
                            selected_columns[field])

    except Exception:
        raise
