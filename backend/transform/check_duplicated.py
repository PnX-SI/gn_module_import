import pandas as pd
import numpy as np

from ..db.queries.user_errors import set_user_error
from .utils import fill_map, set_is_valid, set_invalid_reason
from ..wrappers import checker
from ..logs import logger

import pdb

@checker('Data cleaning : row duplicates checked')
def check_row_duplicates(df, selected_columns, import_id, schema_name):
    try:
        logger.info('CHECKING FOR DUPLICATED ROWS : ')
        selected_cols = selected_columns
        generate_fields = ['unique_id_sinp_generate', 'altitudes_generate']
        for field in generate_fields:
            if field in selected_cols.keys():
                del selected_cols[field]

        df['temp'] = df.duplicated(subset=selected_cols.values(), keep=False)
        df['temp'] = ~df['temp']
        set_is_valid(df, 'temp')
        n_duplicated_rows = df['temp'].astype(str).str.contains('False').sum()
        
        logger.info('%s duplicated rows detected', n_duplicated_rows)
        
        if n_duplicated_rows > 0:
            set_user_error(import_id, 17, '', n_duplicated_rows)
            set_invalid_reason(df, schema_name, 'temp', import_id, 17, '')

        df.drop('temp', axis=1)

    except Exception:
        raise
