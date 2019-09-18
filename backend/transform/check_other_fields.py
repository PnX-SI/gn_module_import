import pdb
import pandas as pd

from ..wrappers import checker
from .utils import set_is_valid, set_invalid_reason, set_user_error


@checker('Data cleaning : other_field operations checked')
def check_entity_source(df, added_cols, selected_columns, dc_user_errors, synthese_info):

    try:
        fields = [field for field in synthese_info]

        if 'entity_source_pk_value' not in fields:
            # df['entity_source_pk_value'] = ''
            # df['entity_source_pk_value'] = df['gn_pk'].astype('str') # utile?
            added_cols['entity_source_pk_value'] = 'gn_pk'
        else:
            # check duplicates
            df['temp'] = df.duplicated(selected_columns['entity_source_pk_value'], keep=False)
            df['temp'] = ~df['temp'].astype('bool')
            set_is_valid(df, 'temp')
            set_invalid_reason(df, 'temp', 'entity_source_pk_value duplicates in {} column', selected_columns['entity_source_pk_value'])
            n_invalid_uuid = df['temp'].astype(str).str.contains('False').sum()
            if n_invalid_uuid > 0:
                set_user_error(dc_user_errors, 11, selected_columns['entity_source_pk_value'], n_invalid_uuid)
            # ajouter check valeur manquante ? (car pas not null dans synthese)
    except Exception:
        raise

