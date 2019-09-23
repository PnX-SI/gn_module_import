import pdb
import pandas as pd

from ..wrappers import checker
from .utils import set_is_valid, set_invalid_reason, set_user_error
from ..logs import logger


@checker('Data cleaning : other_field operations checked')
def check_entity_source(df, added_cols, selected_columns, dc_user_errors, synthese_info):

    try:
        fields = [field for field in synthese_info]

        logger.info('CHECKING ENTITY SOURCE PK VALUE:')

        if 'entity_source_pk_value' not in fields:
            logger.info('- no entity source pk value provided : set gn_pk column as entity source pk column')
            # df['entity_source_pk_value'] = ''
            # df['entity_source_pk_value'] = df['gn_pk'].astype('str') # utile?
            added_cols['entity_source_pk_value'] = 'gn_pk' # récupérer gn_pk en conf
        else:
            # check duplicates
            logger.info('- checking duplicates in entity_source_pk_value column (= %s user column)', selected_columns['entity_source_pk_value'])

            df['temp'] = df.duplicated(selected_columns['entity_source_pk_value'], keep=False)
            df['temp'] = ~df['temp'].astype('bool')

            set_is_valid(df, 'temp')
            set_invalid_reason(df, 'temp', 'entity_source_pk_value duplicates in {} column', selected_columns['entity_source_pk_value'])
            
            n_entity_duplicates = df['temp'].astype(str).str.contains('False').sum()
            logger.info('%s duplicates errors in entity_source_pk_value column (= %s user column)', n_entity_duplicates, selected_columns['entity_source_pk_value'])

            if n_entity_duplicates > 0:
                set_user_error(dc_user_errors, 11, selected_columns['entity_source_pk_value'], n_entity_duplicates)
            # ajouter check valeur manquante ? (car pas not null dans synthese)
    except Exception:
        raise

