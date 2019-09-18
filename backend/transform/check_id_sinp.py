from uuid import uuid4
import numpy as np
import pandas as pd

from .utils import fill_col, fill_map, set_is_valid, set_invalid_reason, set_user_error
from ..wrappers import checker
from ..logs import logger

import pdb


def fill_nan_uuid(value):
    if pd.isnull(value):
        return str(uuid4())
    else:
        return value  


@checker('Data cleaning : uuid values checked')
def check_uuid(df, added_cols, selected_columns, dc_user_errors, synthese_info):

    try:

        # send warnings if some uuid are missing :

        uuid_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'uuid']

        if len(uuid_cols) > 0:

            for col in uuid_cols:

                logger.info('checking uuid values for %s column', col)

                # localize null values
                df['temp'] = ''
                df['temp'] = df['temp']\
                    .where(
                        cond=df[selected_columns[col]].notnull(), 
                        other=False)\
                .map(fill_map)\
                .astype('bool')

                #df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
                # si unique_id_sinp : on créé un uuid pour les champs manquants éventuels:
                
                if col == 'unique_id_sinp':

                    logger.info('unique_id_sinp provided in user data: checking if not missing')

                    set_invalid_reason(df, 'temp', 'warning : champ uuid vide dans colonne {} : un uuid a été créé', selected_columns[col])

                    df[selected_columns[col]] = df[selected_columns[col]]\
                        .apply(lambda x: fill_nan_uuid(x))
                    # !!! attention de pas les générer si pas coché

                # pour les autres colonnes : on envoie un warning sans créer un uuid pour les champs manquants:
                else:
                    df['gn_invalid_reason'] = df['gn_invalid_reason']\
                        .where(
                            cond=df['temp'],
                            other=df['gn_invalid_reason'] + 'warning : champ uuid vide dans colonne {} -- '\
                                .format(selected_columns[col]))

                n_missing_uuid = df['temp'].astype(str).str.contains('False').sum()

                if n_missing_uuid > 0:
                    set_user_error(dc_user_errors, 6, selected_columns[col], n_missing_uuid)  


        # create unique_id_sinp column with uuid values if not existing :

        if 'unique_id_sinp' not in uuid_cols:
            logger.info('no unique_id_sinp in user data: creating uuid for each row')
            df['unique_id_sinp'] = ''
            df['unique_id_sinp'] = df['unique_id_sinp']\
                .apply(lambda x: str(uuid4()))
            added_cols['unique_id_sinp'] = 'unique_id_sinp'

    except Exception:
        raise
