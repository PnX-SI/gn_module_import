from uuid import uuid4
import numpy as np
import pandas as pd

from .utils import fill_col
from ..wrappers import checker
from ..logs import logger

import pdb


def fill_nan_uuid(value):
    if pd.isnull(value):
        return str(uuid4())
    else:
        return value  


@checker('Data cleaning : uuid values checked')
def check_uuid(df,selected_columns,synthese_info, df_type):

    try:

        user_error = []

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

                    df['gn_invalid_reason'] = df['gn_invalid_reason']\
                        .where(
                            cond=df['temp'],
                            other=df['gn_invalid_reason'] + 'warning : champ uuid vide dans colonne {} : un uuid a été créé -- '.format(col))
                    
                    df[selected_columns[col]] = df[selected_columns[col]]\
                        .apply(lambda x: fill_nan_uuid(x))

                # pour les autres colonnes : on envoie un warning sans créer un uuid pour les champs manquants:
                else:
                    df['gn_invalid_reason'] = df['gn_invalid_reason']\
                        .where(
                            cond=df['temp'],
                            other=df['gn_invalid_reason'] + 'warning : champ uuid vide dans colonne {} -- '\
                                .format(col))

                n_missing_uuid = df['temp'].astype(str).str.contains('False').sum()
                
                if df_type == 'dask':
                    n_missing_uuid = n_missing_uuid.compute()

                if n_missing_uuid > 0:
                    user_error.append({
                        'code': 'uuid warning',
                        'message': 'uuid manquants dans la colonne {}'.format(selected_columns[col]),
                        'message_data': 'nombre de lignes avec erreurs : {}'.format(n_missing_uuid)
                    })
        
        # create unique_id_sinp column with uuid values if not existing :

        if 'unique_id_sinp' not in uuid_cols:
            logger.info('no unique_id_sinp in user data: creating uuid for each row')
            df['unique_id_sinp'] = ''
            df['unique_id_sinp'] = df['unique_id_sinp']\
                .apply(lambda x: str(uuid4()))
            selected_columns['unique_id_sinp'] = 'unique_id_sinp'

        if len(user_error) == 0:
            user_error = ''

        return user_error

    except Exception:
        raise
