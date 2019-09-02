from uuid import uuid4
from .utils import fill_col
from ..wrappers import checker
import numpy as np
import pandas as pd

import pdb


def fill_nan_uuid(value):
    if pd.isnull(value):
        return str(uuid4())
    else:
        return value  


@checker('Data cleaning : check uuid values')
def check_uuid(df,selected_columns,synthese_info):

    user_error = []

    # send warnings if some uuid are missing :

    uuid_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'uuid']

    if len(uuid_cols) > 0:

        for col in uuid_cols:

            df['temp'] = ''
            df['temp'] = df['temp'].where(cond=df[selected_columns[col]].notnull(), other=False)
            #df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
            # si unique_id_sinp : on créé un uuid pour les champs manquants éventuels:
            if col == 'unique_id_sinp':
                df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                    cond=df['temp'].apply(lambda x: fill_col(x)),
                    other=df['gn_invalid_reason'] + 'warning : champ uuid vide dans colonne {} : un uuid a été créé -- '.format(col))
                df[selected_columns[col]] = df[selected_columns[col]].apply(lambda x: fill_nan_uuid(x))
            # pour les autres colonnes : on envoie un warning sans créer un uuid pour les champs manquants:
            else:
                df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                    cond=df['temp'].apply(lambda x: fill_col(x)),
                    other=df['gn_invalid_reason'] + 'warning : champ uuid vide dans colonne {} -- '.format(col))

            n_missing_uuid = df['temp'].astype(str).str.contains('False').sum()
            # si trop long de compter les lignes, parcourir temp et stop des que trouve false

            if n_missing_uuid > 0:
                user_error.append({
                    'code': 'uuid warning',
                    'message': 'uuid manquants dans la colonne {}'.format(selected_columns[col]),
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_missing_uuid)
                })
    
    # create unique_id_sinp column with uuid values if not existing :

    if 'unique_id_sinp' not in uuid_cols:
        df['unique_id_sinp'] = ''
        df['unique_id_sinp'] = df['unique_id_sinp'].apply(lambda x: str(uuid4()))

    if len(user_error) == 0:
        user_error = ''

    return user_error
