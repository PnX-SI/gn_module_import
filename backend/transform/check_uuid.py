from uuid import uuid4
import numpy as np
import pandas as pd

from geonature.utils.env import DB

from .utils import fill_col, fill_map, set_is_valid, set_invalid_reason, set_user_error

from ..wrappers import checker
from ..logs import logger
from ..db.queries.user_table_queries import get_uuid_list
from ..utils.utils import create_col_name

import pdb


def fill_nan_uuid(value):
    if pd.isnull(value):
        return str(uuid4())
    else:
        return value  


@checker('Data cleaning : uuid values checked')
def check_uuid(df, added_cols, selected_columns, dc_user_errors, synthese_info, is_generate_uuid, import_id):

    try:

        # send warnings if some uuid are missing :

        logger.info('CHECKING UUID VALUES')

        uuid_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'uuid']

        if len(uuid_cols) > 0:

            for col in uuid_cols:

                # localize null values
                df['temp'] = ''
                df['temp'] = df['temp']\
                    .where(
                        cond=df[selected_columns[col]].notnull(), 
                        other=False)\
                    .map(fill_map)\
                    .astype('bool')

                #df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
                
                if col == 'unique_id_sinp':

                    # if unique_id_sinp col provided, uuid value created if any missing field:
                    if df[selected_columns[col]].isnull().any():
                        if is_generate_uuid:
                            logger.info('generating uuid for missing values in %s synthese column (= %s user column)', col, selected_columns[col])
                            uuid_col_name = selected_columns[col]
                            create_col_name(df, selected_columns, col, import_id)
                            df[selected_columns[col]] = ''
                            df[selected_columns[col]] = df[uuid_col_name]\
                                .apply(lambda x: fill_nan_uuid(x))
                            set_invalid_reason(df, 'temp', 'warning : champ uuid vide dans colonne {} : un uuid a été créé', selected_columns[col])

                    # return False if invalid uuid, else (including missing values) return True
                    uuid_list = get_uuid_list()
                    df['temp'] = ''
                    df['temp'] = df['temp']\
                        .where(
                            cond=df[selected_columns[col]].isin(uuid_list), 
                            other=False)\
                        .map(fill_map)\
                        .astype('bool')
                    df['temp'] = ~df['temp']
                    # set gn_is_valid and invalid_reason
                    set_is_valid(df, 'temp')
                    set_invalid_reason(df, 'temp', 'uuid provided in {} col exists in synthese table', selected_columns[col])
                    n_invalid_uuid = df['temp'].astype(str).str.contains('False').sum()
                    df.drop('temp',axis=1)

                    logger.info('%s uuid value exists already in synthese table (= %s user column)', n_invalid_uuid, selected_columns[col])

                    if n_invalid_uuid > 0:
                        set_user_error(dc_user_errors, 14, selected_columns[col], n_invalid_uuid)
                    
                # pour les autres colonnes : on envoie un warning sans créer un uuid pour les champs manquants:
                else:
                    logger.info('check for missing values in %s synthese column (= %s user column)', col, selected_columns[col])

                    df['gn_invalid_reason'] = df['gn_invalid_reason']\
                        .where(
                            cond=df['temp'],
                            other=df['gn_invalid_reason'] + 'warning : champ uuid vide dans colonne {} -- '\
                                .format(selected_columns[col])
                            )

                    n_missing_uuid = df['temp'].astype(str).str.contains('False').sum()
                    
                    logger.info('%s missing values warnings in %s synthese column (= %s user column)', n_missing_uuid, col, selected_columns[col])

                    if n_missing_uuid > 0:
                        set_user_error(dc_user_errors, 6, selected_columns[col], n_missing_uuid)  


        # create unique_id_sinp column with uuid values if not existing :

        if 'unique_id_sinp' not in uuid_cols:
            if is_generate_uuid:
                logger.info('no unique_id_sinp column provided: creating uuid for each row')
                create_col_name(df, selected_columns, 'unique_id_sinp', import_id)
                df[selected_columns['unique_id_sinp']] = ''
                df[selected_columns['unique_id_sinp']] = df[selected_columns['unique_id_sinp']]\
                    .apply(lambda x: str(uuid4()))
                #added_cols['unique_id_sinp'] = 'unique_id_sinp'

    except Exception:
        raise
