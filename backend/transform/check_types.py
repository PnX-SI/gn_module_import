import pdb
from ..db.query import get_synthese_types
from .utils import fill_col
from ..wrappers import checker
import pandas as pd
from uuid import uuid4, UUID
import numpy as np
import dask


def get_types(synthese_info):
    return [synthese_info[field]['data_type'] for field in synthese_info]


def convert_to_datetime(value):
    try:
        return pd.to_datetime(value)
    except ValueError:
        return value


def is_datetime(value):
    try:
        pd.to_datetime(value)
        return True
    except ValueError:
        return False


def is_uuid(value, version=4):
    try:
        if pd.isnull(value):
            return True
        else:
            converted_uuid = UUID(str(value), version=version)
            return converted_uuid.hex == value.replace('-', '')
    except ValueError:
        return False


@checker('Data cleaning : type of values checked')
def check_types(df, selected_columns, synthese_info, missing_values):

    user_error = []
    types = get_types(synthese_info)
    #types = list(dict.fromkeys(get_synthese_types()))


    # DATE TYPE COLUMNS : 

    date_fields = [field for field in synthese_info if synthese_info[field]['data_type'] == 'timestamp without time zone']
    
    for field in date_fields:
    
        # datetime conversion
        df[selected_columns[field]] = df[selected_columns[field]].apply(lambda x: convert_to_datetime(x))

        # check invalid date type and set user errors in db and front interface
        df['temp'] = ''
        df['temp'] = df['temp'].where(cond=df[selected_columns[field]].apply(lambda x: is_datetime(x)), other=False)
        df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
        df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
            cond=df['temp'].apply(lambda x: fill_col(x)), 
            other=df['gn_invalid_reason'] + 'invalid date for {} column -- '.format(selected_columns[field]))

        #n_invalid_date_error = df['temp'].count().compute()
        #n_invalid_date_error = count_false(df['temp']).compute()
        n_invalid_date_error = df['temp'].astype(str).str.contains('False').sum()
        #n_invalid_date_error = df['temp'].compute().value_counts()[False]

        if n_invalid_date_error > 0:
            user_error.append({
                'code': 'date error',
                'message': 'Des dates sont invalides dans la colonne {}'.format(selected_columns[field]),
                'message_data': 'nombre de lignes avec erreurs : {}'.format(n_invalid_date_error)
            })


    # UUID TYPE COLUMNS :

    if 'uuid' in types:

        uuid_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'uuid']

        for col in uuid_cols:
            df[selected_columns[col]] = df[selected_columns[col]].replace(missing_values, np.nan)
            df['temp'] = ''
            df['temp'] = df['temp'].where(
                cond=df[selected_columns[col]].apply(lambda x: is_uuid(x)), 
                other=False)
            df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                cond=df['temp'].apply(lambda x: fill_col(x)),
                other=df['gn_invalid_reason'] + 'invalid uuid in {} column -- '.format(selected_columns[col]))

            n_invalid_uuid = df['temp'].astype(str).str.contains('False').sum()

            if n_invalid_uuid > 0:
                user_error.append({
                    'code': 'uuid error',
                    'message': 'uuid invalides dans la colonne {}'.format(selected_columns[col]),
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_invalid_uuid)
                })

    # CHARACTER VARYING TYPE COLUMNS : 

    varchar_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'character varying']
    
    for col in varchar_cols:

        n_char = synthese_info[col]['character_max_length']

        if n_char is not None:

            df['temp'] = ''
            df['temp'] = df['temp'].where(
                cond=df[selected_columns[col]].str.len() < n_char, 
                other=False)
            df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                cond=df['temp'].apply(lambda x: fill_col(x)),
                other=df['gn_invalid_reason'] + 'string too long in {} column -- '.format(selected_columns[col]))

            n_invalid_string = df['temp'].astype(str).str.contains('False').sum()

            if n_invalid_string > 0:
                user_error.append({
                    'code': 'character varying error',
                    'message': 'texte trop long dans la colonne {}'.format(selected_columns[col]),
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_invalid_string)
                })


    # INTEGER TYPE COLUMNS :

    if 'integer' in types:

        int_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'integer']

        if 'cd_nom' in int_cols: # voir si on garde (idealement faudrait plutot adapter clean_cd_nom)
            int_cols.remove('cd_nom')

        for col in int_cols:
            df[selected_columns[col]] = df[selected_columns[col]].replace(missing_values, np.nan)
            df['temp'] = ''
            df['temp'] = df['temp']\
                .where(cond=df[selected_columns[col]].str.isnumeric(),other=False)\
                .where(cond=df[selected_columns[col]].notnull(), other='')
            df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
            df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
                cond=df['temp'].apply(lambda x: fill_col(x)),
                other=df['gn_invalid_reason'] + 'invalid integer in {} column; '.format(selected_columns[col]))

            n_invalid_int = df['temp'].astype(str).str.contains('False').sum()

            if n_invalid_int > 0:
                user_error.append({
                    'code': 'integer error',
                    'message': 'integer invalides dans la colonne {}'.format(selected_columns[col]),
                    'message_data': 'nombre de lignes avec erreurs : {}'.format(n_invalid_int)
                })

    if len(user_error) == 0:
        user_error = ''

    return user_error
