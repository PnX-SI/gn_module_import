import pandas as pd
from uuid import uuid4, UUID
import numpy as np
import dask
import dask.dataframe as dd
import datetime

from ..db.query import get_synthese_types
from ..wrappers import checker
from ..logs import logger
from .utils import fill_map, get_types, set_is_valid, set_invalid_reason, set_user_error

import pdb


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
def check_types(df, added_cols, selected_columns, synthese_info, missing_values):

    try:

        logger.info('checking types : ')
        user_error = []
        types = get_types(synthese_info)
        #types = list(dict.fromkeys(get_synthese_types()))

        
        # DATE TYPE COLUMNS : 

        date_fields = [field for field in synthese_info if synthese_info[field]['data_type'] == 'timestamp without time zone']
        
        for field in date_fields:

            logger.info('- checking date type for %s column', field)
            
            # ok : df['my_timestamp'] = dd.to_datetime(df['my_timestamp'],unit='ns')
            # ok : df['test'] = dd.to_datetime(df['my_timestamp'],unit='datetime64[ns]')
            col_name = '_'.join(['gn',selected_columns[field]])

            df[col_name] = pd.to_datetime(df[selected_columns[field]], errors='coerce')
            # datetime conversion
            #df[selected_columns[field]] = df[selected_columns[field]].apply(lambda x: convert_to_datetime(x))
            #df[selected_columns[field]] = df['test_date']
            #df temp qui contient is_datetime, remplacer not datetime par na, convertir en datetime
            #pdb.set_trace()
            # check invalid date type and set user errors in db and front interface
            df['temp'] = ''
            df['temp'] = df['temp']\
                .where(cond=df[col_name].notnull(), other=False)\
                .map(fill_map)\
                .astype('bool')

            set_is_valid(df, 'temp')
            set_invalid_reason(df, 'temp', 'invalid date for {} column', selected_columns[field])
            n_invalid_date_error = df['temp'].astype(str).str.contains('False').sum()

            added_cols['date_min'] = col_name

            if n_invalid_date_error > 0:
                user_error.append(
                        set_user_error(
                            'invalid date type',
                            selected_columns[field],
                            n_invalid_date_error
                        )
                    )
            

        


        # UUID TYPE COLUMNS :

        if 'uuid' in types:

            uuid_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'uuid']

            for col in uuid_cols:

                logger.info('- checking uuid type for %s column', col)

                df[selected_columns[col]] = df[selected_columns[col]]\
                    .replace(missing_values, np.nan) # utile?

                df['temp'] = ''
                df['temp'] = df['temp']\
                    .where(
                        cond=df[selected_columns[col]].apply(lambda x: is_uuid(x), meta=False), 
                        other=False)\
                    .map(fill_map)\
                    .astype('bool')

                set_is_valid(df, 'temp')
                set_invalid_reason(df, 'temp', 'invalid uuid in {} column', selected_columns[col])
                n_invalid_uuid = df['temp'].astype(str).str.contains('False').sum()

                if n_invalid_uuid > 0:
                    user_error.append(
                        set_user_error(
                            'invalid uuid type',
                            selected_columns[col],
                            n_invalid_uuid
                        )
                    )


        
        # CHARACTER VARYING TYPE COLUMNS : 

        varchar_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'character varying']
        
        for col in varchar_cols:

            logger.info('- checking varchar type for %s column', col)

            n_char = synthese_info[col]['character_max_length']

            if n_char is not None:

                df['temp'] = ''
                df['temp'] = df['temp']\
                    .where(
                        cond=df[selected_columns[col]].str.len().fillna(0) < n_char,
                        other=False)\
                    .map(fill_map)\
                    .astype('bool')

                set_is_valid(df, 'temp')
                set_invalid_reason(df, 'temp', 'string too long in {} column', selected_columns[col])
                n_invalid_string = df['temp'].astype(str).str.contains('False').sum()

                if n_invalid_string > 0:
                    user_error.append(
                        set_user_error(
                            'invalid character varying length',
                            selected_columns[col],
                            n_invalid_string
                        )
                    )
        

        # INTEGER TYPE COLUMNS :

        if 'integer' in types:

            int_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'integer']

            if 'cd_nom' in int_cols: # voir si on garde (idealement faudrait plutot adapter clean_cd_nom)
                int_cols.remove('cd_nom')

            for col in int_cols:

                logger.info('- checking integer type for %s column', col)

                df[selected_columns[col]] = df[selected_columns[col]]\
                    .replace(missing_values, np.nan) # utile?

                df['temp'] = ''
                df['temp'] = df['temp']\
                    .where(
                        cond=df[selected_columns[col]].str.isnumeric(),
                        other=False)\
                    .where(
                        cond=df[selected_columns[col]].notnull(), 
                        other='')\
                    .map(fill_map)\
                    .astype('bool')

                set_is_valid(df, 'temp')
                set_invalid_reason(df, 'temp', 'invalid integer in {} column', selected_columns[col])
                n_invalid_int = df['temp'].astype(str).str.contains('False').sum()

                if n_invalid_int > 0:
                    user_error.append(
                        set_user_error(
                            'invalid integer type',
                            selected_columns[col],
                            n_invalid_int
                        )
                    )

        if len(user_error) == 0:
            user_error = ''

        return user_error

    except Exception:
        raise
