import pandas as pd
from uuid import UUID
import numpy as np
from shapely import wkt

from ..db.queries.user_errors import set_user_error
from ..db.queries.nomenclatures import get_SINP_synthese_cols
from ..db.queries.utils import get_types
from ..wrappers import checker
from ..logs import logger
from .utils import fill_map, set_is_valid, set_invalid_reason


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
            return converted_uuid.hex == value.lower().replace('-', '')
    except ValueError:
        return False


def is_wkt_valid(value):
    try:
        # set missing value for invalid wkt
        wkt.loads(value)
        return True
    except Exception:
        return False


@checker('Data cleaning : type of values checked')
def check_types(df, added_cols, selected_columns, synthese_info, missing_values, schema_name, import_id):
    try:

        logger.info('CHECKING TYPES : ')
        types = get_types(schema_name, selected_columns)

        # DATE TYPE COLUMNS : 

        date_fields = [field for field in synthese_info if
                       synthese_info[field]['data_type'] == 'timestamp without time zone']

        for field in date_fields:

            logger.info('- checking and converting to date type in %s synthese column (= %s user column)', field,
                        selected_columns[field])

            if df[selected_columns[field]].dtype != 'datetime64[ns]':
                df[selected_columns[field]] = pd.to_datetime(df[selected_columns[field]], 'coerce').where(
                    ~df[selected_columns[field]].astype('object').str.isdigit().astype(bool))
            df['temp'] = ''
            df['temp'] = df['temp'] \
                .where(cond=df[selected_columns[field]].notnull(), other=False) \
                .map(fill_map) \
                .astype('bool')

            set_is_valid(df, 'temp')
            n_invalid_date_error = df['temp'].astype(str).str.contains('False').sum()

            logger.info('%s date type error detected in %s synthese column (= %s user column)', n_invalid_date_error,
                        field, selected_columns[field])

            if n_invalid_date_error > 0:
                set_user_error(import_id, 2, selected_columns[field], n_invalid_date_error)
                set_invalid_reason(df, schema_name, 'temp', import_id, 2, selected_columns[field])


        # UUID TYPE COLUMNS :

        if 'uuid' in types:

            uuid_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'uuid']

            for col in uuid_cols:

                logger.info('- checking uuid type in %s synthese column (= %s user column)', col, selected_columns[col])

                df[selected_columns[col]] = df[selected_columns[col]] \
                    .replace(missing_values, np.nan)  # utile?

                df['temp'] = ''
                df['temp'] = df['temp'] \
                    .where(
                    cond=df[selected_columns[col]].apply(lambda x: is_uuid(x)),
                    other=False) \
                    .map(fill_map) \
                    .astype('bool')

                set_is_valid(df, 'temp')
                n_invalid_uuid = df['temp'].astype(str).str.contains('False').sum()

                logger.info('%s invalid uuid detected in %s synthese column (= %s user column)', n_invalid_uuid, col,
                            selected_columns[col])

                if n_invalid_uuid > 0:
                    set_user_error(import_id, 3, selected_columns[col], n_invalid_uuid)
                    set_invalid_reason(df, schema_name, 'temp', import_id, 3, selected_columns[col])


        # CHARACTER VARYING TYPE COLUMNS : 

        varchar_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'character varying']

        for col in varchar_cols:

            logger.info('- checking varchar type in %s synthese column (= %s user column)', col, selected_columns[col])

            n_char = synthese_info[col]['character_max_length']

            if n_char is not None:

                df['temp'] = ''
                df['temp'] = df['temp'] \
                    .where(
                    cond=df[selected_columns[col]].astype('object').str.len().fillna(0) < n_char,
                    other=False) \
                    .map(fill_map) \
                    .astype('bool')

                set_is_valid(df, 'temp')
                n_invalid_string = df['temp'].astype(str).str.contains('False').sum()

                logger.info('%s varchar type errors detected in %s synthese column (= %s user column)',
                            n_invalid_string, col, selected_columns[col])

                if n_invalid_string > 0:
                    set_user_error(import_id, 4, selected_columns[col], n_invalid_string)
                    set_invalid_reason(df, schema_name, 'temp', import_id, 4, selected_columns[col])


        # INTEGER TYPE COLUMNS :

        if 'integer' in types:

            int_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'integer']

            SINP_synthese_cols = get_SINP_synthese_cols()

            if 'cd_nom' in int_cols:  # voir si on garde (idealement faudrait plutot adapter clean_cd_nom)
                int_cols.remove('cd_nom')

            for col in int_cols:

                if col not in SINP_synthese_cols:

                    logger.info('- checking integer type in %s synthese column (= %s user column)', col,
                                selected_columns[col])

                    df[selected_columns[col]] = df[selected_columns[col]] \
                        .replace(missing_values, np.nan)  # utile?

                    df['temp'] = ''
                    df['temp'] = df['temp'] \
                        .where(
                        cond=df[selected_columns[col]].astype('object').str.isnumeric(),
                        other=False) \
                        .where(
                        cond=df[selected_columns[col]].notnull(),
                        other='') \
                        .map(fill_map) \
                        .astype('bool')

                    set_is_valid(df, 'temp')
                    n_invalid_int = df['temp'].astype(str).str.contains('False').sum()

                    logger.info('%s integer type errors detected in %s synthese column (= %s user column)',
                                n_invalid_int, col, selected_columns[col])

                    if n_invalid_int > 0:
                        set_user_error(import_id, 1, selected_columns[col], n_invalid_int)
                        set_invalid_reason(df, schema_name, 'temp', import_id, 1, selected_columns[col])


        # REAL TYPE COLUMNS :

        if 'real' in types:

            real_cols = [field for field in synthese_info if synthese_info[field]['data_type'] == 'real']

            for col in real_cols:

                logger.info('- checking real type in %s synthese column (= %s user column)', col, selected_columns[col])
                if df[selected_columns[col]].dtype == 'datetime64[ns]':
                    df['temp'] = False
                else:
                    # replace eventual commas by points
                    df[selected_columns[col]] = df[selected_columns[col]].astype('object').str.replace(',', '.')
                    # check valid real type
                    df['temp'] = pd.to_numeric(
                        df[selected_columns[col]]
                        .str.replace(',', '.')
                        .fillna(0), 'coerce') \
                        .notnull()

                set_is_valid(df, 'temp')
                n_invalid_real = df['temp'].astype(str).str.contains('False').sum()

                logger.info('%s real type errors detected in %s synthese column (= %s user column)', n_invalid_real,
                            col, selected_columns[col])

                if n_invalid_real > 0:
                    set_user_error(import_id, 12, selected_columns[col], n_invalid_real)
                    set_invalid_reason(df, schema_name, 'temp', import_id, 12, selected_columns[col])

                del df['temp']

        # WKT CHECK
        
        if 'wkt' in types:
            
            df['temp'] = df[selected_columns['WKT']].apply(lambda x: is_wkt_valid(x))
            
            set_is_valid(df, 'temp')
            n_invalid_wkt = df['temp'].astype(str).str.contains('False').sum()

            logger.info('%s wkt type errors detected in %s synthese column (= %s user column)', n_invalid_wkt, col,
                        selected_columns['WKT'])

            if n_invalid_wkt > 0:
                set_user_error(import_id, 16, selected_columns['WKT'], n_invalid_wkt)
                set_invalid_reason(df, schema_name, 'temp', import_id, 16, selected_columns['WKT'])

            del df['temp']

    except Exception:
        raise
