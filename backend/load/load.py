import dask
from sqlalchemy import create_engine
import os
import shutil
import io
import pandas as pd
from psycopg2.extensions import connection
import psycopg2

from geonature.utils.env import DB

from ..wrappers import checker
from ..api_error import GeonatureImportApiError
from ..logs import logger
from ..db.query import check_row_number, get_full_table_name, load_csv_to_db
from ..db.models import generate_user_table_class
from ..load.utils import compute_df

import psycopg2.extras as ex
import pdb


@checker('dask df loaded to db table')
def load_df_to_sql(df, table_name, full_table_name, engine, schema_name, separator, import_id):

    #pdb.set_trace()
    #trans = connection.begin()

    try:
        conn = engine.raw_connection()
        create_empty_table(df, table_name, engine, schema_name)
        fbuf = io.StringIO()
        df.to_csv(fbuf, index=False, header=True, sep=separator)
        fbuf.seek(0)
        cur = conn.cursor()
        cmd = """
            COPY {}({}) FROM STDIN WITH (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER '{}'
            )
            """.format(full_table_name, ','.join(df.columns.tolist()), separator)
        cur.copy_expert(cmd, fbuf)
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        conn.rollback()
        raise


def create_empty_table(df, table_name, engine, schema_name):
    try:
        conn = engine.connect()
        trans = conn.begin()
        emp_df = pd.DataFrame(columns=df.columns.tolist())
        emp_df = emp_df.astype(df.dtypes.to_dict())
        emp_df[:0].to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
        trans.commit()
    except:
        trans.rollback()
        raise
    finally:
        trans.close()


"""
@checker('dask df converted in pandas df')
def convert_to_pandas(df):
    return df.compute()
"""


@checker('Loaded (from Python dataframe to DB table)')
def load(df, table_name, schema_name, full_table_name, import_id, engine, index_col, df_type):

    try:

        # convert dask df to pandas df
        if df_type == 'dask':
            logger.info('converting dask dataframe to pandas dataframe:')
            df = convert_to_pandas(df)

        # create empty db table
        logger.info('loading dataframe into DB table:')
        load_df_to_sql(df, table_name, full_table_name, engine, schema_name, ';', import_id)

        # set gn_pk as primary key:
        DB.session.execute("ALTER TABLE ONLY {} ADD CONSTRAINT pk_gn_imports_{} PRIMARY KEY ({});".format(full_table_name, table_name, index_col))
        DB.session.commit()
        DB.session.close()

        # check if df is fully loaded in postgresql table :
        is_nrows_ok = check_row_number(import_id, full_table_name)
        if not is_nrows_ok:
            logger.error('missing rows because of loading server error')
            raise GeonatureImportApiError(
                message='INTERNAL SERVER ERROR ("postMapping() error"): lignes manquantes dans {} - refaire le matching'.format(full_table_name),
                details='')

        #del df
        
        return df

    except Exception:
        raise