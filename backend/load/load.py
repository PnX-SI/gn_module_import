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
from ..db.query import get_full_table_name, load_csv_to_db
from ..db.models import generate_user_table_class
from ..load.utils import compute_df

import psycopg2.extras as ex
import pdb


@checker('dask df loaded to db table')
def load_df_to_sql(df, table_name, full_table_name, engine, schema_name, separator, import_id):

    try:
        conn = engine.raw_connection()
        fbuf = io.StringIO()
        df.to_csv(fbuf, index=False, header=True, sep=separator)
        fbuf.seek(0)
        cur = conn.cursor()
        cmd = """
            COPY {}.{}({}) FROM STDIN WITH (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER '{}'
            )
            """.format(schema_name, table_name, ','.join(df.columns.tolist()), separator)
        cur.copy_expert(cmd, fbuf)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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


@checker('Loaded (from Python dataframe to DB table)')
def load(df, i, table_name, schema_name, full_table_name, temp_table_name, import_id, engine, index_col, module_url, directory_name):

    try:

        logger.info('loading dataframe into DB table:')

        if i == 0:
            create_empty_table(df, temp_table_name, engine, schema_name)

        load_df_to_sql(df, temp_table_name, full_table_name, engine, schema_name, ';', import_id)
        
        #return df

    except Exception:
        raise