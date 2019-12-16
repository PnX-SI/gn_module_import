import io
import pandas as pd
from ..wrappers import checker
from ..logs import logger


@checker('dask df loaded to db table')
def load_df_to_sql(df, table_name, engine, schema_name, separator):
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
        emp_df = emp_df.astype('object')
        emp_df[:0].to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        trans.close()


@checker('Loaded (from Python dataframe to DB table)')
def load(df, i, schema_name, temp_table_name, engine):
    try:

        logger.info('loading dataframe into DB table:')

        if i == 0:
            create_empty_table(df, temp_table_name, engine, schema_name)

        load_df_to_sql(df, temp_table_name, engine, schema_name, ';')

    except Exception:
        raise
