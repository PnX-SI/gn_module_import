import pandas as pd
import dask.dataframe as dd
import sqlalchemy

from geonature.utils.env import DB

from ..wrappers import checker
from ..logs import logger
from ..transform.utils import remove_temp_columns


@checker("Extracted (from DB table to Dask dataframe)")
def extract(table_name, schema_name, column_names, index_col, id):

    # create empty dataframe as model for importing data from sql table to dask dataframe
    # (for meta argument in read_sql_table method)
    empty_df = pd.DataFrame(columns=column_names, dtype="object")
    empty_df[index_col] = pd.to_numeric(empty_df[index_col], errors="coerce")

    # set dask dataframe index
    index_dask = sqlalchemy.sql.column(index_col).label("gn_id")
    query = """
    ALTER TABLE {schema_name}.{table_name}
    ALTER {index_col} TYPE integer
    USING {index_col}::integer;
    """
    query_nb_row = """
    SELECT count(*) 
    FROM {schema_name}.{table_name}
    """.format(
        schema_name=schema_name, table_name=table_name
    )
    try:
        DB.session.execute(
            query.format(
                schema_name=schema_name, table_name=table_name, index_col=index_col
            )
        )
        DB.session.commit()

        query = DB.session.execute(query_nb_row).fetchone()
        nb_row = query[0]
        npartition = 1 if nb_row < 50000 else 2
    except Exception as e:
        DB.session.rollback()

    # get user table row data as a dask dataframe
    df = dd.read_sql_table(
        table=table_name,
        index_col=index_dask,
        uri=str(DB.engine.url),
        schema=schema_name,
        # bytes_per_chunk=100000000,
        npartitions=npartition,
    )

    return df
