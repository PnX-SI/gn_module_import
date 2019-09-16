import pandas as pd
import dask.dataframe as dd
import psutil
from sqlalchemy.sql import column
import sqlalchemy

from geonature.utils.env import DB

from ..wrappers import checker
from ..logs import logger

import pdb

@checker('Extracted (from DB table to Dask dataframe)')
def extract(table_name, schema_name, column_names, index_col, id):

    try:

        # create empty dataframe as model for importing data from sql table to dask dataframe (for meta argument in read_sql_table method)
        empty_df = pd.DataFrame(columns=column_names, dtype='object')
        empty_df[index_col] = pd.to_numeric(empty_df[index_col], errors='coerce')

        # get number of cores to set npartitions:
        ncores = psutil.cpu_count(logical=False)
        logger.warning('ncores used by Dask = %s', ncores)

        # set dask dataframe index
        index_dask = sqlalchemy.sql.column(index_col).label("gn_id")

        # get user table row data as a dask dataframe
        df = dd.read_sql_table(table=table_name, index_col=index_dask, meta=empty_df, npartitions=ncores, uri=str(DB.engine.url), schema=schema_name, bytes_per_chunk=100000)


        """
        taxref_info = DB.session.execute("
                            SELECT *
                            FROM information_schema.columns
                            WHERE table_schema = 'taxonomie'
                            AND table_name   = 'taxref';
                            ").fetchall()
        taxref_col_names = [i.column_name for i in taxref_info]
        empty_taxref_df = pd.DataFrame(columns=taxref_col_names, dtype='object')
        taxref_index_dask = sqlalchemy.sql.column('cd_nom').label("gn_id")
        taxref_df = dd.read_sql_table(table='taxref', index_col='cd_nom', npartitions=ncores, uri=str(DB.engine.url), schema='taxonomie', bytes_per_chunk=100000)
        
        
        pdb.set_trace()
        taxref_df['cd_nom2'] = str(taxref_df['cd_noms'])
        df.merge(taxref_df,left_on='species_id',right_on='cd_nom',how='left')

        df['species_id'].isin(taxref_df['cd_nom'])
        """


        return df



    except Exception:
        raise

