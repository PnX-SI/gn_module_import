import dask
import d6tstack
from sqlalchemy import create_engine
from geonature.utils.env import DB

from ..wrappers import checker
from ..api_error import GeonatureImportApiError
from ..db.query import check_row_number


@checker('Load : from Python dataframe to DB table')
def load(df, table_name, schema_name, full_table_name, import_id, engine, index_col):

    try:
        
        """
        SELECT
        pg_size_pretty(relpages::bigint*8*1024) AS size
        FROM pg_class
        where relname = 'taxref';
        """

        # create engine url for d6tstack:
        my_engine = create_engine("postgresql+{}://{}:{}@{}:{}/{}"\
            .format(
                engine.driver, 
                engine.url.username, 
                engine.url.password, 
                engine.url.host, 
                engine.url.port, 
                engine.url.database))

        # with pandas df:
        #d6tstack.utils.pd_to_psql(df,str(my_engine.url),table_name,schema_name=schema_name,if_exists='replace',sep='\t')
        
        # with dask df:
        dto_sql = dask.delayed(d6tstack.utils.pd_to_psql)
        [dask.compute(dto_sql(df,str(my_engine.url),table_name,schema_name=schema_name,if_exists='replace',sep='\t'))]

        # set gn_pk as primary key:
        DB.session.execute("ALTER TABLE ONLY {} ADD CONSTRAINT pk_gn_imports_{} PRIMARY KEY ({});".format(full_table_name, table_name, index_col))
        
        # check if df is fully loaded in postgresql table :
        is_rows_ok = check_row_number(import_id, full_table_name)
        if not is_rows_ok:
            raise GeonatureImportApiError(
                message='INTERNAL SERVER ERROR ("postMapping() error"): lignes manquantes dans {} - refaire le matching'.format(full_table_name),
                details='')

    except Exception:
        raise