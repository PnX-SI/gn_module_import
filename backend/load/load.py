import dask
import d6tstack
from sqlalchemy import create_engine
import os
import shutil

from geonature.utils.env import DB

from ..wrappers import checker
from ..api_error import GeonatureImportApiError
from ..db.query import check_row_number, get_full_table_name, load_csv_to_db
from ..db.models import generate_user_table_class

import pdb


@checker('temp csv created')
def create_temp_csv(df, path, table_name, separator, column_names):
    names = '{}/{}-*.csv'.format(path, table_name)
    return df.to_csv(names, sep=separator, columns=column_names, index=False)


@checker('copy_from csv to postgresql')
def copy_from_csv(schema_name, table_name, full_table_name, index_name, column_list, import_id, engine, temp_path, separator):
    user_class_gn_imports_load = generate_user_table_class(\
        schema_name, table_name, index_name, column_list, import_id, schema_type='gn_imports')
    user_class_gn_imports_load.__table__.create(bind=engine, checkfirst=True)

    conn = engine.raw_connection()
    cur = conn.cursor()

    for filename in os.listdir(temp_path):
        with open('{}/{}'.format(temp_path,filename), 'r') as f:
            next(f)
            cur.copy_from(f, full_table_name, sep=separator, columns=column_list)

    conn.commit()
    conn.close()


@checker('dask df converted in pandas df')
def pd_convert(df):
    return df.compute()


@checker('Loaded (from Python dataframe to DB table)')
def load(df, table_name, schema_name, full_table_name, import_id, engine, index_col):

    try:
        
        """
        SELECT
        pg_size_pretty(relpages::bigint*8*1024) AS size
        FROM pg_class
        where relname = 'taxref';
        """

        #df2 = pd_convert(df)

        # temps pour df2:
        # - tout le cleaning : erreur
        # - sans cleaning : 17s
        # - seulement missing : 39s / 1m30
        # - seulement type : 10m35s / 7m13s (30s si on enleve date)
        # - seulement uuid : 28s
        # - seulement cd_nom : 30s
        # - seulement counts : 51s
        # - seulement dates : 19s
        # - seulement entity : 
        # - seulement altitude :


        #pdb.set_trace()

        # create engine url for d6tstack:
        my_engine = create_engine("postgresql+{}://{}:{}@{}:{}/{}"\
            .format(
                engine.driver, 
                engine.url.username, 
                engine.url.password, 
                engine.url.host, 
                engine.url.port, 
                engine.url.database))

        #d6tstack.utils.pd_to_psql(df2, str(my_engine.url), 'test60', schema_name=schema_name, if_exists='replace', sep='\t')
        #DB.session.execute("DROP TABLE {}".format(get_full_table_name(schema_name,table_name)))

        # create temp directory
        #df2 = df[['gn_is_valid','gn_invalid_reason','gn_pk','unique_id_sinp']]

        """
        upload_directory_path = os.path.join(os.path.dirname(os.getcwd()), 'external_modules/{}/{}'.format(module_url,directory_name))
        temp_dir_name = '_'.join(['temp',table_name])
        temp_path = os.path.join(upload_directory_path, temp_dir_name)
        if os.path.exists(temp_path):
            shutil.rmtree(temp_path)
        os.mkdir(temp_path)

    
        create_temp_csv(df2, temp_path, table_name, ';', df2.columns.tolist())

        copy_from_csv(schema_name, 'test1000', 'gn_imports.test1000', 'gn_pk', df2.columns.tolist(), import_id, engine, temp_path, ';')


        DB.session.commit()
        DB.session.close()

        pdb.set_trace()
        """
        """
        user_class_gn_imports_load = generate_user_table_class(\
            'gn_imports', 'test5023', 'gn_pk', df.columns.tolist(), import_id, schema_type='gn_imports')
        user_class_gn_imports_load.__table__.create(bind=engine, checkfirst=True)

        conn = engine.raw_connection()
        cur = conn.cursor()
        """


        dto_sql = dask.delayed(d6tstack.utils.pd_to_psql)
        [dask.compute(dto_sql(df, str(my_engine.url), table_name, schema_name=schema_name, if_exists='replace',sep='\t'))]


        """
        
        out = [dto_sql(df, str(my_engine.url), table_name, schema_name='gn_imports', if_exists='replace', sep='\t')]
        dask.compute(*out)
        """
        # 8m43 avec toutes les verif sauf type des dates
        # 1m6 sans error missing et error types
        # 6m42 sans error missing
        # 1m41 sans error type
        # 1m09 sans error date type



        DB.session.commit()
        DB.session.close()

        # set gn_pk as primary key:
        #DB.session.execute("ALTER TABLE ONLY {} ADD CONSTRAINT pk_gn_imports_{} PRIMARY KEY ({});".format(full_table_name, table_name, index_col))
        
        # check if df is fully loaded in postgresql table :
        
        is_rows_ok = check_row_number(import_id, full_table_name)
        if not is_rows_ok:
            raise GeonatureImportApiError(
                message='INTERNAL SERVER ERROR ("postMapping() error"): lignes manquantes dans {} - refaire le matching'.format(full_table_name),
                details='')
        


    except Exception:
        raise