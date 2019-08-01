from flask import (
    Blueprint,
    current_app,
    request
)
from dask.distributed import Client
from werkzeug.utils import secure_filename

import os
import pathlib
import psycopg2
import psutil
import pandas as pd
import pandas.io.sql as psql
import numpy as np

import threading

import io
import tempfile
import dask.dataframe as dd
import dask
import d6tstack


import ast

import datetime

import sqlalchemy
from sqlalchemy import func, text, select, update,create_engine, event
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.sql import column

from geonature.utils.utilssqlalchemy import json_resp

from geonature.utils.env import DB

from psycopg2 import sql
from psycopg2.extensions import QuotedString


import pdb

from .transform import __test

from geonature.core.gn_meta.models import TDatasets

from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
    CorObserverSynthese
)

from geonature.core.gn_commons.models import BibTablesLocation

from geonature.utils.env import DB

from geonature.core.gn_permissions import decorators as permissions

from pypnnomenclature.models import TNomenclatures

from .models import (
    TImports,
    CorRoleImport,
    CorImportArchives,
    generate_user_table_class
)

from .query import (
    get_table_info,
    get_table_list,
    test_user_dataset,
    delete_import_CorImportArchives,
    delete_import_CorRoleImport,
    delete_import_TImports,
    delete_tables,
    get_import_table_name
)

from .utils import*
# ameliorer prise en charge des erreurs

from .upload.upload_process import upload

from .upload.upload_errors import*

from .goodtables_checks.check_user_file import check_user_file

blueprint = Blueprint('import', __name__)



@blueprint.route('', methods=['GET'])
@permissions.check_cruved_scope('R', True, module_code="IMPORT")
@json_resp
def get_import_list(info_role):
    """
        return import list
    """
    try:
        results = DB.session.query(TImports)\
                    .order_by(TImports.id_import)\
                    .filter(TImports.step >= 2)
        nrows = DB.session.query(TImports).count()
        history = []
        if not results or nrows == 0:
            return {
                "empty": True
            }, 200
        for r in results:
            if r.date_end_import is None:
                date_end_import = 'En cours'
            else:
                date_end_import = r.date_end_import
            prop = {
                "id_import": r.id_import,
                "format_source_file": r.format_source_file,
                "srid": r.srid,
                "import_table": r.import_table,
                "id_dataset": r.id_dataset,
                "id_mapping": r.id_mapping,
                "date_create_import": str(r.date_create_import),#recupérer seulement date et pas heure
                "date_update_import": str(r.date_update_import),#recupérer seulement date et pas heure
                "date_end_import": str(date_end_import),
                "source_count": r.source_count,
                "import_count": r.import_count,
                "taxa_count": r.taxa_count,
                "date_min_data": str(r.date_min_data),
                "date_max_data": str(r.date_max_data),
                "step": r.step,
                "dataset_name": DB.session\
                    .query(TDatasets.dataset_name)\
                    .filter(TDatasets.id_dataset == r.id_dataset)\
                    .one()[0]
            }
            history.append(prop)
        return {
                   "empty": False,
                   "history": history,
               }, 200
    except Exception:
        raise
        return 'INTERNAL SERVER ERROR ("get_import_list() error"): contactez l\'administrateur du site', 500


@blueprint.route('/delete_step1', methods=['GET'])
@permissions.check_cruved_scope('R', True, module_code="IMPORT")
@json_resp
def delete_step1(info_role):
    """
        delete id_import previously created by current id_role having step = 1 value
        (might happen when user uploads several files during the same import process (=same id_import)
        and finally interrupts the process without any valid import)
    """
    try:
        list_to_delete = DB.session.query(TImports.id_import)\
                            .filter(CorRoleImport.id_import == TImports.id_import)\
                            .filter(CorRoleImport.id_role == info_role.id_role)\
                            .filter(TImports.step == 1)\

        ids = []

        for id in list_to_delete:
            delete_import_CorImportArchives(id.id_import)
            delete_import_CorRoleImport(id.id_import)
            delete_import_TImports(id.id_import)
            ids.append(id.id_import)
            DB.session.commit()

        return {"deleted_step1_imports":ids},200

    except Exception:
        return 'INTERNAL SERVER ERROR ("delete_step1() error"): contactez l\'administrateur du site', 500


@blueprint.route('/datasets', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def get_user_datasets(info_role):
    """
        load user datasets
    """

    try:
        results = DB.session.query(TDatasets).\
            filter(TDatasets.id_dataset == Synthese.id_dataset).\
            filter(CorObserverSynthese.id_synthese == Synthese.id_synthese).\
            filter(CorObserverSynthese.id_role == info_role.id_role).all()

        if results:
            datasets = []
            for row in results:
                d = {'datasetId': row.id_dataset,\
                     'datasetName': row.dataset_name}
                datasets.append(d)
            return datasets,200
        else:
            return 'Attention, vous n\'avez aucun jeu de données déclaré', 400
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_user_datasets() error"): contactez l\'administrateur du site', 500


@blueprint.route('/cancel_import/<import_id>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def cancel_import(info_role, import_id):
    try:
        
        if import_id == "undefined":
            server_response = {'deleted_id_import': "canceled"}
            return server_response,200
        

        # get step number
        step = DB.session.query(TImports.step)\
            .filter(TImports.id_import == import_id)\
            .one()[0]

        if step > 1:

            # get data table name
            user_data_name = DB.session.query(TImports.import_table)\
                .filter(TImports.id_import == import_id)\
                .one()[0]

            # set data table names
            archives_full_name = get_full_table_name(blueprint.config['ARCHIVES_SCHEMA_NAME'],user_data_name)
            imports_table_name = set_imports_table_name(user_data_name)
            imports_full_name = get_full_table_name('gn_imports', imports_table_name)

            # delete tables
            DB.session.execute("DROP TABLE {}".format(archives_full_name))
            DB.session.execute("DROP TABLE {}".format(imports_full_name))

        # delete metadata
        DB.session.query(TImports).filter(TImports.id_import == import_id).delete()
        DB.session.query(CorRoleImport).filter(CorRoleImport.id_import == import_id).delete()
        DB.session.query(CorImportArchives).filter(CorImportArchives.id_import == import_id).delete()
        
        DB.session.commit()
        DB.session.close()
        server_response = {'deleted_id_import': import_id}

        return server_response, 200
    except Exception:
        raise
        return 'INTERNAL SERVER ERROR ("delete_TImportRow() error"): contactez l\'administrateur du site', 500


@blueprint.route('/uploads', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def post_user_file(info_role):
    """
        - load user file in "upload" directory
        - check data in the userfile (column names, row duplicates, ...)
        - copy user file raw data in 2 geonature db tables
        - fill t_imports, cor_role_import, cor_archive_import
        - return id_import and column names list
        - delete user file in upload directory

        # Note perso:
        # prevoir un boolean (ou autre) pour bloquer la possibilité de l'utilisateur de lancer plusieurs uploads sans passer par interface front

    """

    try:
        is_file_saved = False
        is_id_import = False
        errors = []

        MAX_FILE_SIZE = blueprint.config['MAX_FILE_SIZE']
        ALLOWED_EXTENSIONS = blueprint.config['ALLOWED_EXTENSIONS']
        DIRECTORY_NAME = blueprint.config["UPLOAD_DIRECTORY"]
        MODULE_URL = blueprint.config["MODULE_URL"]
        PREFIX = blueprint.config['PREFIX']
        ARCHIVES_SCHEMA_NAME = blueprint.config['ARCHIVES_SCHEMA_NAME']
        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']

        """
        SAVES USER FILE IN UPLOAD DIRECTORY
        """

        uploaded_file = upload(request, MAX_FILE_SIZE, ALLOWED_EXTENSIONS, DIRECTORY_NAME, MODULE_URL)

        # checks if error in user file or user http request:
        if uploaded_file['error'] == 'no_file':
            errors.append(no_file)
        if uploaded_file['error'] == 'empty':
            errors.append(empty)
        if uploaded_file['error'] == 'bad_extension':
            errors.append(extension_error)
        if uploaded_file['error'] == 'long_name':
            errors.append(long_name)
        if uploaded_file['error'] == 'max_size':
            errors.append(max_size)  
        if uploaded_file['error'] == 'unknown':
            errors.append(unkown)        
        
        if uploaded_file['error'] != '':
            return errors,400

        full_path = uploaded_file['full_path']

        is_file_saved = uploaded_file['is_uploaded']

        """
        CHECKS USER FILE
        """

        report = check_user_file(full_path, 100000000)

        # reports user file errors:
        if len(report['errors']) > 0:
            return report['errors'],400

        """
        CREATES CURRENT IMPORT IN TIMPORTS (SET STEP TO 1 AND DATE/TIME TO CURRENT DATE/TIME)
        """
        
        # get form data
        metadata = dict(request.form.to_dict())
        print(metadata)

        # Check if id_dataset value is allowed (prevent from forbidden manual change in url (url/process/N))
        is_dataset_allowed = test_user_dataset(info_role.id_role, metadata['datasetId'])
        if not is_dataset_allowed:
            return \
            'L\'utilisateur {} n\'est pas autorisé à importer des données vers l\'id_dataset {}'\
            .format(info_role.id_role, int(metadata['datasetId']))\
            ,403

        # start t_imports filling and fill cor_role_import
        if metadata['importId'] == 'undefined':
            # if first file uploaded by the user in the current import process :
            init_date = datetime.datetime.now()
            insert_t_imports = TImports(
                date_create_import = init_date,
                date_update_import = init_date,
            )
            DB.session.add(insert_t_imports)
            DB.session.flush()
            id_import = insert_t_imports.id_import
            # fill cor_role_import
            insert_cor_role_import = CorRoleImport(id_role=info_role.id_role, id_import=id_import)
            DB.session.add(insert_cor_role_import)
            DB.session.flush()
        else:
            # if other file(s) already uploaded by the user in the current import process :
            id_import = int(metadata['importId'])
            DB.session.query(TImports)\
                .filter(TImports.id_import == id_import)\
                .update({
                    TImports.date_update_import:datetime.datetime.now()
                    })

        if isinstance(id_import,int):
            is_id_import = True

        # set step to 1 in t_imports table
        DB.session.query(TImports)\
            .filter(TImports.id_import == id_import)\
            .update({
                TImports.step:1
                })

        """
        GET/SET FILE,SCHEMA,TABLE,COLUMN NAMES
        """

        # file name treatment
        file_name_cleaner = clean_file_name(uploaded_file['file_name'], uploaded_file['extension'], id_import)
        if len(file_name_cleaner['errors']) > 0:
            # if user file name is not valid, an error is returned
            return file_name_cleaner['errors'],400
        cleaned_file_name = file_name_cleaner['clean_name']
        print('cleaned_file_name:')
        print(cleaned_file_name)

        ## get/set table and column names
        separator = metadata['separator']
        archives_table_name = cleaned_file_name
        #imports_table_name = set_imports_table_name(cleaned_file_name)

        # set schema.table names
        archives_full_name = get_full_table_name(ARCHIVES_SCHEMA_NAME, archives_table_name)
        #imports_full_name = get_full_table_name(IMPORTS_SCHEMA_NAME, imports_table_name)

        # pk name to include (because copy_from method requires a primary_key)
        pk_name = "".join([PREFIX, 'pk'])

        # clean column names
        columns = [clean_string(x) for x in report['column_names']]

        # check for reserved sql word in column names
        column_check = check_sql_words(columns)
        if len(column_check) > 0:
            forbidden_names = [report['column_names'][position] for position in column_check]
            errors = []
            error = {
                'code': 'forbidden_colum_names',
                'message': 'Vous ne pouvez pas utiliser certains noms de colonnes car ils sont reservés à sql',
                'message_data': forbidden_names
            }
            errors.append(error)
            return errors,400


        """
        CREATES TABLES CONTAINING RAW USER DATA IN GEONATURE DB
        """

        ## if existing, delete all tables already uploaded which have the same id_import suffix
        delete_tables(id_import, ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME)
        DB.session.commit()

        ## create user table classes (1 for gn_import_archives schema, 1 for gn_imports schema) corresponding to the user file structure (csv)
        user_class_gn_import_archives = generate_user_table_class(ARCHIVES_SCHEMA_NAME, archives_table_name,\
                                                                      pk_name, columns, id_import, schema_type='archives')
        #user_class_gn_imports = generate_user_table_class(IMPORTS_SCHEMA_NAME, imports_table_name,\
        #                                                              pk_name, columns, id_import, schema_type='gn_imports')

        ## create 2 empty tables in geonature db (in gn_import_archives and gn_imports schemas)
        engine = DB.engine
        user_class_gn_import_archives.__table__.create(bind=engine, checkfirst=True)
        #user_class_gn_imports.__table__.create(bind=engine, checkfirst=True)

        ## fill the user table (copy_from function is equivalent to COPY function of postgresql)
        conn = engine.raw_connection()
        cur = conn.cursor()

        with open(full_path, 'r') as f:
            next(f)
            cur.copy_from(f, archives_full_name, sep=separator, columns=columns)

        """
        with open(full_path, 'r') as f:
            next(f)
            cur.copy_from(f, imports_full_name, sep=separator, columns=columns)
        """

        conn.commit()
        conn.close()
        cur.close()

        """
        FILLS CorImportArchives AND UPDATES TImports
        """
        
        delete_import_CorImportArchives(id_import)
        cor_import_archives = CorImportArchives(id_import=id_import, table_archive=archives_table_name)
        DB.session.add(cor_import_archives)

        # update gn_import.t_imports with cleaned file name and step = 2
        DB.session.query(TImports)\
            .filter(TImports.id_import==id_import)\
            .update({
                TImports.import_table: cleaned_file_name,
                TImports.step: 2,
                TImports.id_dataset: int(metadata['datasetId']),
                TImports.srid: int(metadata['srid']),
                TImports.format_source_file: report['file_format'],
                TImports.source_count: report['row_count']-1
                })
        
        DB.session.commit()

        return {
            "importId": id_import,
            "columns": columns,
        },200
               
    except psycopg2.errors.BadCopyFileFormat as e:
        DB.session.rollback()
        if is_id_import:
            delete_tables(id_import, ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME)
        DB.session.commit()
        errors = []
        errors.append({
                'code': 'psycopg2.errors.BadCopyFileFormat',
                'message': 'Erreur probablement due à un problème de separateur',
                'message_data': e.diag.message_primary
        })
        return errors,400

    except Exception:
        DB.session.rollback()
        if is_id_import:
            delete_tables(id_import, ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME)
        DB.session.commit()
        return 'INTERNAL SERVER ERROR ("post_user_file() error"): contactez l\'administrateur du site', 500

    finally:
        if is_file_saved:
            os.remove(full_path)
        DB.metadata.clear()
        DB.session.close()


@blueprint.route('/mapping/<import_id>', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def postMapping(info_role, import_id):
    try:
        # set table names and index col
        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']
        ARCHIVES_SCHEMA_NAME = blueprint.config['ARCHIVES_SCHEMA_NAME']
        PREFIX = blueprint.config['PREFIX']
        index_col = ''.join([PREFIX,'pk'])
        archives_table_name = get_import_table_name(import_id)
        imports_table_name = set_imports_table_name(archives_table_name)
        imports_full_table_name = get_full_table_name(IMPORTS_SCHEMA_NAME,imports_table_name)

        # EXTRACT (from postgresql table to dask dataframe)

        print("start load table data in df")
        deb = datetime.datetime.now()
        # create empty dataframe as model for importing data from sql table to dask dataframe (for meta argument in read_sql_table method)
        column_names = get_table_info(archives_table_name,'column_name')
        empty_df = pd.DataFrame(columns=column_names,dtype='object')
        empty_df[index_col] = pd.to_numeric(empty_df[index_col],errors='coerce')

        # get number of cores to set npartitions:
        ncores = psutil.cpu_count(logical=False)
        index_dask = sqlalchemy.sql.column(index_col).label("gn_id")
        df = dd.read_sql_table(table=archives_table_name, index_col=index_dask, meta=empty_df, npartitions=ncores, uri=str(DB.engine.url), schema=ARCHIVES_SCHEMA_NAME, bytes_per_chunk=100000)
        print("end load table data in df")
        fin = datetime.datetime.now()
        perf = fin-deb
        print('load table data in df: {} secondes'.format(perf))

        # create df2 containing only selected columns + gn_is_valid + gn_invalid_reason:
        data = dict(request.get_json())

        # get non empty synthese fields
        selected_columns = {key:value for key, value in data.items() if value}
        #selected_columns.update({'gn_is_valid':'gn_is_valid'})
        selected_columns.update({index_col:index_col})
        #cols = ','.join(selected_columns.values())

        df2 = df.loc[:,[*list(selected_columns.values())]]
        del(df)
        gn_is_valid = False
        df2['gn_is_valid'] = gn_is_valid
        df2['gn_invalid_reason'] = ''


        # TRANSFORM (data wrangling with Dask dataframe)
        
        print("start data wrangling")
        deb = datetime.datetime.now()
        df2['nom_scientifique'] = df2['nom_scientifique'].apply(lambda x: __test(x))
        fin = datetime.datetime.now()
        perf = fin-deb
        print("end data wrangling")
        print('data wrangling: {} secondes'.format(perf))
        

        # LOAD (from Dask dataframe to postgresql table, with d6tstack pd_to_psql function)

        # df to sql avec d6tstack avec dask delayed
        # create engine url for d6tstack:
        engine = DB.engine
        str_engine = "postgresql+{}://{}:{}@{}:{}/{}".format(engine.driver,engine.url.username,engine.url.password,engine.url.host,engine.url.port,engine.url.database)
        my_engine = create_engine("postgresql+{}://{}:{}@{}:{}/{}".format(engine.driver,engine.url.username,engine.url.password,engine.url.host,engine.url.port,engine.url.database))
        print("start convert df dask to pandas df")
        deb = datetime.datetime.now()
       

        #d6tstack.utils.pd_to_psql(df.compute(),str_engine,imports_table_name,schema_name=IMPORTS_SCHEMA_NAME,if_exists='append',sep='\t')
        dto_sql = dask.delayed(d6tstack.utils.pd_to_psql)
        [dask.compute(dto_sql(d,str(my_engine.url),imports_table_name,schema_name=IMPORTS_SCHEMA_NAME,if_exists='append',sep='\t')) for d in df2.to_delayed()]

        del df2

        # prendre seulement les colonnes : ok
        # mettre gn_pk en primary_key : ok
        # verif que table entierement exportee : ok
        # en cas d'echec : ok pour doublons si on recharge 2x la même table
        # suppression si retour
        # verifier memoire sur tres gros fichiers

        DB.session.execute("ALTER TABLE ONLY {} ADD CONSTRAINT pk_gn_imports_{} PRIMARY KEY ({});".format(imports_full_table_name,imports_table_name,'gn_pk'))
        fin = datetime.datetime.now()
        perf = fin-deb
        print("end convert dask df to pandas df")
        print('convert df dask to pandas df: {} secondes'.format(perf))
        DB.session.commit()
        DB.session.close()

        # check entire df is loaded :
        n_original_rows = DB.session.execute("SELECT source_count FROM gn_imports.t_imports WHERE id_import=198;").fetchone()[0]
        n_loaded_rows = DB.session.execute("SELECT count(*) FROM {}".format(imports_full_table_name)).fetchone()[0]
        if n_original_rows != n_loaded_rows:
            return 'INTERNAL SERVER ERROR ("postMapping() error"): lignes manquantes dans {} - refaire le matching'.format(imports_full_table_name), 500

        pdb.set_trace()

        """
        # get synthese column names
        info_synth = get_table_info('synthese','type')
        print('info_synth : ')
        print(info_synth)
        print(info_synth['id_source'])

        #data wrangling
        #push vers i_table en modifiant le type

        keys = [key for key in data]
        
        filled = []
        for key in keys:
            if data[key] != '':
                filled.append(key)
        print(filled)

        # get table name
        table = DB.session.query(TImports.import_table).filter(TImports.id_import == import_id).one()[0]

        # construct gn_imports.tablename
        table_name = '_'.join(['i',table])
        schema_table_name = '.'.join(['gn_imports',table_name])
        print(schema_table_name)


        results = DB.session.execute("SELECT* FROM gn_imports.i_data_exemple_549")
        for v in results:
            for column, value in v.items():
                print('{0}: {1}'.format(column, value))
        return data
        """

    except psycopg2.errors.UniqueViolation as e:
        DB.session.rollback()
        DB.session.execute("DROP TABLE {}".format(imports_full_table_name))
        DB.session.commit()
        raise
        """
        a corriger, erreur ne s'affiche pas
        errors = []
        errors.append({
                'code': 'psycopg2.errors.UniqueViolation',
                'message': 'Doublon - erreur serveur',
                'message_data': e.diag.message_primary
        })
        return errors,500
        """
    except Exception:
        DB.session.rollback()
        print('full table name :')
        print(imports_full_table_name)
        DB.session.execute("DROP TABLE {}".format(imports_full_table_name))
        DB.session.commit()
        raise
        return 'INTERNAL SERVER ERROR ("postMapping() error"): contactez l\'administrateur du site', 500
    

@blueprint.route('/syntheseInfo', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def getSyntheseInfo(info_role):
    try:
        EXCLUDED_SYNTHESE_FIELDS_FRONT = blueprint.config['EXCLUDED_SYNTHESE_FIELDS_FRONT']
        NOT_NULLABLE_SYNTHESE_FIELDS = blueprint.config['NOT_NULLABLE_SYNTHESE_FIELDS']

        synthese_info = []

        data = DB.session.execute(\
                    "SELECT column_name,is_nullable,column_default,data_type,character_maximum_length\
                     FROM INFORMATION_SCHEMA.COLUMNS\
                     WHERE table_name = 'synthese';"\
        )

        for d in data:
            if d.column_name not in EXCLUDED_SYNTHESE_FIELDS_FRONT:
                if d.column_name in NOT_NULLABLE_SYNTHESE_FIELDS:
                    is_nullabl = 'NO'
                else:
                    is_nullabl = 'YES'
                prop = {
                    "column_name": d.column_name,
                    "is_nullable": is_nullabl,
                    "data_type": d.data_type,
                    "character_maximum_length": d.character_maximum_length,
                }
                synthese_info.append(prop)
        #names2 = ['nom_cite', 'date_min', 'date_max']
        print(synthese_info)
        return synthese_info, 200
    except Exception:
        raise
        return 'INTERNAL SERVER ERROR ("getSyntheseInfo() error"): contactez l\'administrateur du site', 500


"""
@blueprint.route('/return_to_list/<import_id>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def return_to_list(info_role, import_id):
    try:
        
        #Si import_id is not undefined, effacer ligne dans timports_id, cor_role_import, cor_archive_import si step = 1
        

        return server_response, 200
    except Exception:
        return 'INTERNAL SERVER ERROR ("delete_TImportRow() error"): contactez l\'administrateur du site', 500
"""