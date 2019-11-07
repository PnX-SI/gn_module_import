from flask import (
    Blueprint,
    current_app,
    request,
    jsonify
)

from werkzeug.utils import secure_filename
import geopandas
import os
import pathlib
import pandas as pd
import numpy as np
import threading
import datetime
import ast
import itertools
import inspect

import sqlalchemy
from sqlalchemy import func, text, select, update, event, join
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.sql import column

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import QuotedString, AsIs, quote_ident
from psycopg2.extras import execute_values

from geonature.utils.utilssqlalchemy import json_resp
from geonature.utils.env import DB
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

from .db.models import (
    TImports,
    CorRoleImport,
    CorImportArchives,
    BibMappings,
    CorRoleMapping,
    TMappingsFields,
    TMappingsValues,
    BibFields,
    BibThemes,
    generate_user_table_class
)

from .db.query import (
    get_table_info,
    get_table_list,
    test_user_dataset,
    delete_import_CorImportArchives,
    delete_import_CorRoleImport,
    delete_import_TImports,
    delete_tables,
    get_table_name,
    get_table_names,
    check_sql_words,
    get_full_table_name,
    set_imports_table_name,
    get_synthese_info,
    load_csv_to_db,
    get_row_number,
    check_row_number,
    get_local_srid,
    get_cd_nom_list
)

from .db.queries.user_table_queries import (
    delete_table,
    rename_table,
    set_primary_key,
    alter_column_type,
    get_n_loaded_rows,
    get_n_invalid_rows,
    get_n_valid_rows,
    get_n_taxa,
    get_date_ext
)

from .db.queries.save_mapping import save_field_mapping, save_content_mapping
from .db.queries.load_to_synthese import (
    insert_into_t_sources, 
    get_id_source,
    check_id_source
)

from .utils.clean_names import*
from .utils.utils import create_col_name

from .upload.upload_process import upload
from .upload.upload_errors import*

from .goodtables_checks.check_user_file import check_user_file

from .transform.transform import data_cleaning
from .transform.set_geometry import set_geometry
from .transform.set_altitudes import set_altitudes
from .transform.nomenclatures.nomenclatures import (
    get_nomenc_info, 
    set_nomenclature_ids,
    set_default_nomenclature_ids
)

from .logs import logger

from .api_error import GeonatureImportApiError

from .extract.extract import extract

from .load.load import load
from .load.utils import compute_df

from .data_preview.preview import get_preview, set_total_columns

from .load.into_synthese.import_data import load_data_to_synthese

from .wrappers import checker

import pdb

blueprint = Blueprint('import', __name__)


@blueprint.errorhandler(GeonatureImportApiError)
def handle_geonature_import_api(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


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
                # recupérer seulement date et pas heure
                "date_create_import": str(r.date_create_import),
                # recupérer seulement date et pas heure
                "date_update_import": str(r.date_update_import),
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

        return {"deleted_step1_imports": ids}, 200

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
                d = {
                    'datasetId': row.id_dataset,
                    'datasetName': row.dataset_name
                    }
                datasets.append(d)
            return datasets, 200
        else:
            return 'Attention, vous n\'avez aucun jeu de données déclaré', 400
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_user_datasets() error"): contactez l\'administrateur du site', 500


@blueprint.route('/mappings/<mapping_type>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def get_mappings(info_role, mapping_type):
    """
        Load mapping names in frontend (select)
    """

    try:
        results = DB.session.query(BibMappings)\
            .filter(CorRoleMapping.id_role == info_role.id_role)\
            .filter(BibMappings.mapping_type == mapping_type.upper())\
            .all()

        mappings = []

        if len(results) > 0:
            for row in results:
                print(row.mapping_label)
                d = {
                    'id_mapping': row.id_mapping,
                    'mapping_label': row.mapping_label
                    }
                mappings.append(d)
        else:
            mappings.append('empty')

        logger.debug('List of mappings %s', mappings)

        return mappings, 200
        
    except Exception:
        raise
        return 'INTERNAL SERVER ERROR ("get_field_mappings() error"): contactez l\'administrateur du site', 500


@blueprint.route('/field_mappings/<id_mapping>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def get_mapping_fields(info_role, id_mapping):
    """
        load source and target fields from an id_mapping
    """

    try:
        logger.debug('get fields saved in id_mapping = %s', id_mapping)

        fields = DB.session\
            .query(TMappingsFields)\
            .filter(TMappingsFields.id_mapping == int(id_mapping))\
            .all()
        
        mapping_fields = []

        if len(fields) > 0:
            for field in fields:
                d = {
                    'id_match_fields': field.id_match_fields,
                    'id_mapping': field.id_mapping,
                    'source_field': field.source_field,
                    'target_field': field.target_field
                    }
                mapping_fields.append(d)
        else:
            mapping_fields.append('empty')

        logger.debug('mapping_fields = %s from id_mapping number %s', mapping_fields, id_mapping)
        
        return mapping_fields, 200

    except Exception:
        raise
        return 'INTERNAL SERVER ERROR ("get_mapping_fields() error"): contactez l\'administrateur du site', 500


@blueprint.route('/content_mappings/<id_mapping>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def get_mapping_contents(info_role, id_mapping):
    """
        load source and target contents from an id_mapping
    """

    try:
        logger.debug('get contents saved in id_mapping = %s', id_mapping)

        contents = DB.session\
            .query(TMappingsValues)\
            .filter(TMappingsValues.id_mapping == int(id_mapping))\
            .all()
        
        mapping_contents = []
        gb_mapping_contents = []

        if len(contents) > 0:
            for content in contents:
                d = {
                    'id_match_values': content.id_match_values,
                    'id_mapping': content.id_mapping,
                    'source_value': content.source_value,
                    'id_target_value': content.id_target_value
                    }
                mapping_contents.append(d)
            for key, group in itertools.groupby(mapping_contents, key=lambda x:x['id_target_value']):
                gb_mapping_contents.append(list(group))
        else:
            gb_mapping_contents.append('empty')

        logger.debug('mapping_contents = %s from id_mapping number %s', mapping_contents, id_mapping)


        
        return gb_mapping_contents, 200

    except Exception:
        raise
        return 'INTERNAL SERVER ERROR ("get_mapping_contents() error"): contactez l\'administrateur du site', 500


@blueprint.route('/mappingName', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def postMappingName(info_role):
    try:
        logger.info('Posting mapping field name')

        data = request.form.to_dict()

        if data['mappingName'] == '':
            return 'Vous devez donner un nom au mapping', 400

        # check if name already exists
        names_request = DB.session\
            .query(BibMappings)\
            .all()
        names = [name.mapping_label for name in names_request]

        if data['mappingName'] in names:
            return 'Ce nom de mapping existe déjà', 400

        # fill BibMapping
        new_name = BibMappings(
            mapping_label = data['mappingName'],
            mapping_type = data['mapping_type'],
            active = True
            )

        DB.session.add(new_name)
        DB.session.flush()

        # fill CorRoleMapping

        id_mapping = DB.session.query(BibMappings.id_mapping)\
            .filter(BibMappings.mapping_label == data['mappingName'])\
            .one()[0]

        new_map_role = CorRoleMapping(
            id_role = info_role.id_role,
            id_mapping = id_mapping
        )

        DB.session.add(new_map_role)
        DB.session.commit()

        logger.info('-> Mapping field name posted')

        return id_mapping, 200
    
    except Exception as e:
        logger.error('*** ERROR WHEN POSTING MAPPING FIELD NAME')
        logger.exception(e)
        DB.session.rollback()
        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR : Error when posting mapping field name',
            details=str(e))
    finally:
        DB.session.close()


@blueprint.route('/cancel_import/<import_id>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def cancel_import(info_role, import_id):
    try:

        if import_id == "undefined":
            server_response = {'deleted_id_import': "canceled"}
            return server_response, 200

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
            archives_full_name = get_full_table_name(
                blueprint.config['ARCHIVES_SCHEMA_NAME'], user_data_name)
            imports_table_name = set_imports_table_name(user_data_name)
            imports_full_name = get_full_table_name(
                'gn_imports', imports_table_name)

            # delete tables
            engine = DB.engine
            is_gn_imports_table_exist = engine.has_table(imports_table_name, schema=blueprint.config['IMPORTS_SCHEMA_NAME'])
            if is_gn_imports_table_exist:
                DB.session.execute("DROP TABLE {}".format(imports_full_name))

            DB.session.execute("DROP TABLE {}".format(archives_full_name))

        # delete metadata
        DB.session.query(TImports).filter(
            TImports.id_import == import_id).delete()
        DB.session.query(CorRoleImport).filter(
            CorRoleImport.id_import == import_id).delete()
        DB.session.query(CorImportArchives).filter(
            CorImportArchives.id_import == import_id).delete()

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
@checker('Total time to post user file and fill metadata')
def post_user_file(info_role):
    """
        - load user file in "upload" directory
        - check user file data with goodtables library (column names, row duplicates, ...)
        - copy user file raw data in 2 geonature db tables (in archives and imports schemas)
        - fill t_imports, cor_role_import, cor_archive_import
        - return id_import and column names list
        - delete user file in upload directory

        # Note perso:
        # prevoir un boolean (ou autre) pour bloquer la possibilité de l'utilisateur de lancer plusieurs uploads sans passer par interface front

    """

    try:

        logger.info('*** START UPLOAD USER FILE')

        ### INITIALIZE VARIABLES

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
        N_MAX_ROWS_CHECK = 100000000

        
        ### SAVES USER FILE IN UPLOAD DIRECTORY
        
        logger.info('* START SAVE USER FILE IN UPLOAD DIRECTORY')

        uploaded_file = upload(request, MAX_FILE_SIZE,
                               ALLOWED_EXTENSIONS, DIRECTORY_NAME, MODULE_URL)

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
            logger.error('Saving user file : %s', errors)
            return errors,400

        full_path = uploaded_file['full_path']

        is_file_saved = uploaded_file['is_uploaded']

        logger.info('* END SAVE USER FILE IN UPLOAD DIRECTORY')


        ### CHECKS USER FILE

        logger.info('* START CHECK USER FILE VALIDITY')

        report = check_user_file(full_path, N_MAX_ROWS_CHECK)

        # reports user file errors:
        if len(report['errors']) > 0:
            logger.error(report['errors'])
            return report['errors'],400

        logger.info('* END CHECK USER FILE VALIDITY')


        ### CREATES CURRENT IMPORT IN TIMPORTS (SET STEP TO 1 AND DATE/TIME TO CURRENT DATE/TIME)
        
        # get form data
        metadata = request.form.to_dict()

        # Check if id_dataset value is allowed (prevent from forbidden manual change in url (url/process/N))
        is_dataset_allowed = test_user_dataset(
            info_role.id_role, metadata['datasetId'])
        if not is_dataset_allowed:
            logger.error('Provided dataset is not allowed')
            return \
                'L\'utilisateur {} n\'est pas autorisé à importer des données vers l\'id_dataset {}'\
                .format(info_role.id_role, int(metadata['datasetId'])), 403

        # start t_imports filling and fill cor_role_import
        if metadata['importId'] == 'undefined':
            # if first file uploaded by the user in the current import process :
            init_date = datetime.datetime.now()
            insert_t_imports = TImports(
                date_create_import=init_date,
                date_update_import=init_date,
            )
            DB.session.add(insert_t_imports)
            DB.session.flush()
            id_import = insert_t_imports.id_import
            logger.debug('id_import = %s', id_import)
            logger.debug('id_role = %s', info_role.id_role)
            # fill cor_role_import
            insert_cor_role_import = CorRoleImport(
                id_role=info_role.id_role, 
                id_import=id_import)
            DB.session.add(insert_cor_role_import)
            DB.session.flush()
        else:
            # if other file(s) already uploaded by the user in the current import process :
            id_import = int(metadata['importId'])
            DB.session.query(TImports)\
                .filter(TImports.id_import == id_import)\
                .update({
                    TImports.date_update_import: datetime.datetime.now()
                })

        if isinstance(id_import, int):
            is_id_import = True

        # set step to 1 in t_imports table
        DB.session.query(TImports)\
            .filter(TImports.id_import == id_import)\
            .update({
                TImports.step: 1
            })

        
        # GET/SET FILE,SCHEMA,TABLE,COLUMN NAMES

        # file name treatment
        file_name_cleaner = clean_file_name(
            uploaded_file['file_name'], uploaded_file['extension'], id_import)
        if len(file_name_cleaner['errors']) > 0:
            # if user file name is not valid, an error is returned
            return file_name_cleaner['errors'],400

        # get/set table and column names
        separator = metadata['separator']
        table_names = get_table_names(ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME, file_name_cleaner['clean_name'])
        logger.debug('full DB user table name = %s', table_names['imports_full_table_name'])

        # pk name to include (because copy_from method requires a primary_key)
        pk_name = "".join([PREFIX, 'pk'])

        # clean column names
        columns = [clean_string(x) for x in report['column_names']]


        """
        CREATES TABLES CONTAINING RAW USER DATA IN GEONATURE DB
        """

        # if existing, delete all tables already uploaded which have the same id_import suffix
        delete_tables(id_import, ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME)
        DB.session.commit()

        ## create user table classes (1 for gn_import_archives schema, 1 for gn_imports schema) corresponding to the user file structure (csv)
        user_class_gn_import_archives = generate_user_table_class(\
            ARCHIVES_SCHEMA_NAME, table_names['archives_table_name'], pk_name, columns, id_import, schema_type='archives')
        user_class_gn_imports = generate_user_table_class(\
            IMPORTS_SCHEMA_NAME, table_names['imports_table_name'], pk_name, columns, id_import, schema_type='gn_imports')

        # create 2 empty tables in geonature db (in gn_import_archives and gn_imports schemas)
        engine = DB.engine
        user_class_gn_import_archives.__table__.create(bind=engine, checkfirst=True)
        user_class_gn_imports.__table__.create(bind=engine, checkfirst=True)

        # fill the user table (copy_from function is equivalent to COPY function of postgresql)
        conn = engine.raw_connection()
        cur = conn.cursor()

        logger.info('* START COPY FROM CSV TO ARCHIVES SCHEMA')
        load_csv_to_db(full_path, cur, table_names['archives_full_table_name'], separator, columns)
        logger.info('* END COPY FROM CSV TO ARCHIVES SCHEMA')

        logger.info('* START COPY FROM CSV TO IMPORTS SCHEMA')
        load_csv_to_db(full_path, cur, table_names['imports_full_table_name'], separator, columns)
        logger.info('* END COPY FROM CSV TO IMPORTS SCHEMA')

        conn.commit()
        conn.close()
        cur.close()

        """
        FILLS CorImportArchives AND UPDATES TImports
        """

        delete_import_CorImportArchives(id_import)
        cor_import_archives = CorImportArchives(id_import=id_import, table_archive=table_names['archives_table_name'])
        DB.session.add(cor_import_archives)

        # update gn_import.t_imports with cleaned file name and step = 2
        DB.session.query(TImports)\
            .filter(TImports.id_import == id_import)\
            .update({
                TImports.import_table: table_names['archives_table_name'],
                TImports.step: 2,
                TImports.id_dataset: int(metadata['datasetId']),
                TImports.srid: int(metadata['srid']),
                TImports.format_source_file: report['file_format'],
                TImports.source_count: report['row_count']-1
            })

        DB.session.commit()

        logger.info('*** END UPLOAD USER FILE')

        return {
            "importId": id_import,
            "columns": columns,
        }, 200

    except psycopg2.errors.BadCopyFileFormat as e:
        logger.exception(e)
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
        logger.error(errors)
        return errors,400

    except Exception as e:
        logger.error('*** ERROR WHEN POSTING FILE')
        logger.exception(e)
        DB.session.rollback()
        if is_id_import:
            delete_tables(id_import, ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME)
        DB.session.commit()
        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR : Erreur pendant l\'enregistrement du fichier - contacter l\'administrateur',
            details= str(e))
    finally:
        if is_file_saved:
            os.remove(full_path)
        DB.metadata.clear()
        DB.session.close()


@blueprint.route('/mapping/<import_id>/<id_mapping>', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def postMapping(info_role, import_id, id_mapping):
    try:
        is_temp_table_name = False
        is_table_names = False

        data = request.form.to_dict()

        srid = int(data['srid'])
        data.pop('srid')


        ### SAVE MAPPING ###

        logger.info('save field mapping')
        save_field_mapping(data, id_mapping)


        ### INITIALIZE VARIABLES

        logger.info('*** START CORRESPONDANCE MAPPING')

        errors = []

        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']
        ARCHIVES_SCHEMA_NAME = blueprint.config['ARCHIVES_SCHEMA_NAME']
        PREFIX = blueprint.config['PREFIX']
        MISSING_VALUES = blueprint.config['MISSING_VALUES']
        DEFAULT_COUNT_VALUE = blueprint.config['DEFAULT_COUNT_VALUE']
        MODULE_URL = blueprint.config["MODULE_URL"]
        DIRECTORY_NAME = blueprint.config["UPLOAD_DIRECTORY"]

        index_col = ''.join([PREFIX,'pk'])
        table_names = get_table_names(ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME, int(import_id))
        is_table_names = True
        temp_table_name = '_'.join(['temp', table_names['imports_table_name']])
        is_temp_table_name = True

        engine = DB.engine
        column_names = get_table_info(table_names['imports_table_name'], 'column_name')

        local_srid = get_local_srid()

        if data['unique_id_sinp_generate'] == 'true':
            is_generate_uuid = True
        else:
            is_generate_uuid = False

        if data['altitudes_generate'] == 'true':
            is_generate_alt = True
        else:
            is_generate_alt = False

        print('is generating altitudes : {}'.format(is_generate_alt))

        logger.debug('import_id = %s', import_id)
        logger.debug('DB tabel name = %s', table_names['imports_table_name'])

        # get synthese fields filled in the user form:
        selected_columns = {key:value for key, value in data.items() if value}
        selected_user_cols = [*list(selected_columns.values())]
        logger.debug('selected columns in correspondance mapping = %s', selected_columns)


        ### EXTRACT

        logger.info('* START EXTRACT FROM DB TABLE TO PYTHON')
        df = extract(table_names['imports_table_name'], IMPORTS_SCHEMA_NAME, column_names, index_col, import_id)
        logger.info('* END EXTRACT FROM DB TABLE TO PYTHON')
        

        # get cd_nom list
        cd_nom_list = get_cd_nom_list()


        ### TRANSFORM (data checking and cleaning)

        # set empty data cleaning user error report :
        dc_user_errors = []
        user_error = {}
        dc_errors = DB.session.execute("SELECT * FROM gn_imports.user_errors;").fetchall()
        for error in dc_errors:
            for field in selected_columns.values():
                dc_user_errors.append({
                    'id': error.id_error,
                    'type': error.error_type,
                    'name': error.name,
                    'description': error.description,
                    'column': field,
                    'n_errors': 0
                })

        print(df.map_partitions(len).compute())
        
        # start process (transform and load)
        for i in range(df.npartitions):

            logger.info('* START DATA CLEANING partition %s', i)

            partition = df.get_partition(i)
            partition_df = compute_df(partition)
            transform_errors = data_cleaning(partition_df, import_id, selected_columns, dc_user_errors, MISSING_VALUES, DEFAULT_COUNT_VALUE, cd_nom_list, srid, local_srid, is_generate_uuid)

            if len(transform_errors['user_errors']) > 0:
                for error in transform_errors['user_errors']:
                    logger.debug('USER ERRORS : %s', error['message'])
                    errors.append(error)

            added_cols = transform_errors['added_cols']

            if 'temp' in partition_df.columns:
                partition_df = partition_df.drop('temp', axis=1)
            if 'check_dates' in partition_df.columns:
                partition_df = partition_df.drop('check_dates', axis=1)

            logger.info('* END DATA CLEANING partition %s', i)

            partition_df = partition_df.drop('temp_longitude', axis=1)
            partition_df = partition_df.drop('temp_latitude', axis=1)
            partition_df = partition_df.drop('geometry', axis=1)


            ### LOAD (from Dask dataframe to postgresql table, with d6tstack pd_to_psql function)

            logger.info('* START LOAD PYTHON DATAFRAME TO DB TABLE partition %s', i)
            load(partition_df, i, table_names['imports_table_name'], IMPORTS_SCHEMA_NAME, 
                table_names['imports_full_table_name'], temp_table_name, import_id, engine, 
                index_col, MODULE_URL, DIRECTORY_NAME)
            logger.info('* END LOAD PYTHON DATAFRAME TO DB TABLE partition %s', i)
        

        # filters dc_user_errors to get error report:
        error_report = []
        for error in dc_user_errors:
            if error['n_errors'] > 0:
                error['n_errors'] = int(error['n_errors'])
                error_report.append(error)

        # delete original table
        delete_table(table_names['imports_full_table_name'])

        # rename temp table with original table name
        rename_table(IMPORTS_SCHEMA_NAME, temp_table_name, table_names['imports_table_name'])

        # set primary key
        set_primary_key(IMPORTS_SCHEMA_NAME, table_names['imports_table_name'], index_col)

        # alter primary key type into integer
        alter_column_type(IMPORTS_SCHEMA_NAME, table_names['imports_table_name'], index_col, 'integer')

        # calculate geometries and altitudes
        set_geometry(IMPORTS_SCHEMA_NAME, table_names['imports_table_name'], local_srid,
            added_cols['the_geom_4326'], added_cols['the_geom_point'], added_cols['the_geom_local'])
        set_altitudes(df, selected_columns, import_id, IMPORTS_SCHEMA_NAME, 
            table_names['imports_full_table_name'], table_names['imports_table_name'], 
            index_col, is_generate_alt)

        DB.session.commit()
        DB.session.close()

        # check if df is fully loaded in postgresql table :
        is_nrows_ok = check_row_number(import_id, table_names['imports_full_table_name'])
        if not is_nrows_ok:
            logger.error('missing rows because of loading server error')
            raise GeonatureImportApiError(
                message='INTERNAL SERVER ERROR ("postMapping() error"): \
                            lignes manquantes dans {} - refaire le matching'\
                    .format(table_names['imports_full_table_name']),
                details='')

        # calculate number of invalid lines
        n_invalid_rows = get_n_invalid_rows(table_names['imports_full_table_name'])
        
        # check total number of lines
        n_table_rows = get_row_number(table_names['imports_full_table_name'])

        logger.info('*** END CORRESPONDANCE MAPPING')

        DB.session.commit()
        DB.session.close()

        return {
            'user_error_details' : error_report,
            'n_user_errors' : n_invalid_rows,
            'n_table_rows' : n_table_rows,
            'import_id' : import_id,
            'id_mapping' : id_mapping,
            'selected_columns' : selected_columns,
            'added_columns': added_cols,
            'table_name' : table_names['imports_table_name']
        }

    except Exception as e:
        logger.error('*** ERROR IN CORRESPONDANCE MAPPING')
        logger.exception(e)
        DB.session.rollback()

        if is_temp_table_name:
            DB.session.execute("""
                DROP TABLE IF EXISTS {}.{};
                """.format(IMPORTS_SCHEMA_NAME, temp_table_name))
            DB.session.commit()
            DB.session.close()

        if is_table_names:
            n_loaded_rows = get_n_loaded_rows(table_names['imports_full_table_name'])

        if is_table_names:  
            if n_loaded_rows == 0:
                logger.error('Table %s vide à cause d\'une erreur de copie, refaire l\'upload et le mapping', table_names['imports_full_table_name'])
                raise GeonatureImportApiError(\
                    message='INTERNAL SERVER ERROR :: Erreur pendant le mapping de correspondance :: Table {} vide à cause d\'une erreur de copie, refaire l\'upload et le mapping, ou contactez l\'administrateur du site'.format(table_names['imports_full_table_name']),
                    details='')

        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR : Erreur pendant le mapping de correspondance - contacter l\'administrateur',
            details=str(e))


@blueprint.route('/postMetaToStep3', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def postMetaToStep3(info_role):

    try:

        data = request.form.to_dict()
        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']

        nomenc_info = get_nomenc_info(data, IMPORTS_SCHEMA_NAME)

        ### UPDATE TIMPORTS

        logger.info('update t_imports from step 2 to step 3')
        
        DB.session.query(TImports)\
            .filter(TImports.id_import==int(data['import_id']))\
            .update({
                TImports.step: 3,
                TImports.id_mapping: int(data['id_mapping'])
                })

        DB.session.commit()

        table_name = data['table_name']
        import_id = data['import_id']
        data.pop('table_name')
        data.pop('import_id')

        return {
            'table_name' : table_name,
            'import_id' : import_id,
            'selected_columns' : data,
            'content_mapping_info' : nomenc_info
        }
    except Exception as e:
        logger.error('*** ERROR IN STEP 2 NEXT BUTTON')
        logger.exception(e)
        DB.session.rollback()
        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR : Erreur pendant le passage vers l\'étape 3 - contacter l\'administrateur',
            details=str(e))
    finally:
        DB.session.close()


@blueprint.route('/bibFields', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def get_bib_fields(info_role):

    try:

        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']

        bibs = DB.session.execute("""
            SELECT *
            FROM {schema_name}.bib_fields fields
            JOIN {schema_name}.bib_themes themes on fields.id_theme = themes.id_theme
            WHERE fields.display = true;
            """.format(schema_name = IMPORTS_SCHEMA_NAME)
            ).fetchall()

        max_theme = DB.session.execute("""
            SELECT max(id_theme)
            FROM {schema_name}.bib_fields fields
            """.format(schema_name = IMPORTS_SCHEMA_NAME)
            ).fetchone()[0]

        data_theme = []
        for i in range(max_theme):
            data = []
            for row in bibs:
                if row.id_theme == i+1:
                    theme_name = row.fr_label_theme
                    d = {
                        'id_field': row.id_field,
                        'name_field': row.name_field,
                        'required' : row.mandatory,
                        'fr_label' : row.fr_label,
                        'autogenerated': row.autogenerated
                        }
                    data.append(d)
            data = sorted(data, key = lambda i: i['id_field'])
            data_theme.append({
                'theme_name' : theme_name,
                'fields': data
                })
            
        return data_theme, 200

    except Exception as e:
        logger.error('*** SERVER ERROR WHEN GETTING BIB_FIELDS AND BIB_THEMES')
        logger.exception(e)
        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR when getting bib_fields and bib_themes',
            details=str(e))


@blueprint.route('/contentMapping/<import_id>/<id_mapping>', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def content_mapping(info_role, import_id, id_mapping):
    try:

        logger.info('Content mapping : transforming user values to id_types in the user table')

        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']

        form_data = request.form.to_dict(flat=False)
        table_name = form_data['table_name'][0]
        selected_cols = ast.literal_eval(form_data['selected_cols'][0])

        form_data.pop('table_name')
        form_data.pop('selected_cols')

        """
        ### SAVE MAPPING ###

        logger.info('save content mapping')
        save_content_mapping(form_data, id_mapping)
        """

        ### CONTENT MAPPING ###
        
        selected_content = {key:value for key, value in form_data.items() if value != ['']}

        logger.info('Generating nomenclature ids from content mapping form values :')
        set_nomenclature_ids(IMPORTS_SCHEMA_NAME, table_name, selected_content, selected_cols)
        
        logger.info('Generating default nomenclature ids :')
        set_default_nomenclature_ids(IMPORTS_SCHEMA_NAME, table_name, selected_cols)

        logger.info('-> Content mapping : user values transformed to id_types in the user table')


        ### UPDATE TIMPORTS

        logger.info('update t_imports from step 3 to step 4')
        
        DB.session.query(TImports)\
            .filter(TImports.id_import==int(import_id))\
            .update({
                TImports.step: 4
               })

        DB.session.commit()

        logger.info('-> t_imports updated from step 3 to step 4')

        return 'content_mapping done'

    except Exception as e:
        DB.session.rollback()
        DB.session.close()
        logger.error('*** SERVER ERROR DURING CONTENT MAPPING (user values to id_types')
        logger.exception(e)
        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR during content mapping (user values to id_types',
            details=str(e))


@blueprint.route('/importData/<import_id>', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def import_data(info_role, import_id):
    try:

        logger.info('Importing data in gn_synthese.synthese table')

        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']

        # get table name
        table_name = set_imports_table_name(get_table_name(import_id))

        # set total user columns
        form_data = request.form.to_dict(flat=False)
        total_columns = ast.literal_eval(form_data['total_columns'][0])

        # check if id_source already exists in synthese table
        is_id_source = check_id_source(import_id)

        if is_id_source:
            return {'status':'failed : already imported'}
        
        # insert into t_sources
        insert_into_t_sources(IMPORTS_SCHEMA_NAME, table_name, import_id, total_columns)

        # insert into synthese
        load_data_to_synthese(IMPORTS_SCHEMA_NAME, table_name, total_columns, import_id)

        logger.info('-> Data imported in gn_synthese.synthese table')


        ### UPDATE TIMPORTS

        logger.info('update t_imports on final step')

        date_ext = get_date_ext(IMPORTS_SCHEMA_NAME, table_name, total_columns['date_min'], total_columns['date_max'])
        
        DB.session.query(TImports)\
            .filter(TImports.id_import==int(import_id))\
            .update({
                TImports.import_count: get_n_valid_rows(IMPORTS_SCHEMA_NAME, table_name),
                TImports.taxa_count: get_n_taxa(IMPORTS_SCHEMA_NAME, table_name, total_columns['cd_nom']),
                TImports.date_min_data: date_ext['date_min'],
                TImports.date_max_data: date_ext['date_max'],
                TImports.date_end_import: datetime.datetime.now(),
                TImports.step: 5
                })

        logger.info('-> t_imports updated on final step')

        DB.session.commit()

        return {
            'status' : 'imported successfully',
            'total_columns' : total_columns
        }

    except Exception as e:
        DB.session.rollback()
        logger.error('*** SERVER ERROR WHEN IMPORTING DATA IN GN_SYNTHESE.SYNTHESE')
        logger.exception(e)
        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR when importing data in gn_synthese.synthese',
            details=str(e))
    finally:
        DB.session.close()


@blueprint.route('/getValidData/<import_id>', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def get_valid_data(info_role, import_id):
    try:
        logger.info('Get valid data for preview')

        IMPORTS_SCHEMA_NAME = blueprint.config['IMPORTS_SCHEMA_NAME']

        # get table name
        table_name = set_imports_table_name(get_table_name(import_id))

        # set total user columns
        form_data = request.form.to_dict(flat=False)
        selected_cols = ast.literal_eval(form_data['selected_columns'][0])
        added_cols = ast.literal_eval(form_data['added_columns'][0])
        total_columns = set_total_columns(selected_cols, added_cols, import_id)

        # get valid data preview
        valid_data_list = get_preview(IMPORTS_SCHEMA_NAME, table_name, total_columns)

        logger.info('-> got valid data for preview')

        return {
            'total_columns' : total_columns,
            'valid_data': valid_data_list
        }

    except Exception as e:
        logger.error('*** SERVER ERROR WHEN GETTING VALID DATA')
        logger.exception(e)
        raise GeonatureImportApiError(\
            message='INTERNAL SERVER ERROR when getting valid data',
            details=str(e))
