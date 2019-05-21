from flask import (
    Blueprint,
    current_app,
    request
)

import os
import pathlib

import ast

import datetime

from sqlalchemy import func, text, select

from geonature.utils.utilssqlalchemy import json_resp

from geonature.utils.env import DB

import pdb

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
    generate_user_table_class
)

from .utils import (
    clean_string,
    clean_file_name
)

# ameliorer prise en charge des erreurs (en particulier depuis utils.py vers les routes)

blueprint = Blueprint('import', __name__)


@blueprint.route('', methods=['GET'])
@permissions.check_cruved_scope('R', True, module_code="IMPORT")
@json_resp
def get_import_list(info_role):
    """
        return import list
    """
    try:
        results = DB.session.query(TImports).filter(TImports.import_table != None).all()
        history = []
        if not results:
            return {
                       "empty": True
                   }, 200
        for r in results:
            prop = {
                "id_import": r.id_import,
                "format_source_file": r.format_source_file,
                "srid": r.srid,
                "import_table": r.import_table,
                "id_dataset": r.id_dataset,
                "id_mapping": r.id_mapping,
                "date_create_import": str(r.date_create_import),
                "date_update_import": str(r.date_update_import),
                "date_end_import": str(r.date_end_import),
                "source_count": r.source_count,
                "import_count": r.import_count,
                "taxa_count": r.taxa_count,
                "date_min_data": str(r.date_min_data),
                "date_max_data": str(r.date_max_data),
                "step": r.step
            }
            history.append(prop)
        return {
                   "empty": False,
                   "history": history,
               }, 200
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_import_list() error"): contactez l\'administrateur du site', 500


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


"""
@blueprint.route('/init', methods=['POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def post_dataset(info_role):
    # start to create data related to the current user import in the geonature db

    try:
        # get dataset_id from form post
        data = dict(request.get_json())
        selected_dataset_id = int(data['dataset'])
        # data['date_create_import'] = init_date
        # data['date_update_import'] = init_date
        return {'id_dataset': selected_dataset_id}, 200
    except Exception:
        return 'INTERNAL SERVER ERROR ("post_dataset() error"): contactez l\'administrateur du site', 500
"""


@blueprint.route('/ImportDelete/<import_id>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def delete_import(info_role, import_id):
    try:
        if import_id == "undefined":
            return 'Aucun import en cours (id_import = null)', 400
        DB.session.query(TImports).filter(TImports.id_import == import_id).delete()
        DB.session.query(CorRoleImport).filter(CorRoleImport.id_import == import_id).delete()
        DB.session.commit()
        DB.session.close()
        server_response = {'deleted_id_import': import_id}
        return server_response, 200
    except Exception:
        return 'INTERNAL SERVER ERROR ("delete_TImportRow() error"): contactez l\'administrateur du site', 500


@blueprint.route('/uploads', methods=['GET', 'POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def post_user_file(info_role):
    """
        - load user file in "uploads" directory
        - copy user file data in two tables of geonature db containing raw data
        - fill t_imports, cor_role_import
        - return t_imports.id_import
    """

    try:
        # get form data
        metadata = dict(request.form)
        file = request.files['File']

        ### SAVE USER FILE IN UPLOAD DIRECTORY ###

        # prevoir un boolean (ou autre) pour bloquer la possibilité de l'utilisateur de lancer plusieurs uploads
        # relire flask upload
        # ajouter un blocage si pas de fichier choisi par l'utilisateur

        # mettre upload_directory_path en config?
        upload_directory_path = "upload"

        # get full userfile path :
        # remplacer 'import' par blueprint.config.module_url à la ligne suivante ?
        module_directory_path = os.path.join(os.path.dirname(os.getcwd()), 'external_modules/import')
        uploads_directory = os.path.join(module_directory_path, upload_directory_path)
        full_path = os.path.join(uploads_directory, file.filename)

        # check user file extension (changer)
        extension = pathlib.Path(full_path).suffix.lower()
        if extension not in ['.csv', '.json']:
            return "Le fichier uploadé doit avoir une extension en .csv ou .json", 400

        # save user file in upload directory
        file.save(full_path)

        #################################################

        ### CHECK CSV ###

        # use goodtables

        #################################################

        ### START FILLING METADATA (TIMPORTS AND CORROLEIMPORT TABLES) ###

        print(metadata)

        # Check if id_dataset value is allowed (prevent forbidden manual change in url (process/n))
        results = DB.session.query(TDatasets).\
            filter(TDatasets.id_dataset == Synthese.id_dataset).\
            filter(CorObserverSynthese.id_synthese == Synthese.id_synthese).\
            filter(CorObserverSynthese.id_role == info_role.id_role).\
            distinct(Synthese.id_dataset).all()

        dataset_ids = []

        for r in results:
            dataset_ids.append(r.id_dataset)

        if int(metadata['datasetId']) not in dataset_ids:
            return 'L`utilisateur {} n\'est pas autorisé à importer des données vers l\'id_dataset {}'.format(
                info_role.id_role, int(metadata['datasetId'])), 400

        # start t_imports filling
        init_date = datetime.datetime.now()
        insert_t_imports = TImports(
            date_create_import=init_date,
            date_update_import=init_date,
            id_dataset=int(metadata['datasetId']),
            srid=int(metadata['srid']),
            step=1,  # mettre 2 à la fin du processus
            format_source_file=str(extension)  # a remplacer par resultat de goodtable
        )
        DB.session.add(insert_t_imports)
        # DB.session.commit()

        # get id_import
        id_import = DB.session.query(TImports.id_import).filter(TImports.date_create_import == init_date).one()[0]

        # file name treatment (clean string for special character, remove extension, add id_import (_n) suffix)
        cleaned_file_name = clean_file_name(file.filename, extension, id_import)

        ###### CREATE TABLES CONTAINING RAW USER DATA IN GEONATURE DB ######

        ## definition of variables :

        ARCHIVES_SCHEMA_NAME = 'gn_import_archives'  # put in parameters
        SEPARATOR = ";"  # put in parameters
        PREFIX = "gn_"  # put in parameters

        # table names
        archives_table_name = cleaned_file_name
        t_imports_table_name = ''.join(['i_', cleaned_file_name])

        # schema.table names
        archive_schema_table_name = '.'.join([ARCHIVES_SCHEMA_NAME, archives_table_name])
        gn_imports_schema_table_name = '.'.join(['gn_imports', t_imports_table_name])

        # pk name to include (because copy_from method requires a primary_key)
        pk_name = "".join([PREFIX, 'pk'])

        ## get column names of the user file (csv) :

        # get list of column names
        with open(full_path, 'r') as f:
            columns = next(f).split(SEPARATOR)
        # clean column names
        columns = [clean_string(x) for x in columns]

        ## create user table classes (1 for gn_import_archives schema, 1 for gn_imports schema) corresponding to the structure of the user file (csv)
        user_class_gn_import_archives = generate_user_table_class(ARCHIVES_SCHEMA_NAME, archives_table_name,
                                                                      pk_name, columns, schema_type='archives')
        user_class_gn_imports = generate_user_table_class('gn_imports', t_imports_table_name, pk_name, columns,
                                                              schema_type='gn_imports')

        ## create 2 empty tables in geonature db (in gn_import_archives and gn_imports schemas)
        engine = DB.engine
        user_class_gn_import_archives.__table__.create(bind=engine, checkfirst=True)
        user_class_gn_imports.__table__.create(bind=engine, checkfirst=True)

        ## fill the user table (copy_from function is equivalent to COPY function of postgresql)
        conn = engine.raw_connection()

        cur = conn.cursor()
        with open(full_path, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, archive_schema_table_name, sep=SEPARATOR, columns=columns)

        with open(full_path, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, gn_imports_schema_table_name, sep=SEPARATOR, columns=columns)

        conn.commit()

        ###### FILL cor_role_import ######

        insert_cor_role_import = CorRoleImport(id_role=info_role.id_role, id_import=id_import)
        DB.session.add(insert_cor_role_import)

        DB.session.commit()

        # essayer voir si donnees inserees si erreur dans le try a la fin
        # suppression du fichier dans uploads
        # gerer : - push 2x le meme nom ; - quand step2 à step1 ça recréé un import id ce qui fait que le nom n'est pas en double, à corriger

        return {
                   "importId": id_import
               }, 200

    except Exception:
        DB.session.rollback()
        # raise
        return 'INTERNAL SERVER ERROR ("post_user_file() error"): contactez l\'administrateur du site', 500
    finally:
        DB.session.close()
