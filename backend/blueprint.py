from flask import (
    Blueprint,
    current_app,
    request
)

from werkzeug.utils import secure_filename

import os
import pathlib
import psycopg2

from goodtables import validate

import ast

import datetime

from sqlalchemy import func, text, select, update,create_engine
from sqlalchemy.sql.elements import quoted_name

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
    CorImportArchives,
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
        results = DB.session.query(TImports).filter(TImports.step >= 2).all()
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
        - copy user file raw data in 2 geonature db tables
        - fill t_imports, cor_role_import, cor_archive_import
        - return id_import and column names list
    """

    try:

        is_archives_table_exist = False
        is_timports_table_exist = False


        """
        SAVE USER FILE IN UPLOAD DIRECTORY
        """

        # prevoir un boolean (ou autre) pour bloquer la possibilité de l'utilisateur de lancer plusieurs uploads

        is_file_saved = False

        MAX_FILE_SIZE = 200 #MB # mettre en conf

        ALLOWED_EXTENSIONS = ['.csv', '.json'] # mettre en conf

        if request.method == 'POST':

            if 'File' not in request.files:
                return {
                    "errorMessage": '',
                    "errorContext": '',
                    "errorInterpretation": 'Aucun fichier n\'est détecté'
                },400
            
            file = request.files['File']

            if file.filename == '':
                return {
                    "errorMessage": '',
                    "errorContext": '',
                    "errorInterpretation": 'Aucun fichier envoyé'
                },400

            # get file path
            upload_directory_path = blueprint.config["UPLOAD_DIRECTORY"]
            module_directory_path = os.path.join(os.path.dirname(os.getcwd()), 'external_modules/{}'.format(blueprint.config["MODULE_URL"]))
            uploads_directory = os.path.join(module_directory_path, upload_directory_path)

            filename = secure_filename(file.filename)

            if len(filename) > 100:
                return {
                    "errorMessage": '',
                    "errorContext": '',
                    "errorInterpretation": 'Le nom du fichier doit être inférieur à 100 caractères'
                },400

            full_path = os.path.join(uploads_directory, filename)

            # check user file extension (changer)
            extension = pathlib.Path(full_path).suffix.lower()
            if extension not in ALLOWED_EXTENSIONS:
                return {
                    "errorMessage": '',
                    "errorContext": '',
                    "errorInterpretation": 'Le fichier uploadé doit avoir une extension en .csv ou .json'
                },400

            # check file size
            print(file)
            file.seek(0, 2)
            size = file.tell() / (1024 * 1024)
            file.seek(0)
            if size > MAX_FILE_SIZE:
                return {
                    "errorMessage": '',
                    "errorContext": '',
                    "errorInterpretation": 'La taille du fichier doit être < à {} MB, votre fichier a une taille de {} MB'\
                        .format(MAX_FILE_SIZE,size)
                },400

            # save user file in upload directory
            file.save(full_path)

            if not os.path.isfile(full_path):
                return {
                    "errorMessage": '',
                    "errorContext": '',
                    "errorInterpretation": 'Le fichier n\'a pas pu être uploadé pour une raison inconnue'
                },400

            is_file_saved = True


        """
        CHECK USER FILE
        """

        """
        Vérifications :
        - doublon de nom colonne
        - aucun nom de colonne
        - un nom de colonne manquant
        - fichier vide (ni nom de colonne, ni ligne)
        - pas de données (noms de colonne mais pas de ligne contenant les données)
        - doublon ligne
        - extra-value : une ligne a une valeur en trop
        - less-value : une ligne a moins de colonnes que de noms de colonnes

        Notes encodages :
        - encodage : utf8 et 16 : pas d'erreur
        - encodage : Europe occidentale ISO-8859-15/EURO (=latin-9) et ISO-8859-1 (=latin-1) : erreur ('source-error')
        - Donc il faut convertir en utf-8 avant de passer dans goodtables

        Améliorations à faire :
        - ajouter message plus précis? par exemple pour dire numero de lignes en doublon? numero de ligne avec extra value, ...
        """

        print(full_path)

        report = validate(full_path, row_limit=100000000)

        print(report)

        if report['valid'] is False:

            errors = []
            for error in report['tables'][0]['errors']:
                errors.append(error['code'])

            error_list = list(dict.fromkeys(errors))
            return {
                "errorMessage": error_list,
                "errorContext": 'Error detected in check csv file with goodtables',
                "errorInterpretation": 'Error in csv file'
            },400

        if report['tables'][0]['row-count'] == 0:
            return {
                "errorMessage": 'empty file',
                "errorContext": 'Error detected in check csv file with goodtables',
                "errorInterpretation": 'Error in csv file'
            },400

        # get column names:
        file_columns = report['tables'][0]['headers']
        # get file format:
        file_format = report['tables'][0]['format']
        # get row number:
        source_count = report['tables'][0]['row-count']


        """
        START FILLING METADATA (gn_imports.t_imports and cor_role_import tables)
        """
        
        # get form data
        metadata = dict(request.form)
        #print(metadata)

        # Check if id_dataset value is allowed (prevent forbidden manual change in url (process/n))
        results = DB.session.query(TDatasets)\
            .filter(TDatasets.id_dataset == Synthese.id_dataset)\
            .filter(CorObserverSynthese.id_synthese == Synthese.id_synthese)\
            .filter(CorObserverSynthese.id_role == info_role.id_role)\
            .distinct(Synthese.id_dataset)\
            .all()

        dataset_ids = []

        for r in results:
            dataset_ids.append(r.id_dataset)

        if int(metadata['datasetId']) not in dataset_ids:
            return {
                "errorMessage": '',
                "errorContext": '',
                "errorInterpretation": 'L`utilisateur {} n\'est pas autorisé à importer des données vers l\'id_dataset {}'
                    .format(info_role.id_role, int(metadata['datasetId']))
            },400

        # start t_imports filling
        if metadata['importId'] == 'undefined':
            init_date = datetime.datetime.now()
            insert_t_imports = TImports(
                date_create_import = init_date,
                date_update_import = init_date,
            )
            DB.session.add(insert_t_imports)
            id_import = DB.session.query(TImports.id_import)\
                        .filter(TImports.date_create_import == init_date)\
                        .one()[0]
        else:
            id_import = int(metadata['importId'])
            DB.session.query(CorImportArchives)\
                .filter(CorImportArchives.id_import == id_import)\
                .delete()
            DB.session.query(CorRoleImport)\
                .filter(CorRoleImport.id_import == id_import)\
                .delete()

        DB.session.query(TImports)\
            .filter(TImports.id_import == id_import)\
            .update({TImports.step:1})

        # file name treatment (clean string for special character, remove extension, add id_import (_n) suffix)
        cleaned_file_name = clean_file_name(file.filename, extension, id_import)


        """
        CREATE TABLES CONTAINING RAW USER DATA IN GEONATURE DB
        """

        ## definition of variables :

        archives_schema_name = blueprint.config['ARCHIVES_SCHEMA_NAME']
        #import_schema_name = blueprint.config['IMPORT_SCHEMA_NAME']
        SEPARATOR = ";" # metadata['separator']
        prefix = blueprint.config['PREFIX']

        # table names
        archives_table_name = cleaned_file_name
        t_imports_table_name = ''.join(['i_', cleaned_file_name])

        # schema.table names
        archive_schema_table_name = '.'.join([archives_schema_name, archives_table_name])
        gn_imports_schema_table_name = '.'.join(['gn_imports', t_imports_table_name]) # remplacer 'gn_imports' par import_schema_name

        # pk name to include (because copy_from method requires a primary_key)
        pk_name = "".join([prefix, 'pk'])

        # clean column names
        columns = [clean_string(x) for x in file_columns]

        ## create user table classes (1 for gn_import_archives schema, 1 for gn_imports schema) corresponding to the user file structure (csv)
        user_class_gn_import_archives = generate_user_table_class(archives_schema_name, archives_table_name,\
                                                                      pk_name, columns, id_import, schema_type='archives')
        user_class_gn_imports = generate_user_table_class('gn_imports', t_imports_table_name,\
                                                                      pk_name, columns, id_import, schema_type='gn_imports')

        ## create 2 empty tables in geonature db (in gn_import_archives and gn_imports schemas)
    
        engine = DB.engine
        is_archive_table_exist = engine.has_table(archives_table_name, schema=archives_schema_name)
        is_gn_imports_table_exist = engine.has_table(t_imports_table_name, schema='gn_imports')

        print(is_archive_table_exist)
        
        if is_archive_table_exist:
            DB.session.execute("DROP TABLE {}".format(quoted_name(archive_schema_table_name, False)))


        if is_gn_imports_table_exist:
            DB.session.execute("DROP TABLE {}".format(quoted_name(gn_imports_schema_table_name, False)))

        DB.session.commit()

        user_class_gn_import_archives.__table__.create(bind=engine, checkfirst=True)
        user_class_gn_imports.__table__.create(bind=engine, checkfirst=True)

        ## fill the user table (copy_from function is equivalent to COPY function of postgresql)
        conn = engine.raw_connection()

        cur = conn.cursor()
        with open(full_path, 'r') as f:
            next(f)
            cur.copy_from(f, archive_schema_table_name, sep=SEPARATOR, columns=columns)

        with open(full_path, 'r') as f:
            next(f)
            cur.copy_from(f, gn_imports_schema_table_name, sep=SEPARATOR, columns=columns)

        conn.commit()
        conn.close()
        cur.close()

        """
        FILL cor_role_import and cor_import_archives
        """

        # fill cor_role_import
        insert_cor_role_import = CorRoleImport(id_role=info_role.id_role, id_import=id_import)
        DB.session.add(insert_cor_role_import)

        # fill cor_import_archives
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
                TImports.format_source_file: file_format,
                TImports.source_count: source_count-1
                })
        
        DB.session.commit()

        return {
                   "importId": id_import,
                   "columns": columns
               }, 200
               
    except psycopg2.errors.BadCopyFileFormat as e:
        return {
            "errorMessage": e.diag.message_primary,
            "errorContext": e.diag.context,
            "errorInterpretation": 'erreur probablement due à un problème de separateur'
        },400

    except Exception:
        DB.session.rollback()

        if is_archives_table_exist:
            DB.session.execute("DROP TABLE {}".format(quoted_name(archive_schema_table_name, False)))
        if is_timports_table_exist:
            DB.session.execute("DROP TABLE {}".format(quoted_name(gn_imports_schema_table_name, False)))
        DB.session.commit()
        return 'INTERNAL SERVER ERROR ("post_user_file() error"): contactez l\'administrateur du site', 500

    finally:
        if is_file_saved:
            os.remove(full_path)
        DB.metadata.clear()
        DB.session.close()
