from flask import (
    Blueprint,
    current_app,
    request
    )

import os

import ast

import datetime

from sqlalchemy import func, text, select

from geonature.utils.utilssqlalchemy import json_resp

import pdb

from geonature.core.gn_meta.models import TDatasets

from geonature.core.gn_synthese.models import (
    Synthese,
    TSources
    )

from geonature.core.gn_commons.models import BibTablesLocation

from geonature.utils.env import DB

from geonature.core.gn_permissions import decorators as permissions

from pypnnomenclature.models import TNomenclatures

from .models import (
    TImports,
    CorRoleImport
)

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
                "empty":True
            },200
        for r in results:
            prop = {
                "id_import":r.id_import,
                "format_source_file":r.format_source_file,
                "srid":r.srid,
                "import_table":r.import_table,
                "id_dataset":r.id_dataset,
                "id_mapping":r.id_mapping,
                "date_create_import":str(r.date_create_import),
                "date_update_import":str(r.date_update_import),
                "date_end_import":str(r.date_end_import),
                "source_count":r.source_count,
                "import_count":r.import_count,
                "taxa_count":r.taxa_count,
                "date_min_data":str(r.date_min_data),
                "date_max_data":str(r.date_max_data)
            }
            history.append(prop)
        return {
            "empty":False,
            "history":history,
        },200
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_import_list() error"): contactez l\'administrateur du site',500



@blueprint.route('/datasets', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def get_user_datasets(info_role):

    """
        load user datasets
    """

    try:
        datasets = []
        results = DB.session.query(func.gn_imports.get_datasets(info_role.id_role)).all()
        DB.session.close()
        if results:
            for data in results:
                d = {}
                d['datasetId'] = int(data[0].replace('(', '').replace(')', '').split(',')[0])
                d['datasetName'] = data[0].replace('(', '').replace(')', '').split(',')[1].replace('"','')
                datasets.append(d)
            return datasets,200
        else:
            return 'Attention, vous n\'avez aucun jeu de données déclaré',400
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_user_datasets() error"): contactez l\'administrateur du site',500



@blueprint.route('/init', methods=['POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def post_dataset(info_role):

    #start to create data related to the current user import in the geonature db

    try:
        # get dataset_id from form post
        data = dict(request.get_json())
        selected_dataset_id = int(data['dataset'])
        #data['date_create_import'] = init_date
        #data['date_update_import'] = init_date
        return {'id_dataset' : selected_dataset_id},200
    except Exception:
        return 'INTERNAL SERVER ERROR ("post_dataset() error"): contactez l\'administrateur du site',500

@blueprint.route('/ImportDelete/<import_id>', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def delete_ImportDelete(info_role,import_id):

    try:
        if import_id == "undefined":
            return 'Aucun import en cours (id_import = null)',400
        DB.session.query(TImports).filter(TImports.id_import == import_id).delete()
        DB.session.query(CorRoleImport).filter(CorRoleImport.id_import == import_id).delete()
        DB.session.commit()
        DB.session.close()
        serverResponse = {}
        serverResponse['deleted_id_import'] = import_id
        return serverResponse,200
    except Exception:
        return 'INTERNAL SERVER ERROR ("delete_TImportRow() error"): contactez l\'administrateur du site',500


@blueprint.route('/uploads', methods=['GET','POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def post_user_file(info_role):

    """
        load user file in "uploads" directory
    """

    try:
        metadata = dict(request.form)
        print(metadata)

        file = request.files['File']
        pdb.set_trace()
        # prevoir un boolean (ou autre) pour bloquer la possibilité de l'utilisateur de lancer plusieurs uploads
        # lire flask upload
        # ajouter un blocage si pas de fichier choisi par l'utilisateur
        # remplacer 'import' par blueprint.config.module_url à la ligne suivante ?
        module_directory_path = os.path.join(os.path.dirname(os.getcwd()),'external_modules/import')
        # mettre upload_directory_path en config dans la ligne suivante ?
        upload_directory_path = "upload"
        uploadsDirectory = os.path.join(module_directory_path,upload_directory_path)
        file.save(os.path.join(uploadsDirectory, file.filename))

        # fill db :

        # initiate t_imports with id_import, dataset_id, date_create_import and date_update_import
        init_date = datetime.datetime.now()
        insert_t_imports = TImports(
            date_create_import=init_date,
            date_update_import=init_date,
            id_dataset=metadata['datasetId']
            )
        DB.session.add(insert_t_imports)
        DB.session.commit()

        # cor_role_import filling
        id_import = DB.session.query(TImports.id_import).filter(TImports.date_create_import == init_date).one()[0]
        insert_cor_role_import = CorRoleImport(id_role=info_role.id_role,id_import=id_import)
        DB.session.add(insert_cor_role_import)
        DB.session.commit()
        DB.session.close()

        return {
            "importId" : id_import
        }, 200
    except Exception:
        return 'INTERNAL SERVER ERROR ("post_user_file() error"): contactez l\'administrateur du site',500


@blueprint.route('/load', methods=['GET'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def load_csv(info_role):

    """
        load user csv file into geonature db as archive table
    """

    try:
        # penser à un moyen de mettre un nom unique 
        loading = DB.session.query(func.gn_imports.load_csv_file(gn_imports,'t_essai'));
        DB.session.execute(loading)
        DB.session.close()

        return 'csv file loaded in geonature db',200
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_import_list() error"): contactez l\'administrateur du site',500


