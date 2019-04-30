from flask import (
    Blueprint,
    current_app,
    request
    )

import os

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
        return 'hello I am the import list response',200
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_import_list() error"): contactez l\'administrateur du site',500


@blueprint.route('/uploads', methods=['GET','POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def post_user_file(info_role):

    """
        load user file in "uploads" directory
    """

    try:
        file = request.files['File']
        # prevoir un boolean (ou autre) pour bloquer la possibilité de l'utilisateur de lancer plusieurs uploads
        # lire flask upload
        # ajouter un blocage si pas de fichier choisi par l'utilisateur
        # remplacer 'import' par blueprint.config.module_url à la ligne suivante ?
        module_directory_path = os.path.join(os.path.dirname(os.getcwd()),'external_modules/import')
        # mettre upload_directory_path en config dans la ligne suivante ?
        upload_directory_path = "upload"
        uploadsDirectory = os.path.join(module_directory_path,upload_directory_path)
        file.save(os.path.join(uploadsDirectory, file.filename))
        return "hello I am the user file response", 200
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
        #DB.session.close()
        if results:
            for data in results:
                datasets.append(data[0])
            return datasets,200
        else:
            return 'Attention, vous n\'avez aucun jeu de données déclaré',400
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_user_datasets() error"): contactez l\'administrateur du site',500


@blueprint.route('/dataset', methods=['GET','POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def post_dataset(info_role):

    """
        user dataset
    """

    try:
        dataset = dict(request.get_json())
        print(dataset)
        return 'dataset posted',200
    except Exception:
        return 'INTERNAL SERVER ERROR ("post_dataset() error"): contactez l\'administrateur du site',500


@blueprint.route('/init', methods=['GET','POST'])
@permissions.check_cruved_scope('C', True, module_code="IMPORT")
@json_resp
def init_process(info_role):

    """
        start to create data related to the current user import in the geonature db
    """
    try:
        # initiate t_imports with id_import, date_create_import and date_update_import
        init_date = datetime.datetime.now()
        insert_t_imports = TImports(date_create_import=init_date,date_update_import=init_date)
        DB.session.add(insert_t_imports)
        DB.session.commit()

        # fill cor_role_import
        id_import = DB.session.query(TImports.id_import).filter(TImports.date_create_import == init_date).one()[0]
        insert_cor_role_import = CorRoleImport(id_role=info_role.id_role,id_import=id_import)
        DB.session.add(insert_cor_role_import)
        DB.session.commit()

        DB.session.close()

        return 'db initialized with the new import',200
    except Exception:
        return 'INTERNAL SERVER ERROR ("init_process() error"): contactez l\'administrateur du site',500