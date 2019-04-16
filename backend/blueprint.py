from flask import (
    Blueprint,
    current_app,
    request
    )

import os

from sqlalchemy import func

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

blueprint = Blueprint('import', __name__)


@blueprint.route('', methods=['GET'])
@permissions.check_cruved_scope('R', True, module_code="IMPORT")
@json_resp
def get_import_list(info_role):

    """
        return import list
    """
    print(info_role)

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
    
        return 'csv file loaded in geonature db',200
    except Exception:
        return 'INTERNAL SERVER ERROR ("get_import_list() error"): contactez l\'administrateur du site',500