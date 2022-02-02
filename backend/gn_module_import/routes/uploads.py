import os
from datetime import datetime
import codecs

from flask import request, jsonify, current_app, g
from werkzeug.exceptions import BadRequest, NotFound, Forbidden
from sqlalchemy.orm.exc import NoResultFound

from geonature.core.gn_permissions import decorators as permissions
from geonature.utils.env import DB as db

from pypnusershub.db.models import User

from ..db.models import TImports, TDatasets, ImportUserError

from gn_module_import.steps import Step
from gn_module_import.blueprint import blueprint
from gn_module_import.utils.imports import load_data, get_clean_column_name, \
                                           create_tables, delete_tables, \
                                           detect_encoding



@blueprint.route("/imports/upload", methods=["POST"])
@blueprint.route("/imports/<int:import_id>/upload", methods=["PUT"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def upload_file(scope, import_id=None):
    """
    .. :quickref: Import; Add an import or update an existing import.

    Add an import or update an existing import.

    :form file: file to import
    :form int datasetId: dataset ID to which import data
    """
    author = g.current_user
    if import_id:
        imprt = TImports.query.get_or_404(import_id)
    else:
        imprt = None
    f = request.files['file']
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0)
    # value in config file is in Mo
    max_file_size = current_app.config['IMPORT']['MAX_FILE_SIZE'] * 1024 * 1024
    if size > max_file_size:
        raise BadRequest(description=f"File too big ({size} > {max_file_size}).")  # FIXME better error signaling?
    detected_encoding = detect_encoding(f)
    if imprt:
        imprt.source_file = f.read()
        imprt.full_file_name = f.filename
        imprt.detected_encoding = detected_encoding
        db.session.commit()
    else:
        try:
            dataset_id = int(request.form['datasetId'])
        except ValueError as e:
            raise BadRequest(description="'datasetId' must be an integer.")
        datasets = TDatasets.query.filter_by_scope(scope)
        try:
            dataset = datasets.filter(TDatasets.id_dataset==dataset_id).one()
        except NoResultFound:
            raise Forbidden(description='Vous n’avez pas les permissions sur ce jeu de données.')
        now = datetime.now()
        imprt = TImports(source_file=f.read(), full_file_name=f.filename,
                         detected_encoding=detected_encoding, dataset=dataset,
                         date_create_import=now, date_update_import=now)
        imprt.authors.append(author)
        db.session.add(imprt)
        db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/decode", methods=["POST"])
@permissions.check_cruved_scope("C", module_code="IMPORT", object_code="IMPORT")
def decode_file(import_id):
    imprt = TImports.query.get_or_404(import_id)
    if imprt.source_file is None:
        raise BadRequest(description='A file must be first uploaded.')
    if 'encoding' not in request.json:
        raise BadRequest(description='Missing encoding.')
    encoding = request.json['encoding']
    try:
        codecs.lookup(encoding)
    except LookupError:
        raise BadRequest(description='Unknown encoding.')
    imprt.encoding = encoding
    if 'format' not in request.json:
        raise BadRequest(description='Missing format.')
    if request.json['format'] not in TImports.AVAILABLE_FORMATS:
        raise BadRequest(description='Unknown format.')
    imprt.format_source_file = request.json['format']
    if 'srid' not in request.json:
        raise BadRequest(description='Missing srid.')
    try:
        imprt.srid = int(request.json['srid'])
    except ValueError:
        raise BadRequest(description='SRID must be an integer.')
    imprt.date_update_import = datetime.now()
    db.session.commit()  # commit parameters
    ImportUserError.query.filter_by(imprt=imprt).delete()  # clear all errors
    report = load_data(imprt.id_import, imprt.source_file,
                       encoding=imprt.encoding, fmt=imprt.format_source_file)
    db.session.commit()  # commit errors
    if report['errors']:
        # FIXME
        error = report['errors'][0]
        raise BadRequest(description=error['message'])
    else:
        if imprt.import_table:
            delete_tables(imprt)
            db.session.commit()
        # FIXME handle duplicate column name
        imprt.columns = { get_clean_column_name(col): col for col in report['column_names'] }
        create_tables(imprt)
        imprt.source_count = report['row_count'] - 1
        db.session.commit()
        return jsonify(imprt.as_dict())
