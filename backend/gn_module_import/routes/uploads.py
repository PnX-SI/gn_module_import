import os
from datetime import datetime
import codecs
from io import StringIO
import csv

from flask import request, jsonify, current_app, g
from werkzeug.exceptions import BadRequest, Forbidden

from geonature.core.gn_permissions import decorators as permissions
from geonature.core.gn_meta.models import TDatasets
from geonature.utils.env import db

from gn_module_import.models import TImports
from gn_module_import.blueprint import blueprint
from gn_module_import.utils import (
    detect_encoding,
    insert_import_data_in_database,
)


@blueprint.route("/imports/upload", defaults={'import_id': None}, methods=["POST"])
@blueprint.route("/imports/<int:import_id>/upload", methods=["PUT"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def upload_file(scope, import_id):
    """
    .. :quickref: Import; Add an import or update an existing import.

    Add an import or update an existing import.

    :form file: file to import
    :form int datasetId: dataset ID to which import data
    """
    author = g.current_user
    if import_id:
        imprt = TImports.query.get_or_404(import_id)
        if not imprt.has_instance_permission(scope):
            raise Forbidden
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
    if imprt is None:
        try:
            dataset_id = int(request.form['datasetId'])
        except ValueError:
            raise BadRequest(description="'datasetId' must be an integer.")
        dataset = TDatasets.query.get(dataset_id)
        if dataset is None:
            raise BadRequest(description=f"Dataset '{dataset_id}' does not exist.")
        if not dataset.has_instance_permission(scope):  # FIXME wrong scope
            raise Forbidden(description='Vous n’avez pas les permissions sur ce jeu de données.')
        imprt = TImports(dataset=dataset)
        imprt.authors.append(author)
        db.session.add(imprt)
    imprt.source_file = f.read()
    imprt.full_file_name = f.filename
    imprt.detected_encoding = detected_encoding

    # reset decode step
    imprt.columns = None
    imprt.source_count = None
    imprt.synthese_data = []
    imprt.errors = []

    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/decode", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def decode_file(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
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

    try:
        csvfile = StringIO(imprt.source_file.decode(imprt.encoding))
    except UnicodeError as e:
        raise BadRequest(description=str(e))
    headline = csvfile.readline()
    csvfile.seek(0)
    dialect = csv.Sniffer().sniff(headline)
    csvreader = csv.reader(csvfile, delimiter=";")#dialect.delimiter)
    columns = next(csvreader)
    duplicates = set([col for col in columns if columns.count(col) > 1])
    if duplicates:
        raise BadRequest(f"Duplicates column names: {duplicates}")

    imprt.columns = columns
    imprt.source_count = None
    imprt.synthese_data = []
    imprt.errors = []

    db.session.commit()

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/load", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def load_import(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if imprt.source_file is None:
        raise BadRequest(description='A file must be first uploaded.')
    if imprt.fieldmapping is None:
        raise BadRequest(description='File fields must be first mapped.')
    line_no = insert_import_data_in_database(imprt)
    if not line_no:
        raise BadRequest("File with 0 lines.")
    imprt.source_count = line_no
    db.session.commit()
    return jsonify(imprt.as_dict())
