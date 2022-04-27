from datetime import datetime
from io import BytesIO
import codecs
from io import StringIO
import csv

from flask import request, current_app, jsonify, g
from werkzeug.exceptions import Conflict, BadRequest, Forbidden
from sqlalchemy.orm import joinedload, Load, load_only, undefer

from geonature.utils.env import db
from geonature.core.gn_permissions import decorators as permissions
from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
)
from geonature.core.gn_meta.models import TDatasets

from gn_module_import.models import (
    TImports,
    ImportSyntheseData,
    ImportUserError,
    BibFields,
    FieldMapping,
    ContentMapping,
)
from gn_module_import.checks import run_all_checks
from gn_module_import.blueprint import blueprint
from gn_module_import.utils import (
    get_valid_bbox,
    do_nomenclatures_mapping,
    load_import_data_in_dataframe,
    update_import_data_from_dataframe,
    set_the_geom_column,
    complete_others_geom_columns,
    toggle_synthese_triggers,
    import_data_to_synthese,
    populate_cor_area_synthese,
    detect_encoding,
    detect_separator,
    insert_import_data_in_database,
    get_file_size,
)


@blueprint.route("/imports/", methods=["GET"])
@permissions.check_cruved_scope(
    "R", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def get_import_list(scope):
    """
    .. :quickref: Import; Get all imports.

    Get all imports to which logged-in user has access.
    """
    imports = (
        TImports.query.options(
            Load(TImports).raiseload("*"),
            joinedload("authors"),
            joinedload("dataset"),
        )
        .filter_by_scope(scope)
        .order_by(TImports.id_import)
        .all()
    )

    return jsonify([imprt.as_dict() for imprt in imports])


@blueprint.route("/imports/<int:import_id>/", methods=["GET"])
@permissions.check_cruved_scope(
    "R", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def get_one_import(scope, import_id):
    """
    .. :quickref: Import; Get an import.

    Get an import.
    """
    imprt = TImports.query.get_or_404(import_id)
    # check that the user has read permission to this particular import instance:
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/upload", defaults={"import_id": None}, methods=["POST"])
@blueprint.route("/imports/<int:import_id>/upload", methods=["PUT"])
@permissions.check_cruved_scope(
    "C", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
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
        if not imprt.dataset.active:
            raise Forbidden("Le jeu de données est fermé.")
    else:
        imprt = None
    f = request.files["file"]
    size = get_file_size(f)
    # value in config file is in Mo
    max_file_size = current_app.config["IMPORT"]["MAX_FILE_SIZE"] * 1024 * 1024
    if size > max_file_size:
        raise BadRequest(
            description=f"File too big ({size} > {max_file_size})."
        )  # FIXME better error signaling?
    if imprt is None:
        try:
            dataset_id = int(request.form["datasetId"])
        except ValueError:
            raise BadRequest(description="'datasetId' must be an integer.")
        dataset = TDatasets.query.get(dataset_id)
        if dataset is None:
            raise BadRequest(description=f"Dataset '{dataset_id}' does not exist.")
        if not dataset.has_instance_permission(scope):  # FIXME wrong scope
            raise Forbidden(
                description="Vous n’avez pas les permissions sur ce jeu de données."
            )
        if not dataset.active:
            raise Forbidden("Le jeu de données est fermé.")
        imprt = TImports(dataset=dataset)
        imprt.authors.append(author)
        db.session.add(imprt)
    imprt.detected_encoding = detect_encoding(f)
    imprt.detected_separator = detect_separator(
        f,
        encoding=imprt.encoding or imprt.detected_encoding,
    )
    imprt.source_file = f.read()
    imprt.full_file_name = f.filename

    # reset decode step
    imprt.columns = None
    imprt.source_count = None
    imprt.synthese_data = []
    imprt.errors = []

    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/decode", methods=["POST"])
@permissions.check_cruved_scope(
    "C", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def decode_file(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.dataset.active:
        raise Forbidden("Le jeu de données est fermé.")
    if imprt.source_file is None:
        raise BadRequest(description="A file must be first uploaded.")
    if "encoding" not in request.json:
        raise BadRequest(description="Missing encoding.")
    encoding = request.json["encoding"]
    try:
        codecs.lookup(encoding)
    except LookupError:
        raise BadRequest(description="Unknown encoding.")
    imprt.encoding = encoding
    if "format" not in request.json:
        raise BadRequest(description="Missing format.")
    if request.json["format"] not in TImports.AVAILABLE_FORMATS:
        raise BadRequest(description="Unknown format.")
    imprt.format_source_file = request.json["format"]
    if "srid" not in request.json:
        raise BadRequest(description="Missing srid.")
    try:
        imprt.srid = int(request.json["srid"])
    except ValueError:
        raise BadRequest(description="SRID must be an integer.")
    if 'separator' not in request.json:
        raise BadRequest(description='Missing separator')
    if request.json['separator'] not in TImports.AVAILABLE_SEPARATORS:
        raise BadRequest(description='Unknown separator')
    imprt.separator = request.json['separator']
    db.session.commit()  # commit parameters

    try:
        csvfile = StringIO(imprt.source_file.decode(imprt.encoding))
    except UnicodeError as e:
        raise BadRequest(description=str(e))
    csvreader = csv.reader(csvfile, delimiter=imprt.separator)
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


@blueprint.route("/imports/<int:import_id>/fieldmapping", methods=["POST"])
@permissions.check_cruved_scope(
    "C", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def set_import_field_mapping(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.dataset.active:
        raise Forbidden("Le jeu de données est fermé.")
    try:
        FieldMapping.validate_values(request.json)
    except ValueError as e:
        raise BadRequest(*e.args)
    imprt.fieldmapping = request.json
    imprt.source_count = None
    imprt.synthese_data = []
    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/load", methods=["POST"])
@permissions.check_cruved_scope(
    "C", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def load_import(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.dataset.active:
        raise Forbidden("Le jeu de données est fermé.")
    if imprt.source_file is None:
        raise BadRequest(description="A file must be first uploaded.")
    if imprt.fieldmapping is None:
        raise BadRequest(description="File fields must be first mapped.")
    line_no = insert_import_data_in_database(imprt)
    if not line_no:
        raise BadRequest("File with 0 lines.")
    imprt.source_count = line_no
    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/columns", methods=["GET"])
@permissions.check_cruved_scope(
    "R", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def get_import_columns_name(scope, import_id):
    """
    .. :quickref: Import;

    Return all the columns of the file of an import
    """
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.columns:
        raise Conflict(description="Data have not been decoded.")
    return jsonify(imprt.columns)


@blueprint.route("/imports/<int:import_id>/values", methods=["GET"])
@permissions.check_cruved_scope(
    "R", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def get_import_values(scope, import_id):
    """
    .. :quickref: Import;

    Return all values present in imported file for nomenclated fields
    """
    imprt = TImports.query.get_or_404(import_id)
    # check that the user has read permission to this particular import instance:
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.source_count:
        raise Conflict(
            description="Data have not been loaded {}.".format(imprt.source_count)
        )
    nomenclated_fields = (
        BibFields.query.filter(BibFields.mnemonique != None)
        .options(joinedload("nomenclature_type").joinedload("nomenclatures"))
        .all()
    )
    # Note: response format is validated with jsonschema in tests
    response = {}
    for field in nomenclated_fields:
        if field.name_field not in imprt.fieldmapping:
            # this nomenclated field is not mapped
            continue
        source = imprt.fieldmapping[field.name_field]
        if source not in imprt.columns:
            # the file do not contain this field expected by the mapping
            continue
        # TODO: vérifier que l’on a pas trop de valeurs différentes ?
        column = field.source_column
        values = [
            getattr(data, column)
            for data in (
                ImportSyntheseData.query.filter_by(imprt=imprt)
                .options(load_only(column))
                .distinct(getattr(ImportSyntheseData, column))
                .all()
            )
        ]
        response[field.name_field] = {
            "nomenclature_type": field.nomenclature_type.as_dict(),
            "nomenclatures": [
                n.as_dict() for n in field.nomenclature_type.nomenclatures
            ],
            "values": values,
        }
    return jsonify(response)


@blueprint.route("/imports/<int:import_id>/contentmapping", methods=["POST"])
@permissions.check_cruved_scope(
    "C", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def set_import_content_mapping(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.dataset.active:
        raise Forbidden("Le jeu de données est fermé.")
    try:
        ContentMapping.validate_values(request.json)
    except ValueError as e:
        raise BadRequest(*e.args)
    imprt.contentmapping = request.json
    imprt.errors = []
    # TODO: set gn_is_valid = None
    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/prepare", methods=["POST"])
@permissions.check_cruved_scope(
    "C", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def prepare_import(scope, import_id):
    """
    Prepare data to be imported: apply all checks and transformations.
    """
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.dataset.active:
        raise Forbidden("Le jeu de données est fermé.")

    # Check preconditions to execute this action
    if not imprt.source_count:
        raise Conflict("Field data must have been loaded before executing this action.")
    if not imprt.contentmapping:
        raise Conflict(
            "Content mapping must have been set before executing this action."
        )

    # Remove previous errors
    imprt.errors = []

    selected_fields = [
        field_name
        for field_name, source_field in imprt.fieldmapping.items()
        if source_field in imprt.columns
    ]
    fields = {
        field.name_field: field
        for field in (
            BibFields.query.filter(BibFields.name_field.in_(selected_fields))
            .filter(
                BibFields.mnemonique == None
            )  # nomenclated fields handled with SQL FIXME
            .all()
        )
    }

    do_nomenclatures_mapping(imprt)
    df = load_import_data_in_dataframe(imprt, fields)
    run_all_checks(imprt, fields, df)
    set_the_geom_column(imprt, fields, df)
    update_import_data_from_dataframe(imprt, fields, df)
    complete_others_geom_columns(imprt, fields)

    # TODO: the geom point
    # TODO: maille
    # TODO: generate uuid (?)
    # TODO: generate altitude
    # TODO: missing nomenclature (?)
    # TODO: nomenclature conditional checks

    db.session.commit()

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/preview_valid_data/", methods=["GET"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT")
def preview_valid_data(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    fields = BibFields.query.filter(
        BibFields.synthese_field != None,
        BibFields.name_field.in_(imprt.fieldmapping.keys()),
    ).all()
    columns = [field.name_field for field in fields]
    valid_data = (
        ImportSyntheseData.query.filter_by(
            imprt=imprt,
            valid=True,
        )
        .options(
            load_only(*columns),
        )
        .limit(100)
    )
    valid_bbox = get_valid_bbox(imprt)
    n_valid_data = ImportSyntheseData.query.filter_by(
        imprt=imprt,
        valid=True,
    ).count()
    n_invalid_data = ImportSyntheseData.query.filter_by(
        imprt=imprt,
        valid=False,
    ).count()
    return jsonify(
        {
            "columns": columns,
            "valid_data": [o.as_dict(fields=columns) for o in valid_data],
            "n_valid_data": n_valid_data,
            "n_invalid_data": n_invalid_data,
            "valid_bbox": valid_bbox,
        }
    )


@blueprint.route("/imports/<int:import_id>/errors", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT")
def get_import_errors(scope, import_id):
    """
    .. :quickref: Import; Get errors of an import.

    Get errors of an import.
    """
    imprt = TImports.query.options(joinedload("errors")).get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    return jsonify([error.as_dict(fields=["type"]) for error in imprt.errors])


@blueprint.route("/imports/<int:import_id>/invalid_rows", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT")
def get_import_invalid_rows_as_csv(scope, import_id):
    """
    .. :quickref: Import; Get invalid rows of an import as CSV.

    Export invalid data in CSV.
    """
    imprt = TImports.query.options(undefer("source_file")).get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden

    invalid_rows = (
        ImportSyntheseData.query.filter_by(imprt=imprt, valid=False)
        .options(load_only("line_no"))
        .order_by(ImportSyntheseData.line_no)
        .all()
    )
    invalid_rows = {row.line_no for row in invalid_rows}
    if imprt.source_count and imprt.source_count == len(invalid_rows):
        raise BadRequest("Import file has not been processed.")

    filename = imprt.full_file_name.rsplit(".", 1)[0]  # remove extension
    filename = f"{filename}_errors.csv"

    def generate_invalid_rows_csv():
        inputfile = BytesIO(imprt.source_file)
        yield inputfile.readline()  # header
        line_no = 0
        for row in inputfile:
            line_no += 1
            if line_no in invalid_rows:
                yield row

    response = current_app.response_class(
        generate_invalid_rows_csv(),
        mimetype=f"text/csv; charset={imprt.encoding}; header=present",
    )
    response.headers.set("Content-Disposition", "attachment", filename=filename)
    return response


@blueprint.route("/imports/<int:import_id>/import", methods=["POST"])
@permissions.check_cruved_scope(
    "C", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def import_valid_data(scope, import_id):
    """
    .. :quickref: Import; Import the valid data.

    Import valid data in GeoNature synthese.
    """
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.dataset.active:
        raise Forbidden("Le jeu de données est fermé.")
    valid_data_count = ImportSyntheseData.query.filter_by(
        imprt=imprt, valid=True
    ).count()
    if not valid_data_count:
        raise BadRequest("Not valid data to import")

    source = TSources.query.filter_by(name_source=imprt.source_name).one_or_none()
    if source:
        if current_app.config["DEBUG"]:
            Synthese.query.filter_by(source=source).delete()
        else:
            return BadRequest(description="This import has been already finalized.")
    else:
        entity_source_pk_field = BibFields.query.filter_by(
            name_field="entity_source_pk_value"
        ).one()
        source = TSources(
            name_source=imprt.source_name,
            desc_source="Imported data from import module (id={import_id})",
            entity_source_pk_field=entity_source_pk_field.synthese_field,
        )
        db.session.add(source)

    toggle_synthese_triggers(enable=False)
    import_data_to_synthese(imprt, source)
    toggle_synthese_triggers(enable=True)
    populate_cor_area_synthese(imprt, source)

    imprt.date_end_import = datetime.now()

    db.session.commit()

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/", methods=["DELETE"])
@permissions.check_cruved_scope(
    "D", get_scope=True, module_code="IMPORT", object_code="IMPORT"
)
def delete_import(scope, import_id):
    """
    .. :quickref: Import; Delete an import.

    Delete an import.
    """
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.dataset.active:
        raise Forbidden("Le jeu de données est fermé.")
    ImportUserError.query.filter_by(imprt=imprt).delete()
    ImportSyntheseData.query.filter_by(imprt=imprt).delete()
    source = TSources.query.filter_by(name_source=imprt.source_name).one_or_none()
    if source:
        Synthese.query.filter_by(source=source).delete()
        db.session.delete(source)
    db.session.delete(imprt)
    db.session.commit()
    return jsonify()
