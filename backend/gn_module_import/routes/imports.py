from io import BytesIO
import codecs
from io import StringIO
import csv
import unicodedata

from flask import request, current_app, jsonify, g, stream_with_context, send_file
from werkzeug.exceptions import Conflict, BadRequest, Forbidden, Gone
from werkzeug.urls import url_quote
from sqlalchemy import or_, func, desc
from sqlalchemy.orm import joinedload, Load, load_only, undefer, contains_eager
from sqlalchemy.orm.attributes import set_committed_value
from sqlalchemy.sql.expression import collate

from geonature.utils.env import db
from geonature.utils.sentry import start_sentry_child
from geonature.core.gn_permissions import decorators as permissions
from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
)
from geonature.core.gn_meta.models import TDatasets

from pypnnomenclature.models import BibNomenclaturesTypes, TNomenclatures

from gn_module_import.models import (
    TImports,
    ImportSyntheseData,
    ImportUserError,
    BibFields,
    FieldMapping,
    ContentMapping,
)
from pypnusershub.db.models import User
from gn_module_import.blueprint import blueprint
from gn_module_import.utils import (
    ImportStep,
    get_valid_bbox,
    detect_encoding,
    detect_separator,
    insert_import_data_in_database,
    get_file_size,
    clean_import,
    generate_pdf_from_template,
)
from gn_module_import.tasks import do_import_checks, do_import_in_synthese

IMPORTS_PER_PAGE = 15


@blueprint.route("/nomenclatures", methods=["GET"])
def get_nomenclatures():
    nomenclature_fields = (
        BibFields.query.filter(BibFields.nomenclature_type != None)
        .options(
            joinedload(BibFields.nomenclature_type).joinedload(
                BibNomenclaturesTypes.nomenclatures
            ),
        )
        .all()
    )
    return jsonify(
        {
            field.nomenclature_type.mnemonique: {
                "nomenclature_type": field.nomenclature_type.as_dict(),
                "nomenclatures": {
                    nomenclature.cd_nomenclature: nomenclature.as_dict()
                    for nomenclature in field.nomenclature_type.nomenclatures
                },
            }
            for field in nomenclature_fields
        }
    )


@blueprint.route("/imports/", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def get_import_list(scope):
    """
    .. :quickref: Import; Get all imports.

    Get all imports to which logged-in user has access.
    """
    page = request.args.get("page", default=1, type=int)
    limit = request.args.get("limit", default=IMPORTS_PER_PAGE, type=int)
    search = request.args.get("search", default=None, type=str)
    sort = request.args.get("sort", default="date_create_import", type=str)
    sort_dir = request.args.get("sort_dir", default="desc", type=str)
    filters = []
    if search:
        filters.append(TImports.full_file_name.ilike(f"%{search}%"))
        filters.append(
            TImports.dataset.has(
                func.lower(TDatasets.dataset_name).contains(func.lower(search)),
            )
        )
        filters.append(
            TImports.authors.any(
                or_(
                    User.prenom_role.ilike(f"%{search}%"),
                    User.nom_role.ilike(f"%{search}%"),
                ),
            )
        )
        filters.append(
            TImports.authors.any(
                func.lower(User.nom_role).contains(func.lower(search)),
            )
        )
    # Todo: order_by foreign keys
    try:
        order_by = getattr(TImports, sort)
    except AttributeError:
        raise BadRequest(f"Import field '{sort}' does not exist.")
    if sort_dir == "desc":
        order_by = desc(order_by)
    imports = (
        TImports.query.options(
            Load(TImports).raiseload("*"),
            joinedload("authors"),
            joinedload("dataset"),
        )
        .filter_by_scope(scope)
        .filter(or_(*filters))
        .order_by(order_by)
        .paginate(page=page, error_out=False, max_per_page=limit)
    )

    data = {
        "imports": [imprt.as_dict() for imprt in imports.items],
        "count": imports.total,
        "limit": limit,
        "offset": page - 1,
    }
    return jsonify(data)


@blueprint.route("/imports/<int:import_id>/", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
    if size == 0:
        raise BadRequest(description="Impossible to upload empty files")
    if imprt is None:
        try:
            dataset_id = int(request.form["datasetId"])
        except ValueError:
            raise BadRequest(description="'datasetId' must be an integer.")
        dataset = TDatasets.query.get(dataset_id)
        if dataset is None:
            raise BadRequest(description=f"Dataset '{dataset_id}' does not exist.")
        if not dataset.has_instance_permission(scope):  # FIXME wrong scope
            raise Forbidden(description="Vous n’avez pas les permissions sur ce jeu de données.")
        if not dataset.active:
            raise Forbidden("Le jeu de données est fermé.")
        imprt = TImports(dataset=dataset)
        imprt.authors.append(author)
        db.session.add(imprt)
    else:
        clean_import(imprt, ImportStep.UPLOAD)
    with start_sentry_child(op="task", description="detect encoding"):
        imprt.detected_encoding = detect_encoding(f)
    with start_sentry_child(op="task", description="detect separator"):
        imprt.detected_separator = detect_separator(
            f,
            encoding=imprt.encoding or imprt.detected_encoding,
        )
    imprt.source_file = f.read()
    imprt.full_file_name = f.filename

    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/decode", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def decode_file(scope, import_id):
    imprt = TImports.query.options(undefer("source_file")).get_or_404(import_id)
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
    if "separator" not in request.json:
        raise BadRequest(description="Missing separator")
    if request.json["separator"] not in TImports.AVAILABLE_SEPARATORS:
        raise BadRequest(description="Unknown separator")
    imprt.separator = request.json["separator"]

    clean_import(imprt, ImportStep.DECODE)

    db.session.commit()  # commit parameters

    decode = request.args.get("decode", 1)
    try:
        decode = int(decode)
    except ValueError:
        raise BadRequest(description="decode parameter must but an int")
    if decode:
        try:
            csvfile = StringIO(imprt.source_file.decode(imprt.encoding))
        except UnicodeError as e:
            raise BadRequest(
                description="Erreur d’encodage lors de la lecture du fichier source. "
                "Avez-vous sélectionné le bon encodage de votre fichier ?"
            )
        csvreader = csv.reader(csvfile, delimiter=imprt.separator)
        columns = next(csvreader)
        duplicates = set([col for col in columns if columns.count(col) > 1])
        if duplicates:
            raise BadRequest(f"Duplicates column names: {duplicates}")
        imprt.columns = columns
        db.session.commit()

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/fieldmapping", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
    clean_import(imprt, ImportStep.LOAD)
    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/load", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
    clean_import(imprt, ImportStep.LOAD)
    with start_sentry_child(op="task", description="insert data in db"):
        line_no = insert_import_data_in_database(imprt)
    if not line_no:
        raise BadRequest("File with 0 lines.")
    imprt.source_count = line_no
    imprt.loaded = True
    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/columns", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def get_import_values(scope, import_id):
    """
    .. :quickref: Import;

    Return all values present in imported file for nomenclated fields
    """
    imprt = TImports.query.get_or_404(import_id)
    # check that the user has read permission to this particular import instance:
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.loaded:
        raise Conflict(description="Data have not been loaded")
    nomenclated_fields = (
        BibFields.query.filter(BibFields.mnemonique != None)
        .join(BibFields.nomenclature_type)
        .options(
            contains_eager(BibFields.nomenclature_type),
        )
        .order_by(BibFields.id_theme, BibFields.order_field)
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
        set_committed_value(
            field.nomenclature_type,
            "nomenclatures",
            TNomenclatures.query.filter_by(nomenclature_type=field.nomenclature_type).order_by(
                collate(TNomenclatures.cd_nomenclature, "fr_numeric")
            ),
        )
        response[field.name_field] = {
            "nomenclature_type": field.nomenclature_type.as_dict(),
            "nomenclatures": [n.as_dict() for n in field.nomenclature_type.nomenclatures],
            "values": values,
        }
    return jsonify(response)


@blueprint.route("/imports/<int:import_id>/contentmapping", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
    clean_import(imprt, ImportStep.PREPARE)
    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/prepare", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
    if not imprt.loaded:
        raise Conflict("Field data must have been loaded before executing this action.")

    # Remove previous errors
    clean_import(imprt, ImportStep.PREPARE)

    # Run background import checks
    sig = do_import_checks.s(imprt.id_import)
    task = sig.freeze()
    imprt.task_id = task.task_id
    db.session.commit()
    sig.delay()

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/preview_valid_data", methods=["GET"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT")
def preview_valid_data(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if not imprt.processed:
        raise Conflict("Import must have been prepared before executing this action.")
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


@blueprint.route("/imports/<int:import_id>/source_file", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT")
def get_import_source_file(scope, import_id):
    imprt = TImports.query.options(undefer("source_file")).get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    if imprt.source_file is None:
        raise Gone
    return send_file(
        BytesIO(imprt.source_file),
        download_name=imprt.full_file_name,
        as_attachment=True,
        mimetype=f"text/csv; charset={imprt.encoding}; header=present",
    )


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
    if not imprt.processed:
        raise Conflict("Import must have been prepared before executing this action.")

    filename = imprt.full_file_name.rsplit(".", 1)[0]  # remove extension
    filename = f"{filename}_errors.csv"

    @stream_with_context
    def generate_invalid_rows_csv():
        inputfile = BytesIO(imprt.source_file)
        yield inputfile.readline()  # header
        line_no = 0
        for row in inputfile:
            line_no += 1
            if line_no in imprt.erroneous_rows:
                yield row

    response = current_app.response_class(
        generate_invalid_rows_csv(),
        mimetype=f"text/csv; charset={imprt.encoding}; header=present",
    )
    try:
        filename.encode("ascii")
    except UnicodeEncodeError:
        simple = unicodedata.normalize("NFKD", filename)
        simple = simple.encode("ascii", "ignore").decode("ascii")
        quoted = url_quote(filename, safe="")
        names = {"filename": simple, "filename*": f"UTF-8''{quoted}"}
    else:
        names = {"filename": filename}
    response.headers.set("Content-Disposition", "attachment", **names)
    return response


@blueprint.route("/imports/<int:import_id>/import", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
    if not imprt.processed:
        raise Forbidden("L’import n’a pas été préalablement vérifié.")
    valid_data_count = ImportSyntheseData.query.filter_by(imprt=imprt, valid=True).count()
    if not valid_data_count:
        raise BadRequest("Not valid data to import")

    clean_import(imprt, ImportStep.IMPORT)

    sig = do_import_in_synthese.s(imprt.id_import)
    task = sig.freeze()
    imprt.task_id = task.task_id
    db.session.commit()
    sig.delay()

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/", methods=["DELETE"])
@permissions.check_cruved_scope("D", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
    if imprt.source:
        Synthese.query.filter_by(source=imprt.source).delete()
        imprt.source = None
    db.session.delete(imprt)
    db.session.commit()
    return jsonify()


@blueprint.route("/export_pdf/<int:import_id>", methods=["POST"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT")
def export_pdf(scope, import_id):
    """
    Downloads the report in pdf format
    """
    imprt = TImports.query.options(
        Load(TImports).raiseload("*"),
        joinedload("authors"),
        joinedload("dataset"),
        joinedload("errors"),
    ).get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    ctx = imprt.as_dict(fields=["errors", "errors.type", "dataset.dataset_name"])

    ctx["map"] = request.form.get("map")
    ctx["chart"] = request.form.get("chart")
    url_list = [
        current_app.config["URL_APPLICATION"],
        "#",
        current_app.config["IMPORT"].get("MODULE_URL", "").replace("/", ""),
        str(ctx["id_import"]),
        "report",
    ]
    ctx["url"] = "/".join(url_list)
    pdf_file = generate_pdf_from_template("import_template_pdf.html", ctx)
    return send_file(
        BytesIO(pdf_file),
        mimetype="application/pdf",
        as_attachment=True,
        download_name="rapport.pdf",
    )
