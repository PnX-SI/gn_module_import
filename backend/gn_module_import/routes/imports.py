from datetime import datetime

from flask import request, current_app, jsonify, Response
from werkzeug.exceptions import Conflict, BadRequest, Forbidden
import sqlalchemy as sa
from sqlalchemy.orm import joinedload, Load
from sqlalchemy.sql.expression import select

from ref_geo.models import LAreas

from geonature.utils.env import DB as db
from geonature.core.gn_permissions import decorators as permissions
from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
    corAreaSynthese,
)

from gn_module_import.models import (
    TImports,
    ImportUserError,
    BibFields,
    FieldMapping,
    ContentMapping,
)
from gn_module_import.checks import run_all_checks
from gn_module_import.blueprint import blueprint
from gn_module_import.utils.imports import (
    get_table_class,
    drop_import_table,
    load_import_to_dataframe,
    save_dataframe_to_database,
    get_import_table_name,
    geom_to_wkb,
    stream_csv,
    get_synthese_columns_mapping,
)
from gn_module_import.utils import get_missing_fields
from gn_module_import.transform.set_geometry import GeometrySetter
from gn_module_import.transform.set_altitudes import set_altitudes
from gn_module_import.transform.nomenclatures.nomenclatures import NomenclatureTransformer
from gn_module_import.utils.imports import get_valid_bbox
from gn_module_import.logs import logger


@blueprint.route("/imports/", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="IMPORT")
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
            joinedload("errors"),
        )
        .filter_by_scope(scope)
        .order_by(TImports.id_import)
        .all()
    )

    fields = [
        "errors.pk",
        "dataset.dataset_name",
        "authors",
    ]

    return jsonify([
        imprt.as_dict(fields=fields)
        for imprt in imports
    ])


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
    return jsonify(imprt.as_dict(fields=["errors"]))


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
    if not imprt.import_table:
        raise Conflict(description="Data have not been decoded.")
    return jsonify(list(imprt.columns.keys()))


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
    if not imprt.import_table:
        raise Conflict(description="Data have not been decoded.")
    if not imprt.fieldmapping:
        raise Conflict("Field mapping must have been set before executing this action.")
    ImportEntry = get_table_class(get_import_table_name(imprt))
    nomenclated_fields = {
        field.name_field: field
        for field in (
            BibFields.query
            .filter(BibFields.mnemonique != None)
            .options(joinedload("nomenclature_type").joinedload("nomenclatures"))
            .all()
        )
    }
    # Note: response format is validated with jsonschema in tests
    response = {}
    for target, source in imprt.fieldmapping.items():
        if source not in imprt.columns.keys():
            # the file do not contain this field expected by the mapping
            continue
        if target not in nomenclated_fields:
            continue
        nomenclature_type = nomenclated_fields[target].nomenclature_type
        # TODO: vérifier que l’on a pas trop de valeurs différentes ?
        values = [
            v
            for v, in db.session.query(ImportEntry.columns[source])
            .distinct()
            .all()
        ]
        response[target] = {
            "nomenclature_type": nomenclature_type.as_dict(),
            "nomenclatures": [n.as_dict() for n in nomenclature_type.nomenclatures],
            "values": values,
        }
    return jsonify(response)


@blueprint.route("/imports/<int:import_id>/fieldmapping", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def set_import_field_mapping(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    try:
        FieldMapping.validate_values(request.json)
    except ValueError as e:
        raise BadRequest(*e.args)
    imprt.fieldmapping = request.json
    db.session.commit()
    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/contentmapping", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def set_import_content_mapping(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    try:
        ContentMapping.validate_values(request.json)
    except ValueError as e:
        raise BadRequest(*e.args)
    imprt.contentmapping = request.json
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

    # Check preconditions to execute this action
    if not imprt.import_table:
        raise Conflict("Import file must have been decoded before executing this action.")
    if not imprt.fieldmapping:
        raise Conflict("Field mapping must have been set before executing this action.")
    if not imprt.contentmapping:
        raise Conflict("Content mapping must have been set before executing this action.")

    missing_fields = get_missing_fields(imprt)
    if missing_fields:
        raise BadRequest(
            description="Erreur de correspondance des champs. "
            "Certains champs obligatoires sont manquants : " + ", ".join(missing_fields)
        )

    # Clean all errors
    imprt.errors = []

    # Note: this mapping may be modified during data cleaning, when computed columns are added
    selected_columns = {
        target: source
        for target, source in imprt.fieldmapping.items()
        if source in imprt.columns.keys()
    }
    synthese_fields = (
        BibFields.query.filter_by(synthese_field=True)
        .filter(BibFields.name_field.in_(selected_columns))
        .all()
    )

    # 1) load the table in a dataframe
    # 2) apply some check and transformation on the dataframe
    # 3) save the dataframe in database
    # 4) apply some other checks and transformation on the database
    #    (geometry, altitudes, nomenclatures)

    df = load_import_to_dataframe(imprt)

    df["gn_is_valid"] = True
    df["gn_invalid_reason"] = ""

    run_all_checks(df, imprt, selected_columns, synthese_fields)

    df["_geom"] = df[df["_geom"].notna()]["_geom"].apply(geom_to_wkb)
    save_dataframe_to_database(imprt, df)

    local_srid = db.session.execute(sa.func.Find_SRID('ref_geo', 'l_areas', 'geom')).scalar()
    geometry_setter = GeometrySetter(
        imprt,
        local_srid=local_srid,
        code_commune_col=selected_columns.get("codecommune"),
        code_maille_col=selected_columns.get("codemaille"),
        code_dep_col=selected_columns.get("codedepartement"),
    )
    geometry_setter.set_geometry()

    is_generate_alt = selected_columns.get("altitudes_generate") == "true"
    set_altitudes(
        selected_columns,
        import_id,
        get_import_table_name(imprt),
        "gn_pk",
        is_generate_alt,
        "gn_the_geom_local",
    )

    nomenclature_transformer = NomenclatureTransformer()
    nomenclature_transformer.init(
        imprt.contentmapping, selected_columns, get_import_table_name(imprt)
    )
    nomenclature_transformer.set_nomenclature_ids()
    nomenclature_transformer.set_default_nomenclature_ids(where_user_val_none=True)
    nomenclature_transformer.find_nomenclatures_errors(import_id)
    nomenclature_transformer.check_conditionnal_values(import_id)
    if current_app.config["IMPORT"]["FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"]:
        nomenclature_transformer.set_default_nomenclature_ids()

    # database have been modified (new columns, …), reload the Table object
    db.metadata.remove(get_table_class(get_import_table_name(imprt)))
    get_table_class(get_import_table_name(imprt))

    db.session.commit()

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/preview_valid_data/", methods=["GET"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT")
def preview_valid_data(scope, import_id):
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    ImportEntry = get_table_class(get_import_table_name(imprt))
    synthese_field = [f.name_field for f in BibFields.query.filter_by(synthese_field=True)]
    total_columns = {
        target: source
        for target, source in imprt.fieldmapping.items()
        if source in imprt.columns.keys() and target in synthese_field
    }
    total_columns.update(
        {
            "the_geom_4326": "gn_the_geom_4326",
            "the_geom_local": "gn_the_geom_local",
            "the_geom_point": "gn_the_geom_point",
            "id_area_attachment": "id_area_attachment",
        }
    )
    target_columns, source_columns = zip(*get_synthese_columns_mapping(imprt, cast=False).items())
    select_valid_data = (
        select(source_columns)
        .where(ImportEntry.c.gn_is_valid == True)
        .limit(100)
    )
    valid_bbox = get_valid_bbox(ImportEntry.c.gn_the_geom_4326)
    n_valid_data = db.session.query(ImportEntry).filter_by(gn_is_valid=True).count()
    n_invalid_data = db.session.query(ImportEntry).filter_by(gn_is_valid=False).count()
    return jsonify(
        {
            # 'columns': [ col for col in total_columns.values() ],
            "columns": target_columns,
            # "valid_data": valid_data_list,
            "valid_data": [list(row) for row in db.session.execute(select_valid_data)],
            "n_valid_data": n_valid_data,
            "n_invalid_data": n_invalid_data,
            "valid_bbox": valid_bbox,
        }
    )


@blueprint.route("/imports/<int:import_id>/invalid_rows", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT")
def get_import_invalid_rows_as_csv(scope, import_id):
    """
    .. :quickref: Import; Get invalid rows of an import as CSV.

    Export invalid data in CSV.
    """
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden

    if not imprt.import_table:
        raise BadRequest("Import file has not been decoded.")

    ImportEntry = get_table_class(get_import_table_name(imprt))
    # TODO source columns: gn_invalid_reason + original columns
    source_columns = get_synthese_columns_mapping(imprt, cast=False).values()
    invalid_data = db.session.execute(
        select(source_columns)
        .where(ImportEntry.c.gn_is_valid == False)
    )

    filename = imprt.full_file_name.rsplit(".", 1)[0]  # remove extension
    response = Response(stream_csv(invalid_data), mimetype="text/csv")
    response.headers.set("Content-Disposition", "attachment", filename=f"{filename}_errors.csv")
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
    ImportEntry = get_table_class(get_import_table_name(imprt))
    valid_data_count = (
        db.session.query(ImportEntry)
        .filter(ImportEntry.c.gn_is_valid == True)
        .count()
    )
    if not valid_data_count:
        raise BadRequest("Not valid data to import")

    source_name = f"Import(id={imprt.id_import})"
    if db.session.query(TSources.query.filter_by(name_source=source_name).exists()).scalar():
        return BadRequest(description="This import has been already finalized.")

    # Get the name of the field to use as pk. Fallback to gn_pk.
    entity_source_pk_field = imprt.fieldmapping.get("entity_source_pk_value", "gn_pk")

    source = TSources(
        name_source=source_name,
        desc_source="Imported data from import module (id={import_id})",
        entity_source_pk_field=entity_source_pk_field,
    )
    db.session.add(source)

    logger.info(f"[Import {imprt.id_import}] Disable synthese triggers")

    triggers = ["tri_meta_dates_change_synthese", "tri_insert_cor_area_synthese"]

    # disable triggers
    with db.session.begin_nested():
        for trigger in triggers:
            db.session.execute(f"ALTER TABLE gn_synthese.synthese DISABLE TRIGGER {trigger}")

    logger.info(f"[Import {imprt.id_import}] Insert data in synthese")

    ImportEntry = get_table_class(get_import_table_name(imprt))
    target_columns, source_columns = zip(*get_synthese_columns_mapping(imprt).items())
    insert_stmt = Synthese.__table__.insert().from_select(
        names=target_columns,
        select=select(
            source_columns,
        ).where(ImportEntry.c.gn_is_valid == True),
    )
    db.session.execute(insert_stmt)

    logger.info(f"[Import {imprt.id_import}] Re-enable synthese triggers")

    # re-enable triggers
    with db.session.begin_nested():
        for trigger in triggers:
            db.session.execute(f"ALTER TABLE gn_synthese.synthese ENABLE TRIGGER {trigger}")

    logger.info(f"[Import {imprt.id_import}] Populate cor_area_synthese")

    # Populate synthese / area association table
    # A synthese entry is associated to an area when the area is enabled,
    # and when the synthese geom intersects with the area
    # (we also check the intersection is more than just touches when the geom is not a point)
    synthese_geom = Synthese.__table__.c.the_geom_local
    area_geom = LAreas.__table__.c.geom
    db.session.execute(
        corAreaSynthese.insert().from_select(
            names=[
                corAreaSynthese.c.id_synthese,
                corAreaSynthese.c.id_area,
            ],
            select=select(
                [
                    Synthese.__table__.c.id_synthese,
                    LAreas.__table__.c.id_area,
                ]
            )
            .select_from(
                Synthese.__table__.join(
                    LAreas.__table__,
                    sa.func.ST_Intersects(synthese_geom, area_geom),
                )
            )
            .where(
                (LAreas.__table__.c.enable == True)
                & (
                    (sa.func.ST_GeometryType(synthese_geom) == "ST_Point")
                    | ~(sa.func.ST_Touches(synthese_geom, area_geom))
                )
                & (Synthese.__table__.c.id_source == source.id_source)
            ),
        )
    )

    logger.info(f"[Import {imprt.id_import}] Updating synthese metadata")

    Synthese.query.filter_by(id_source=source.id_source).update(
        {
            "last_action": "I",
        }
    )
    Synthese.query.filter_by(id_source=source.id_source, meta_create_date=None).update(
        {
            "meta_create_date": datetime.now(),
        }
    )
    Synthese.query.filter_by(id_source=source.id_source, meta_update_date=None).update(
        {
            "meta_update_date": datetime.now(),
        }
    )

    logger.info(f"[Import {imprt.id_import}] Committing")

    db.session.commit()

    logger.info(f"[Import {imprt.id_import}] Committed")

    return jsonify(imprt.as_dict())


@blueprint.route("/imports/<int:import_id>/", methods=["DELETE"])
@permissions.check_cruved_scope("D", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def delete_import(scope, import_id):
    """
    .. :quickref: Import; Delete an import.

    Delete an import.
    """
    imprt = TImports.query.get_or_404(import_id)
    # check that the user has delete permission to this particular import instance:
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    ImportUserError.query.filter_by(imprt=imprt).delete()
    if imprt.is_finished:
        # delete imported data if the import is already finished
        name_source = "Import(id=" + import_id + ")"
        source = TSources.query.filter_by(name_source=name_source).one()
        Synthese.query.filter_by(source=source).delete()
        db.session.delete(source)
    if imprt.import_table:
        drop_import_table(imprt)
    db.session.delete(imprt)
    db.session.commit()
    return jsonify()
