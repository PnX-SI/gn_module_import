from datetime import datetime
from io import BytesIO

import pandas as pd
import numpy as np

from flask import request, current_app, jsonify
from werkzeug.exceptions import Conflict, BadRequest, Forbidden
import sqlalchemy as sa
from sqlalchemy.orm import joinedload, Load, load_only, undefer
from sqlalchemy.sql.expression import select, update, insert, literal
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql import column
from geoalchemy2.functions import ST_Transform, ST_GeomFromWKB

from ref_geo.models import LAreas

from geonature.utils.env import DB as db
from geonature.core.gn_permissions import decorators as permissions
from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
    corAreaSynthese,
)
from geonature.core.gn_commons.models import TModules

from pypnnomenclature.models import TNomenclatures, BibNomenclaturesTypes

from gn_module_import import MODULE_CODE
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
from gn_module_import.utils import get_valid_bbox
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
    if not imprt.source_count:
        raise Conflict(description="Data have not been loaded {}.".format(imprt.source_count))
    nomenclated_fields = (
        BibFields.query
        .filter(BibFields.mnemonique != None)
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
                ImportSyntheseData.query
                .filter_by(imprt=imprt)
                .options(load_only(column))
                .distinct(getattr(ImportSyntheseData, column))
                .all()
            )
        ]
        response[field.name_field] = {
            "nomenclature_type": field.nomenclature_type.as_dict(),
            "nomenclatures": [n.as_dict() for n in field.nomenclature_type.nomenclatures],
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
    imprt.source_count = None
    imprt.synthese_data = []
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
    imprt.errors = []
    # TODO: set gn_is_valid = None
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
    if not imprt.source_count:
        raise Conflict("Field data must have been loaded before executing this action.")
    if not imprt.contentmapping:
        raise Conflict("Content mapping must have been set before executing this action.")

    # Remove previous errors and mark all rows as invalid
    imprt.errors = []
    db.session.query(ImportSyntheseData).filter_by(id_import=imprt.id_import).update(
        {"valid": False}
    )

    # Set nomenclatures using content mapping
    for field in BibFields.query.filter(BibFields.mnemonique!=None).all():
        source_field = getattr(ImportSyntheseData, field.source_field)
        # This CTE return the list of source value / cd_nomenclature for a given nomenclature type
        cte = (
            select([column('key').label('value'), column('value').label('cd_nomenclature')])
            .select_from(sa.func.JSON_EACH_TEXT(TImports.contentmapping[field.mnemonique]))
            .where(TImports.id_import==imprt.id_import)
            .cte("cte")
        )
        # This statement join the cte results with nomenclatures in order to set the id_nomenclature
        stmt = (
            update(ImportSyntheseData)
            .where(ImportSyntheseData.imprt==imprt)
            .where(source_field==cte.c.value)
            .where(TNomenclatures.cd_nomenclature==cte.c.cd_nomenclature)
            .where(BibNomenclaturesTypes.mnemonique==field.mnemonique)
            .where(TNomenclatures.id_type==BibNomenclaturesTypes.id_type)
            .values({field.synthese_field: TNomenclatures.id_nomenclature})
        )
        db.session.execute(stmt)
        if current_app.config["IMPORT"]["FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"]:
            # Set default nomenclature for empty user fields
            (
                ImportSyntheseData.query
                .filter(ImportSyntheseData.imprt==imprt)
                .filter(sa.or_(source_field==None, source_field==""))
                .update(
                    values={
                        field.synthese_field: sa.func.gn_synthese.get_default_nomenclature_value(
                            field.mnemonique,
                        ),
                    },
                    synchronize_session=False,
                )
            )
        # TODO: Check nomenclature errors (synthese_field NULL)
        # TODO: Check conditional values

    selected_fields = [
        field_name
        for field_name, source_field in imprt.fieldmapping.items()
        if source_field in imprt.columns
    ]
    fields = {
        field.name_field: field
        for field in (
            BibFields.query
            .filter(BibFields.name_field.in_(selected_fields))
            .filter(BibFields.mnemonique==None)  # nomenclated fields handled with SQL
            .all()
        )
    }
    source_cols = (
        [
            "id_import",
            "line_no",
            "valid",
        ] + [
            field.source_column
            for field in fields.values()
        ]
    )
    records = (
        db.session.query(
            *[ImportSyntheseData.__table__.c[col] for col in source_cols]
        )
        .filter(
            ImportSyntheseData.imprt==imprt,
        )
        .all()
    )
    df = pd.DataFrame.from_records(
        records,
        columns=source_cols,
    )

    df["valid"] = True
    run_all_checks(df, imprt, fields)

    file_srid = imprt.srid
    local_srid = db.session.execute(sa.func.Find_SRID('ref_geo', 'l_areas', 'geom')).scalar()
    geom_col = df[df["_geom"].notna()]["_geom"]
    if file_srid == 4326:
        df["the_geom_4326"] = geom_col.apply(lambda geom: ST_GeomFromWKB(geom.wkb, file_srid))
        fields["the_geom_4326"] = BibFields.query.filter_by(name_field="the_geom_4326").one()
    elif file_srid == local_srid:
        df["the_geom_local"] = geom_col.apply(lambda geom: ST_GeomFromWKB(geom.wkb, file_srid))
        fields["the_geom_local"] = BibFields.query.filter_by(name_field="the_geom_local").one()
    else:
        df["the_geom_4326"] = geom_col.apply(lambda geom: ST_Transform(ST_GeomFromWKB(geom.wkb, file_srid), 4326))
        fields["the_geom_4326"] = BibFields.query.filter_by(name_field="the_geom_4326").one()

    updated_cols = [
        "id_import",
        "line_no",
        "valid",
    ]
    updated_cols += [
        field.synthese_field
        for field in fields.values()
        if field.synthese_field
    ]
    df.replace({np.nan: None}, inplace=True)
    df.replace({pd.NaT: None}, inplace=True)
    records = df[df["valid"] == True][updated_cols].to_dict(orient='records')
    insert_stmt = pg_insert(ImportSyntheseData)
    insert_stmt = (
        insert_stmt
        .values(records)
        .on_conflict_do_update(
            index_elements=updated_cols[:2],
            set_={col: insert_stmt.excluded[col] for col in updated_cols[2:]},
        )
    )
    db.session.execute(insert_stmt)

    if "the_geom_4326" not in fields:
        assert "the_geom_local" in fields
        source_col = "the_geom_local"
        dest_col = "the_geom_4326"
        dest_srid = 4326
    else:
        assert "the_geom_4326" in fields
        source_col = "the_geom_4326"
        dest_col = "the_geom_local"
        dest_srid = local_srid
    (
        ImportSyntheseData.query
        .filter(
            ImportSyntheseData.imprt == imprt,
            ImportSyntheseData.valid == True,
            getattr(ImportSyntheseData, source_col) != None,
        )
        .update(
            values={
                dest_col: ST_Transform(getattr(ImportSyntheseData, source_col), dest_srid),
            },
            synchronize_session=False,
        )
    )

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
    fields = (
        BibFields.query
        .filter(
            BibFields.synthese_field != None,
            BibFields.name_field.in_(imprt.fieldmapping.keys()),
        )
        .all()
    )
    columns = [field.name_field for field in fields]
    valid_data = (
        ImportSyntheseData.query
        .filter_by(
            imprt=imprt,
            valid=True,
        )
        .options(
            load_only(*columns),
        )
        .limit(100)
    )
    valid_bbox = get_valid_bbox(imprt)
    n_valid_data = (
        ImportSyntheseData.query
        .filter_by(
            imprt=imprt,
            valid=True,
        )
        .count()
    )
    n_invalid_data = (
        ImportSyntheseData.query
        .filter_by(
            imprt=imprt,
            valid=False,
        )
        .count()
    )
    return jsonify(
        {
            "columns": columns,
            "valid_data": [o.as_dict(fields=columns) for o in valid_data],
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
    imprt = TImports.query.options(undefer("source_file")).get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden

    invalid_rows = (
        ImportSyntheseData.query
        .filter_by(imprt=imprt, valid=False)
        .options(load_only("line_no"))
        .order_by(ImportSyntheseData.line_no)
        .all()
    )
    invalid_rows = { row.line_no for row in invalid_rows }
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
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="IMPORT")
def import_valid_data(scope, import_id):
    """
    .. :quickref: Import; Import the valid data.

    Import valid data in GeoNature synthese.
    """
    imprt = TImports.query.get_or_404(import_id)
    if not imprt.has_instance_permission(scope):
        raise Forbidden
    valid_data_count = (
        ImportSyntheseData.query
        .filter_by(imprt=imprt, valid=True)
        .count()
    )
    if not valid_data_count:
        raise BadRequest("Not valid data to import")

    source = TSources.query.filter_by(name_source=imprt.source_name).one_or_none()
    if source:
        if current_app.config["DEBUG"]:
            Synthese.query.filter_by(source=source).delete()
        else:
            return BadRequest(description="This import has been already finalized.")
    else:
        entity_source_pk_field = BibFields.query.filter_by(name_field="entity_source_pk_value").one()
        source = TSources(
            name_source=imprt.source_name,
            desc_source="Imported data from import module (id={import_id})",
            entity_source_pk_field=entity_source_pk_field.synthese_field,
        )
        db.session.add(source)

    logger.info(f"[Import {imprt.id_import}] Disable synthese triggers")

    triggers = ["tri_meta_dates_change_synthese", "tri_insert_cor_area_synthese"]

    # disable triggers
    with db.session.begin_nested():
        for trigger in triggers:
            db.session.execute(f"ALTER TABLE gn_synthese.synthese DISABLE TRIGGER {trigger}")

    logger.info(f"[Import {imprt.id_import}] Insert data in synthese")

    generated_fields = {"datetime_min", "datetime_max", "the_geom_4326", "the_geom_local", "the_geom_point"}
    # TODO: altitude? uuid? (when not in mapping)
    fields = (
        BibFields.query
        .filter(
            BibFields.synthese_field != None,
            BibFields.name_field.in_(
                imprt.fieldmapping.keys() | generated_fields
            ),
        )
        .all()
    )
    select_stmt = (
        ImportSyntheseData.query
        .filter_by(imprt=imprt, valid=True)
        .with_entities(*[
            getattr(ImportSyntheseData, field.synthese_field)
            for field in fields
        ])
        .add_columns(
            literal(source.id_source),
            literal(TModules.query.filter_by(module_code=MODULE_CODE).one().id_module),
        )
    )
    names = [ field.synthese_field for field in fields ] + [ "id_source", "id_module" ]
    insert_stmt = insert(Synthese).from_select(
        names=names,
        select=select_stmt,
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
    ImportSyntheseData.query.filter_by(imprt=imprt).delete()
    if imprt.is_finished:
        source = TSources.query.filter_by(name_source=imprt.source_name).one()
        Synthese.query.filter_by(source=source).delete()
        db.session.delete(source)
    db.session.delete(imprt)
    db.session.commit()
    return jsonify()
