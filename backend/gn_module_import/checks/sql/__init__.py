from datetime import date

from flask import current_app
from sqlalchemy import func
from sqlalchemy.sql.expression import select, update, insert, literal
from sqlalchemy.sql import column
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array_agg, aggregate_order_by
from geoalchemy2.functions import ST_Transform, ST_GeomFromWKB, ST_Centroid
from geonature.utils.env import db

from gn_module_import.models import (
    TImports,
    BibFields,
    ImportSyntheseData,
    ImportUserError,
    ImportUserErrorType,
)

from geonature.core.gn_synthese.models import Synthese
from ref_geo.models import LAreas, BibAreasTypes
from pypnnomenclature.models import TNomenclatures, BibNomenclaturesTypes
from apptax.taxonomie.models import Taxref
from pypn_habref_api.models import Habref


def do_nomenclatures_mapping(imprt, fields):
    # Set nomenclatures using content mapping
    for field in filter(lambda field: field.mnemonique != None, fields.values()):
        source_field = getattr(ImportSyntheseData, field.source_field)
        # This CTE return the list of source value / cd_nomenclature for a given nomenclature type
        cte = (
            select(
                [column("key").label("value"), column("value").label("cd_nomenclature")]
            )
            .select_from(
                sa.func.JSON_EACH_TEXT(TImports.contentmapping[field.mnemonique])
            )
            .where(TImports.id_import == imprt.id_import)
            .cte("cte")
        )
        # This statement join the cte results with nomenclatures
        # in order to set the id_nomenclature
        stmt = (
            update(ImportSyntheseData)
            .where(ImportSyntheseData.imprt == imprt)
            .where(source_field == cte.c.value)
            .where(TNomenclatures.cd_nomenclature == cte.c.cd_nomenclature)
            .where(BibNomenclaturesTypes.mnemonique == field.mnemonique)
            .where(TNomenclatures.id_type == BibNomenclaturesTypes.id_type)
            .values({field.synthese_field: TNomenclatures.id_nomenclature})
        )
        db.session.execute(stmt)
        # TODO: Check conditional values

    if current_app.config["IMPORT"]["FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"]:
        for field in BibFields.query.filter(BibFields.mnemonique != None).all():
            fields[field.name_field] = field
            # Set default nomenclature for empty user fields
            (
                ImportSyntheseData.query.filter(ImportSyntheseData.imprt == imprt)
                .filter(sa.or_(source_field == None, source_field == ""))
                .update(
                    values={
                        field.synthese_field: sa.func.gn_synthese.get_default_nomenclature_value(
                            field.mnemonique,
                        ),
                    },
                    synchronize_session=False,
                )
            )

    for field in filter(lambda field: field.mnemonique != None, fields.values()):
        source_field = getattr(ImportSyntheseData, field.source_field)
        synthese_field = getattr(ImportSyntheseData, field.synthese_field)
        # Note: with a correct contentmapping, no errors should occur.
        report_erroneous_rows(
            imprt,
            error_type="INVALID_NOMENCLATURE",
            error_column=field.name_field,
            whereclause=sa.and_(
                source_field != None,
                source_field != "",
                synthese_field == None,
            )
        )


def set_the_geom_column(imprt, fields, df):
    file_srid = imprt.srid
    local_srid = db.session.execute(
        sa.func.Find_SRID("ref_geo", "l_areas", "geom")
    ).scalar()
    geom_col = df[df["_geom"].notna()]["_geom"]
    if file_srid == 4326:
        df["the_geom_4326"] = geom_col.apply(
            lambda geom: ST_GeomFromWKB(geom.wkb, file_srid)
        )
        fields["the_geom_4326"] = BibFields.query.filter_by(
            name_field="the_geom_4326"
        ).one()
    elif file_srid == local_srid:
        df["the_geom_local"] = geom_col.apply(
            lambda geom: ST_GeomFromWKB(geom.wkb, file_srid)
        )
        fields["the_geom_local"] = BibFields.query.filter_by(
            name_field="the_geom_local"
        ).one()
    else:
        df["the_geom_4326"] = geom_col.apply(
            lambda geom: ST_Transform(ST_GeomFromWKB(geom.wkb, file_srid), 4326)
        )
        fields["the_geom_4326"] = BibFields.query.filter_by(
            name_field="the_geom_4326"
        ).one()


def set_column_from_referential(imprt, field, reference, error_type):
    source_field = getattr(ImportSyntheseData, field.source_field)
    synthese_field = getattr(ImportSyntheseData, field.synthese_field)
    stmt = (
        update(ImportSyntheseData)
        .values({
            synthese_field: reference,
        })
        .where(
            source_field == sa.cast(reference, sa.Unicode)
        )
    )
    db.session.execute(stmt)
    report_erroneous_rows(
        imprt,
        error_type=error_type,
        error_column=field.name_field,
        whereclause=sa.and_(
            source_field != None,
            source_field != "",
            synthese_field == None,
        )
    )


def set_cd_nom(imprt, fields):
    if "cd_nom" not in fields:
        return
    field = fields["cd_nom"]
    set_column_from_referential(imprt, field, Taxref.cd_nom, "CD_NOM_NOT_FOUND")


def set_cd_hab(imprt, fields):
    if "cd_hab" not in fields:
        return
    field = fields["cd_hab"]
    set_column_from_referential(imprt, field, Habref.cd_hab, "CD_HAB_NOT_FOUND")


def set_altitudes(imprt, fields):
    if not imprt.fieldmapping.get("altitudes_generate", False):
        return
    altitudes = (
        select([
            column("altitude_min"),
            column("altitude_max"),
        ])
        .select_from(
            func.ref_geo.fct_get_altitude_intersection(ImportSyntheseData.the_geom_local)
        )
        .lateral("altitudes")
    )
    cte = (
        select([
            ImportSyntheseData.id_import,
            ImportSyntheseData.line_no,
            altitudes.c.altitude_min,
            altitudes.c.altitude_max,
        ])
        .where(ImportSyntheseData.the_geom_local != None)
        .where(sa.or_(
            ImportSyntheseData.src_altitude_min == None,
            ImportSyntheseData.src_altitude_min == "",
            ImportSyntheseData.src_altitude_max == None,
            ImportSyntheseData.src_altitude_max == "",
        ))
        .cte("cte")
    )
    stmt = (
        update(ImportSyntheseData)
        .where(ImportSyntheseData.id_import == cte.c.id_import)
        .where(ImportSyntheseData.line_no == cte.c.line_no)
        .values({
            ImportSyntheseData.altitude_min: sa.case(
                whens=[
                    (
                        sa.or_(
                            ImportSyntheseData.src_altitude_min == None,
                            ImportSyntheseData.src_altitude_min == "",
                        ),
                        cte.c.altitude_min,
                    ),
                ],
                else_=ImportSyntheseData.altitude_min,
            ),
            ImportSyntheseData.altitude_max: sa.case(
                whens=[
                    (
                        sa.or_(
                            ImportSyntheseData.src_altitude_max == None,
                            ImportSyntheseData.src_altitude_max == "",
                        ),
                        cte.c.altitude_max,
                    ),
                ],
                else_=ImportSyntheseData.altitude_max,
            ),
        })

    )
    db.session.execute(stmt)


def get_duplicates_query(imprt, synthese_field, whereclause=sa.true()):
    whereclause = sa.and_(
        ImportSyntheseData.imprt == imprt,
        whereclause,
    )
    partitions = (
        select([
            array_agg(ImportSyntheseData.line_no).over(
                partition_by=synthese_field,
            ).label("duplicate_lines")
        ])
        .where(whereclause)
        .alias("partitions")
    )
    duplicates = (
        select([func.unnest(partitions.c.duplicate_lines).label("lines")])
        .where(func.array_length(partitions.c.duplicate_lines, 1) > 1)
        .alias("duplicates")
    )
    return duplicates


def set_uuid(imprt, fields):
    if "unique_id_sinp" in fields:
        field = fields["unique_id_sinp"]
        synthese_field = getattr(ImportSyntheseData, field.synthese_field)
        duplicates = get_duplicates_query(
            imprt,
            synthese_field, whereclause=synthese_field != None,
        )
        report_erroneous_rows(
            imprt,
            error_type="DUPLICATE_UUID",
            error_column=field.name_field,  # TODO: convert to csv col name
            whereclause=(
                ImportSyntheseData.line_no == duplicates.c.lines
            ),
        )
        report_erroneous_rows(
            imprt,
            error_type="EXISTING_UUID",
            error_column=field.name_field,  # TODO: convert to csv col name
            whereclause=sa.and_(
                ImportSyntheseData.unique_id_sinp == Synthese.unique_id_sinp,
                Synthese.id_dataset == imprt.id_dataset,
            ),
        )
    if imprt.fieldmapping.get("unique_id_sinp_generate", False):
        stmt = (
            update(ImportSyntheseData)
            .values({
                'unique_id_sinp': func.uuid_generate_v4(),
            })
            .where(ImportSyntheseData.imprt == imprt)
            .where(sa.or_(
                ImportSyntheseData.src_unique_id_sinp == None,
                ImportSyntheseData.src_unique_id_sinp == '',
            ))
            .where(ImportSyntheseData.unique_id_sinp == None)
        )
        db.session.execute(stmt)


def check_mandatory_fields(imprt, fields):
    for field in fields.values():
        if not field.mandatory or not field.synthese_field:
            continue
        source_field = getattr(ImportSyntheseData, field.source_column)
        whereclause = sa.or_(
            source_field == None,
            source_field == "",
        )
        report_erroneous_rows(
            imprt,
            error_type="MISSING_VALUE",
            error_column=field.name_field,  # TODO: convert to csv col name
            whereclause=whereclause,
        )


def check_duplicates_source_pk(imprt, fields):
    if "entity_source_pk_value" not in fields:
        return
    field = fields["entity_source_pk_value"]
    synthese_field = getattr(ImportSyntheseData, field.synthese_field)
    duplicates = get_duplicates_query(
        imprt,
        synthese_field,
        whereclause=sa.and_(
            synthese_field != None,
            synthese_field != "",
        ),
    )
    report_erroneous_rows(
        imprt,
        error_type="DUPLICATE_ENTITY_SOURCE_PK",
        error_column=field.name_field,  # TODO: convert to csv col name
        whereclause=(
            ImportSyntheseData.line_no == duplicates.c.lines
        ),
    )


def check_dates(imprt, fields):
    date_min_field = fields["datetime_min"]
    date_max_field = fields["datetime_max"]
    date_min_synthese_field = getattr(ImportSyntheseData, date_min_field.synthese_field)
    date_max_synthese_field = getattr(ImportSyntheseData, date_max_field.synthese_field)
    today = date.today()
    report_erroneous_rows(
        imprt,
        error_type="DATE_MIN_TOO_HIGH",
        error_column=date_min_field.name_field,  # TODO: convert to csv col name
        whereclause=(
            date_min_synthese_field > today
        ),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MAX_TOO_HIGH",
        error_column=date_max_field.name_field,  # TODO: convert to csv col name
        whereclause=sa.and_(
            date_max_synthese_field > today,
            date_min_synthese_field <= today,
        ),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MIN_SUP_DATE_MAX",
        error_column=date_min_field.name_field,  # TODO: convert to csv col name
        whereclause=(
            date_min_synthese_field > date_max_synthese_field
        ),
    )


def check_altitudes(imprt, fields):
    if "altitude_min" not in fields or "altitude_max" not in fields:
        return
    alti_min_field = fields["altitude_min"]
    alti_max_field = fields["altitude_max"]
    alti_min_synthese_field = getattr(ImportSyntheseData, alti_min_field.synthese_field)
    alti_max_synthese_field = getattr(ImportSyntheseData, alti_max_field.synthese_field)
    report_erroneous_rows(
        imprt,
        error_type="ALTI_MIN_SUP_ALTI_MAX",
        error_column=alti_min_field.name_field,  # TODO: convert to csv col name
        whereclause=(
            alti_min_synthese_field > alti_max_synthese_field
        ),
    )


def check_depths(imprt, fields):
    if "depth_min" not in fields or "depth_max" not in fields:
        return
    depth_min_field = fields["depth_min"]
    depth_max_field = fields["depth_max"]
    depth_min_synthese_field = getattr(ImportSyntheseData, depth_min_field.synthese_field)
    depth_max_synthese_field = getattr(ImportSyntheseData, depth_max_field.synthese_field)
    report_erroneous_rows(
        imprt,
        error_type="DEPTH_MIN_SUP_ALTI_MAX",  # Yes, there is a typo in db...
        error_column=depth_min_field.name_field,  # TODO: convert to csv col name
        whereclause=(
            depth_min_synthese_field > depth_max_synthese_field
        ),
    )


def set_geom_from_area_code(imprt, source_column, area_type_filter):
    # Find area in CTE, then update corresponding column in statement
    cte = (
        ImportSyntheseData.query
        .filter(
            ImportSyntheseData.imprt == imprt,
            ImportSyntheseData.valid == True,
            ImportSyntheseData.the_geom_4326 == None,
        )
        .join(
            LAreas, LAreas.area_code == source_column,
        )
        .join(
            LAreas.area_type, aliased=True,
        )
        .filter(area_type_filter)
        .with_entities(
            ImportSyntheseData.id_import,
            ImportSyntheseData.line_no,
            LAreas.id_area,
            LAreas.geom,
        )
        .cte("cte")
    )
    stmt = (
        update(ImportSyntheseData)
        .values({
            ImportSyntheseData.id_area_attachment: cte.c.id_area,
            ImportSyntheseData.the_geom_local: cte.c.geom,
            ImportSyntheseData.the_geom_4326: ST_Transform(cte.c.geom, 4326),
        })
        .where(ImportSyntheseData.id_import == cte.c.id_import)
        .where(ImportSyntheseData.line_no == cte.c.line_no)
    )
    db.session.execute(stmt)


def report_erroneous_rows(imprt, error_type, error_column, whereclause):
    cte = (
        update(ImportSyntheseData)
        .values({
            ImportSyntheseData.valid: False,
        })
        .where(ImportSyntheseData.imprt == imprt)
        .where(whereclause)
        .returning(ImportSyntheseData.line_no)
        .cte("cte")
    )
    error = (
        select([
            literal(imprt.id_import).label("id_import"),
            ImportUserErrorType.query.filter_by(
                name=error_type,
            )
            .with_entities(ImportUserErrorType.pk)
            .label("id_type"),
            array_agg(
                aggregate_order_by(cte.c.line_no, cte.c.line_no),
            )
            .label("rows"),
            literal(error_column).label("error_column"),
        ])
        .alias("error")
    )
    stmt = (
        insert(ImportUserError)
        .from_select(
            names=[
                ImportUserError.id_import,
                ImportUserError.id_type,
                ImportUserError.rows,
                ImportUserError.column,
            ],
            select=(
                select([error])
                .where(error.c.rows != None)
            ),
        )
    )
    db.session.execute(stmt)


def complete_others_geom_columns(imprt, fields):
    local_srid = db.session.execute(
        sa.func.Find_SRID("ref_geo", "l_areas", "geom")
    ).scalar()
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
                dest_col: ST_Transform(
                    getattr(ImportSyntheseData, source_col), dest_srid
                ),
            },
            synchronize_session=False,
        )
    )
    for name_field, area_type_filter in [
        ("codecommune", BibAreasTypes.type_code == "COM"),
        ("codedepartement", BibAreasTypes.type_code == "DEP"),
        ("codemaille", BibAreasTypes.type_code.in_(["M1", "M5", "M10"]))
    ]:
        if name_field not in fields:
            continue
        field = fields[name_field]
        source_column = getattr(ImportSyntheseData, field.source_field)
        # Set geom from area of the given type and with matching area_code:
        set_geom_from_area_code(imprt, source_column, area_type_filter)
        # Mark rows with code specified but geom still empty as invalid:
        report_erroneous_rows(
            imprt,
            error_type="INVALID_ATTACHMENT_CODE",
            error_column=field.name_field,  # TODO: convert to csv col name
            whereclause=sa.and_(
                ImportSyntheseData.the_geom_4326 == None,
                ImportSyntheseData.valid == True,
                source_column != None,
                source_column != "",
            ),
        )
    # Mark rows with no geometry as invalid:
    report_erroneous_rows(
        imprt,
        error_type="NO-GEOM",
        error_column="Colonnes géométriques",
        whereclause=sa.and_(
            ImportSyntheseData.the_geom_4326 == None,
            ImportSyntheseData.valid == True,
        ),
    )
    # Set the_geom_point:
    (
        ImportSyntheseData.query
        .filter(
            ImportSyntheseData.imprt == imprt,
            ImportSyntheseData.valid == True,
        )
        .update(
            values={
                ImportSyntheseData.the_geom_point: ST_Centroid(ImportSyntheseData.the_geom_4326),
            },
            synchronize_session=False,
        )
    )
