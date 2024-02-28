from datetime import date

from flask import current_app
from sqlalchemy import func
from sqlalchemy.sql.expression import select, update, insert, literal, join
from sqlalchemy.sql import column
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array_agg, aggregate_order_by
from geoalchemy2.functions import (
    ST_Transform,
    ST_IsValid,
    ST_Centroid,
    ST_GeomFromText,
    ST_MakePoint,
    ST_SetSRID,
)
from geonature.utils.env import db

from gn_module_import.models import (
    TImports,
    BibFields,
    ImportSyntheseData,
    ImportUserError,
    ImportUserErrorType,
)
from gn_module_import.utils import generated_fields

from geonature.core.gn_synthese.models import Synthese, TSources
from ref_geo.models import LAreas, BibAreasTypes
from pypnnomenclature.models import TNomenclatures, BibNomenclaturesTypes
from apptax.taxonomie.models import Taxref, CorNomListe, BibNoms
from pypn_habref_api.models import Habref


def do_nomenclatures_mapping(imprt, fields):
    # Set nomenclatures using content mapping
    for field in filter(lambda field: field.mnemonique != None, fields.values()):
        source_field = getattr(ImportSyntheseData, field.source_field)
        # This CTE return the list of source value / cd_nomenclature for a given nomenclature type
        cte = (
            select(column("key").label("value"), column("value").label("cd_nomenclature"))
            .select_from(sa.func.JSON_EACH_TEXT(TImports.contentmapping[field.mnemonique]))
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
        db.session.execute(stmt, execution_options=dict({"synchronize_session": "fetch"}))

    for field in BibFields.query.filter(BibFields.mnemonique != None).all():
        if (
            not current_app.config["IMPORT"]["FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"]
            and field.name_field in fields
        ):
            continue
        source_field = getattr(ImportSyntheseData, field.source_field)
        synthese_field = getattr(ImportSyntheseData, field.synthese_field)
        # Set default nomenclature for empty user fields
        (
            ImportSyntheseData.query.filter(ImportSyntheseData.imprt == imprt)
            .filter(
                sa.or_(source_field == None, source_field == ""),
                synthese_field == None,
            )
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
            whereclause=synthese_field == None,
        )


def check_nomenclatures(imprt, fields):
    if current_app.config["IMPORT"]["CHECK_EXIST_PROOF"]:
        nomenclature_field = BibFields.query.filter_by(
            name_field="id_nomenclature_exist_proof"
        ).one()
        digital_proof_field = fields.get("digital_proof")
        non_digital_proof_field = fields.get("non_digital_proof")
        if digital_proof_field is None and non_digital_proof_field is None:
            return
        oui = TNomenclatures.query.filter(
            TNomenclatures.nomenclature_type.has(
                BibNomenclaturesTypes.mnemonique == "PREUVE_EXIST"
            ),
            TNomenclatures.mnemonique == "Oui",
        ).one()
        oui_filter = (
            getattr(ImportSyntheseData, nomenclature_field.synthese_field) == oui.id_nomenclature
        )
        proof_set_filters = []
        if digital_proof_field is not None:
            proof_set_filters.append(
                sa.and_(
                    getattr(ImportSyntheseData, digital_proof_field.synthese_field) != None,
                    getattr(ImportSyntheseData, digital_proof_field.synthese_field) != "",
                )
            )
        if non_digital_proof_field is not None:
            proof_set_filters.append(
                sa.and_(
                    getattr(ImportSyntheseData, non_digital_proof_field.synthese_field) != None,
                    getattr(ImportSyntheseData, non_digital_proof_field.synthese_field) != "",
                )
            )
        proof_set_filter = sa.or_(*proof_set_filters) if proof_set_filters else sa.false()
        report_erroneous_rows(
            imprt,
            error_type="INVALID_EXISTING_PROOF_VALUE",
            error_column=nomenclature_field.name_field,
            whereclause=sa.or_(
                sa.and_(oui_filter, ~proof_set_filter),
                sa.and_(~oui_filter, proof_set_filter),
            ),
        )
    if (
        current_app.config["IMPORT"]["CHECK_PRIVATE_JDD_BLURING"]
        and not current_app.config["IMPORT"]["FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"]
        and imprt.dataset.nomenclature_data_origin.mnemonique == "Privée"
    ):
        blurring_field = BibFields.query.filter_by(name_field="id_nomenclature_blurring").one()
        report_erroneous_rows(
            imprt,
            error_type="CONDITIONAL_MANDATORY_FIELD_ERROR",
            error_column=blurring_field.name_field,
            whereclause=(getattr(ImportSyntheseData, blurring_field.synthese_field) == None),
        )
    if current_app.config["IMPORT"]["CHECK_REF_BIBLIO_LITTERATURE"]:
        litterature = TNomenclatures.query.filter(
            TNomenclatures.nomenclature_type.has(
                BibNomenclaturesTypes.mnemonique == "STATUT_SOURCE"
            ),
            TNomenclatures.cd_nomenclature == "Li",
        ).one()
        source_status_field = BibFields.query.filter_by(
            name_field="id_nomenclature_source_status"
        ).one()
        ref_biblio_field = BibFields.query.filter_by(name_field="reference_biblio").one()
        report_erroneous_rows(
            imprt,
            error_type="CONDITIONAL_MANDATORY_FIELD_ERROR",
            error_column=source_status_field.name_field,
            whereclause=sa.and_(
                getattr(ImportSyntheseData, source_status_field.synthese_field)
                == litterature.id_nomenclature,
                sa.or_(
                    getattr(ImportSyntheseData, ref_biblio_field.synthese_field) == None,
                    getattr(ImportSyntheseData, ref_biblio_field.synthese_field) == "",
                ),
            ),
        )


def check_referential(imprt, field, reference_field, error_type, reference_table=None):
    synthese_field = getattr(ImportSyntheseData, field.synthese_field)
    if reference_table is None:
        reference_table = reference_field.class_
    # We outerjoin the referential, and select rows where there is a value in synthese field
    # but no value in referential, which means no value in the referential matched synthese field.
    cte = (
        select(ImportSyntheseData.line_no)
        .select_from(
            join(
                ImportSyntheseData,
                reference_table,
                synthese_field == reference_field,
                isouter=True,
            )
        )
        .where(ImportSyntheseData.imprt == imprt)
        .where(synthese_field != None)
        .where(reference_field == None)
        .cte("invalid_ref")
    )
    report_erroneous_rows(
        imprt,
        error_type=error_type,
        error_column=field.name_field,
        whereclause=ImportSyntheseData.line_no == cte.c.line_no,
    )


def check_cd_nom(imprt, fields):
    if "cd_nom" not in fields:
        return
    field = fields["cd_nom"]
    whereclause = None
    # Filter out on a taxhub list if provided
    list_id = current_app.config["IMPORT"].get("ID_LIST_TAXA_RESTRICTION", None)
    if list_id is not None:
        reference_table = join(Taxref, BibNoms).join(
            CorNomListe,
            sa.and_(BibNoms.id_nom == CorNomListe.id_nom, CorNomListe.id_liste == list_id),
        )
    else:
        reference_table = Taxref
    check_referential(
        imprt, field, Taxref.cd_nom, "CD_NOM_NOT_FOUND", reference_table=reference_table
    )


def check_cd_hab(imprt, fields):
    if "cd_hab" not in fields:
        return
    field = fields["cd_hab"]
    check_referential(imprt, field, Habref.cd_hab, "CD_HAB_NOT_FOUND")


def set_altitudes(imprt, fields):
    if not imprt.fieldmapping.get("altitudes_generate", False):
        return
    altitudes = (
        select(
            column("altitude_min"),
            column("altitude_max"),
        )
        .select_from(func.ref_geo.fct_get_altitude_intersection(ImportSyntheseData.the_geom_local))
        .lateral("altitudes")
    )
    cte = (
        select(
            ImportSyntheseData.id_import,
            ImportSyntheseData.line_no,
            altitudes.c.altitude_min,
            altitudes.c.altitude_max,
        )
        .where(ImportSyntheseData.id_import == imprt.id_import)
        .where(ImportSyntheseData.the_geom_local != None)
        .where(
            sa.or_(
                ImportSyntheseData.src_altitude_min == None,
                ImportSyntheseData.src_altitude_min == "",
                ImportSyntheseData.src_altitude_max == None,
                ImportSyntheseData.src_altitude_max == "",
            )
        )
        .cte("cte")
    )
    stmt = (
        update(ImportSyntheseData)
        .where(ImportSyntheseData.id_import == cte.c.id_import)
        .where(ImportSyntheseData.line_no == cte.c.line_no)
        .values(
            {
                ImportSyntheseData.altitude_min: sa.case(
                    (
                        sa.or_(
                            ImportSyntheseData.src_altitude_min == None,
                            ImportSyntheseData.src_altitude_min == "",
                        ),
                        cte.c.altitude_min,
                    ),
                    else_=ImportSyntheseData.altitude_min,
                ),
                ImportSyntheseData.altitude_max: sa.case(
                    (
                        sa.or_(
                            ImportSyntheseData.src_altitude_max == None,
                            ImportSyntheseData.src_altitude_max == "",
                        ),
                        cte.c.altitude_max,
                    ),
                    else_=ImportSyntheseData.altitude_max,
                ),
            }
        )
    )
    fields.update(
        {
            "altitude_min": BibFields.query.filter_by(name_field="altitude_min").one(),
            "altitude_max": BibFields.query.filter_by(name_field="altitude_max").one(),
        }
    )
    db.session.execute(stmt.execution_options(synchronize_session="fetch"))


def get_duplicates_query(imprt, synthese_field, whereclause=sa.true()):
    whereclause = sa.and_(
        ImportSyntheseData.imprt == imprt,
        whereclause,
    )
    partitions = (
        select(
            array_agg(ImportSyntheseData.line_no)
            .over(
                partition_by=synthese_field,
            )
            .label("duplicate_lines")
        )
        .where(whereclause)
        .alias("partitions")
    )
    duplicates = (
        select(func.unnest(partitions.c.duplicate_lines).label("lines"))
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
            synthese_field,
            whereclause=synthese_field != None,
        )
        report_erroneous_rows(
            imprt,
            error_type="DUPLICATE_UUID",
            error_column=field.name_field,
            whereclause=(ImportSyntheseData.line_no == duplicates.c.lines),
        )
        report_erroneous_rows(
            imprt,
            error_type="EXISTING_UUID",
            error_column=field.name_field,
            whereclause=sa.and_(
                ImportSyntheseData.unique_id_sinp == Synthese.unique_id_sinp,
                Synthese.id_dataset == imprt.id_dataset,
                Synthese.source != imprt.source,
            ),
        )
    if imprt.fieldmapping.get(
        "unique_id_sinp_generate", current_app.config["IMPORT"]["DEFAULT_GENERATE_MISSING_UUID"]
    ):
        stmt = (
            update(ImportSyntheseData)
            .values(
                {
                    "unique_id_sinp": func.uuid_generate_v4(),
                }
            )
            .where(ImportSyntheseData.imprt == imprt)
            .where(
                sa.or_(
                    ImportSyntheseData.src_unique_id_sinp == None,
                    ImportSyntheseData.src_unique_id_sinp == "",
                )
            )
            .where(ImportSyntheseData.unique_id_sinp == None)
        )
        db.session.execute(stmt)


# Currently not used as done during dataframe checks
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
            error_column=field.name_field,
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
        error_column=field.name_field,
        whereclause=(ImportSyntheseData.line_no == duplicates.c.lines),
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
        error_column=date_min_field.name_field,
        whereclause=(date_min_synthese_field > today),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MAX_TOO_HIGH",
        error_column=date_max_field.name_field,
        whereclause=sa.and_(
            date_max_synthese_field > today,
            date_min_synthese_field <= today,
        ),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MIN_SUP_DATE_MAX",
        error_column=date_min_field.name_field,
        whereclause=(date_min_synthese_field > date_max_synthese_field),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MIN_TOO_LOW",
        error_column=date_min_field.name_field,
        whereclause=(date_min_synthese_field < date(1900, 1, 1)),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MAX_TOO_LOW",
        error_column=date_max_field.name_field,
        whereclause=(date_max_synthese_field < date(1900, 1, 1)),
    )


def check_altitudes(imprt, fields):
    if "altitude_min" in fields:
        alti_min_field = fields["altitude_min"]
        alti_min_name_field = alti_min_field.name_field
        alti_min_synthese_field = getattr(ImportSyntheseData, alti_min_field.synthese_field)
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=alti_min_name_field,
            whereclause=(alti_min_synthese_field < 0),
        )

    if "altitude_max" in fields:
        alti_max_field = fields["altitude_max"]
        alti_max_name_field = alti_max_field.name_field
        alti_max_synthese_field = getattr(ImportSyntheseData, alti_max_field.synthese_field)
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=alti_max_name_field,
            whereclause=(alti_max_synthese_field < 0),
        )

    if "altitude_min" in fields and "altitude_max" in fields:
        report_erroneous_rows(
            imprt,
            error_type="ALTI_MIN_SUP_ALTI_MAX",
            error_column=alti_min_name_field,
            whereclause=(alti_min_synthese_field > alti_max_synthese_field),
        )


def check_depths(imprt, fields):
    if "depth_min" in fields:
        depth_min_field = fields["depth_min"]
        depth_min_name_field = depth_min_field.name_field
        depth_min_synthese_field = getattr(ImportSyntheseData, depth_min_field.synthese_field)
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=depth_min_name_field,
            whereclause=(depth_min_synthese_field < 0),
        )

    if "depth_max" in fields:
        depth_max_field = fields["depth_max"]
        depth_max_name_field = depth_max_field.name_field
        depth_max_synthese_field = getattr(ImportSyntheseData, depth_max_field.synthese_field)
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=depth_max_name_field,
            whereclause=(depth_max_synthese_field < 0),
        )

    if "depth_min" in fields and "depth_max" in fields:
        report_erroneous_rows(
            imprt,
            error_type="DEPTH_MIN_SUP_ALTI_MAX",  # Yes, there is a typo in db... Should be "DEPTH_MIN_SUP_DEPTH_MAX"
            error_column=depth_min_name_field,
            whereclause=(depth_min_synthese_field > depth_max_synthese_field),
        )


def check_digital_proof_urls(imprt, fields):
    if "digital_proof" not in fields:
        return
    digital_proof_field = fields["digital_proof"]
    digital_proof_synthese_field = getattr(ImportSyntheseData, digital_proof_field.synthese_field)
    report_erroneous_rows(
        imprt,
        error_type="INVALID_URL_PROOF",
        error_column=digital_proof_field.name_field,
        whereclause=(
            sa.and_(
                digital_proof_synthese_field is not None,
                digital_proof_synthese_field != "",
                digital_proof_synthese_field.op("!~")(
                    r"^(?:(?:https?|ftp):\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+$"
                ),
            )
        ),
    )


def set_geom_from_area_code(imprt, source_column, area_type_filter):
    # Find area in CTE, then update corresponding column in statement
    cte = (
        ImportSyntheseData.query.filter(
            ImportSyntheseData.imprt == imprt,
            ImportSyntheseData.valid == True,
            ImportSyntheseData.the_geom_4326 == None,
        )
        .join(
            LAreas,
            LAreas.area_code == source_column,
        )
        .join(
            LAreas.area_type,
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
        .values(
            {
                ImportSyntheseData.id_area_attachment: cte.c.id_area,
                ImportSyntheseData.the_geom_local: cte.c.geom,
                ImportSyntheseData.the_geom_4326: ST_Transform(cte.c.geom, 4326),
            }
        )
        .where(ImportSyntheseData.id_import == cte.c.id_import)
        .where(ImportSyntheseData.line_no == cte.c.line_no)
    )
    db.session.execute(stmt.execution_options(synchronize_session="fetch"))


def report_erroneous_rows(imprt, error_type, error_column, whereclause):
    error_type = ImportUserErrorType.query.filter_by(name=error_type).one()
    error_column = generated_fields.get(error_column, error_column)
    error_column = imprt.fieldmapping.get(error_column, error_column)
    if error_type.level == "ERROR":
        cte = (
            update(ImportSyntheseData)
            .values(
                {
                    ImportSyntheseData.valid: False,
                }
            )
            .where(ImportSyntheseData.imprt == imprt)
            .where(whereclause)
            .returning(ImportSyntheseData.line_no)
            .cte("cte")
        )
    else:
        cte = (
            select(ImportSyntheseData.line_no)
            .where(ImportSyntheseData.imprt == imprt)
            .where(whereclause)
            .cte("cte")
        )
    error = select(
        literal(imprt.id_import).label("id_import"),
        literal(error_type.pk).label("id_type"),
        array_agg(
            aggregate_order_by(cte.c.line_no, cte.c.line_no),
        ).label("rows"),
        literal(error_column).label("error_column"),
    ).alias("error")
    stmt = insert(ImportUserError).from_select(
        names=[
            ImportUserError.id_import,
            ImportUserError.id_type,
            ImportUserError.rows,
            ImportUserError.column,
        ],
        select=(select(error).where(error.c.rows != None)),
    )
    db.session.execute(stmt)


def complete_others_geom_columns(imprt, fields):
    local_srid = db.session.execute(sa.func.Find_SRID("ref_geo", "l_areas", "geom")).scalar()
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
        ImportSyntheseData.query.filter(
            ImportSyntheseData.imprt == imprt,
            ImportSyntheseData.valid == True,
            getattr(ImportSyntheseData, source_col) != None,
        ).update(
            values={
                dest_col: ST_Transform(getattr(ImportSyntheseData, source_col), dest_srid),
            },
            synchronize_session=False,
        )
    )
    for name_field, area_type_filter in [
        ("codecommune", BibAreasTypes.type_code == "COM"),
        ("codedepartement", BibAreasTypes.type_code == "DEP"),
        ("codemaille", BibAreasTypes.type_code.in_(["M1", "M5", "M10"])),
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
            error_column=field.name_field,
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
        ImportSyntheseData.query.filter(
            ImportSyntheseData.imprt == imprt,
            ImportSyntheseData.valid == True,
        ).update(
            values={
                ImportSyntheseData.the_geom_point: ST_Centroid(ImportSyntheseData.the_geom_4326),
            },
            synchronize_session=False,
        )
    )


def check_is_valid_geography(imprt, fields):
    if "WKT" in fields:
        # It is useless to check valid WKT when created from X/Y
        where_clause = sa.and_(
            ImportSyntheseData.src_WKT != None,
            ImportSyntheseData.src_WKT != "",
            sa.not_(ST_IsValid(ImportSyntheseData.the_geom_4326)),
        )
        report_erroneous_rows(
            imprt,
            error_type="INVALID_GEOMETRY",
            error_column="WKT",
            whereclause=where_clause,
        )


def check_geography_outside(imprt, fields):
    id_area = current_app.config["IMPORT"]["ID_AREA_RESTRICTION"]
    if id_area:
        local_srid = db.session.execute(sa.func.Find_SRID("ref_geo", "l_areas", "geom")).scalar()
        area = LAreas.query.filter(LAreas.id_area == id_area).one()
        report_erroneous_rows(
            imprt,
            error_type="GEOMETRY_OUTSIDE",
            error_column="Champs géométriques",
            whereclause=sa.and_(
                ImportSyntheseData.valid == True,
                ImportSyntheseData.the_geom_local.ST_Disjoint(area.geom),
            ),
        )
