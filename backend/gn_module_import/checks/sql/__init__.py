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
    transient_table = imprt.destination.get_transient_table()
    # Set nomenclatures using content mapping
    for field in filter(lambda field: field.mnemonique != None, fields.values()):
        source_field = transient_table.c[field.source_field]
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
            update(transient_table)
            .where(transient_table.c.id_import == imprt.id_import)
            .where(source_field == cte.c.value)
            .where(TNomenclatures.cd_nomenclature == cte.c.cd_nomenclature)
            .where(BibNomenclaturesTypes.mnemonique == field.mnemonique)
            .where(TNomenclatures.id_type == BibNomenclaturesTypes.id_type)
            .values({field.dest_field: TNomenclatures.id_nomenclature})
        )
        db.session.execute(stmt)

    for field in BibFields.query.filter(
        BibFields.destination == imprt.destination, BibFields.mnemonique != None
    ).all():
        if (
            not current_app.config["IMPORT"]["FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"]
            and field.name_field in fields
        ):
            continue
        source_field = transient_table.c[field.source_field]
        dest_field = transient_table.c[field.dest_field]
        # Set default nomenclature for empty user fields
        stmt = (
            update(transient_table)
            .where(transient_table.c.id_import == imprt.id_import)
            .where(sa.or_(source_field == None, source_field == ""))
            .where(dest_field == None)
            .values(
                {
                    field.dest_field: sa.func.gn_synthese.get_default_nomenclature_value(
                        field.mnemonique,
                    )
                }
            )
        )
        db.session.execute(stmt)

    for field in filter(lambda field: field.mnemonique != None, fields.values()):
        source_field = transient_table.c[field.source_field]
        dest_field = transient_table.c[field.dest_field]
        # Note: with a correct contentmapping, no errors should occur.
        report_erroneous_rows(
            imprt,
            error_type="INVALID_NOMENCLATURE",
            error_column=field.name_field,
            whereclause=dest_field == None,
        )


def check_nomenclatures(imprt, fields):
    transient_table = imprt.destination.get_transient_table()
    if current_app.config["IMPORT"]["CHECK_EXIST_PROOF"]:
        nomenclature_field = BibFields.query.filter_by(
            destination=imprt.destination, name_field="id_nomenclature_exist_proof"
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
        oui_filter = transient_table.c[nomenclature_field.dest_field] == oui.id_nomenclature
        proof_set_filters = []
        if digital_proof_field is not None:
            proof_set_filters.append(
                sa.and_(
                    transient_table.c[digital_proof_field.dest_field] != None,
                    transient_table.c[digital_proof_field.dest_field] != "",
                )
            )
        if non_digital_proof_field is not None:
            proof_set_filters.append(
                sa.and_(
                    transient_table.c[non_digital_proof_field.dest_field] != None,
                    transient_table.c[non_digital_proof_field.dest_field] != "",
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
        blurring_field = BibFields.query.filter_by(
            destination=imprt.destination, name_field="id_nomenclature_blurring"
        ).one()
        report_erroneous_rows(
            imprt,
            error_type="CONDITIONAL_MANDATORY_FIELD_ERROR",
            error_column=blurring_field.name_field,
            whereclause=(transient_table.c[blurring_field.dest_field] == None),
        )
    if current_app.config["IMPORT"]["CHECK_REF_BIBLIO_LITTERATURE"]:
        litterature = TNomenclatures.query.filter(
            TNomenclatures.nomenclature_type.has(
                BibNomenclaturesTypes.mnemonique == "STATUT_SOURCE"
            ),
            TNomenclatures.cd_nomenclature == "Li",
        ).one()
        source_status_field = BibFields.query.filter_by(
            destination=imprt.destination, name_field="id_nomenclature_source_status"
        ).one()
        ref_biblio_field = BibFields.query.filter_by(
            destination=imprt.destination, name_field="reference_biblio"
        ).one()
        report_erroneous_rows(
            imprt,
            error_type="CONDITIONAL_MANDATORY_FIELD_ERROR",
            error_column=source_status_field.name_field,
            whereclause=sa.and_(
                transient_table.c[source_status_field.dest_field] == litterature.id_nomenclature,
                sa.or_(
                    transient_table.c[ref_biblio_field.dest_field] == None,
                    transient_table.c[ref_biblio_field.dest_field] == "",
                ),
            ),
        )


def check_referential(imprt, field, reference_field, error_type, reference_table=None):
    transient_table = imprt.destination.get_transient_table()
    dest_field = transient_table.c[field.dest_field]
    if reference_table is None:
        reference_table = reference_field.class_
    # We outerjoin the referential, and select rows where there is a value in synthese field
    # but no value in referential, which means no value in the referential matched synthese field.
    cte = (
        select(transient_table.c.line_no)
        .select_from(
            join(
                transient_table,
                reference_table,
                dest_field == reference_field,
                isouter=True,
            )
        )
        .where(transient_table.c.id_import == imprt.id_import)
        .where(dest_field != None)
        .where(reference_field == None)
        .cte("invalid_ref")
    )
    report_erroneous_rows(
        imprt,
        error_type=error_type,
        error_column=field.name_field,
        whereclause=transient_table.c.line_no == cte.c.line_no,
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
    transient_table = imprt.destination.get_transient_table()
    altitudes = (
        select(
            column("altitude_min"),
            column("altitude_max"),
        )
        .select_from(func.ref_geo.fct_get_altitude_intersection(transient_table.c.the_geom_local))
        .lateral("altitudes")
    )
    cte = (
        select(
            transient_table.c.id_import,
            transient_table.c.line_no,
            altitudes.c.altitude_min,
            altitudes.c.altitude_max,
        )
        .where(transient_table.c.id_import == imprt.id_import)
        .where(transient_table.c.the_geom_local != None)
        .where(
            sa.or_(
                transient_table.c.src_altitude_min == None,
                transient_table.c.src_altitude_min == "",
                transient_table.c.src_altitude_max == None,
                transient_table.c.src_altitude_max == "",
            )
        )
        .cte("cte")
    )
    stmt = (
        update(transient_table)
        .where(transient_table.c.id_import == cte.c.id_import)
        .where(transient_table.c.line_no == cte.c.line_no)
        .values(
            {
                transient_table.c.altitude_min: sa.case(
                    (
                        sa.or_(
                            transient_table.c.src_altitude_min == None,
                            transient_table.c.src_altitude_min == "",
                        ),
                        cte.c.altitude_min,
                    ),
                    else_=transient_table.c.altitude_min,
                ),
                transient_table.c.altitude_max: sa.case(
                    (
                        sa.or_(
                            transient_table.c.src_altitude_max == None,
                            transient_table.c.src_altitude_max == "",
                        ),
                        cte.c.altitude_max,
                    ),
                    else_=transient_table.c.altitude_max,
                ),
            }
        )
    )
    fields.update(
        {
            "altitude_min": BibFields.query.filter_by(
                destination=imprt.destination, name_field="altitude_min"
            ).one(),
            "altitude_max": BibFields.query.filter_by(
                destination=imprt.destination, name_field="altitude_max"
            ).one(),
        }
    )
    db.session.execute(stmt)


def get_duplicates_query(imprt, dest_field, whereclause=sa.true()):
    transient_table = imprt.destination.get_transient_table()
    whereclause = sa.and_(
        transient_table.c.id_import == imprt.id_import,
        whereclause,
    )
    partitions = (
        select(
            array_agg(transient_table.c.line_no)
            .over(
                partition_by=dest_field,
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
        transient_table = imprt.destination.get_transient_table()
        dest_field = transient_table.c[field.dest_field]
        duplicates = get_duplicates_query(
            imprt,
            dest_field,
            whereclause=dest_field != None,
        )
        report_erroneous_rows(
            imprt,
            error_type="DUPLICATE_UUID",
            error_column=field.name_field,
            whereclause=(transient_table.c.line_no == duplicates.c.lines),
        )
        report_erroneous_rows(
            imprt,
            error_type="EXISTING_UUID",
            error_column=field.name_field,
            whereclause=sa.and_(
                transient_table.c.unique_id_sinp == Synthese.unique_id_sinp,
                Synthese.id_dataset == imprt.id_dataset,
                Synthese.source != imprt.source,
            ),
        )
    if imprt.fieldmapping.get(
        "unique_id_sinp_generate", current_app.config["IMPORT"]["DEFAULT_GENERATE_MISSING_UUID"]
    ):
        transient_table = imprt.destination.get_transient_table()
        stmt = (
            update(transient_table)
            .values(
                {
                    "unique_id_sinp": func.uuid_generate_v4(),
                }
            )
            .where(transient_table.c.id_import == imprt.id_import)
            .where(
                sa.or_(
                    transient_table.c.src_unique_id_sinp == None,
                    transient_table.c.src_unique_id_sinp == "",
                )
            )
            .where(transient_table.c.unique_id_sinp == None)
        )
        db.session.execute(stmt)


# Currently not used as done during dataframe checks
def check_mandatory_fields(imprt, fields):
    for field in fields.values():
        if not field.mandatory or not field.dest_field:
            continue
        transient_table = imprt.destination.get_transient_table()
        source_field = transient_table.c[field.source_column]
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
    transient_table = imprt.destination.get_transient_table()
    dest_field = transient_table.c[field.dest_field]
    duplicates = get_duplicates_query(
        imprt,
        dest_field,
        whereclause=sa.and_(
            dest_field != None,
            dest_field != "",
        ),
    )
    report_erroneous_rows(
        imprt,
        error_type="DUPLICATE_ENTITY_SOURCE_PK",
        error_column=field.name_field,
        whereclause=(transient_table.c.line_no == duplicates.c.lines),
    )


def check_dates(imprt, fields):
    transient_table = imprt.destination.get_transient_table()
    date_min_field = fields["datetime_min"]
    date_max_field = fields["datetime_max"]
    date_min_dest_field = transient_table.c[date_min_field.dest_field]
    date_max_dest_field = transient_table.c[date_max_field.dest_field]
    today = date.today()
    report_erroneous_rows(
        imprt,
        error_type="DATE_MIN_TOO_HIGH",
        error_column=date_min_field.name_field,
        whereclause=(date_min_dest_field > today),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MAX_TOO_HIGH",
        error_column=date_max_field.name_field,
        whereclause=sa.and_(
            date_max_dest_field > today,
            date_min_dest_field <= today,
        ),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MIN_SUP_DATE_MAX",
        error_column=date_min_field.name_field,
        whereclause=(date_min_dest_field > date_max_dest_field),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MIN_TOO_LOW",
        error_column=date_min_field.name_field,
        whereclause=(date_min_dest_field < date(1900, 1, 1)),
    )
    report_erroneous_rows(
        imprt,
        error_type="DATE_MAX_TOO_LOW",
        error_column=date_max_field.name_field,
        whereclause=(date_max_dest_field < date(1900, 1, 1)),
    )


def check_altitudes(imprt, fields):
    transient_table = imprt.destination.get_transient_table()
    if "altitude_min" in fields:
        alti_min_field = fields["altitude_min"]
        alti_min_name_field = alti_min_field.name_field
        alti_min_dest_field = transient_table.c[alti_min_field.dest_field]
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=alti_min_name_field,
            whereclause=(alti_min_dest_field < 0),
        )

    if "altitude_max" in fields:
        alti_max_field = fields["altitude_max"]
        alti_max_name_field = alti_max_field.name_field
        alti_max_dest_field = transient_table.c[alti_max_field.dest_field]
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=alti_max_name_field,
            whereclause=(alti_max_dest_field < 0),
        )

    if "altitude_min" in fields and "altitude_max" in fields:
        report_erroneous_rows(
            imprt,
            error_type="ALTI_MIN_SUP_ALTI_MAX",
            error_column=alti_min_name_field,
            whereclause=(alti_min_dest_field > alti_max_dest_field),
        )


def check_depths(imprt, fields):
    transient_table = imprt.destination.get_transient_table()
    if "depth_min" in fields:
        depth_min_field = fields["depth_min"]
        depth_min_name_field = depth_min_field.name_field
        depth_min_dest_field = transient_table.c[depth_min_field.dest_field]
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=depth_min_name_field,
            whereclause=(depth_min_dest_field < 0),
        )

    if "depth_max" in fields:
        depth_max_field = fields["depth_max"]
        depth_max_name_field = depth_max_field.name_field
        depth_max_dest_field = transient_table.c[depth_max_field.dest_field]
        report_erroneous_rows(
            imprt,
            error_type="INVALID_INTEGER",
            error_column=depth_max_name_field,
            whereclause=(depth_max_dest_field < 0),
        )

    if "depth_min" in fields and "depth_max" in fields:
        report_erroneous_rows(
            imprt,
            error_type="DEPTH_MIN_SUP_ALTI_MAX",  # Yes, there is a typo in db... Should be "DEPTH_MIN_SUP_DEPTH_MAX"
            error_column=depth_min_name_field,
            whereclause=(depth_min_dest_field > depth_max_dest_field),
        )


def check_digital_proof_urls(imprt, fields):
    if "digital_proof" not in fields:
        return
    transient_table = imprt.destination.get_transient_table()
    digital_proof_field = fields["digital_proof"]
    digital_proof_dest_field = transient_table.c[digital_proof_field.dest_field]
    report_erroneous_rows(
        imprt,
        error_type="INVALID_URL_PROOF",
        error_column=digital_proof_field.name_field,
        whereclause=(
            sa.and_(
                digital_proof_dest_field is not None,
                digital_proof_dest_field != "",
                digital_proof_dest_field.op("!~")(
                    r"^(?:(?:https?|ftp):\/\/)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&'\(\)\*\+,;=.]+$"
                ),
            )
        ),
    )


def set_geom_from_area_code(imprt, source_column, area_type_filter):
    transient_table = imprt.destination.get_transient_table()
    # Find area in CTE, then update corresponding column in statement
    cte = (
        select(
            transient_table.c.id_import,
            transient_table.c.line_no,
            LAreas.id_area,
            LAreas.geom,
            # TODO: add LAreas.geom_4326
        )
        .select_from(
            join(transient_table, LAreas, source_column == LAreas.area_code).join(BibAreasTypes)
        )
        .where(transient_table.c.id_import == imprt.id_import)
        .where(transient_table.c.valid == True)
        .where(transient_table.c.the_geom_4326 == None)
        .where(area_type_filter)
        .cte("cte")
    )
    stmt = (
        update(transient_table)
        .values(
            {
                transient_table.c.id_area_attachment: cte.c.id_area,
                transient_table.c.the_geom_local: cte.c.geom,
                transient_table.c.the_geom_4326: ST_Transform(
                    cte.c.geom, 4326
                ),  # TODO: replace with cte.c.geom_4326
            }
        )
        .where(transient_table.c.id_import == cte.c.id_import)
        .where(transient_table.c.line_no == cte.c.line_no)
    )
    db.session.execute(stmt)


def report_erroneous_rows(imprt, error_type, error_column, whereclause):
    transient_table = imprt.destination.get_transient_table()
    error_type = ImportUserErrorType.query.filter_by(name=error_type).one()
    error_column = generated_fields.get(error_column, error_column)
    error_column = imprt.fieldmapping.get(error_column, error_column)
    if error_type.level == "ERROR":
        cte = (
            update(transient_table)
            .values(
                {
                    transient_table.c.valid: False,
                }
            )
            .where(transient_table.c.id_import == imprt.id_import)
            .where(whereclause)
            .returning(transient_table.c.line_no)
            .cte("cte")
        )
    else:
        cte = (
            select(transient_table.c.line_no)
            .where(transient_table.c.id_import == imprt.id_import)
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
    transient_table = imprt.destination.get_transient_table()
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
    stmt = (
        update(transient_table)
        .where(transient_table.c.id_import == imprt.id_import)
        .where(transient_table.c.valid == True)
        .where(transient_table.c[source_col] != None)
        .values(
            {
                dest_col: ST_Transform(transient_table.c[source_col], dest_srid),
            }
        )
    )
    db.session.execute(stmt)
    for name_field, area_type_filter in [
        ("codecommune", BibAreasTypes.type_code == "COM"),
        ("codedepartement", BibAreasTypes.type_code == "DEP"),
        ("codemaille", BibAreasTypes.type_code.in_(["M1", "M5", "M10"])),
    ]:
        if name_field not in fields:
            continue
        field = fields[name_field]
        source_column = transient_table.c[field.source_field]
        # Set geom from area of the given type and with matching area_code:
        set_geom_from_area_code(imprt, source_column, area_type_filter)
        # Mark rows with code specified but geom still empty as invalid:
        report_erroneous_rows(
            imprt,
            error_type="INVALID_ATTACHMENT_CODE",
            error_column=field.name_field,
            whereclause=sa.and_(
                transient_table.c.the_geom_4326 == None,
                transient_table.c.valid == True,
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
            transient_table.c.the_geom_4326 == None,
            transient_table.c.valid == True,
        ),
    )
    # Set the_geom_point:
    stmt = (
        update(transient_table)
        .where(transient_table.c.id_import == imprt.id_import)
        .where(transient_table.c.valid == True)
        .values(
            {
                "the_geom_point": ST_Centroid(transient_table.c.the_geom_4326),
            }
        )
    )
    db.session.execute(stmt)


def check_is_valid_geography(imprt, fields):
    # It is useless to check valid WKT when created from X/Y
    if "WKT" in fields:
        transient_table = imprt.destination.get_transient_table()
        where_clause = sa.and_(
            transient_table.c.src_WKT != None,
            transient_table.c.src_WKT != "",
            sa.not_(ST_IsValid(transient_table.c.the_geom_4326)),
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
        transient_table = imprt.destination.get_transient_table()
        area = LAreas.query.filter(LAreas.id_area == id_area).one()
        report_erroneous_rows(
            imprt,
            error_type="GEOMETRY_OUTSIDE",
            error_column="Champs géométriques",
            whereclause=sa.and_(
                transient_table.c.valid == True,
                transient_table.c.the_geom_local.ST_Disjoint(area.geom),
            ),
        )
