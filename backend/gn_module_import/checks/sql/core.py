import sqlalchemy as sa

from geonature.utils.env import db

from gn_module_import.models import (
    Entity,
    EntityField,
    BibFields,
)
from gn_module_import.checks.sql.utils import report_erroneous_rows


__all__ = ["init_rows_validity", "check_orphan_rows"]


def init_rows_validity(imprt):
    """
    Validity columns are three-states:
      - None: the row does not contains data for the given entity
      - False: the row contains data for the given entity, but data are erroneous
      - True: the row contains data for the given entity, and data are valid
    """
    transient_table = imprt.destination.get_transient_table()
    entities = (
        Entity.query.filter_by(destination=imprt.destination).order_by(sa.desc(Entity.order)).all()
    )
    # Set validity=NULL (not parcicipating in the entity) for all rows
    db.session.execute(
        sa.update(transient_table)
        .where(transient_table.c.id_import == imprt.id_import)
        .values({entity.validity_column: None for entity in entities})
    )
    # TODO handle multi columns
    # Currently if only a multi-fields is mapped, it will be ignored without raising any error.
    # This is not a very big issue as it is unlikely to map **only** a multi-field.
    selected_fields_names = [
        field_name
        for field_name, source_field in imprt.fieldmapping.items()
        if source_field in imprt.columns
    ]
    for entity in entities:
        # Select fields associated to this entity *and only to this entity*
        fields = (
            db.session.query(BibFields)
            .filter(BibFields.name_field.in_(selected_fields_names))
            .filter(BibFields.entities.any(EntityField.entity == entity))
            .filter(~BibFields.entities.any(EntityField.entity != entity))
            .all()
        )
        # Set validity=True for rows with not null field associated to this entity
        db.session.execute(
            sa.update(transient_table)
            .where(transient_table.c.id_import == imprt.id_import)
            .where(
                sa.or_(*[transient_table.c[field.source_column].isnot(None) for field in fields])
            )
            .values({entity.validity_column: True})
        )
    # Rows with values only in fields shared between several entities will be ignored here.
    # But they will raise an error through check_orphan_rows.


def check_orphan_rows(imprt):
    transient_table = imprt.destination.get_transient_table()
    # TODO: handle multi-source fields
    # This is actually not a big issue as multi-source fields are unlikely to also be multi-entity fields.
    selected_fields_names = [
        field_name
        for field_name, source_field in imprt.fieldmapping.items()
        if source_field in imprt.columns
    ]
    # Select fields associated to multiple entities
    AllEntityField = sa.orm.aliased(EntityField)
    fields = (
        db.session.query(BibFields)
        .join(EntityField)
        .join(Entity)
        .order_by(Entity.order)  # errors are associated to the first Entity
        .filter(BibFields.name_field.in_(selected_fields_names))
        .join(AllEntityField, AllEntityField.id_field == BibFields.id_field)
        .group_by(BibFields.id_field, EntityField.id_field, Entity.id_entity)
        .having(sa.func.count(AllEntityField.id_entity) > 1)
        .all()
    )
    for field in fields:
        report_erroneous_rows(
            imprt,
            entity=None,  # OK because ORPHAN_ROW has only WARNING level
            error_type="ORPHAN_ROW",
            error_column=field.name_field,
            whereclause=sa.and_(
                *[transient_table.c[col].is_(None) for col in imprt.destination.validity_columns]
            ),
        )


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