from sqlalchemy import func
from sqlalchemy.sql.expression import select, update, insert, literal
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import array_agg, aggregate_order_by

from geonature.utils.env import db

from gn_module_import.models import (
    ImportUserError,
    ImportUserErrorType,
)
from gn_module_import.utils import generated_fields


__all__ = ["get_duplicates_query", "report_erroneous_rows"]


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
        select([func.unnest(partitions.c.duplicate_lines).label("lines")])
        .where(func.array_length(partitions.c.duplicate_lines, 1) > 1)
        .alias("duplicates")
    )
    return duplicates


def report_erroneous_rows(imprt, entity, error_type, error_column, whereclause):
    """
    This function report errors where whereclause in true.
    But the function also set validity column to False for errors with ERROR level.
    """
    transient_table = imprt.destination.get_transient_table()
    error_type = ImportUserErrorType.query.filter_by(name=error_type).one()
    error_column = generated_fields.get(error_column, error_column)
    error_column = imprt.fieldmapping.get(error_column, error_column)
    if error_type.level == "ERROR":
        cte = (
            update(transient_table)
            .values(
                {
                    transient_table.c[entity.validity_column]: False,
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