from typing import Dict
from uuid import uuid4
from itertools import chain

from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.dialects.postgresql import insert as pg_insert
from flask import current_app

from geonature.utils.env import db
from geonature.core.gn_synthese.models import Synthese
from geonature.utils.sentry import start_sentry_child

from gn_module_import.models import ImportUserError, ImportUserErrorType, BibFields
from gn_module_import.utils import generated_fields

from .missing import clean_missing_values, check_required_values
from .geography import check_geography
from .types import check_types
from .dates import concat_dates


def update_dicts(generator, **kwargs):
    for item in generator:
        item.update(**kwargs)
        yield item


def _check_duplicate(df, field):
    duplicated = df[field].duplicated(keep=False)
    yield {
        "invalid_rows": df[df[field].notnull() & duplicated],
    }


def _check_ordering(df, min_field, max_field):
    ordered = df[min_field] <= df[max_field]
    ordered = ordered.fillna(False)
    invalid_rows = df[~ordered & df[min_field].notna() & df[max_field].notna()]
    yield dict(invalid_rows=invalid_rows)


def check_counts(df, fields: Dict[str, BibFields]):
    default_count = current_app.config["IMPORT"]["DEFAULT_COUNT_VALUE"]
    if "count_min" in fields:
        count_min_field = fields["count_min"]
        df[count_min_field.dest_field] = df[count_min_field.dest_field].where(
            df[count_min_field.dest_field].notna(),
            other=default_count,
        )
        if "count_max" in fields:
            count_max_field = fields["count_max"]
            yield from update_dicts(
                _check_ordering(df, count_min_field.dest_field, count_max_field.dest_field),
                column="count_min",
                error_code="COUNT_MIN_SUP_COUNT_MAX",
            )
            df[count_max_field.dest_field] = df[count_max_field.dest_field].where(
                df[count_max_field.dest_field].notna(),
                other=df[count_min_field.dest_field],
            )
        else:
            count_max_field = BibFields.query.filter_by(name_field="count_max").one()
            fields["count_max"] = count_max_field
            df[count_max_field.dest_field] = df[count_min_field.dest_field]
    else:
        if "count_max" in fields:
            count_max_field = fields["count_max"]
            count_min_field = BibFields.query.filter_by(name_field="count_min").one()
            fields["count_min"] = count_min_field
            df[count_max_field.dest_field] = df[count_max_field.dest_field].where(
                df[count_max_field.dest_field].notna(),
                other=default_count,
            )
            df[count_min_field.dest_field] = df[count_max_field.dest_field]
        else:
            count_min_field = BibFields.query.filter_by(name_field="count_min").one()
            fields["count_min"] = count_min_field
            df[count_min_field.dest_field] = default_count
            count_max_field = BibFields.query.filter_by(name_field="count_max").one()
            fields["count_max"] = count_max_field
            df[count_max_field.dest_field] = default_count


def _run_all_checks(imprt, fields: Dict[str, BibFields], df):
    with start_sentry_child(op="check.df", description="clean missing values"):
        clean_missing_values(df, fields)
    with start_sentry_child(op="check.df", description="concat dates"):
        concat_dates(df, fields)
    with start_sentry_child(op="check.df", description="check required values"):
        yield from check_required_values(df, fields)
    with start_sentry_child(op="check.df", description="check types"):
        yield from check_types(df, fields)
    with start_sentry_child(op="check.df", description="check geography"):
        yield from check_geography(
            df,
            fields,
            file_srid=imprt.srid,
            id_area=current_app.config["IMPORT"]["ID_AREA_RESTRICTION"],
        )
    with start_sentry_child(op="check.df", description="check counts"):
        yield from check_counts(df, fields)


def run_all_checks(imprt, fields: Dict[str, BibFields], df):
    df["valid"] = True
    for error in _run_all_checks(imprt, fields, df):
        if error["invalid_rows"].empty:
            continue
        try:
            error_type = ImportUserErrorType.query.filter_by(name=error["error_code"]).one()
        except NoResultFound:
            raise Exception(f"Error code '{error['error_code']}' not found.")
        invalid_rows = error["invalid_rows"]
        df.loc[invalid_rows.index, "valid"] = False
        # df['gn_invalid_reason'][invalid_rows.index.intersection(df['gn_invalid_reason'].isnull())] = \
        #        f'{error_type.name}'  # FIXME comment
        ordered_invalid_rows = sorted(invalid_rows["line_no"])
        column = generated_fields.get(error["column"], error["column"])
        column = imprt.fieldmapping.get(column, column)
        # If an error for same import, same column and of the same type already exists,
        # we concat existing erroneous rows with current rows.
        stmt = pg_insert(ImportUserError).values(
            {
                "id_import": imprt.id_import,
                "id_error": error_type.pk,
                "column_error": column,
                "id_rows": ordered_invalid_rows,
                "comment": error.get("comment"),
            }
        )
        stmt = stmt.on_conflict_do_update(
            constraint="t_user_errors_un",  # unique (import, error_type, column)
            set_={
                "id_rows": func.array_cat(ImportUserError.rows, stmt.excluded["id_rows"]),
            },
        )
        db.session.execute(stmt)
