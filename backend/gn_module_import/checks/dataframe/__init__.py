from typing import Dict
from uuid import uuid4
from itertools import chain

from sqlalchemy.orm.exc import NoResultFound
from flask import current_app

from geonature.utils.env import db
from geonature.core.gn_synthese.models import Synthese

from gn_module_import.models import ImportUserError, ImportUserErrorType, BibFields

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
        'invalid_rows': df[df[field].notnull() & duplicated],
    }


def _check_ordering(df, min_field, max_field):
    ordered = df[min_field] <= df[max_field]
    ordered = ordered.fillna(False)
    invalid_rows = df[~ordered & df[min_field].notna() & df[max_field].notna()]
    yield dict(invalid_rows=invalid_rows)


def check_counts(df, fields: Dict[str, BibFields]):
    default_count = current_app.config["IMPORT"]["DEFAULT_COUNT_VALUE"]
    if 'count_min' in fields:
        count_min_field = fields['count_min']
        df[count_min_field.synthese_field] = (
            df[count_min_field.source_field]
            .where(
                df[count_min_field.source_field].notna(),
                other=default_count,
            )
        )
        if 'count_max' in fields:
            count_max_field = fields['count_max']
            yield from update_dicts(
                _check_ordering(df, count_min_field.source_field, count_max_field.source_field),
                column="count_min",
                error_code='COUNT_MIN_SUP_COUNT_MAX')
            df[count_max_field.synthese_field] = (
                df[count_max_field.source_field]
                .where(
                    df[count_max_field.source_field].notna(),
                    other=df[count_min_field.synthese_field],
                )
            )
        else:
            count_max_field = BibFields.query.filter_by(name_field="count_max").one()
            fields["count_max"] = count_max_field
            df[count_max_field.synthese_field] = df[count_min_field.synthese_field]
    else:
        if 'count_max' in fields:
            count_max_field = fields['count_max']
            count_min_field = BibFields.query.filter_by(name_field="count_min").one()
            fields["count_min"] = count_min_field
            df[count_max_field.synthese_field] = (
                df[count_max_field.source_field]
                .where(
                    df[count_max_field.source_field].notna(),
                    other=default_count,
                )
            )
            df[count_min_field.synthese_field] = df[count_max_field.synthese_field]
        else:
            count_min_field = BibFields.query.filter_by(name_field="count_min").one()
            fields["count_min"] = count_min_field
            df[count_min_field.synthese_field] = default_count
            count_max_field = BibFields.query.filter_by(name_field="count_max").one()
            fields["count_max"] = count_max_field
            df[count_max_field.synthese_field] = default_count


def _run_all_checks(imprt, fields: Dict[str, BibFields], df):
    clean_missing_values(df, fields)
    concat_dates(df, fields)
    yield from check_required_values(df, fields)
    yield from check_types(df, fields)
    yield from check_geography(df, fields, file_srid=imprt.srid)
    yield from check_counts(df, fields)


def run_all_checks(imprt, fields: Dict[str, BibFields], df):
    df["valid"] = True
    for error in _run_all_checks(imprt, fields, df):
        if error['invalid_rows'].empty:
            continue
        try:
            error_type = ImportUserErrorType.query.filter_by(name=error['error_code']).one()
        except NoResultFound:
            raise Exception(f"Error code '{error_code}' not found.")
        invalid_rows = error['invalid_rows']
        df.loc[invalid_rows.index, "valid"] = False
        #df['gn_invalid_reason'][invalid_rows.index.intersection(df['gn_invalid_reason'].isnull())] = \
        #        f'{error_type.name}'  # FIXME comment
        ordered_invalid_rows = sorted(invalid_rows["line_no"])
        column = imprt.fieldmapping.get(error['column'], error['column'])
        error = ImportUserError(
            imprt=imprt,
            type=error_type,
            column=column,
            rows=ordered_invalid_rows,
            comment=error.get('comment'),
        )
        db.session.add(error)
