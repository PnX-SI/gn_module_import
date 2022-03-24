import re
from uuid import UUID

from flask import current_app
import pandas as pd
import numpy as np
from shapely import wkt
from sqlalchemy.dialects.postgresql import UUID as UUIDType
from dateutil.parser import parse as parse_datetime

from geonature.core.gn_synthese.models import Synthese
from geonature.utils.env import db

from gn_module_import.logs import logger
from gn_module_import.models import BibFields


def convert_to_datetime(value):
    try:
        return parse_datetime(value)
    except Exception:
        return None

def convert_to_uuid(value, version=4):
    try:
        return UUID(str(value), version=version).hex
    except Exception:
        return None

def convert_to_integer(value):
    try:
        return int(value)
    except Exception:
        return None


def check_datetime_field(df, target_field, source_field, required):
    logger.info(
        "- checking and converting to date type in %s synthese column (= %s user column)",
        target_field,
        source_field,
    )
    datetime_col = df[source_field].apply(
        lambda x: convert_to_datetime(x) if pd.notnull(x) else x
    )
    if required:
        invalid_rows = df[datetime_col.isna()]
    else:
        # invalid rows are NaN rows which were not already set to NaN
        invalid_rows = df[datetime_col.isna() & df[source_field].notna()]
    df[source_field] = datetime_col
    values_error = invalid_rows[source_field]
    logger.info(
        "%s date type error detected in %s synthese column (= %s user column)",
        len(invalid_rows),
        target_field,
        source_field,
    )
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_DATE",
            invalid_rows=invalid_rows,
            comment="Les dates suivantes ne sont pas au bon format: {}".format(
                ", ".join(map(lambda x: str(x), values_error))
            ),
        )


def check_uuid_field(df, target_field, source_field, required):
    logger.info(
        "- checking and converting to uuid type in %s synthese column (= %s user column)",
        target_field,
        source_field,
    )
    uuid_col = df[source_field].apply(
        lambda x: convert_to_uuid(x) if pd.notnull(x) else x
    )
    if required:
        invalid_rows = df[uuid_col.isna()]
    else:
        # invalid rows are NaN rows which were not already set to NaN
        invalid_rows = df[uuid_col.isna() & df[source_field].notna()]
    df[source_field] = uuid_col
    values_error = invalid_rows[source_field]
    logger.info(
        "%s uuid type error detected in %s synthese column (= %s user column)",
        len(invalid_rows),
        target_field,
        source_field,
    )
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_UUID",
            invalid_rows=invalid_rows,
            comment="Les UUID suivantes ne sont pas au bon format: {}".format(
                ", ".join(map(lambda x: str(x), values_error))
            ),
        )


def check_integer_field(df, target_field, source_field, required):
    logger.info(
        "- checking and converting to integer type in %s synthese column (= %s user column)",
        target_field,
        source_field,
    )
    integer_col = df[source_field].apply(
        lambda x: convert_to_integer(x) if pd.notnull(x) else x
    )
    if required:
        invalid_rows = df[integer_col.isna()]
    else:
        # invalid rows are NaN rows which were not already set to NaN
        invalid_rows = df[integer_col.isna() & df[source_field].notna()]
    df[source_field] = integer_col
    values_error = invalid_rows[source_field]
    logger.info(
        "%s integer type error detected in %s synthese column (= %s user column)",
        len(invalid_rows),
        target_field,
        source_field,
    )
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_INTEGER",
            invalid_rows=invalid_rows,
            comment="Les valeurs suivantes ne sont pas des nombres : {}".format(
                ", ".join(map(lambda x: str(x), values_error))
            ),
        )


def check_unicode_field(df, target_field, source_field, field_type):
    if field_type.length is None:
        return
    logger.info(
        "- checking unicode length in %s synthese column (= %s user column)",
        target_field,
        source_field,
    )
    length = df[source_field].apply(
        lambda x: len(x) if pd.notnull(x) else x
    )
    invalid_rows = df[length > field_type.length]
    logger.info(
        "%s unicode length error detected in %s synthese column (= %s user column)",
        len(invalid_rows),
        target_field,
        source_field,
    )
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_CHAR_LENGTH",
            invalid_rows=invalid_rows,
        )


def check_anytype_field(df, target_field, source_field, field_type, required):
    if type(field_type) == db.DateTime:
        yield from check_datetime_field(df, target_field, source_field, required)
    elif type(field_type) == db.Integer:
        yield from check_integer_field(df, target_field, source_field, required)
    elif type(field_type) == UUIDType:
        yield from check_uuid_field(df, target_field, source_field, required)
    elif type(field_type) in [db.Unicode, db.UnicodeText]:
        yield from check_unicode_field(df, target_field, source_field, field_type)
    else:
        raise Exception("Unknown field type {}".format(field_type))  # pragma: no cover


def check_types(df, imprt, selected_columns, synthese_fields):
    for field in synthese_fields:
        if field.nomenclature_type:  # we do not check fields with nomenclatures
            continue
        target_field = field.name_field
        source_field = selected_columns[target_field]
        field_type = Synthese.__table__.c[field.name_field].type
        for error in check_anytype_field(df, target_field, source_field, field_type, field.mandatory):
            error.update({
                'column': selected_columns.get(f'orig_{target_field}', source_field),
            })
            yield error
