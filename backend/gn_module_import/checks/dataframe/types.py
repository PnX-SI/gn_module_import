from typing import Dict
import re
from uuid import UUID
import re
from itertools import product
from datetime import datetime

from flask import current_app
import pandas as pd
import numpy as np
from shapely import wkt
from sqlalchemy.dialects.postgresql import UUID as UUIDType

from geonature.core.gn_synthese.models import Synthese
from geonature.utils.env import db

from gn_module_import.models import BibFields


def convert_to_datetime(value):
    value = value.strip()
    value = re.sub("[ ]+", " ", value)
    value = re.sub("[/.:]", "-", value)
    date_formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
    ]
    time_formats = [
        None,
        "%H",
        "%H-%M",
        "%H-%M-%S",
        "%H-%M-%S-%f",
        "%Hh",
        "%Hh%M",
        "%Hh%Mm",
        "%Hh%Mm%Ss",
    ]
    for date_format, time_format in product(date_formats, time_formats):
        fmt = (date_format + " " + time_format) if time_format else date_format
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
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


def check_datetime_field(df, source_field, target_field, required):
    datetime_col = df[source_field].apply(lambda x: convert_to_datetime(x) if pd.notnull(x) else x)
    if required:
        invalid_rows = df[datetime_col.isna()]
    else:
        # invalid rows are NaN rows which were not already set to NaN
        invalid_rows = df[datetime_col.isna() & df[source_field].notna()]
    df[target_field] = datetime_col
    values_error = invalid_rows[source_field]
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_DATE",
            invalid_rows=invalid_rows,
            comment="Les dates suivantes ne sont pas au bon format: {}".format(
                ", ".join(map(lambda x: str(x), values_error))
            ),
        )


def check_uuid_field(df, source_field, target_field, required):
    uuid_col = df[source_field].apply(lambda x: convert_to_uuid(x) if pd.notnull(x) else x)
    if required:
        invalid_rows = df[uuid_col.isna()]
    else:
        # invalid rows are NaN rows which were not already set to NaN
        invalid_rows = df[uuid_col.isna() & df[source_field].notna()]
    df[target_field] = uuid_col
    values_error = invalid_rows[source_field]
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_UUID",
            invalid_rows=invalid_rows,
            comment="Les UUID suivantes ne sont pas au bon format: {}".format(
                ", ".join(map(lambda x: str(x), values_error))
            ),
        )


def check_integer_field(df, source_field, target_field, required):
    integer_col = df[source_field].apply(lambda x: convert_to_integer(x) if pd.notnull(x) else x)
    if required:
        invalid_rows = df[integer_col.isna()]
    else:
        # invalid rows are NaN rows which were not already set to NaN
        invalid_rows = df[integer_col.isna() & df[source_field].notna()]
    df[target_field] = integer_col
    values_error = invalid_rows[source_field]
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_INTEGER",
            invalid_rows=invalid_rows,
            comment="Les valeurs suivantes ne sont pas des nombres : {}".format(
                ", ".join(map(lambda x: str(x), values_error))
            ),
        )


def check_unicode_field(df, field, field_length):
    if field_length is None:
        return
    length = df[field].apply(lambda x: len(x) if pd.notnull(x) else x)
    invalid_rows = df[length > field_length]
    if len(invalid_rows) > 0:
        yield dict(
            error_code="INVALID_CHAR_LENGTH",
            invalid_rows=invalid_rows,
        )


def check_anytype_field(df, field_type, source_col, dest_col, required):
    if type(field_type) == db.DateTime:
        yield from check_datetime_field(df, source_col, dest_col, required)
    elif type(field_type) == db.Integer:
        yield from check_integer_field(df, source_col, dest_col, required)
    elif type(field_type) == UUIDType:
        yield from check_uuid_field(df, source_col, dest_col, required)
    elif type(field_type) in [db.Unicode, db.UnicodeText]:
        yield from check_unicode_field(df, dest_col, field_length=field_type.length)
    else:
        raise Exception("Unknown field type {}".format(field_type))  # pragma: no cover


def check_types(df, fields: Dict[str, BibFields]):
    for name, field in fields.items():
        if not field.synthese_field:
            continue
        if field.mnemonique:  # set from content mapping
            continue
        field_type = Synthese.__table__.c[field.synthese_field].type
        for error in check_anytype_field(
            df,
            field_type=field_type,
            source_col=field.source_column,
            dest_col=field.synthese_field,
            required=False,
        ):
            error.update(
                {
                    "column": name,
                }
            )
            yield error
