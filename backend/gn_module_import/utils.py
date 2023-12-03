import os
from io import BytesIO, TextIOWrapper
import csv
import ast
import json
from enum import IntEnum
from datetime import datetime, timedelta

from flask import current_app, render_template
from sqlalchemy import func, delete
from chardet.universaldetector import UniversalDetector
from sqlalchemy.sql.expression import select, insert, update, literal
import sqlalchemy as sa
import pandas as pd
import numpy as np
from sqlalchemy.dialects.postgresql import insert as pg_insert
from werkzeug.exceptions import BadRequest
from geonature.utils.env import db
from weasyprint import HTML

from gn_module_import import MODULE_CODE
from gn_module_import.models import BibFields, ImportUserError

from geonature.core.gn_commons.models import TModules
from geonature.utils.sentry import start_sentry_child
from ref_geo.models import LAreas


class ImportStep(IntEnum):
    UPLOAD = 1
    DECODE = 2
    LOAD = 3
    PREPARE = 4
    IMPORT = 5


generated_fields = {
    "datetime_min": "date_min",
    "datetime_max": "date_max",
}


def clean_import(imprt, step: ImportStep):
    imprt.task_id = None
    if step <= ImportStep.UPLOAD:
        # source_file will be necessary overwritten
        # source_count will be necessary overwritten
        pass
    if step <= ImportStep.DECODE:
        imprt.columns = None
    if step <= ImportStep.LOAD:
        transient_table = imprt.destination.get_transient_table()
        stmt = delete(transient_table).where(transient_table.c.id_import == imprt.id_import)
        with start_sentry_child(op="task", description="clean transient data"):
            db.session.execute(stmt)
        imprt.source_count = None
        imprt.loaded = False
    if step <= ImportStep.PREPARE:
        with start_sentry_child(op="task", description="clean errors"):
            ImportUserError.query.filter(ImportUserError.imprt == imprt).delete()
        imprt.erroneous_rows = None
        imprt.processed = False
    if step <= ImportStep.IMPORT:
        imprt.taxa_count = None
        imprt.import_count = None
        imprt.date_end_import = None
        imprt.destination.remove_data_from_destination(imprt)


def get_file_size(f):
    current_position = f.tell()
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(current_position)
    return size


def detect_encoding(f):
    begin = datetime.now()
    max_duration = timedelta(
        seconds=current_app.config["IMPORT"]["MAX_ENCODING_DETECTION_DURATION"]
    )
    position = f.tell()
    f.seek(0)
    detector = UniversalDetector()
    for row in f:
        detector.feed(row)
        if detector.done or (datetime.now() - begin) > max_duration:
            break
    detector.close()
    f.seek(position)
    return detector.result["encoding"] or "UTF-8"


def detect_separator(f, encoding):
    position = f.tell()
    f.seek(0)
    try:
        sample = f.readline().decode(encoding)
    except UnicodeDecodeError:
        # encoding is likely to be detected encoding, so prompt to errors
        return None
    if sample == "\n":  # files that do not start with column names
        raise BadRequest("File must start with columns")
    dialect = csv.Sniffer().sniff(sample)
    f.seek(position)
    return dialect.delimiter


def get_valid_bbox(imprt):
    transient_table = imprt.destination.get_transient_table()
    stmt = (
        select(func.ST_AsGeojson(func.ST_Extent(transient_table.c.the_geom_4326)))
        .where(transient_table.c.id_import == imprt.id_import)
        .where(transient_table.c.valid == True)
    )
    (valid_bbox,) = db.session.execute(stmt).fetchone()
    if valid_bbox:
        return json.loads(valid_bbox)


def insert_import_data_in_transient_table(imprt):
    transient_table = imprt.destination.get_transient_table()
    columns = imprt.columns
    fieldmapping, used_columns = build_fieldmapping(imprt, columns)

    extra_columns = set(columns) - set(used_columns)

    csvfile = TextIOWrapper(BytesIO(imprt.source_file), encoding=imprt.encoding)
    csvreader = csv.DictReader(csvfile, fieldnames=columns, delimiter=imprt.separator)
    header = next(csvreader, None)  # skip header
    for key, value in header.items():  # FIXME
        assert key == value

    obs = []
    line_no = line_count = 0
    for row in csvreader:
        line_no += 1
        if None in row:
            raise Exception(f"La ligne {line_no} contient des valeurs excédentaires : {row[None]}")
        assert list(row.keys()) == columns
        if not any(row.values()):
            continue
        o = {
            "id_import": imprt.id_import,
            "line_no": line_no,
        }
        o.update(
            {
                dest_field: (
                    build_additional_data(row, source_field["value"])
                    if source_field["field"].multi
                    else row[source_field["value"]]
                )
                for dest_field, source_field in fieldmapping.items()
            }
        )
        o.update(
            {
                "extra_fields": {col: row[col] for col in extra_columns},
            }
        )
        obs.append(o)
        line_count += 1
        if len(obs) > 1000:
            db.session.execute(insert(transient_table).values(obs))
            obs = []
    if obs:
        db.session.execute(insert(transient_table).values(obs))
    return line_count


def build_additional_data(row, columns):
    result = {}
    for column in columns:
        if is_json(row[column]):
            result.update(ast.literal_eval(row[column]))
        else:
            result[column] = row[column]
    return result


def is_json(str):
    try:
        if isinstance(ast.literal_eval(str), (float, int)):
            return False
    except:
        return False
    return True


def build_fieldmapping(imprt, columns):
    fields = BibFields.query.filter_by(autogenerated=False).all()
    fieldmapping = {}
    used_columns = []

    for field in fields:
        if field.name_field in imprt.fieldmapping:
            if field.multi:
                correct = list(set(columns) & set(imprt.fieldmapping[field.name_field]))
                if len(correct) > 0:
                    fieldmapping[field.source_column] = {
                        "value": correct,
                        "field": field,
                    }
                    used_columns.extend(correct)
            else:
                if imprt.fieldmapping[field.name_field] in columns:
                    fieldmapping[field.source_column] = {
                        "value": imprt.fieldmapping[field.name_field],
                        "field": field,
                    }
                    used_columns.append(imprt.fieldmapping[field.name_field])
    return fieldmapping, used_columns


def load_transient_data_in_dataframe(imprt, fields, offset, limit):
    transient_table = imprt.destination.get_transient_table()
    source_cols = [
        "id_import",
        "line_no",
        "valid",
    ] + [field.source_column for field in fields.values()]
    stmt = (
        select(*[transient_table.c[col] for col in source_cols])
        .where(transient_table.c.id_import == imprt.id_import)
        .order_by(transient_table.c.line_no)
        .offset(offset)
        .limit(limit)
    )
    records = db.session.execute(stmt).fetchall()
    df = pd.DataFrame.from_records(
        records,
        columns=source_cols,
    ).astype("object")
    return df


def mark_all_rows_as_invalid(imprt):
    transient_table = imprt.destination.get_transient_table()
    stmt = (
        update(transient_table)
        .where(transient_table.c.id_import == imprt.id_import)
        .values({"valid": False})
    )
    db.session.execute(stmt)


def update_transient_data_from_dataframe(imprt, fields, df):
    if not len(df[df["valid"] == True]):
        return
    transient_table = imprt.destination.get_transient_table()
    updated_cols = [
        "id_import",
        "line_no",
        "valid",
    ]
    updated_cols += [field.dest_field for field in fields.values() if field.dest_field]
    df.replace({np.nan: None}, inplace=True)
    records = df[df["valid"] == True][updated_cols].to_dict(orient="records")
    insert_stmt = pg_insert(transient_table)
    insert_stmt = insert_stmt.values(records).on_conflict_do_update(
        index_elements=updated_cols[:2],
        set_={col: insert_stmt.excluded[col] for col in updated_cols[2:]},
    )
    db.session.execute(insert_stmt)


def generate_pdf_from_template(template, data):
    template_rendered = render_template(template, data=data)
    html_file = HTML(
        string=template_rendered,
        base_url=current_app.config["API_ENDPOINT"],
        encoding="utf-8",
    )
    return html_file.write_pdf()
