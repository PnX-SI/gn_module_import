import os
from io import BytesIO, TextIOWrapper
import csv
import ast
import json
from enum import IntEnum
from datetime import datetime, timedelta

from flask import current_app, render_template
from sqlalchemy import func
from chardet.universaldetector import UniversalDetector
from sqlalchemy.sql.expression import select, insert, literal
import sqlalchemy as sa
import pandas as pd
import numpy as np
from sqlalchemy.dialects.postgresql import insert as pg_insert
from werkzeug.exceptions import BadRequest
from geonature.utils.env import db
from weasyprint import HTML

from gn_module_import import MODULE_CODE
from gn_module_import.models import (
    BibFields,
    ImportSyntheseData,
    ImportUserError,
)

from geonature.core.gn_commons.models import TModules
from geonature.core.gn_synthese.models import Synthese, corAreaSynthese, TSources
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
        with start_sentry_child(op="task", description="clean synthese data"):
            ImportSyntheseData.query.filter(ImportSyntheseData.imprt == imprt).delete()
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
        if imprt.source:
            with start_sentry_child(op="task", description="clean source"):
                Synthese.query.filter(Synthese.source == imprt.source).delete()
                imprt.source = None


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
    stmt = db.select(func.ST_AsGeojson(func.ST_Extent(ImportSyntheseData.the_geom_4326))).filter(
        ImportSyntheseData.imprt == imprt,
        ImportSyntheseData.valid == True,
    )
    (valid_bbox,) = db.session.execute(stmt).fetchone()
    if valid_bbox:
        return json.loads(valid_bbox)


def insert_import_data_in_database(imprt):
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
            db.session.bulk_insert_mappings(ImportSyntheseData, obs)
            obs = []
    if obs:
        db.session.bulk_insert_mappings(ImportSyntheseData, obs)
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


def load_import_data_in_dataframe(imprt, fields, offset, limit):
    source_cols = [
        "id_import",
        "line_no",
        "valid",
    ] + [field.source_column for field in fields.values()]
    records = (
        db.session.query(*[ImportSyntheseData.__table__.c[col] for col in source_cols])
        .filter(
            ImportSyntheseData.imprt == imprt,
        )
        .order_by(ImportSyntheseData.line_no)
        .offset(offset)
        .limit(limit)
        .all()
    )
    df = pd.DataFrame.from_records(
        records,
        columns=source_cols,
    ).astype("object")
    return df


def mark_all_rows_as_invalid(imprt):
    db.session.query(ImportSyntheseData).filter_by(id_import=imprt.id_import).update(
        {"valid": False}
    )


def update_import_data_from_dataframe(imprt, fields, df):
    if not len(df[df["valid"] == True]):
        return
    updated_cols = [
        "id_import",
        "line_no",
        "valid",
    ]
    updated_cols += [field.synthese_field for field in fields.values() if field.synthese_field]
    df.replace({np.nan: None}, inplace=True)
    records = df[df["valid"] == True][updated_cols].to_dict(orient="records")
    insert_stmt = pg_insert(ImportSyntheseData)
    insert_stmt = insert_stmt.values(records).on_conflict_do_update(
        index_elements=updated_cols[:2],
        set_={col: insert_stmt.excluded[col] for col in updated_cols[2:]},
    )
    db.session.execute(insert_stmt)


def import_data_to_synthese(imprt):
    generated_fields = {
        "datetime_min",
        "datetime_max",
        "the_geom_4326",
        "the_geom_local",
        "the_geom_point",
        "id_area_attachment",
    }
    if imprt.fieldmapping.get(
        "unique_id_sinp_generate", current_app.config["IMPORT"]["DEFAULT_GENERATE_MISSING_UUID"]
    ):
        generated_fields |= {"unique_id_sinp"}
    if imprt.fieldmapping.get("altitudes_generate", False):
        generated_fields |= {"altitude_min", "altitude_max"}
    fields = BibFields.query.filter(
        BibFields.synthese_field != None,
        BibFields.name_field.in_(imprt.fieldmapping.keys() | generated_fields),
    ).all()
    select_stmt = (
        ImportSyntheseData.query.filter_by(imprt=imprt, valid=True)
        .with_entities(*[getattr(ImportSyntheseData, field.synthese_field) for field in fields])
        .add_columns(
            literal(imprt.id_source),
            literal(TModules.query.filter_by(module_code=MODULE_CODE).one().id_module),
            literal(imprt.id_dataset),
            literal("I"),
        )
    )
    names = [field.synthese_field for field in fields] + [
        "id_source",
        "id_module",
        "id_dataset",
        "last_action",
    ]
    insert_stmt = insert(Synthese).from_select(
        names=names,
        select=select_stmt,
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
