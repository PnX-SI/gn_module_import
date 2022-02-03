from io import BytesIO, StringIO, TextIOWrapper
import csv
from datetime import datetime
import logging
import json
import numpy as np
from shapely import wkb

from slugify import slugify
from flask import current_app, stream_with_context
from werkzeug.exceptions import BadRequest
from sqlalchemy.schema import Table
from sqlalchemy import func
import pandas as pd
from chardet.universaldetector import UniversalDetector
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import cast as sa_cast, Numeric, DateTime

from geonature.utils.env import DB as db

from gn_module_import.steps import Step
from gn_module_import.exceptions import ImportFileError
from gn_module_import.upload.geojson_to_csv import parse_geojson


MAX_TABLE_NAME_LEN = 30
MAX_COLUMN_NAME_LEN = 40

COMPUTED_SOURCE_FIELDS = {
    'date_min': 'concatened_date_min',
    'date_max': 'concatened_date_max',
}


def geom_to_wkb(geom):
    return wkb.dumps(geom).hex()


def get_valid_bbox(geo_field):
    valid_bbox, = db.session.execute(
                     db.session.query(
                         func.ST_AsGeojson(
                             func.ST_Extent(
                                geo_field
                             )
                         )
                     )
                 ).fetchone()
    return json.loads(valid_bbox)


def get_import_table_name(imprt):
    return '{schema}.i_{table}_{import_id}'.format(
        schema=current_app.config['IMPORT']['IMPORTS_SCHEMA_NAME'],
        table=imprt.import_table,
        import_id=imprt.id_import,
    )


def load_geojson_data(imprt, geojson_data):
    csv_data = StringIO()
    parse_geojson(geojson_data, csv_data, geometry_col_name)
    return load_csv_data(imprt, csv_data.encode('utf-8'), encoding='utf-8')


def load_csv_data(imprt, csv_data, encoding):
    csvfile = TextIOWrapper(csv_data, encoding=encoding)
    headline = csvfile.readline()
    dialect = csv.Sniffer().sniff(headline)
    csvreader = csv.reader(csvfile, delimiter=dialect.delimiter)
    csvfile.seek(0)
    columns = next(csvreader)
    duplicates = set([col for col in columns if columns.count(col) > 1])
    if duplicates:
        raise BadRequest(f"Duplicates column names: {duplicates}")
    imprt.columns = { col: get_clean_column_name(col) for col in columns }
    csvfile.seek(0)
    try:
        df = pd.read_csv(csvfile, delimiter=dialect.delimiter,
                         header=0,  # this will determine the "official" cols number
                         index_col=None,
                         on_bad_lines='error')
    except pd.errors.ParserError as e:
        raise BadRequest(description=str(e))
    # Use slugified db-compatible column names
    df = df.reindex(columns=imprt.columns.values())
    return df


def load_data(imprt, raw_data, encoding, fmt):
    if fmt == 'csv':
        df = load_csv_data(imprt, BytesIO(raw_data), encoding=encoding)
    elif fmt == 'geojson':
        try:
            data = raw_data.decode(encoding)
        except UnicodeDecodeError:
            raise BadRequest(description='Erreur d’encodage')
        df = load_geojson_data(imprt, data)
    return df


def get_clean_table_name(file_name):
    return slugify(file_name.rsplit('.', 1)[0], separator='_',
                   max_length=MAX_TABLE_NAME_LEN)


def get_clean_column_name(column_name):
    return slugify(column_name, separator='_',
                   max_length=MAX_COLUMN_NAME_LEN)


def detect_encoding(f):
    logger = logging.getLogger('chardet')
    logger.setLevel(logging.CRITICAL)
    detector = UniversalDetector()
    for row in f:
        detector.feed(row)
        if detector.done:
            break
    f.seek(0)
    detector.close()
    return detector.result['encoding']


def get_table_class(long_table_name):
    table_class = db.metadata.tables.get(long_table_name)
    if table_class is None:
        schema_name, table_name = long_table_name.split('.', 1)
        table_class = Table(table_name, db.metadata, schema=schema_name,
                            autoload=True, autoload_with=db.session.connection())
    return table_class


def create_table_class(table_class_name, long_table_name, columns):
    assert(long_table_name not in db.metadata.tables)
    schema_name, table_name = long_table_name.split('.', 1)
    # columns may be Column objects, or string considered as name of text column
    _columns = []
    for col in columns:
        if isinstance(col, db.Column):
            _columns.append(col)
        else:
            _columns.append(db.Column(col, db.Text, nullable=True))
    return Table(table_name, db.metadata, schema=schema_name, *_columns)


def delete_tables(imprt):
    assert(imprt.import_table is not None)
    table_names = [
        get_import_table_name(imprt),
    ]
    for table_name in table_names:
        table_class = get_table_class(table_name)
        table_class.drop(db.session.connection())
        db.metadata.remove(table_class)
    imprt.import_table = None


def load_import_to_dataframe(imprt):
    ImportEntry = get_table_class(get_import_table_name(imprt))
    columns = ImportEntry.c.keys()
    query = db.session.query(ImportEntry)
    dtypes = { col: 'string' for col in columns }
    dtypes.update({
        'gn_pk': 'int64',
        'gn_is_valid': 'bool',
    })
    df = pd.DataFrame.from_records(
        query.all(),
        columns=columns,
    ).astype(dtypes)
    return df


def save_dataframe_to_database(imprt, df, drop_table=True):
    if drop_table:
        ImportEntry = get_table_class(get_import_table_name(imprt))
        ImportEntry.drop(db.session.connection())
        db.metadata.remove(ImportEntry)
    columns = []
    for column, dtype, in zip(df.columns, df.dtypes):
        if column == 'gn_pk':
            columns.append(db.Column(column, db.Integer, primary_key=True))
        elif column == 'gn_is_valid':
            columns.append(db.Column(column, db.Boolean, nullable=False))
        else:
            columns.append(column)
    ImportEntry = create_table_class(
        "UserTImportsTableClass{}".format(imprt.id_import),
        get_import_table_name(imprt),
        columns,
    )
    ImportEntry.create(db.session.connection())
    df.replace({np.nan: None}, inplace=True)
    df.replace({pd.NaT: None}, inplace=True)
    list_of_dicts = df.to_dict(orient='records')
    db.session.execute(ImportEntry.insert(), list_of_dicts)


overrided_source_fields = {
    'date_min': 'concatened_date_min',
    'date_max': 'concatened_date_max',
}


def get_selected_synthese_fields(imprt):
    """
        Return a list [ fieldmapping ]
    """
    return [ f for f in imprt.field_mapping.fields
               if f.source_field in imprt.columns.keys()
               and f.target.synthese_field ]

def get_synthese_columns_mapping(imprt, cast=True):
    """
        Return a mapping { target column: source column }
        Target columns are synthese fields
        Source colmuns are import fields after transformation
    """
    ImportEntry = get_table_class(get_import_table_name(imprt))
    fields = get_selected_synthese_fields(imprt)
    columns_mapping = {}
    for f in fields:
        source_field = COMPUTED_SOURCE_FIELDS.get(f.target_field, f.source_field)
        target_column = f.target_field
        if f.target.mnemonique:
            source_column = ImportEntry.c['_tr_{}_{}'.format(f.target_field, source_field)]
        else:
            source_column = ImportEntry.c[source_field]
        if cast:
            if f.target.type_field == 'uuid':
                cast_type = UUID
            elif f.target.type_field == 'integer':
                cast_type = Numeric
            elif f.target.type_field == 'timestamp without time zone':
                cast_type = DateTime
            elif f.target.type_field in ['character varying', 'text']:
                cast_type = db.String
            else:
                cast_type = db.String
            source_column = sa_cast(source_column, cast_type)
        columns_mapping[target_column] = source_column
    return columns_mapping


@stream_with_context
def stream_csv(rows):
    data = StringIO()
    w = csv.writer(data)
    for row in rows:
        w.writerow(row)
        yield data.getvalue()
        data.seek(0)
        data.truncate(0)
