from io import BytesIO, StringIO
import csv
from datetime import datetime
import logging
import json
import numpy as np
from shapely import wkb

from slugify import slugify
from flask import current_app, stream_with_context
from sqlalchemy.schema import Table
from sqlalchemy import func
import pandas as pd
from chardet.universaldetector import UniversalDetector
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import cast as sa_cast, Numeric, DateTime

from geonature.utils.env import DB as db

from gn_module_import.file_checks.check_user_file import check_user_file_good_table
from gn_module_import.steps import Step
from gn_module_import.exceptions import ImportFileError


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


def get_archive_table_name(imprt):
    return '{schema}.{table}_{import_id}'.format(
        schema=current_app.config['IMPORT']['ARCHIVES_SCHEMA_NAME'],
        table=imprt.import_table,
        import_id=imprt.id_import,
    )


def load_geojson_data(id_import, geojson_data):
    csv_data = StringIO()
    parse_geojson(geojson_data, csv_data, geometry_col_name)
    return load_csv_data(id_import, csv_data.encode('utf-8'), encoding='utf-8')


def load_csv_data(id_import, csv_data, encoding):
    return check_user_file_good_table(id_import, csv_data, encoding)


def load_data(id_import, raw_data, encoding, fmt):
    if fmt == 'csv':
        report = load_csv_data(id_import, BytesIO(raw_data), encoding=encoding)
    elif fmt == 'geojson':
        try:
            data = raw_data.decode(encoding)
        except UnicodeDecodeError:
            raise ImportFileError(description='Erreur d’encodage')
        report = load_geojson_data(id_import, data)
    return report


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


def create_import_table_class(imprt, columns):
    columns = [
        db.Column('gn_pk', db.Integer, primary_key=True),
        db.Column('gn_is_valid', db.Boolean, nullable=True),
        db.Column('gn_invalid_reason', db.Text, nullable=True),
    ] + columns
    return create_table_class(
        "UserTImportsTableClass{}".format(imprt.id_import),
        get_import_table_name(imprt),
        columns,
    )


def create_archive_table_class(imprt, columns):
    columns = [ 'gn_pk' ] + columns
    return create_table_class(
        "UserArchiveTableClass{}".format(imprt.id_import),
        get_archive_table_name(imprt),
        columns,
    )


def delete_tables(imprt):
    assert(imprt.import_table is not None)
    table_names = [
        get_import_table_name(imprt),
        get_archive_table_name(imprt),
    ]
    for table_name in table_names:
        table_class = get_table_class(table_name)
        table_class.drop(db.session.connection())
        db.metadata.remove(table_class)
    imprt.import_table = None


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def create_tables(imprt, session=None):
    assert(imprt.import_table == None)
    session = session or db.session

    imprt.import_table = get_clean_table_name(imprt.full_file_name)

    # keys are origin names, values are cleaned names
    columns = list(imprt.columns.keys())

    import_table_class = create_import_table_class(imprt, columns)
    import_table_class.create(db.session.connection())
    archive_table_class = create_archive_table_class(imprt, columns)
    archive_table_class.create(db.session.connection())

    csvfile = StringIO(imprt.source_file.decode(imprt.encoding))
    dialect = csv.Sniffer().sniff(csvfile.readline())
    df = pd.read_csv(csvfile, delimiter=dialect.delimiter, names=columns)
    # FIXME does this opperate on the transaction?
    df.to_sql(get_import_table_name(imprt), db.session.connection(), if_exists='append', chunksize=10000, method=psql_insert_copy, index=False)
    df.to_sql(get_archive_table_name(imprt), db.session.connection(), if_exists='append', chunksize=10000, method=psql_insert_copy, index=False)
    # FIXME save data to archive
    #save_dataframe_to_database(imprt, df)
    #ImportEntry = get_table_class(get_import_table_name(imprt))
    #list_of_dicts = df.to_dict(orient='records')
    #db.session.execute(ImportEntry.insert(), list_of_dicts)
    db.session.commit()


def load_import_to_dataframe(imprt):
    ImportEntry = get_table_class(get_import_table_name(imprt))
    query = db.session.query(ImportEntry)
    dtypes = { col: 'string' for col in ImportEntry.c.keys() }
    dtypes.update({
        'gn_pk': 'int64',
        'gn_is_valid': 'bool',
    })
    df = pd.DataFrame.from_records(
        query.all(),
        columns=ImportEntry.columns.keys(),
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
