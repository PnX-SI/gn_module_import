'''
   Spécification du schéma toml des paramètres de configurations
'''

from marshmallow import Schema, fields

DEFAULT_LIST_COLUMN = [
    {'prop': 'id_import', 'name': 'Id', 'max_width': 50, 'show': True},
    {'prop': 'format_source_file', 'name': 'File format',
        'max_width': 50, 'show': False},
    {'prop': 'dataset_name', 'name': 'JDD', 'max_width': 400, 'show': True},
    {'prop': 'taxa_count', 'name': 'Nb de taxons', 'max_width': 200, 'show': True},
    {'prop': 'import_count', 'name': 'Nb de donnees', 'max_width': 200, 'show': True},
    {'prop': 'date_create_import', 'name': 'Debut import',
        'max_width': 200, 'show': True}
]

UPLOAD_DIRECTORY = "upload"

ARCHIVES_SCHEMA_NAME = "gn_import_archives"

IMPORTS_SCHEMA_NAME = "gn_imports"

PREFIX = "gn_"

SRID = [
    {"name": "WGS84", "code": 4326},
    {"name": "Lambert93", "code": 2154}
]

ENCODAGE = [
    "UTF-8"
]

SEPARATOR = [
    {"name": "virgule (,)", "code": ",", "db_code": "comma"},
    {"name": "tabulation", "code": "\t", "db_code": "tab"},
    {"name": "point-virgule (;)", "code": ";", "db_code": "colon"},
    {"name": "espace", "code": " ", "db_code": "space"}
]

MAX_FILE_SIZE = 500

ALLOWED_EXTENSIONS = [
    '.csv',
    '.geojson'
]

MISSING_VALUES = [
    '', 
    'NA', 
    'NaN', 
    'na'
]

DEFAULT_COUNT_VALUE = 1

EXCLUDED_SYNTHESE_FIELDS_FRONT = ['id_synthese', 'id_source', 'id_module',
                                  'id_dataset', 'the_geom_4326', 'the_geom_point', 'the_geom_local', 'last_action']

NOT_NULLABLE_SYNTHESE_FIELDS = ['cd_nom', 'nom_cite', 'date_min']

INVALID_CSV_NAME = 'invalid'


class GnModuleSchemaConf(Schema):
    LIST_COLUMNS_FRONTEND = fields.List(
        fields.Dict, missing=DEFAULT_LIST_COLUMN)
    UPLOAD_DIRECTORY = fields.String(missing=UPLOAD_DIRECTORY)
    ARCHIVES_SCHEMA_NAME = fields.String(missing=ARCHIVES_SCHEMA_NAME)
    IMPORTS_SCHEMA_NAME = fields.String(missing=IMPORTS_SCHEMA_NAME)
    PREFIX = fields.String(missing=PREFIX)
    SRID = fields.List(fields.Dict, missing=SRID)
    SEPARATOR = fields.List(fields.Dict, missing=SEPARATOR)
    ENCODAGE = fields.List(fields.String, missing=ENCODAGE)
    MAX_FILE_SIZE = fields.Integer(missing=MAX_FILE_SIZE)
    ALLOWED_EXTENSIONS = fields.List(fields.String, missing=ALLOWED_EXTENSIONS)
    MISSING_VALUES = fields.List(fields.String, missing=MISSING_VALUES)
    DEFAULT_COUNT_VALUE = fields.Integer(missing=DEFAULT_COUNT_VALUE)
    EXCLUDED_SYNTHESE_FIELDS_FRONT = fields.List(
        fields.String, missing=EXCLUDED_SYNTHESE_FIELDS_FRONT)
    NOT_NULLABLE_SYNTHESE_FIELDS = fields.List(
        fields.String, missing=NOT_NULLABLE_SYNTHESE_FIELDS)
    INVALID_CSV_NAME = fields.String(missing=INVALID_CSV_NAME)
