'''
   Spécification du schéma toml des paramètres de configurations
'''

from marshmallow import Schema, fields

DEFAULT_LIST_COLUMN = [
    {'prop': 'id_import', 'name': 'Id', 'max_width': 50},
    {'prop': 'import_table', 'name': 'JDD', 'max_width': 400},
    {'prop': 'taxa_count', 'name': 'Nb de taxons', 'max_width': 200},
    {'prop': 'import_count', 'name': 'Nb de données', 'max_width': 200}
]

UPLOAD_DIRECTORY = "upload"

ARCHIVES_SCHEMA_NAME = "gn_import_archives"

PREFIX = "gn_"

SRID = [
    {"name":"WGS84", "code":4326},
    {"name":"Lambert93", "code":2154}
]

ENCODAGE = [
    "UTF-8"
]

SEPARATOR = [
    {"name":"virgule (,)", "code":","},
    {"name":"tabulation", "code":"/n"},
    {"name":"point-virgule (;)", "code":";"},
    {"name":"espace", "code":" "}
]

class GnModuleSchemaConf(Schema):
      LIST_COLUMNS_FRONTEND = fields.List(fields.Dict, missing=DEFAULT_LIST_COLUMN)
      UPLOAD_DIRECTORY = fields.String(missing=UPLOAD_DIRECTORY)
      ARCHIVES_SCHEMA_NAME = fields.String(missing=ARCHIVES_SCHEMA_NAME)
      PREFIX = fields.String(missing=PREFIX)
      SRID = fields.List(fields.Dict, missing=SRID)
      ENCODAGE = fields.List(fields.String, missing=ENCODAGE)