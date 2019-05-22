'''
   Spécification du schéma toml des paramètres de configurations
'''

from marshmallow import Schema, fields

DEFAULT_LIST_COLUMN = [
    {'prop': 'id_import', 'name': 'Id Import', 'max_width': 100},
    {'prop': 'id_dataset', 'name': 'Id JDD', 'max_width': 100}
]

UPLOAD_DIRECTORY = "upload"

ARCHIVES_SCHEMA_NAME = "gn_import_archives"

PREFIX = "gn_"



class GnModuleSchemaConf(Schema):
      LIST_COLUMNS_FRONTEND = fields.List(fields.Dict, missing=DEFAULT_LIST_COLUMN)
      UPLOAD_DIRECTORY = fields.String(missing=UPLOAD_DIRECTORY)
      ARCHIVES_SCHEMA_NAME = fields.String(missing=ARCHIVES_SCHEMA_NAME)
      PREFIX = fields.String(missing=PREFIX)

