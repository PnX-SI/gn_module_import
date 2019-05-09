'''
   Spécification du schéma toml des paramètres de configurations
'''

from marshmallow import Schema, fields

DEFAULT_LIST_COLUMN = [
    {'prop': 'id_import', 'name': 'Id Import', 'max_width': 100},
    {'prop': 'id_dataset', 'name': 'Id JDD', 'max_width': 100}
]

class GnModuleSchemaConf(Schema):
      LIST_COLUMNS_FRONTEND = fields.List(fields.Dict, missing=DEFAULT_LIST_COLUMN)


