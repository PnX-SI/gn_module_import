"""
   Spécification du schéma toml des paramètres de configurations
"""

from marshmallow import Schema, fields

DEFAULT_LIST_COLUMN = [
    {
        "prop": "id_import",
        "name": "Id import",
        "max_width": 50,
        "show": True,
        "filter": True,
    },
    {
        "prop": "id_source",
        "name": "Id source",
        "max_width": 80,
        "show": True,
        "filter": True,
    },
    {
        "prop": "format_source_file",
        "name": "Format",
        "max_width": 80,
        "show": False,
        "filter": False,
    },
    {
        "prop": "full_file_name",
        "name": "Fichier",
        "max_width": 320,
        "show": True,
        "filter": True,
    },
    {
        "prop": "dataset.dataset_name",
        "name": "Voir la fiche du JDD",
        "max_width": 400,
        "show": True,
        "filter": False,
    },
    {
        "prop": "taxa_count",
        "name": "Nb de taxons",
        "max_width": 120,
        "show": True,
        "filter": False,
    },
    {
        "prop": "import_count",
        "name": "Nb de donnees",
        "max_width": 120,
        "show": True,
        "filter": False,
    },
    {
        "prop": "date_create_import",
        "name": "Debut import",
        "max_width": 200,
        "show": True,
        "filter": True,
    },
    {
        "prop": "authors_name",
        "name": "Auteur",
        "max_width": 320,
        "show": True,
        "filter": False,
    },
]


UPLOAD_DIRECTORY = "upload"


IMPORTS_SCHEMA_NAME = "gn_imports"

PREFIX = "gn_"

SRID = [{"name": "WGS84", "code": 4326}, {"name": "Lambert93", "code": 2154}]

ENCODAGE = ["UTF-8"]


MAX_FILE_SIZE = 1000

ALLOWED_EXTENSIONS = [".csv", ".geojson"]

MISSING_VALUES = ["", "NA", "NaN", "na"]

DEFAULT_COUNT_VALUE = 1

INVALID_CSV_NAME = "invalid"

ALLOW_VALUE_MAPPING = True


# If VALUE MAPPING is not allowed, you must specify the DEFAULT_VALUE_MAPPING_ID
DEFAULT_VALUE_MAPPING_ID = 3

# Once the MAX_LINE_LIMIT has been exceeded, control processing will take place in the background,
# you will be notified once this is completed
MAX_LINE_LIMIT = 1000

INSTANCE_BOUNDING_BOX = [-5.0, 41, 10, 51.15]

ALLOW_MODIFY_DEFAULT_MAPPING = True
ALLOW_FIELD_MAPPING = True
DEFAULT_FIELD_MAPPING_ID = 1
# Parameter to define if the checkbox allowing to change display mode is displayed or not.
DISPLAY_CHECK_BOX_MAPPED_FIELD = True


class GnModuleSchemaConf(Schema):
    LIST_COLUMNS_FRONTEND = fields.List(fields.Dict, load_default=DEFAULT_LIST_COLUMN)
    UPLOAD_DIRECTORY = fields.String(load_default=UPLOAD_DIRECTORY)
    IMPORTS_SCHEMA_NAME = fields.String(load_default=IMPORTS_SCHEMA_NAME)
    PREFIX = fields.String(load_default=PREFIX)
    SRID = fields.List(fields.Dict, load_default=SRID)
    ENCODAGE = fields.List(fields.String, load_default=ENCODAGE)
    MAX_FILE_SIZE = fields.Integer(load_default=MAX_FILE_SIZE)
    ALLOWED_EXTENSIONS = fields.List(fields.String, load_default=ALLOWED_EXTENSIONS)
    MISSING_VALUES = fields.List(fields.String, load_default=MISSING_VALUES)
    DEFAULT_COUNT_VALUE = fields.Integer(load_default=DEFAULT_COUNT_VALUE)
    INVALID_CSV_NAME = fields.String(load_default=INVALID_CSV_NAME)
    ALLOW_VALUE_MAPPING = fields.Boolean(load_default=ALLOW_VALUE_MAPPING)
    DEFAULT_VALUE_MAPPING_ID = fields.Integer(load_default=DEFAULT_VALUE_MAPPING_ID)
    FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE = fields.Boolean(load_default=False)
    # Parameter to define if the mapped fields are displayed or not.
    DISPLAY_MAPPED_VALUES = fields.Boolean(load_default=True)
    DISPLAY_CHECK_BOX_MAPPED_VALUES = fields.Boolean(load_default=True)
    MAX_LINE_LIMIT = fields.Integer(load_default=MAX_LINE_LIMIT)
    INSTANCE_BOUNDING_BOX = fields.List(fields.Float, load_default=INSTANCE_BOUNDING_BOX)
    ENABLE_BOUNDING_BOX_CHECK = fields.Boolean(load_default=True)
    ENABLE_SYNTHESE_UUID_CHECK = fields.Boolean(load_default=True)
    DISPLAY_CHECK_BOX_MAPPED_VALUES = fields.Boolean(load_default=True)
    ALLOW_MODIFY_DEFAULT_MAPPING = fields.Boolean(load_default=ALLOW_MODIFY_DEFAULT_MAPPING)
    ALLOW_FIELD_MAPPING = fields.Boolean(load_default=ALLOW_FIELD_MAPPING)
    DEFAULT_FIELD_MAPPING_ID = fields.Integer(load_default=DEFAULT_FIELD_MAPPING_ID)
    DISPLAY_MAPPED_FIELD = fields.Boolean(load_default=False)
    DISPLAY_CHECK_BOX_MAPPED_FIELD = fields.Boolean(load_default=True)
    CHECK_PRIVATE_JDD_BLURING = fields.Boolean(load_default=True)
    CHECK_REF_BIBLIO_LITTERATURE = fields.Boolean(load_default=True)
    CHECK_EXIST_PROOF = fields.Boolean(load_default=True)
    CHECK_TYPE_INFO_GEO = fields.Boolean(load_default=True)
    DEFAULT_GENERATE_MISSING_UUID = fields.Boolean(load_default=True)
    # If ID is provided (!=-1) will take the geometry in ref_geo.l_areas
    # and checks if all imported points are inside it. Otherwise throws an error
    ID_AREA_RESTRICTION = fields.Integer(missing=None)
