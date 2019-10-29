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
        'max_width': 200, 'show': True},
    {'prop': 'date_end_import', 'name': 'Fin import', 'max_width': 200, 'show': True}
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
    {"name": "virgule (,)", "code": ","},
    {"name": "tabulation", "code": "\t"},
    {"name": "point-virgule (;)", "code": ";"},
    {"name": "espace", "code": " "}
]

MAX_FILE_SIZE = 500

ALLOWED_EXTENSIONS = [
    '.csv',
    '.json'
]

MISSING_VALUES = [
    '', 
    'NA', 
    'NaN', 
    'na'
]

DEFAULT_COUNT_VALUE = 1


SINP_SYNTHESE_NOMENCLATURES = [
    {
        'synthese_col': 'id_nomenclature_geo_object_nature',
        'nomenclature_abb': 'NAT_OBJ_GEO'
    },
    {
        'synthese_col': 'id_nomenclature_grp_typ',
        'nomenclature_abb': 'TYP_GRP'
    },
    {
        'synthese_col': 'id_nomenclature_obs_meth',
        'nomenclature_abb': 'METH_OBS'
    },
    {
        'synthese_col': 'id_nomenclature_obs_technique',
        'nomenclature_abb': 'TECHNIQUE_OBS' 
    },
    {
        'synthese_col': 'id_nomenclature_bio_status', 
        'nomenclature_abb': 'STATUT_BIO'
    },
    {
        'synthese_col': 'id_nomenclature_naturalness', 
        'nomenclature_abb': 'ETA_BIO' 
    },
    {
        'synthese_col': 'id_nomenclature_exist_proof',
        'nomenclature_abb': 'NATURALITE'
    },
    {
        'synthese_col': 'id_nomenclature_valid_status',
        'nomenclature_abb': 'STATUT_VALID'
    },
    {
        'synthese_col': 'id_nomenclature_diffusion_level',
        'nomenclature_abb': 'NIV_PRECIS'
    },
    {
        'synthese_col': 'id_nomenclature_life_stage',
        'nomenclature_abb': 'STADE_VIE'
    },
    {
        'synthese_col': 'id_nomenclature_sex',
        'nomenclature_abb': 'SEXE'
    },
    {
        'synthese_col': 'id_nomenclature_obj_count',
        'nomenclature_abb': 'OBJ_DENBR'
    },
    {
        'synthese_col': 'id_nomenclature_type_count',
        'nomenclature_abb': 'TYP_DENBR'
    },
    {
        'synthese_col': 'id_nomenclature_sensitivity',
        'nomenclature_abb': 'SENSIBILITE'
    },
    {
        'synthese_col': 'id_nomenclature_observation_status',
        'nomenclature_abb': 'STATUT_OBS'
    },
    {
        'synthese_col': 'id_nomenclature_blurring',
        'nomenclature_abb': 'DEE_FLOU'
    },
    {
        'synthese_col': 'id_nomenclature_source_status',
        'nomenclature_abb': 'STATUT_SOURCE'
    },
    {
        'synthese_col': 'id_nomenclature_info_geo_type',
        'nomenclature_abb': 'TYP_INF_GEO'
    },
    {
        'synthese_col': 'id_nomenclature_determination_method',
        'nomenclature_abb': 'METH_DETERMIN'
    }
]


EXCLUDED_SYNTHESE_FIELDS_FRONT = ['id_synthese', 'id_source', 'id_module',
                                  'id_dataset', 'the_geom_4326', 'the_geom_point', 'the_geom_local', 'last_action']

NOT_NULLABLE_SYNTHESE_FIELDS = ['cd_nom', 'nom_cite', 'date_min']


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
    SINP_SYNTHESE_NOMENCLATURES = fields.List(fields.Dict, missing=SINP_SYNTHESE_NOMENCLATURES)
    DEFAULT_COUNT_VALUE = fields.Integer(missing=DEFAULT_COUNT_VALUE)
    EXCLUDED_SYNTHESE_FIELDS_FRONT = fields.List(
        fields.String, missing=EXCLUDED_SYNTHESE_FIELDS_FRONT)
    NOT_NULLABLE_SYNTHESE_FIELDS = fields.List(
        fields.String, missing=NOT_NULLABLE_SYNTHESE_FIELDS)
