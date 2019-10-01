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

EXCLUDED_SYNTHESE_FIELDS_FRONT = ['id_synthese', 'id_source', 'id_module',
                                  'id_dataset', 'the_geom_4326', 'the_geom_point', 'the_geom_local', 'last_action']

NOT_NULLABLE_SYNTHESE_FIELDS = ['cd_nom', 'nom_cite', 'date_min']

MAPPING_DATA = [
    {
        'name': 'general_info',
        'label': 'Informations générales',
        'fields': [
            {
                'name': 'entity_source_pk_value',
                'label': 'Identifiant source',
                'required': True
            },
            {
                'name': 'unique_id_sinp',
                'label': 'Identifiant SINP (uuid)',
                'required': False
            },
            {
                'name': 'meta_create_date',
                'label': 'Date de création de la donnée',
                'required': False
            },
            {
                'name': 'meta_v_taxref',
                'label': 'Version du référentiel taxonomique',
                'required': False
            },
            {
                'name': 'meta_update_date',
                'label': 'Date de mise à jour de la donnée',
                'required': False
            },
        ]
    },
    {
        'name': 'statement_info',
        'label': 'Informations de relevés',
        'fields': [
            {
                'name': 'date_min',
                'label': 'Date début',
                'required': True
            },
            {
                'name': 'date_max',
                'label': 'Date fin',
                'required': False
            },
            {
                'name': 'altitude_min',
                'label': 'Altitude min',
                'required': False
            },
            {
                'name': 'altitude_max',
                'label': 'Altitude max',
                'required': False
            },
            {
                'name': 'longitude',
                'label': 'Longitude (coord x)',
                'required': True
            },
            {
                'name': 'latitude',
                'label': 'Latitude (coord y)',
                'required': True
            },
            {
                'name': 'altitude_max',
                'label': 'Altitude max',
                'required': False
            },
            {
                'name': 'observers',
                'label': 'Observateur(s)',
                'required': False
            },
            {
                'name': 'comment_description',
                'label': 'Commentaire de relevé',
                'required': False
            },
            {
                'name': 'id_nomenclature_info_geo_type',
                'label': "Type d'information géographique",
                'required': False
            },
            {
                'name': 'id_nomenclature_grp_typ',
                'label': "Type de relevé/regroupement",
                'required': False
            }
        ]
    },
    {
        'name': 'occurrence_sensitivity',
        'label': "Informations d'occurrences & sensibilté ",
        'fields': [
            {
                'name': 'nom_cite',
                'label': 'Nom du taxon cité',
                'required': True
            },
            {
                'name': 'cd_nom',
                'label': 'Cd nom taxref',
                'required': True
            },
            {
                'name': 'id_nomenclature_obs_meth',
                'label': "Méthode d'observation",
                'required': False
            },
            {
                'name': 'id_nomenclature_bio_status',
                'label': 'Statut biologique',
                'required': False
            },
            {
                'name': 'id_nomenclature_bio_condition',
                'label': 'Etat biologique',
                'required': False
            },
            {
                'name': 'id_nomenclature_naturalness',
                'label': 'Naturalité',
                'required': False
            },
            {
                'name': 'comment_context',
                'label': "Commentaire d'occurrence",
                'required': False
            },
            {
                'name': 'id_nomenclature_sensitivity',
                'label': "Sensibilité"
            },
            {
                'name': 'id_nomenclature_diffusion_level',
                'label': "Niveau de diffusion",
                'required': False
            },
            {
                'name': 'id_nomenclature_blurring',
                'label': "Floutage",
                'required': False
            },
        ]
    },
    {
        'name': 'enumeration',
        'label': 'Dénombrements',
        'fields': [
            {
                'name': 'id_nomenclature_life_stage',
                'label': 'Stade de vie',
                'required': False
            },
            {
                'name': 'id_nomenclature_sex',
                'label': 'Sexe',
                'required': False
            },
            {
                'name': 'id_nomenclature_type_count',
                'label': 'Type du dénombrement'
            },
            {
                'name': 'id_nomenclature_obj_count',
                'label': 'Objet du dénombrement',
                'required': False
            },
            {
                'name': 'count_min',
                'label': 'Nombre minimal',
                'required': False
            },
            {
                'name': 'count_max',
                'label': 'Nombre maximal',
                'required': False
            },
        ]
    },
    {
        'name': 'validation',
        'label': 'Détermination & validité',
        'fields': [
            {
                'name': 'id_nomenclature_determination_method',
                'label': 'Méthode de détermination',
                'required': False
            },
            {
                'name': 'determiner',
                'label': 'Déterminateur',
                'required': False
            },
            {
                'name': 'id_digitiser',
                'label': "Auteur de la saisie",
                'required': False
            },
            {
                'name': 'id_nomenclature_exist_proof',
                'label': "Existance d'une preuve",
                'required': False
            },
            {
                'name': 'digital_proof',
                'label': 'Preuve numérique',
                'required': False
            },
            {
                'name': 'non_digital_proof',
                'label': 'Preuve non-numérique',
                'required': False
            },
            {
                'name': 'sample_number_proof',
                'label': "Identifiant de l'échantillon preuve",
                'required': False
            },
            {
                'name': 'id_nomenclature_valid_status',
                'label': 'Statut de validation',
                'required': False
            },
            {
                'name': 'validator',
                'label': 'Validateur',
                'required': False
            },
            {
                'name': 'meta_validation_date',
                'label': 'Date de validation',
                'required': False
            },
            {
                'name': 'validation_comment',
                'label': 'Commentaire de validation',
                'required': False
            },
        ]
    },
]


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
    MAPPING_DATA_FRONTEND = fields.List(
        fields.Dict, missing=MAPPING_DATA)
