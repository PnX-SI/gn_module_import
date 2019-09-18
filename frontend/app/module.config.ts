export const ModuleConfig = {
 "ALLOWED_EXTENSIONS": [
  ".csv",
  ".json"
 ],
 "ARCHIVES_SCHEMA_NAME": "gn_import_archives",
 "ENCODAGE": [
  "UTF-8"
 ],
 "EXCLUDED_SYNTHESE_FIELDS_FRONT": [
  "id_synthese",
  "id_source",
  "id_module",
  "id_dataset",
  "the_geom_4326",
  "the_geom_point",
  "the_geom_local",
  "last_action"
 ],
 "ID_MODULE": 15,
 "IMPORTS_SCHEMA_NAME": "gn_imports",
 "LIST_COLUMNS_FRONTEND": [
  {
   "max_width": 50,
   "name": "Id",
   "prop": "id_import",
   "show": true
  },
  {
   "max_width": 100,
   "name": "File format",
   "prop": "format_source_file",
   "show": false
  },
  {
   "max_width": 400,
   "name": "JDD",
   "prop": "dataset_name",
   "show": true
  },
  {
   "max_width": 100,
   "name": "Nb de taxons",
   "prop": "taxa_count",
   "show": true
  },
  {
   "max_width": 100,
   "name": "Nb de donnees",
   "prop": "import_count",
   "show": true
  },
  {
   "max_width": 200,
   "name": "Debut import",
   "prop": "date_create_import",
   "show": true
  },
  {
   "max_width": 200,
   "name": "Fin import",
   "prop": "date_end_import",
   "show": true
  }
 ],
 "MAPPING_DATA_FRONTEND": [
  {
   "fields": [
    {
     "label": "Identifiant source",
     "name": "entity_source_pk_value",
     "required": false
    },
    {
     "label": "Identifiant SINP (uuid)",
     "name": "unique_id_sinp",
     "required": false
    },
    {
     "label": "Date de cr\u00e9ation de la donn\u00e9e",
     "name": "meta_v_taxref",
     "required": false
    },
    {
     "label": "Version du r\u00e9f\u00e9rentiel taxonomique",
     "name": "meta_create_date",
     "required": false
    },
    {
     "label": "Date de mise \u00e0 jour de la donn\u00e9e",
     "name": "meta_update_date",
     "required": false
    }
   ],
   "label": "Informations g\u00e9n\u00e9rales",
   "name": "general_info"
  },
  {
   "fields": [
    {
     "label": "Date d\u00e9but",
     "name": "date_min",
     "required": true
    },
    {
     "label": "Date fin",
     "name": "date_max",
     "required": false
    },
    {
     "label": "Altitude min",
     "name": "altitude_min",
     "required": false
    },
    {
     "label": "Altitude max",
     "name": "altitude_max",
     "required": false
    },
    {
     "label": "Observateur(s)",
     "name": "observers",
     "required": false
    },
    {
     "label": "Commentaire de relev\u00e9",
     "name": "comment_description",
     "required": false
    },
    {
     "label": "Type d'information g\u00e9ographique",
     "name": "id_nomenclature_info_geo_type",
     "required": false
    },
    {
     "label": "Type de relev\u00e9/regroupement",
     "name": "id_nomenclature_grp_typ",
     "required": false
    }
   ],
   "label": "Informations de relev\u00e9s",
   "name": "statement_info"
  },
  {
   "fields": [
    {
     "label": "Nom du taxon cit\u00e9",
     "name": "nom_cite",
     "required": true
    },
    {
     "label": "Cd nom taxref",
     "name": "cd_nom",
     "required": true
    },
    {
     "label": "M\u00e9thode d'observation",
     "name": "id_nomenclature_obs_meth",
     "required": false
    },
    {
     "label": "Statut biologique",
     "name": "id_nomenclature_bio_status",
     "required": false
    },
    {
     "label": "Etat biologique",
     "name": "id_nomenclature_bio_condition",
     "required": false
    },
    {
     "label": "Naturalit\u00e9",
     "name": "id_nomenclature_naturalness",
     "required": false
    },
    {
     "label": "Commentaire d'occurrence",
     "name": "comment_context",
     "required": false
    },
    {
     "label": "Sensibilit\u00e9",
     "name": "id_nomenclature_sensitivity"
    },
    {
     "label": "Niveau de diffusion",
     "name": "id_nomenclature_diffusion_level",
     "required": false
    },
    {
     "label": "Floutage",
     "name": "id_nomenclature_blurring",
     "required": false
    }
   ],
   "label": "Informations d'occurrences & sensibilt\u00e9 ",
   "name": "occurrence_sensitivity"
  },
  {
   "fields": [
    {
     "label": "Stade de vie",
     "name": "id_nomenclature_life_stage",
     "required": false
    },
    {
     "label": "Sexe",
     "name": "id_nomenclature_sex",
     "required": false
    },
    {
     "label": "Type du d\u00e9nombrement",
     "name": "id_nomenclature_type_count"
    },
    {
     "label": "Objet du d\u00e9nombrement",
     "name": "id_nomenclature_obj_count",
     "required": false
    },
    {
     "label": "Nombre minimal",
     "name": "count_min",
     "required": false
    },
    {
     "label": "Nombre maximal",
     "name": "count_max",
     "required": false
    }
   ],
   "label": "D\u00e9nombrements",
   "name": "enumeration"
  },
  {
   "fields": [
    {
     "label": "M\u00e9thode de d\u00e9termination",
     "name": "id_nomenclature_determination_method",
     "required": false
    },
    {
     "label": "D\u00e9terminateur",
     "name": "determiner",
     "required": false
    },
    {
     "label": "Auteur de la saisie",
     "name": "id_digitiser",
     "required": false
    },
    {
     "label": "Existance d'une preuve",
     "name": "id_nomenclature_exist_proof",
     "required": false
    },
    {
     "label": "Preuve num\u00e9rique",
     "name": "digital_proof",
     "required": false
    },
    {
     "label": "Preuve non-num\u00e9rique",
     "name": "non_digital_proof",
     "required": false
    },
    {
     "label": "Identifiant de l'\u00e9chantillon preuve",
     "name": "sample_number_proof",
     "required": false
    },
    {
     "label": "Statut de validation",
     "name": "id_nomenclature_valid_status",
     "required": false
    },
    {
     "label": "Validateur",
     "name": "validator",
     "required": false
    },
    {
     "label": "Date de validation",
     "name": "meta_validation_date",
     "required": false
    },
    {
     "label": "Commentaire de validation",
     "name": "validation_comment",
     "required": false
    }
   ],
   "label": "D\u00e9termination & validit\u00e9",
   "name": "validation"
  }
 ],
 "MAX_FILE_SIZE": 500,
 "MODULE_CODE": "IMPORT",
 "MODULE_URL": "import",
 "NOT_NULLABLE_SYNTHESE_FIELDS": [
  "cd_nom",
  "nom_cite",
  "date_min"
 ],
 "PREFIX": "gn_",
 "SEPARATOR": [
  {
   "code": ",",
   "name": "virgule (,)"
  },
  {
   "code": "\t",
   "name": "tabulation"
  },
  {
   "code": ";",
   "name": "point-virgule (;)"
  },
  {
   "code": " ",
   "name": "espace"
  }
 ],
 "SRID": [
  {
   "code": 4326,
   "name": "WGS84"
  },
  {
   "code": 2154,
   "name": "Lambert93"
  }
 ],
 "UPLOAD_DIRECTORY": "upload"
}