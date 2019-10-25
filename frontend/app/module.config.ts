export const ModuleConfig = {
 "ALLOWED_EXTENSIONS": [
  ".csv",
  ".json"
 ],
 "ARCHIVES_SCHEMA_NAME": "gn_import_archives",
 "DEFAULT_COUNT_VALUE": 1,
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
   "max_width": 50,
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
   "max_width": 200,
   "name": "Nb de taxons",
   "prop": "taxa_count",
   "show": true
  },
  {
   "max_width": 200,
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
 "MAX_FILE_SIZE": 500,
 "MISSING_VALUES": [
  "",
  "NA",
  "NaN",
  "na"
 ],
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
 "SINP_SYNTHESE_NOMENCLATURES": [
  {
   "nomenclature_abb": "NAT_OBJ_GEO",
   "synthese_col": "id_nomenclature_geo_object_nature"
  },
  {
   "nomenclature_abb": "TYP_GRP",
   "synthese_col": "id_nomenclature_grp_typ"
  },
  {
   "nomenclature_abb": "METH_OBS",
   "synthese_col": "id_nomenclature_obs_meth"
  },
  {
   "nomenclature_abb": "TECHNIQUE_OBS",
   "synthese_col": "id_nomenclature_obs_technique"
  },
  {
   "nomenclature_abb": "STATUT_BIO",
   "synthese_col": "id_nomenclature_bio_status"
  },
  {
   "nomenclature_abb": "ETA_BIO",
   "synthese_col": "id_nomenclature_naturalness"
  },
  {
   "nomenclature_abb": "NATURALITE",
   "synthese_col": "id_nomenclature_exist_proof"
  },
  {
   "nomenclature_abb": "STATUT_VALID",
   "synthese_col": "id_nomenclature_valid_status"
  },
  {
   "nomenclature_abb": "NIV_PRECIS",
   "synthese_col": "id_nomenclature_diffusion_level"
  },
  {
   "nomenclature_abb": "STADE_VIE",
   "synthese_col": "id_nomenclature_life_stage"
  },
  {
   "nomenclature_abb": "SEXE",
   "synthese_col": "id_nomenclature_sex"
  },
  {
   "nomenclature_abb": "OBJ_DENBR",
   "synthese_col": "id_nomenclature_obj_count"
  },
  {
   "nomenclature_abb": "TYP_DENBR",
   "synthese_col": "id_nomenclature_type_count"
  },
  {
   "nomenclature_abb": "SENSIBILITE",
   "synthese_col": "id_nomenclature_sensitivity"
  },
  {
   "nomenclature_abb": "STATUT_OBS",
   "synthese_col": "id_nomenclature_observation_status"
  },
  {
   "nomenclature_abb": "DEE_FLOU",
   "synthese_col": "id_nomenclature_blurring"
  },
  {
   "nomenclature_abb": "STATUT_SOURCE",
   "synthese_col": "id_nomenclature_source_status"
  },
  {
   "nomenclature_abb": "TYP_INF_GEO",
   "synthese_col": "id_nomenclature_info_geo_type"
  },
  {
   "nomenclature_abb": "METH_DETERMIN",
   "synthese_col": "id_nomenclature_determination_method"
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