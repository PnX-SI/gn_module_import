export const ModuleConfig = {
 "ALLOWED_EXTENSIONS": [
  ".csv",
  ".geojson"
 ],
 "ALLOW_VALUE_MAPPING": false,
 "ARCHIVES_SCHEMA_NAME": "gn_import_archives",
 "DEFAULT_COUNT_VALUE": 1,
 "DEFAULT_MAPPING_ID": 3,
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
 "ID_MODULE": 7,
 "IMPORTS_SCHEMA_NAME": "gn_imports",
 "INVALID_CSV_NAME": "invalid",
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
   "db_code": "comma",
   "name": "virgule (,)"
  },
  {
   "code": "\t",
   "db_code": "tab",
   "name": "tabulation"
  },
  {
   "code": ";",
   "db_code": "colon",
   "name": "point-virgule (;)"
  },
  {
   "code": " ",
   "db_code": "space",
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