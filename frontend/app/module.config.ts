export const ModuleConfig = {
    "ARCHIVES_SCHEMA_NAME": "gn_import_archives",
    "ID_MODULE": 13,
    "LIST_COLUMNS_FRONTEND": [
     {
      "max_width": 50,
      "name": "Id",
      "prop": "id_import"
     },
     {
      "max_width": 400,
      "name": "JDD",
      "prop": "dataset_name"
     },
     {
        "max_width": 200,
        "name": "Nb de taxons",
        "prop": "taxa_count"
     },
     {
        "max_width": 200,
        "name": "Nb de données",
        "prop": "import_count"
     }
    ],
    "MODULE_CODE": "IMPORT",
    "MODULE_URL": "import",
    "SRID":[
       {
         "name":"WGS84",
         "code":4326
       },
       {
         "name":"Lambert93",
         "code":2154
       }
     ],
     "ENCODAGE":[
       "UTF-8"
     ],
     "SEPARATOR":[
        {
          "name":"virgule (,)",
          "code":","
        },
        {
          "name":"tabulation",
          "code":"\t"
        },
        {
          "name":"point-virgule (;)",
          "code":";"
        },
        {
          "name":"espace",
          "code":" "
        }
      ],
     "PREFIX":"gn_",
     "UPLOAD_DIRECTORY":"upload"
   }