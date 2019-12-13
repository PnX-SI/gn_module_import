export const ModuleConfig = {
	ALLOWED_EXTENSIONS: [ '.csv', '.json' ],
	ARCHIVES_SCHEMA_NAME: 'gn_import_archives',
	DEFAULT_COUNT_VALUE: 1,
	ENCODAGE: [ 'UTF-8' ],
	EXCLUDED_SYNTHESE_FIELDS_FRONT: [
		'id_synthese',
		'id_source',
		'id_module',
		'id_dataset',
		'the_geom_4326',
		'the_geom_point',
		'the_geom_local',
		'last_action'
	],
	ID_MODULE: 17,
	IMPORTS_SCHEMA_NAME: 'gn_imports',
	LIST_COLUMNS_FRONTEND: [
		{
			max_width: 50,
			name: 'Id',
			prop: 'id_import',
			show: true
		},
		{
			max_width: 100,
			name: 'File format',
			prop: 'format_source_file',
			show: false
		},
		{
			max_width: 400,
			name: 'JDD',
			prop: 'dataset_name',
			show: true
		},
		{
			max_width: 120,
			name: 'Nb de taxons',
			prop: 'taxa_count',
			show: true
		},
		{
			max_width: 120,
			name: 'Nb de donnees',
			prop: 'import_count',
			show: true
		},
		{
			max_width: 150,
			name: 'Debut import',
			prop: 'date_create_import',
			show: true
		},
		
	],
	MAX_FILE_SIZE: 500,
	MISSING_VALUES: [ '', 'NA', 'NaN', 'na' ],
	MODULE_CODE: 'IMPORT',
	MODULE_URL: 'import',
	NOT_NULLABLE_SYNTHESE_FIELDS: [ 'cd_nom', 'nom_cite', 'date_min' ],
	PREFIX: 'gn_',

	SEPARATOR: [
		{ name: 'virgule (,)', code: ',', db_code: 'comma' },
		{ name: 'tabulation', code: '\t', db_code: 'tab' },
		{ name: 'point-virgule (;)', code: ';', db_code: 'colon' },
		{ name: 'espace', code: ' ', db_code: 'space' }
	],

	SRID: [
		{
			code: 4326,
			name: 'WGS84'
		},
		{
			code: 2154,
			name: 'Lambert93'
		}
	],
	UPLOAD_DIRECTORY: 'upload'
};
