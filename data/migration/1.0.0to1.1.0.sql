ALTER TABLE gn_imports.t_user_errors
ALTER COLUMN description TYPE text,
ADD COLUMN error_level character varying(25)
;

ALTER TABLE gn_imports.t_user_error_list
ADD COLUMN id_rows integer[];

ALTER TABLE gn_imports.t_imports
ADD COLUMN error_report_path character varying(255);