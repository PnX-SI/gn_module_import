DROP VIEW gn_imports.v_imports_errors;


ALTER TABLE gn_imports.t_user_error_list 
ALTER COLUMN id_rows type text[];

CREATE VIEW gn_imports.v_imports_errors AS 
SELECT 
id_user_error,
id_import,
error_type,
name AS error_name,
error_level,
description AS error_description,
column_error,
id_rows,
comment
FROM  gn_imports.t_user_error_list el 
JOIN gn_imports.t_user_errors ue on ue.id_error = el.id_error;


INSERT INTO gn_imports.t_user_errors (error_type,"name",description,error_level) VALUES 
('Incohérence','CONDITIONAL_INVALID_DATA','Erreur de valeur','ERROR');


UPDATE gn_imports.dict_fields
SET comment = 'Correspondance champs standard: idSINPOccTax'
WHERE name_field = 'unique_id_sinp'
;

UPDATE gn_imports.dict_fields
SET comment = 'Correspondance champs standard: profondeurMin. Entier attendu'
WHERE name_field = 'depth_min'
;

UPDATE gn_imports.dict_fields
SET comment = 'Correspondance champs standard: profondeurMax. Entier attendu'
WHERE name_field = 'depth_max'
;

UPDATE gn_imports.dict_fields
SET comment = 'Correspondance champs standard: precisionGeometrie. Entier attendu'
WHERE name_field = 'precision'
;

UPDATE gn_imports.dict_fields
SET comment = 'Correspondance champs standard: CodeHabitatValue. Entier attendu'
WHERE name_field = 'cd_hab'
;

UPDATE gn_imports.dict_fields
SET comment = 'Correspondance champs standard: denombrementMin. Entier attendu'
WHERE name_field = 'count_min'
;

UPDATE gn_imports.dict_fields
SET comment = 'Correspondance champs standard: denombrementMax. Entier attendu'
WHERE name_field = 'count_max'
;


ALTER TABLE gn_imports.t_mappings
ADD COLUMN is_public boolean default false;
