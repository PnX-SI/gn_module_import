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
