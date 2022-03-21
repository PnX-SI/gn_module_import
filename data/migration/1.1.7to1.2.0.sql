-----------------------
-- Add id_source column
-----------------------
ALTER TABLE only gn_imports.t_imports
  ADD COLUMN id_source_synthese integer,
  ADD CONSTRAINT fk_gn_imports_t_import_id_source_synthese FOREIGN KEY (id_source_synthese) REFERENCES gn_synthese.t_sources(id_source) ON UPDATE CASCADE ON DELETE CASCADE;

-- Populate id_source column
UPDATE gn_imports.t_imports
SET id_source_synthese = s.id_source
FROM gn_synthese.t_sources s
WHERE s.name_source='Import(id='||id_import||')';

--------------------------------------
-- Add need_fix and fix_comment column
--------------------------------------
ALTER TABLE gn_imports.t_imports 
ADD need_fix boolean default false,
ADD fix_comment text;

--------------------------------------
-- Add new error
--------------------------------------
INSERT INTO gn_imports.t_user_errors (error_type, name, description, error_level) VALUES ('Géométrie','GEOMETRY_OUTSIDE', 'La géométrie se trouve à l''extérieur du territoire renseigné','ERROR');

--------------------------------------
-- Change error description
--------------------------------------
UPDATE gn_imports.t_user_errors
set description = 'Le Cd_nom renseigné ne peut être importé car il est absent du référentiel TAXREF ou de la liste de taxons importables configurée par l’administrateur'
where name = 'CD_NOM_NOT_FOUND';

------------------------------------------------
-- Enable additional field in a dedicated theme
------------------------------------------------
-- Fix typo : set only one n in additionnal data for existing fields
UPDATE gn_imports.dict_fields
SET name_field='additional_data'
WHERE name_field='additionnal_data';

-- Create new theme for additional field
INSERT INTO gn_imports.dict_themes (name_theme, fr_label_theme, eng_label_theme, desc_theme, order_theme)
VALUES ('additional_data', 'Champs additionnels', '', '', 6);

-- Enable additional field in form
UPDATE gn_imports.dict_fields
SET id_theme = t.id_theme,
order_field = 1,
display = TRUE
FROM gn_imports.dict_themes t 
WHERE t.name_theme='additional_data'
AND name_field='additional_data';
