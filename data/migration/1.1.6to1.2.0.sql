-----------------------
-- Add id_source column
-----------------------
ALTER TABLE only gn_imports.t_imports
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

------------------------------------------------
-- Enable additionnal field in a dedicated theme
------------------------------------------------
INSERT INTO gn_imports.dict_themes (name_theme, fr_label_theme, eng_label_theme, desc_theme, order_theme)
VALUES ('additionnal_data', 'Champs additionnels', '', '', 6);

-- Enable additionnal field in form
UPDATE gn_imports.dict_fields
SET id_theme = t.id_theme,
order_field = 1,
display = TRUE
FROM gn_imports.dict_themes t 
WHERE t.name_theme='additionnal_data'
AND name_field='additionnal_data';
