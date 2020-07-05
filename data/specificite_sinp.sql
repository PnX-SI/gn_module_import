ALTER TABLE gn_imports.dict_fields
SET display = FALSE 
WHERE name_field IN (
    'id_nomenclature_sensitivity',
    'id_digitiser',
    'meta_v_taxref',
    'meta_create_date',
    'meta_update_date'
);