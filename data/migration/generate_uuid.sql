UPDATE gn_synthese.synthese 
SET unique_id_sinp = uuid_generate_v4()
WHERE unique_id_sinp IS NULL AND id_module = (
    SELECT id_module FROM gn_commons.t_modules WHERE module_code = 'IMPORT'
    ) 