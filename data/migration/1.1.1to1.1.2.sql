-- Update field_type for gn_synthese.synthese.reference_biblio in dict_fields

UPDATE gn_imports.dict_fields 
SET type_field='character varying(5000)'
WHERE name_field='reference_biblio';
