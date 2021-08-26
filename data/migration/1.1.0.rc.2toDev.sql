 --------------
 --GN_IMPORTS--
 --------------
-- Insert a new error 
INSERT INTO gn_imports.t_user_errors (error_type, name, description, error_level) VALUES ('Géométrie','GEOMETRY_OUTSIDE3', 'La géométrie se trouve à l''extérieur du territoire renseigné','ERROR');
-- Make description more generic if taxon list is provided in the config
UPDATE gn_imports.t_user_errors SET description = 'Le Cd_nom renseigné ne peut être importé car il est absent du référentiel TAXREF ou de la liste de taxons importables configurée par l''administrateur' WHERE name = 'CD_NOM_NOT_FOUND';
