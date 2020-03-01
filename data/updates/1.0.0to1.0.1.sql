-- Ajout d'un champ commentaire sur gn_import.t_imports
ALTER TABLE gn_import.t_imports
  ADD COLUMN comment text ;
