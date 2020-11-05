SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = gn_imports, pg_catalog;
SET default_with_oids = false;

-- Création d'un mapping de champs (modèle d'import) pour DEPOBIO
-- A tester et mettre à jour

--------------
--INSERTIONS--
--------------

INSERT INTO gn_imports.t_mappings (mapping_label, mapping_type, active)
VALUES
('DEPOBIO', 'FIELD', true);

INSERT INTO gn_imports.t_mappings_fields (id_mapping, source_field, target_field, is_selected, is_added)
VALUES 
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'altmax', 'altitude_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'altmin', 'altitude_min',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'altMoy','altitudeMoyenne',true,false), On ne stocke pas cette info dans la synthese, elle est calculable
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'anRefCom','anneeRefCommune',true,false), On ne stocke pas cette info dans la synthese, elle est calculable
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'anRefDep','anneeRefDepartement',true,false), On ne stocke pas cette info dans la synthese, elle est calculable
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'cdnom', 'cd_nom',true,false), 
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'cdRef','cdRef',true,false), Calculable à partir du cd_nom 
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'cdcom', 'codecommune',true,false), 
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'cddept', 'codedepartement',true,false), 
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'cdm10', 'codemaille',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'comment','comment_context',true,false), OBSCtx et Comment sont les mêmes ?
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'dateDet','dateDetermination',true,false), N’existe pas dans GN. On considère que c’est la date de création de la donnée
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'denombrementmax', 'count_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'denombrementmin', 'count_min',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'detId','determinateurIdentite',true,false), n'est pas stocké.
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'detNomOrg','determinateurNomOrganisme',true,false), Calculable à partir de « synthese.cor_observers_synthese » qui stocke l’id_role (on retrouve l’organisme dans « utilisateurs.bib_organismes »)
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'dSPublique','dSPublique',true,false), Est stocké au niveau du JDD conformément à la futur évolution du standard Métadonnées
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'geometrie','WKT',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'heuredebut','date_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'heurefin','date_max',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'idOrigine','identifiantOrigine',true,false), On ne le stocke pas dans la synthese mais il est retrouvable à partir de la table t_sources, champs « entity_source_pk_field »
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'permid', 'unique_id_sinp',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'permid','entity_source_pk_value',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'permidgrp','unique_id_sinp_grp',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'jddSourId','jddSourceId',true,false), On ne stocke pas cette info mais elle est potentiellement retrouvable à partie de la table source (si elle a un id jeu de données)
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'datedebut','date_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'datefin','date_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'methgrp','id_nomenclature_grp_typ',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'natobjgeo','id_nomenclature_geo_object_nature',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'nomcite','nom_cite',true,false), 
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'nomCom','nomCommune',true,false), L’ensemble des intersection géographiques sont dans la table « gn_synthese.cor_area_synthese »
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'nomRefM10','nomRefMaille',true,false), L’ensemble des intersection géographiques sont dans la table « gn_synthese.cor_area_synthese »
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'objdenbr','id_nomenclature_obj_count',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'obsctx','comment_context',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'obsdescr','comment_description',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'obsId','observateurIdentite',true,false), Stocké dans la table « synthese.cor_observers_synthese » si la personne est referencée dans « utilisateurs.t_roles »
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'obsNomOrg','observateurNomOrganisme',true,false), Calculable à partir de « synthese.cor_observers_synthese » qui stocke l’id_role (on retrouve l’organisme dans « utilisateurs.bib_organismes »)
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'obsmeth','id_nomenclature_obs_meth',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'ocetatbio','id_nomenclature_bio_condition',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'ocmethdet','id_nomenclature_determination_method',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'ocnat','id_nomenclature_naturalness',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'ocsex','id_nomenclature_sex',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'ocstade','id_nomenclature_life_stage',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'ocBiogeo','occStatutBioGeographique',true,false), N’existe pas encore dans GN. Nouveau champs lié au dernier standard Occtax ?
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'ocstatbio','id_nomenclature_bio_status',true,false), 
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'orgGestDat','organismeGestionnaireDonnee',true,false), Stocké dans la table gn_meta.cor_dataset_actor
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'precisGeo','precisionGeometrie',true,false), champ absent
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'preuveoui','id_nomenclature_exist_proof',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'Preuvnonum','non_digital_proof',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'Preuvnum','digital_proof',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'profMax','profondeurMax',true,false), Pas de champs dans GN. Si altitude négative = profondeur ?
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'profMin','profondeurMin',true,false), Pas de champs dans GN. Si altitude négative = profondeur ?
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'profMoy','profondeurMoyenne',true,false), Calculable 
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'refBiblio','referenceBiblio',true,false), Il n'est pas prévu d'ajouter ce champs
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'statobs','id_nomenclature_observation_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'statsource','id_nomenclature_source_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'typdenbr','id_nomenclature_type_count',true,false),
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'typInfGeoC','typeInfoGeoCommune',true,false), Il n'est pas prévu d'ajouter ce champs
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'typInfGeoD','typeInfoGeoDepartement',true,false), Il n'est pas prévu d'ajouter ce champs
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'typInfGeoM','typeInfoGeoMaille',true,false), Il n'est pas prévu d'ajouter ce champs
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'typgrp','id_nomenclature_grp_typ',true,false);
-- ((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='DEPOBIO'), 'vRefM10','versionRefMaille',true,false); Stocker dans « ref_geo.bib_area_types »

