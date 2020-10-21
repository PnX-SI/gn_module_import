SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = gn_imports, pg_catalog;
SET default_with_oids = false;


--------------
--INSERTIONS--
--------------

INSERT INTO gn_imports.t_mappings (mapping_label, mapping_type, active)
VALUES
('Synthèse GeoNature', 'FIELD', true),
('Nomenclatures SINP (labels)', 'CONTENT', true),
('Nomenclatures SINP (codes)', 'CONTENT', true);

-- On donne des droits à tous les groupes GN pour tous les mappings 
INSERT INTO gn_imports.cor_role_mapping(id_mapping, id_role)
SELECT 
tm.id_mapping,
id_role
FROM utilisateurs.t_roles, gn_imports.t_mappings tm
WHERE groupe IS True 
AND id_role IN (
    SELECT id_role
    FROM utilisateurs.cor_role_app_profil 
    WHERE id_application IN (
        SELECT id_application FROM utilisateurs.t_applications
        WHERE code_application = 'GN'
    )
);

INSERT INTO gn_imports.t_mappings_fields (id_mapping, source_field, target_field, is_selected, is_added)
VALUES 
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'permid','unique_id_sinp',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'idsynthese','entity_source_pk_value',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'permidgrp','unique_id_sinp_grp',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), '','unique_id_sinp_generate',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), '','meta_create_date',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'vtaxref','meta_v_taxref',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), '','meta_update_date',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'datedebut','date_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'datefin','date_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'heuredebut','hour_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'heurefin','hour_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'altmin','altitude_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'altmax','altitude_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'profmin','depth_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'profmax','depth_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), '','altitudes_generate',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'x_centroid','longitude',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'y_centroid','latitude',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'observer','observers',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'obsdescr','comment_description',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'typinfgeo','id_nomenclature_info_geo_type',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'typGrp','id_nomenclature_grp_typ',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'nomcite','nom_cite',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'cdnom','cd_nom',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'obsmeth','id_nomenclature_obs_technique',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'ocstatbio','id_nomenclature_bio_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'ocetatbio','id_nomenclature_bio_condition',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'occComport','id_nomenclature_behaviour',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'ocnat','id_nomenclature_naturalness',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'obsctx','comment_context',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'sensiniv','id_nomenclature_sensitivity',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'difnivprec','id_nomenclature_diffusion_level',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'deeflou','id_nomenclature_blurring',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'ocstade','id_nomenclature_life_stage',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'ocsex','id_nomenclature_sex',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'denbrtyp','id_nomenclature_type_count',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'objdenbr','id_nomenclature_obj_count',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'denbrmin','count_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'denbrmax','count_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'ocMethDet','id_nomenclature_determination_method',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'detminer','determiner',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'id_digitiser','id_digitiser',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'preuveoui','id_nomenclature_exist_proof',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'urlpreuv','digital_proof',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'preuvnonum','non_digital_proof',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'nivval','id_nomenclature_valid_status',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'validateur','validator',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'datectrl','meta_validation_date',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), '','validation_comment',false,false),

((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'natObjGeo','id_nomenclature_geo_object_nature',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'obstech','id_nomenclature_obs_technique',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'statobs','id_nomenclature_observation_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'statsource','id_nomenclature_source_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'refbiblio','reference_biblio',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'cdhab','cd_hab',true,false),

((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'geometry','WKT',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'gn_1_the_geom_point_2','the_geom_point',false,true),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'gn_1_the_geom_local_2','the_geom_local',false,true),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'gn_1_the_geom_4326_2','the_geom_4326',false,true),

((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'codecommune','codecommune',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'codemaille','codemaille',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Synthèse GeoNature'), 'codedepartement','codedepartement',true,false)

;


-- Intégration du mapping de valeurs SINP (labels) par défaut pour les nomenclatures de la synthèse 
INSERT INTO gn_imports.t_mappings_values (id_mapping, source_value, id_target_value)
SELECT
m.id_mapping, 
n.label_default,
n.id_nomenclature
FROM gn_imports.t_mappings m, ref_nomenclatures.t_nomenclatures n
JOIN ref_nomenclatures.bib_nomenclatures_types bnt ON bnt.id_type=n.id_type 
WHERE m.mapping_label='Nomenclatures SINP (labels)' AND bnt.mnemonique IN (SELECT DISTINCT(mnemonique) FROM gn_imports.cor_synthese_nomenclature);


-- Intégration du mapping de valeurs SINP (codes) par défaut pour les nomenclatures de la synthèse
INSERT INTO gn_imports.t_mappings_values (id_mapping, source_value, id_target_value)
SELECT
m.id_mapping,
n.cd_nomenclature,
n.id_nomenclature
FROM gn_imports.t_mappings m, ref_nomenclatures.t_nomenclatures n
JOIN ref_nomenclatures.bib_nomenclatures_types bnt ON bnt.id_type=n.id_type 
WHERE m.mapping_label='Nomenclatures SINP (codes)' AND bnt.mnemonique IN (SELECT DISTINCT(mnemonique) FROM gn_imports.cor_synthese_nomenclature);
