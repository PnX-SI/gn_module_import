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
('Format DEE (champs 10 char)', 'FIELD', true),
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
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'permid','unique_id_sinp',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'idsynthese','entity_source_pk_value',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'permidgrp','unique_id_sinp_grp',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), '','unique_id_sinp_generate',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), '','meta_create_date',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'vtaxref','meta_v_taxref',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), '','meta_update_date',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'datedebut','date_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'datefin','date_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'heuredebut','hour_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'heurefin','hour_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'altmin','altitude_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'altmax','altitude_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'profmin','depth_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'profmax','depth_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), '','altitudes_generate',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'x_centroid','longitude',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'y_centroid','latitude',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'observer','observers',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'obsdescr','comment_description',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'typinfgeo','id_nomenclature_info_geo_type',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'typgrp','id_nomenclature_grp_typ',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'methgrp','grp_method',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'nomcite','nom_cite',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'cdnom','cd_nom',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'obsmeth','id_nomenclature_obs_technique',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'ocstatbio','id_nomenclature_bio_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'ocetatbio','id_nomenclature_bio_condition',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'occcomport','id_nomenclature_behaviour',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'ocnat','id_nomenclature_naturalness',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'obsctx','comment_context',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'sensiniv','id_nomenclature_sensitivity',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'difnivprec','id_nomenclature_diffusion_level',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'deeflou','id_nomenclature_blurring',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'ocstade','id_nomenclature_life_stage',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'ocsex','id_nomenclature_sex',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'denbrtyp','id_nomenclature_type_count',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'objdenbr','id_nomenclature_obj_count',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'denbrmin','count_min',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'denbrmax','count_max',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'ocmethdet','id_nomenclature_determination_method',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'detminer','determiner',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'id_digitiser','id_digitiser',false,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'preuveoui','id_nomenclature_exist_proof',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'urlpreuv','digital_proof',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'preuvnonum','non_digital_proof',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'nivval','id_nomenclature_valid_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'validateur','validator',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'datectrl','meta_validation_date',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'validcom','validation_comment',true,false),

((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'natobjgeo','id_nomenclature_geo_object_nature',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'obstech','id_nomenclature_obs_technique',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'statobs','id_nomenclature_observation_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'statsource','id_nomenclature_source_status',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'refbiblio','reference_biblio',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'cdhab','cd_hab',true,false),

((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'geometrie','WKT',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'nomlieu','place_name',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'precisGeo','precision',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'gn_1_the_geom_point_2','the_geom_point',false,true),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'gn_1_the_geom_local_2','the_geom_local',false,true),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'gn_1_the_geom_4326_2','the_geom_4326',false,true),

((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'codecommune','codecommune',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'codemaille','codemaille',true,false),
((SELECT id_mapping FROM gn_imports.t_mappings WHERE mapping_label='Format DEE (champs 10 char)'), 'codedepartement','codedepartement',true,false)
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
