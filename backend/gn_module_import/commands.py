import click

from geonature.utils.env import db as DB

from .blueprint import blueprint
from .db.models import TMappings, TMappingsFields, TMappingsValues


synthese_fieldmappings =  [
    ('uuid_perm_sinp','unique_id_sinp',True,False),
    ('id_synthese','entity_source_pk_value',True,False),
    ('uuid_perm_grp_sinp','unique_id_sinp_grp',True,False),
    ('','unique_id_sinp_generate',False,False),
    ('date_creation','meta_create_date',False,False),
    ('','meta_v_taxref',True,False),
    ('date_modification','meta_update_date',False,False),
    ('date_debut','date_min',True,False),
    ('date_fin','date_max',True,False),
    ('heure_debut','hour_min',True,False),
    ('heure_fin','hour_max',True,False),
    ('alti_min','altitude_min',True,False),
    ('alti_max','altitude_max',True,False),
    ('prof_min','depth_min',True,False),
    ('prof_max','depth_max',True,False),
    ('','altitudes_generate',False,False),
    ('','longitude',False,False),
    ('','latitude',False,False),
    ('observateurs','observers',True,False),
    ('comment_occurrence','comment_description',True,False),
    ('type_info_geo','id_nomenclature_info_geo_type',True,False),
    ('type_regroupement','id_nomenclature_grp_typ',True,False),
    ('methode_regroupement','grp_method',True,False),
    ('nom_cite','nom_cite',True,False),
    ('cd_nom','cd_nom',True,False),
    ('technique_observation','id_nomenclature_obs_technique',True,False),
    ('biologique_statut','id_nomenclature_bio_status',True,False),
    ('etat_biologique','id_nomenclature_bio_condition',True,False),
    ('biogeographique_statut','id_nomenclature_biogeo_status',True,False),
    ('comportement','id_nomenclature_behaviour',True,False),
    ('naturalite','id_nomenclature_naturalness',True,False),
    ('comment_releve','comment_context',True,False),
    ('niveau_sensibilite','id_nomenclature_sensitivity',True,False),
    ('niveau_precision_diffusion','id_nomenclature_diffusion_level',True,False),
    ('floutage_dee','id_nomenclature_blurring',True,False),
    ('stade_vie','id_nomenclature_life_stage',True,False),
    ('sexe','id_nomenclature_sex',True,False),
    ('type_denombrement','id_nomenclature_type_count',True,False),
    ('objet_denombrement','id_nomenclature_obj_count',True,False),
    ('nombre_min','count_min',True,False),
    ('nombre_max','count_max',True,False),
    ('methode_determination','id_nomenclature_determination_method',True,False),
    ('determinateur','determiner',True,False),
    ('','id_digitiser',False,False),
    ('preuve_existante','id_nomenclature_exist_proof',True,False),
    ('preuve_numerique_url','digital_proof',True,False),
    ('preuve_non_numerique','non_digital_proof',True,False),
    ('niveau_validation','id_nomenclature_valid_status',True,False),
    ('validateur','validator',True,False),
    ('date_validation','meta_validation_date',True,False),
    ('comment_validation','validation_comment',True,False),
    ('nature_objet_geo','id_nomenclature_geo_object_nature',True,False),
    ('statut_observation','id_nomenclature_observation_status',True,False),
    ('statut_source','id_nomenclature_source_status',True,False),
    ('reference_biblio','reference_biblio',True,False),
    ('cd_habref','cd_hab',True,False),
    ('geometrie_wkt_4326','WKT',True,False),
    ('nom_lieu','place_name',True,False),
    ('precision_geographique','precision',True,False),
    ('','the_geom_point',False,True),
    ('','the_geom_local',False,True),
    ('','the_geom_4326',False,True),
    ('','codecommune',True,False),
    ('','codemaille',True,False),
    ('','codedepartement',True,False),
]
dee_fieldmappings = [
    ('altmax', 'altitude_max',True,False),
    ('altmin', 'altitude_min',True,False),
    ('cdnom', 'cd_nom',True,False),
    ('cdcom', 'codecommune',True,False),
    ('cddept', 'codedepartement',True,False),
    ('cdm10', 'codemaille',True,False),
    ('denombrementmax', 'count_max',True,False),
    ('denombrementmin', 'count_min',True,False),
    ('geometrie','WKT',True,False),
    ('heuredebut','date_min',True,False),
    ('heurefin','date_max',True,False),
    ('permid', 'unique_id_sinp',True,False),
    ('permid','entity_source_pk_value',True,False),
    ('permidgrp','unique_id_sinp_grp',True,False),
    ('datedebut','date_min',True,False),
    ('datefin','date_max',True,False),
    ('methgrp','id_nomenclature_grp_typ',True,False),
    ('natobjgeo','id_nomenclature_geo_object_nature',True,False),
    ('nomcite','nom_cite',True,False),
    ('objdenbr','id_nomenclature_obj_count',True,False),
    ('obsctx','comment_context',True,False),
    ('obsdescr','comment_description',True,False),
    ('obsmeth','id_nomenclature_obs_meth',True,False),
    ('ocetatbio','id_nomenclature_bio_condition',True,False),
    ('ocmethdet','id_nomenclature_determination_method',True,False),
    ('ocnat','id_nomenclature_naturalness',True,False),
    ('ocsex','id_nomenclature_sex',True,False),
    ('ocstade','id_nomenclature_life_stage',True,False),
    ('ocstatbio','id_nomenclature_bio_status',True,False),
    ('preuveoui','id_nomenclature_exist_proof',True,False),
    ('Preuvnonum','non_digital_proof',True,False),
    ('Preuvnum','digital_proof',True,False),
    ('statobs','id_nomenclature_observation_status',True,False),
    ('statsource','id_nomenclature_source_status',True,False),
    ('typdenbr','id_nomenclature_type_count',True,False),
    ('typgrp','id_nomenclature_grp_typ',True,False),
]


@blueprint.cli.command()
def fix_mappings():
    for mapping_label, fieldmappings in [
                ('Synthese GeoNature', synthese_fieldmappings),
                ('Format DEE (champs 10 char)', dee_fieldmappings),
            ]:
        fix_fieldmapping(fieldmappings, mapping_label)

def fix_fieldmapping(fieldmappings, mapping_label):
    fieldmappings = {
        target: (source, target, is_selected, is_added)
        for (source, target, is_selected, is_added) in fieldmappings
    }
    mapping = (
            TMappings.query
            .filter_by(mapping_type="FIELD")
            .filter_by(mapping_label=mapping_label)
            .one()
    )
    if not mapping.active:
        print("Correction du mapping : active ← True")
        mapping.active = True
    if mapping.temporary:
        print("Correction du mapping : temporary ← False")
        mapping.temporary = False
    if not mapping.is_public:
        print("Correction du mapping : is_public ← True")
        mapping.is_public = True
    actual_fieldmappings = TMappingsFields.query.filter_by(id_mapping=mapping.id_mapping)
    actual_fieldmappings = {
            fieldmapping.target_field: fieldmapping
            for fieldmapping in actual_fieldmappings.all()
    }
    # Missing mappings
    for target in set(fieldmappings.keys()) - set(actual_fieldmappings.keys()):
        print("Missing mapping", target)
    # Unwanted mappings
    for target in set(actual_fieldmappings.keys()) - set(fieldmappings.keys()):
        print("Unwanted mapping", target)
    # Mapping to check
    for target in set(actual_fieldmappings.keys()) & set(fieldmappings.keys()):
        source, target, is_selected, is_added = fieldmappings[target]
        fm = actual_fieldmappings[target]
        if fm.source_field != source:
            print(f"Correction source {target}: '{fm.source_field}' → '{source}'")
            fm.source_field = source
        if fm.is_selected != is_selected:
            print(f"Correction selected {target}: '{fm.is_selected}' → '{is_selected}'")
            fm.is_selected = is_selected
        if fm.is_added != is_added:
            print(f"Correction added {target}: '{fm.is_added}' → '{is_added}'")
            fm.is_added = is_added
        DB.session.commit()
