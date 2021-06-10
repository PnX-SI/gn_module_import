-- TODO: supprimer la colonne separator des imports



alter table gn_imports.cor_synthese_nomenclature add constraint fk_gn_imports_cor_synthese_nomenclature_synthese_col foreign key (synthese_col) references gn_imports.dict_fields(name_field) on update cascade on delete cascade;
# TODO: corrige la PK de cor_synthese_nomenclature…


-- Delete duplicate on t_mappings_fields
delete from gn_imports.t_mappings_fields where id_match_fields in (
    select id_match_fields from gn_imports.t_mappings_fields tmf1
    left outer join (
    select count(*) as c, max(id_match_fields) as m, id_mapping, target_field
    from gn_imports.t_mappings_fields
    group by (id_mapping, target_field)
    ) as tmf2 on tmf1.id_match_fields = tmf2.m
    where tmf2.m is null
);
-- Set a unique constraint on t_mappings_fields
ALTER TABLE gn_imports.t_mappings_fields ADD CONSTRAINT un_t_mappings_fields UNIQUE (id_mapping, target_field)


-- Add mnemonique directly on dict_fields and drop table cor_synthese_nomenclature
alter table gn_imports.dict_fields add mnemonique varchar null;
alter table gn_imports.dict_fields add constraint fk_gn_imports_dict_fields_nomenclature foreign key (mnemonique) references ref_nomenclatures.bib_nomenclatures_types(mnemonique) on update set null on delete set null;
update gn_imports.dict_fields df
set mnemonique = df.mnemonique
from gn_imports.cor_synthese_nomenclature csn
where csn.synthese_col = df.name_field ;
drop table gn_imports.cor_synthese_nomenclature ;


-- Set source_value NULL as it is used to map empty cell from source csv file
ALTER TABLE gn_imports.t_mappings_values ALTER COLUMN source_value DROP NOT NULL;


-- Add target_field column in gn_imports.t_mappings_values, allowing null values for now
ALTER TABLE gn_imports.t_mappings_values ADD target_field varchar NULL;

-- Set target_field as foreign key referencing dict_fields
ALTER TABLE gn_imports.t_mappings_values ADD CONSTRAINT fk_gn_imports_t_mappings_values_target_field FOREIGN KEY (target_field) REFERENCES gn_imports.dict_fields(name_field) ON UPDATE CASCADE ON DELETE cascade;

-- Populating target_field from id_target_value through t_nomenclatures, bib_nomenclatures_type and cor_synthese_nomenclature
update
	gn_imports.t_mappings_values tmv
set
	target_field = df.name_field
from
	gn_imports.dict_fields df
	inner join ref_nomenclatures.bib_nomenclatures_types bnt on bnt.mnemonique = df.mnemonique
	inner join ref_nomenclatures.t_nomenclatures tn on tn.id_type = bnt.id_type
where
	tmv.id_target_value = tn.id_nomenclature ;

-- Set target_field as not null as it is now populated
ALTER TABLE gn_imports.t_mappings_values ALTER COLUMN target_field SET NOT NULL;

-- Create a function to check the consistency between target_field and id_target_value (same way we calculate it previously)
create or replace function gn_imports.check_nomenclature_type_consistency(_target_field varchar, _id_target_value integer)
	returns boolean
as $$
begin
	return exists (
		select 1
		from gn_imports.dict_fields df
		inner join ref_nomenclatures.bib_nomenclatures_types bnt on bnt.mnemonique = df.mnemonique
		inner join ref_nomenclatures.t_nomenclatures tn on tn.id_type = bnt.id_type 
		where df.name_field = _target_field and tn.id_nomenclature = _id_target_value
	);
end
$$ language plpgsql;

-- Add a constraint calling the created function
ALTER TABLE gn_imports.t_mappings_values ADD CONSTRAINT check_nomenclature_type_consistency CHECK (gn_imports.check_nomenclature_type_consistency(target_field, id_target_value));

-- Set a constraint making (id_mapping, target_field, source_value) unique
ALTER TABLE gn_imports.t_mappings_values ADD CONSTRAINT un_t_mappings_values UNIQUE (id_mapping, target_field, source_value);




-- Set nullable mapping_label and remove temporary column
ALTER TABLE gn_imports.t_mappings ALTER COLUMN mapping_label DROP NOT NULL;
update gn_imports.t_mappings
set mapping_label = null
where "temporary" = true;
alter table gn_imports.t_mappings drop column temporary;
alter table gn_imports.t_mappings alter column active set default true;
-- Add constraint to ensure mapping label unicity
ALTER TABLE gn_imports.t_mappings ADD CONSTRAINT t_mappings_un UNIQUE (mapping_label);


-- TODO: supprimer la vue des erreurs
