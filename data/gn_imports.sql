SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = gn_imports, pg_catalog;
SET default_with_oids = false;


------------------------
--TABLES AND SEQUENCES--
------------------------

CREATE TABLE gn_imports.t_imports(
  id_import serial NOT NULL,
  format_source_file character varying(5),
  SRID integer,
  import_table character varying(255),
  id_dataset integer,
  id_mapping integer,
  date_create_import timestamp without time zone DEFAULT now(),
  date_update_import timestamp without time zone DEFAULT now(),
  date_end_import timestamp without time zone,
  source_count integer,
  import_count integer,
  taxa_count integer,
  date_min_data timestamp without time zone,
  date_max_data timestamp without time zone
);


CREATE TABLE gn_imports.cor_role_import(
  id_role integer NOT NULL,
  id_import integer NOT NULL
);






---------------
--PRIMARY KEY--
---------------

ALTER TABLE ONLY t_imports 
    ADD CONSTRAINT pk_gn_imports_t_imports PRIMARY KEY (id_import);

ALTER TABLE ONLY cor_role_import 
    ADD CONSTRAINT pk_cor_role_import PRIMARY KEY (id_role, id_import);


---------------
--FOREIGN KEY--
---------------

ALTER TABLE ONLY t_imports
    ADD CONSTRAINT fk_gn_meta_t_datasets FOREIGN KEY (id_dataset) REFERENCES gn_meta.t_datasets(id_dataset) ON UPDATE CASCADE ON DELETE CASCADE;

ALTER TABLE ONLY cor_role_import
    ADD CONSTRAINT fk_utilisateurs_t_roles FOREIGN KEY (id_role) REFERENCES utilisateurs.t_roles(id_role) ON UPDATE CASCADE ON DELETE CASCADE;


------------
--TRIGGERS--
------------
-- faire un trigger pour cor_role_mapping qui rempli quand create ou delete t_mappings.id_mapping?



-------------
--FUNCTIONS--
-------------

CREATE OR REPLACE FUNCTION gn_imports.get_datasets(IN id integer)
  RETURNS TABLE(dataset_id integer,dataset character varying) AS
$BODY$

-- Function that allows to get user dataset names
-- USAGE : SELECT gn_import.get_datasets(id_role);
  BEGIN
	return query
	select id_dataset,dataset_name
	from gn_meta.t_datasets
	where id_dataset in (
		select distinct id_dataset
		from gn_synthese.cor_observer_synthese cos
		join gn_synthese.synthese s on s.id_synthese=cos.id_synthese
		where id_role = id);
	return;
  END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100
  ROWS 1000;





