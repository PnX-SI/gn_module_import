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

CREATE TABLE t_imports(
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
  date_max_data timestamp without time zone,
  step integer
);


CREATE TABLE cor_role_import(
  id_role integer NOT NULL,
  id_import integer NOT NULL
);


CREATE TABLE user_errors(
  id_error integer NOT NULL,
  error_type character varying(100) NOT NULL,
  name character varying(255) NOT NULL UNIQUE,
  description character varying(255) NOT NULL
);





---------------
--PRIMARY KEY--
---------------

ALTER TABLE ONLY t_imports 
    ADD CONSTRAINT pk_gn_imports_t_imports PRIMARY KEY (id_import);

ALTER TABLE ONLY cor_role_import 
    ADD CONSTRAINT pk_cor_role_import PRIMARY KEY (id_role, id_import);

ALTER TABLE ONLY user_errors 
    ADD CONSTRAINT pk_user_errors PRIMARY KEY (id_error);


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


--------------
--INSERTIONS--
--------------

INSERT INTO user_errors (id_error, error_type, name, description) VALUES
	(1, 'invalid type error', 'invalid integer type', 'type integer invalide'),
	(2, 'invalid type error', 'invalid date type', 'type date invalide'),
	(3, 'invalid type error', 'invalid uuid type', 'type uuid invalide'),
	(4, 'invalid type error', 'invalid character varying length', 'champs de type character varying trop long'),
	(5, 'missing value error', 'missing value in required field', 'valeur manquante dans un champs obligatoire'),
	(6, 'missing value warning', 'warning : missing uuid type value', 'warning : valeur de type uuid manquante (non bloquant)'),
	(7, 'inconsistency error', 'date_min > date_max', 'date_min > date_max'),
	(8, 'inconsistency error', 'count_min > count_max', 'count_min > count_max'),
	(9, 'invalid value', 'invalid cd_nom', 'cd_nom invalide (absent de TaxRef)');














