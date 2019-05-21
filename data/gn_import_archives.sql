SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;


CREATE SCHEMA gn_import_archives;


SET search_path = gn_import_archives, pg_catalog;
SET default_with_oids = false;


----------
--TABLES--
----------

CREATE TABLE cor_import_archives(
  id_import integer NOT NULL,
  table_archive character varying(255) NOT NULL
);


---------------
--PRIMARY KEY--
---------------

ALTER TABLE ONLY cor_import_archives ADD CONSTRAINT pk_cor_import_archives PRIMARY KEY (id_import, table_archive);


---------------
--FOREIGN KEY--
---------------

ALTER TABLE ONLY cor_import_archives
    ADD CONSTRAINT fk_gn_imports_t_imports FOREIGN KEY (id_import) REFERENCES gn_imports.t_imports(id_import) ON UPDATE CASCADE ON DELETE CASCADE;


------------
--TRIGGERS--
------------

-- faire trigger pour rappatrier données dans cor_import_archives quand creation dun nouvel import?


-------------
--FUNCTIONS--
-------------

