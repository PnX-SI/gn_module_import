#!/bin/bash

mkdir -p var/log

. config/settings.ini

touch config/conf_gn_module.toml
echo -n "create tables and functions in gn_imports_schema "
export PGPASSWORD=$user_pg_pass; psql -h $db_host -p $db_port -U $user_pg -d $db_name -b -f data/gn_imports.sql  &>> var/log/install_gn_imports.log
return_value=$?
#check_psql_status $return_value

echo -n "create gn_import_archives schema, tables and functions"
export PGPASSWORD=$user_pg_pass; psql -h $db_host -p $db_port -U $user_pg -d $db_name -b -f data/gn_import_archives.sql  &>> var/log/install_gn_import_archives.log
return_value=$?
#check_psql_status $return_value
