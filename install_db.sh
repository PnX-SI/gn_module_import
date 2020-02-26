#!/bin/bash

. config/settings.ini

# Create log folder in module folders if it don't already exists
if [ ! -d 'var' ]
then
  mkdir var
fi

if [ ! -d 'var/log' ]
then
  mkdir var/log
fi


touch config/conf_gn_module.toml
echo -n "create tables and functions for gn_module_import "
export PGPASSWORD=$user_pg_pass; psql -h $db_host -p $db_port -U $user_pg -d $db_name -b -f data/import_db.sql  &>> var/log/install_gn_imports.log
return_value=$?
#check_psql_status $return_value

echo -n "insert essential data"
export PGPASSWORD=$user_pg_pass; psql -h $db_host -p $db_port -U $user_pg -d $db_name -b -f data/data.sql  &>> var/log/install_gn_import_archives.log
return_value=$?
#check_psql_status $return_value

echo -n "create default mapping for GeoNature extracted data"
export PGPASSWORD=$user_pg_pass; psql -h $db_host -p $db_port -U $user_pg -d $db_name -b -f data/default_mappings_data.sql  &>> var/log/install_gn_import_archives.log
return_value=$?
#check_psql_status $return_value
