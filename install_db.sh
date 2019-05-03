#!/bin/bash

. config/settings.ini

echo -n "create gn_import_archives schema as well as tables and functions in gn_import_archives and gn_imports_schema "
PGPASSWORD=$user_pg_pass;psql -h $db_host -p $db_port -U $user_pg -d $db_name -b -f data/gn_imports.sql  &>> var/log/install_gn_imports.log
return_value=$?
check_psql_status $return_value

PGPASSWORD=$user_pg_pass;psql -h $db_host -p $db_port -U $user_pg -d $db_name -b -f data/gn_import_archives.sql  &>> var/log/install_gn_import_archives.log
return_value=$?
check_psql_status $return_value