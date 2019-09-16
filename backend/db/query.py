from sqlalchemy.sql.elements import quoted_name

from psycopg2.extensions import AsIs,QuotedString

from geonature.utils.env import DB

from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
    CorObserverSynthese
)

from geonature.core.gn_meta.models import TDatasets

from .models import (
    TImports,
    CorRoleImport,
    CorImportArchives,
    generate_user_table_class
)

from ..wrappers import checker

import pdb


def get_table_info(table_name,info='all'):
    try:
        table_info = DB.session.execute(\
            "SELECT column_name,is_nullable,column_default,data_type,character_maximum_length\
            FROM INFORMATION_SCHEMA.COLUMNS\
            WHERE table_name = {};".format(QuotedString(table_name))\
        )

        if info == 'all':
            return table_info

        if info == 'type':
            data = {}
            for d in table_info:
                data.update({d.column_name:d.data_type})
            return data
        
        if info == 'column_name':
            data = []
            for d in table_info:
                data.append(d.column_name)
            return data
    except Exception:
        raise


"""
def delete_tables_if_existing(schema_archive, schema_gnimports, archive_table, gnimport_table):
    engine = DB.engine
    archive_schema_table_name = get_full_table_name(schema_archive,archive_table)
    gn_imports_schema_table_name = get_full_table_name(schema_gnimports,gnimport_table)
    
    is_archive_table_exist = engine.has_table(archive_table, schema=schema_archive)
    is_gn_imports_table_exist = engine.has_table(gnimport_table, schema=schema_gnimports)
    
    if is_archive_table_exist:
        DB.session.execute("DROP TABLE {}".format(quoted_name(archive_schema_table_name, False)))

    if is_gn_imports_table_exist:
        DB.session.execute("DROP TABLE {}".format(quoted_name(gn_imports_schema_table_name, False)))
"""


def get_table_list(schema_name):

    """ List of table names from a schema.

        Args:
            schema_name (str) : name of the schema
        
        Returns:
            table_names (List[str]) : list of table names in schema_name
    """
    
    try:
        table_names = DB.session.execute(\
            "SELECT table_name \
             FROM information_schema.tables \
             WHERE table_schema={schema};".format(schema=QuotedString(schema_name))\
        ) 
        table_names = [table.table_name for table in table_names]
        return table_names
    except Exception:
        raise


def test_user_dataset(id_role, current_dataset_id):

    """ Test if the dataset_id provided in the url path ("url/process/dataset_id") is allowed
        (allowed = in the list of dataset_ids previously created by the user)

        Args:
            id_role (int) : id_role of the user
            current_dataset_id (str?) : dataset_id provided in the url path
        
        Returns:
            Boolean : True if allowed, False if not allowed
    """

    results = DB.session.query(TDatasets)\
        .filter(TDatasets.id_dataset == Synthese.id_dataset)\
        .filter(CorObserverSynthese.id_synthese == Synthese.id_synthese)\
        .filter(CorObserverSynthese.id_role == id_role)\
        .distinct(Synthese.id_dataset)\
        .all()

    dataset_ids = []

    for r in results:
        dataset_ids.append(r.id_dataset)

    if int(current_dataset_id) not in dataset_ids:
        return False
        
    return True


def delete_import_CorImportArchives(id):

    """ Delete an import from cor_import_archives table.

        Args:
            id (int) : import id to delete
        Returns:
            None

    """
    DB.session.query(CorImportArchives)\
        .filter(CorImportArchives.id_import == id)\
        .delete()


def delete_import_CorRoleImport(id):
    """ Delete an import from cor_role_import table.

        Args:
            id (int) : import id to delete
        Returns:
            None

    """
    DB.session.query(CorRoleImport)\
        .filter(CorRoleImport.id_import == id)\
        .delete()


def delete_import_TImports(id):
    """ Delete an import from t_imports table.

    Args:
        id (int) : import id to delete
    Returns:
        None

    """
    DB.session.query(TImports)\
        .filter(TImports.id_import == id)\
        .delete()


def delete_tables(id, archives_schema, imports_schema):
    """ Delete all the tables related to an id import in archives and imports schemas

    Args:
        id (int) : import id
        archives_schema (str) : archives schema name
        imports_schema (str) : imports schema name
    Returns:
        None

    """   
    table_names_list = get_table_list(archives_schema)
    if len(table_names_list) > 0:
        for table_name in table_names_list:
            try:
                if int(table_name.split('_')[-1]) == id:
                    imports_table_name = set_imports_table_name(table_name)
                    DB.session.execute("DROP TABLE {}".format(get_full_table_name(archives_schema,table_name)))
                    DB.session.execute("DROP TABLE {}".format(get_full_table_name(imports_schema,imports_table_name)))
            except ValueError:
                pass



def check_sql_words(my_string_list):
    forbidden_word_positions = []
    for my_string in my_string_list:
        if my_string.upper() in words:
            forbidden_word_positions.append(my_string_list.index(my_string))
    return forbidden_word_positions


## Table names queries


def get_table_name(id_import):
    results = DB.session.query(TImports.import_table)\
        .filter(TImports.id_import == id_import)\
        .one()
    return results.import_table


def get_table_names(archives_schema_name, import_schema_name, table_identifier):
    try:
        if isinstance(table_identifier, int):
            archives_table_name = get_table_name(table_identifier)
        else:
            archives_table_name = table_identifier
        imports_table_name = set_imports_table_name(archives_table_name)
        archives_full_table_name = get_full_table_name(archives_schema_name, archives_table_name)
        imports_full_table_name = get_full_table_name(import_schema_name, imports_table_name)

        return {
            'archives_table_name' : archives_table_name,
            'imports_table_name' : imports_table_name,
            'archives_full_table_name' : archives_full_table_name,
            'imports_full_table_name' : imports_full_table_name
        }
    except Exception:
        return 'error in get_table_names()',500


def get_full_table_name(schema_name, table_name):
    """ Get full table name (schema_name.table_name)

    Args:
        - schema_name (str)
        - table_name (str)
    Returns:
        - full name (str)
    """
    return '.'.join([schema_name, table_name])


def set_imports_table_name(table_name):
    """ Set imports table name (prefixed with 'i_')

    Args:
        - table_name (str)
    Returns:
        - table name with 'i_' prefix (str)
    """
    return ''.join(['i_', table_name])


def check_row_number(id,loaded_table):
    n_original_rows = DB.session.execute("SELECT source_count FROM gn_imports.t_imports WHERE id_import={};".format(id)).fetchone()[0]
    n_loaded_rows = DB.session.execute("SELECT count(*) FROM {}".format(loaded_table)).fetchone()[0]
    if n_original_rows != n_loaded_rows:
        return False
    else:
        return True


def get_synthese_info(selected_synthese_cols):
    formated_selected_synthese_cols = '\',\''.join(selected_synthese_cols)
    formated_selected_synthese_cols = '{}{}{}'.format('(\'',formated_selected_synthese_cols,'\')')

    synthese_info = DB.session.execute(\
        "SELECT column_name,is_nullable,column_default,data_type,character_maximum_length\
         FROM INFORMATION_SCHEMA.COLUMNS\
         WHERE table_name = 'synthese'\
         AND column_name IN {};"\
         .format(formated_selected_synthese_cols)\
    ).fetchall()

    my_dict = {d[0] : {'is_nullable':d[1],'column_default':d[2],'data_type':d[3],'character_max_length':d[4]} for d in synthese_info}

    return my_dict


def get_synthese_types():
    synthese_info = DB.session.execute(\
        "SELECT data_type\
         FROM INFORMATION_SCHEMA.COLUMNS\
         WHERE table_name = 'synthese';"
    ).fetchall()

    types = [d.data_type for d in synthese_info]

    return types


@checker('CSV loaded to DB table')
def load_csv_to_db(full_path, cur, full_table_name, separator, columns):
    with open(full_path, 'rb') as f:
        cmd = """
            COPY {}({}) FROM STDIN WITH (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER '{}'
            )
            """.format(full_table_name, ','.join(columns), separator)
        cur.copy_expert(cmd, f)


def get_row_number(archives_schema_name, imports_schema_name, id):
    table_names = get_table_names(archives_schema_name, imports_schema_name, id)
    nrows = DB.session.execute("SELECT count(*) AS count_1 FROM {};".format(table_names['imports_full_table_name'])).scalar()
    DB.session.close()
    return nrows
