from geonature.utils.env import DB
from psycopg2.extensions import AsIs, QuotedString
from ..models import TImports

from ...wrappers import checker


def delete_table(full_table_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            DROP TABLE IF EXISTS {};
            """.format(full_table_name))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def rename_table(schema_name, original_name, new_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            ALTER TABLE {schema_name}.{original_name} 
            RENAME TO {new_name};
            """.format(
            schema_name=schema_name,
            original_name=original_name,
            new_name=new_name))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def set_primary_key(schema_name, table_name, pk_col_name):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            ALTER TABLE ONLY {schema_name}.{table_name} 
            ADD CONSTRAINT pk_{schema_name}_{table_name} PRIMARY KEY ({pk_col_name});
            """.format(
            schema_name=schema_name,
            table_name=table_name,
            pk_col_name=pk_col_name))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_table_info(table_name, info='all'):
    try:
        table_info = DB.session.execute("""
            SELECT column_name,is_nullable,column_default,data_type,character_maximum_length\
            FROM INFORMATION_SCHEMA.COLUMNS\
            WHERE table_name = {};""".format(QuotedString(table_name)))

        if info == 'all':
            return table_info

        if info == 'type':
            data = {}
            for d in table_info:
                data.update({d.column_name: d.data_type})
            return data

        if info == 'column_name':
            data = []
            for d in table_info:
                data.append(d.column_name)
            return data
    except Exception:
        raise


def get_table_list(schema_name):
    """ List of table names from a schema.

        Args:
            schema_name (str) : name of the schema

        Returns:
            table_names (List[str]) : list of table names in schema_name
    """

    try:
        table_names = DB.session.execute("""
            SELECT table_name \
            FROM information_schema.tables \
            WHERE table_schema={schema};""".format(schema=QuotedString(schema_name)))
        table_names = [table.table_name for table in table_names]
        return table_names
    except Exception:
        raise


def delete_tables(id_import, archives_schema, imports_schema):
    """ Delete all the tables related to an id import in archives and imports schemas

    Args:
        id_import (int) : import id
        archives_schema (str) : archives schema name
        imports_schema (str) : imports schema name
    Returns:
        None

    """
    try:
        table_names_list = get_table_list(archives_schema)
        if len(table_names_list) > 0:
            for table_name in table_names_list:
                try:
                    if int(table_name.split('_')[-1]) == id_import:
                        imports_table_name = set_imports_table_name(table_name)
                        DB.session.execute("""
                            DROP TABLE IF EXISTS {}""".format(get_full_table_name(archives_schema, table_name)))
                        DB.session.execute("""
                            DROP TABLE IF EXISTS {}""".format(get_full_table_name(imports_schema, imports_table_name)))
                except ValueError:
                    pass
    except Exception:
        raise


def get_table_name(id_import):
    try:
        results = DB.session.query(TImports.import_table) \
            .filter(TImports.id_import == id_import) \
            .one()
        return results.import_table
    except Exception:
        raise


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
            'archives_table_name': archives_table_name,
            'imports_table_name': imports_table_name,
            'archives_full_table_name': archives_full_table_name,
            'imports_full_table_name': imports_full_table_name
        }
    except Exception:
        raise


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


def check_row_number(id, loaded_table):
    n_original_rows = DB.session.execute("""
        SELECT source_count 
        FROM gn_imports.t_imports 
        WHERE id_import={};""".format(id)).fetchone()[0]

    n_loaded_rows = DB.session.execute("""
        SELECT count(*) 
        FROM {}""".format(loaded_table)).fetchone()[0]

    if n_original_rows != n_loaded_rows:
        return False
    else:
        return True


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


def get_row_number(full_table_name):
    nrows = DB.session.execute("""
        SELECT count(*) AS count_1 
        FROM {};""".format(full_table_name)).scalar()
    DB.session.close()
    return nrows


def alter_column_type(schema_name, table_name, col_name, col_type):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            ALTER TABLE {schema_name}.{table_name}
            ALTER COLUMN {col_name} 
            TYPE {col_type} USING {col_name}::{col_type};
            """.format(
            schema_name=schema_name,
            table_name=table_name,
            col_name=col_name,
            col_type=col_type))
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_n_loaded_rows(full_table_name):
    try:
        n_loaded_rows = DB.session.execute("""
            SELECT count(*) 
            FROM {};
            """.format(full_table_name)).fetchone()[0]
        return n_loaded_rows
    except Exception:
        raise


def get_n_invalid_rows(full_table_name):
    try:
        n_invalid_rows = DB.session.execute("""
            SELECT count(*) 
            FROM {} WHERE gn_is_valid = 'False';
            """.format(full_table_name)).fetchone()[0]
        return n_invalid_rows
    except Exception:
        raise


def get_n_valid_rows(schema_name, table_name):
    try:
        n_valid_rows = DB.session.execute("""
            SELECT count(*)
            FROM {schema_name}.{table_name}
            WHERE gn_is_valid = 'True';
            """.format(
            schema_name=schema_name,
            table_name=table_name
        )).fetchone()[0]
        return n_valid_rows
    except Exception:
        raise


def get_n_taxa(schema_name, table_name, cd_nom_col):
    try:
        n_taxa = DB.session.execute("""
            SELECT COUNT(DISTINCT TAXREF.cd_ref)
            FROM {schema_name}.{table_name} SOURCE
            JOIN taxonomie.taxref TAXREF ON TAXREF.cd_nom = SOURCE.{cd_nom_col}::integer
            WHERE SOURCE.gn_is_valid = 'True';
            """.format(
            schema_name=schema_name,
            table_name=table_name,
            cd_nom_col=cd_nom_col
        )).fetchone()[0]
        return n_taxa
    except Exception:
        raise


def get_date_ext(schema_name, table_name, date_min_col, date_max_col):
    try:
        dates = DB.session.execute("""
            SELECT min({date_min_col}), max({date_max_col})
            FROM {schema_name}.{table_name}
            WHERE gn_is_valid = 'True';
            """.format(
            schema_name=schema_name,
            table_name=table_name,
            date_min_col=date_min_col,
            date_max_col=date_max_col
        )).fetchall()[0]

        return {
            'date_min': dates[0],
            'date_max': dates[1]
        }
    except Exception:
        raise


def save_invalid_data(cur, full_archive_table_name, full_imports_table_name, full_path, pk_name, delimiter):
    try:
        cmd = \
            """
                COPY
                    (
                    SELECT I.gn_invalid_reason, A.*
                    FROM {full_imports_table_name} I
                    LEFT JOIN {full_archive_table_name} A ON I.{pk_name} = A.{pk_name}
                    WHERE I.gn_is_valid = 'False'
                    ORDER BY I.{pk_name} ASC
                    )
                TO STDOUT WITH DELIMITER '{delimiter}' CSV HEADER;
            """.format(
                full_archive_table_name=full_archive_table_name,
                full_imports_table_name=full_imports_table_name,
                pk_name=pk_name,
                delimiter=str(delimiter[0])
            )
        with open(full_path, 'w') as f:
            cur.copy_expert(cmd, f)
    except Exception:
        raise


def get_uuid_list():
    try:
        uuid_synthese = DB.session.execute(
            """
            SELECT unique_id_sinp as unique_uuid
            FROM gn_synthese.synthese
            WHERE unique_id_sinp::text != '';
            """).fetchall()

        uuid_synthese_list = [str(row.unique_uuid) for row in uuid_synthese]
        DB.session.close()
        return uuid_synthese_list
    except Exception:
        raise


def get_required(schema_name, table_name):
    try:
        required_cols = DB.session.execute(
            """
            SELECT name_field
            FROM {schema_name}.{table_name}
            WHERE mandatory = True;
            """.format(
                schema_name=schema_name,
                table_name=table_name
            )).fetchall()
        col_names = [col[0] for col in required_cols]
        return col_names
    except Exception:
        raise


def get_delimiter(schema_name, import_id, separator):
    try:
        delimiter = DB.session.execute(
            """
            SELECT separator
            FROM {schema_name}.t_imports
            WHERE id_import = {import_id};
            """.format(
                schema_name=schema_name,
                import_id=int(import_id)
            )
        ).fetchone()[0]
        for sep in separator:
            if sep['db_code'] == delimiter:
                return str(sep['code'])
    except Exception:
        raise
