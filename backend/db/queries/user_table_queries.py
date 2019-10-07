from geonature.utils.env import DB


def delete_table(full_table_name):
    try:        
        DB.session.execute("""
            DROP TABLE {};
            """.format(full_table_name))
    except Exception:
        raise


def rename_table(schema_name, original_name, new_name):
    try:
        DB.session.execute("""
            ALTER TABLE {schema_name}.{original_name} 
            RENAME TO {new_name};
            """.format(
                schema_name = schema_name, 
                original_name = original_name, 
                new_name = new_name))
    except Exception:
        raise


def set_primary_key(schema_name, table_name, pk_col_name):
    try:
        DB.session.execute("""
            ALTER TABLE ONLY {schema_name}.{table_name} 
            ADD CONSTRAINT pk_{schema_name}_{table_name} PRIMARY KEY ({pk_col_name});
            """.format(
                schema_name = schema_name, 
                table_name = table_name, 
                pk_col_name = pk_col_name))
    except Exception:
        raise


def alter_column_type(schema_name, table_name, col_name, col_type):
        DB.session.execute("""
            ALTER TABLE {schema_name}.{table_name}
            ALTER COLUMN {col_name} 
            TYPE {col_type} USING {col_name}::{col_type};
            """.format(
                schema_name = schema_name, 
                table_name = table_name,
                col_name = col_name,
                col_type = col_type))