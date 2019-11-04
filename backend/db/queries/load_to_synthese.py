from geonature.utils.env import DB


def get_data_type(column_name):
    try:
        key_type = DB.session.execute("""
            SELECT data_type 
            FROM information_schema.columns
            WHERE table_name = 'synthese'
            AND column_name = '{column_name}';
            """.format(column_name = column_name)).fetchone()[0]
        return key_type
    except Exception:
        raise


def insert_into_synthese(schema_name, table_name, select_part, total_columns):
    try:    
        DB.session.execute("""
            INSERT INTO gn_synthese.synthese ({into_part})
            SELECT {select_part}
            FROM {schema_name}.{table_name}
            WHERE gn_is_valid='True';
            """.format(
                into_part = ','.join(total_columns.keys()),
                select_part = ','.join(select_part),
                schema_name = schema_name,
                table_name = table_name
            ))
    except Exception:
        raise


def insert_into_t_sources(schema_name, table_name, import_id, total_columns):
    try:
        DB.session.execute("""
            INSERT INTO gn_synthese.t_sources(name_source,desc_source,entity_source_pk_field,url_source) VALUES
            (
                'Import(id={import_id})',
                'Imported data from import module (id={import_id})',
                '{schema_name}.{table_name}.{entity_col_name}',
                NULL
            )
            """.format(
                import_id = import_id,
                entity_col_name = total_columns['entity_source_pk_value'],
                schema_name = schema_name,
                table_name = table_name,
            ))
        DB.session.flush()
    except Exception:
        DB.session.rollback()
        raise


def get_id_source(import_id):
    try:
        id_source = DB.session.execute("""
            SELECT id_source
            FROM gn_synthese.t_sources
            WHERE name_source = 'Import(id={import_id})'
            """\
            .format(import_id = import_id))\
            .fetchone()[0]
        return id_source
    except Exception:
        raise


def check_id_source(import_id):
    try:
        is_id_source = DB.session.execute("""
            SELECT exists (
                SELECT 1 
                FROM gn_synthese.t_sources 
                WHERE name_source = 'Import(id={import_id})' 
                LIMIT 1);
            """\
            .format(import_id = import_id))\
            .fetchone()[0]
        return is_id_source
    except Exception:
        raise