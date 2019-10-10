from geonature.utils.env import DB


def generate_altitudes(schema, table, alt_col, table_pk, geom_col):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            UPDATE {schema}.{table} as T
            SET {alt_col} = 
                CASE WHEN (COALESCE({alt_col}, '') = '') 
                    THEN (
                        SELECT (ref_geo.fct_get_altitude_intersection({geom_col})).altitude_min::text
                        FROM {schema}.{table}
                        WHERE {table_pk} = T.{table_pk}
                        )
                    ELSE T.{alt_col}
                END
        """.format(
            schema = schema, 
            table = table, 
            alt_col = alt_col, 
            table_pk = table_pk,
            geom_col = geom_col
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def create_column(full_table_name, alt_col):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute("""
            ALTER TABLE {full_table_name} 
            ADD COLUMN {alt_col} text"""\
            .format(
                full_table_name=full_table_name, 
                alt_col=alt_col
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise