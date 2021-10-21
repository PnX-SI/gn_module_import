from geonature.utils.env import DB


def generate_altitudes(
    schema, table, alti_min_col, alti_max_col, table_pk, geom_col, generate_type,
):
    try:

        where_clause = ""
        if generate_type == "generate_missing":
            where_clause = f"WHERE {alti_min_col} IS  NULL OR {alti_max_col} IS NULL"

        query = f"""
        WITH alti AS (
             SELECT t1.{table_pk} as id, row_to_json(ref_geo.fct_get_altitude_intersection({geom_col})) as alt
             FROM {schema}.{table} as t1
             {where_clause}
        )
        UPDATE {schema}.{table} as t2
        SET 
            {alti_min_col} = CASE
                WHEN {alti_min_col} IS NOT NULL THEN {alti_min_col}
                WHEN {alti_min_col} IS NULL AND (alti.alt->'altitude_min')::text != 'null' THEN (alti.alt->'altitude_min')::text
                WHEN {alti_min_col} IS NULL AND (alti.alt->'altitude_min')::text = 'null' THEN NULL
            END 
        """
        if alti_min_col != alti_max_col:
            query = f"""{query} ,
                {alti_max_col} = CASE 
                        WHEN {alti_max_col} IS NOT NULL THEN {alti_max_col}
                        WHEN {alti_max_col} IS NULL AND (alti.alt->'altitude_max')::text != 'null' THEN (alti.alt->'altitude_max')::text
                        WHEN {alti_max_col} IS NULL AND (alti.alt->'altitude_max')::text = 'null' THEN NULL
            END """
        query = f"""{query} 
                FROM alti 
                WHERE alti.id = t2.{table_pk}
        """
        DB.session.execute(query)
        DB.session.commit()

    except Exception:
        DB.session.rollback()
        raise


def create_column(full_table_name, alt_col):
    DB.session.begin(subtransactions=True)
    try:
        DB.session.execute(
            """
            ALTER TABLE {full_table_name} 
            ADD COLUMN {alt_col} text""".format(
                full_table_name=full_table_name, alt_col=alt_col
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise
