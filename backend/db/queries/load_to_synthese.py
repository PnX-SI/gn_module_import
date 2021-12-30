from geonature.utils.env import DB

import pdb


def get_data_type(column_name):
    try:
        key_type = DB.session.execute(
            """
            SELECT data_type 
            FROM information_schema.columns
            WHERE table_name = 'synthese'
            AND column_name = '{column_name}';
            """.format(
                column_name=column_name
            )
        ).fetchone()[0]
        return key_type
    except Exception:
        raise


def insert_into_synthese(
    schema_name, table_name, select_part, total_columns, import_obj,
    additional_data=[]
):
    try:
        id_source = DB.session.execute(
            f"""
            SELECT id_source
            FROM gn_synthese.t_sources
            WHERE name_source = 'Import(id={import_obj.id_import})';
            """
        ).fetchone()[0]
        # insert user values in synthese
        subquery = "NULL"
        if additional_data:
            subquery = """
            json_build_object({select_part})
            """.format(
                select_part=','.join([f"'{a}',{a}" for a in additional_data]))
        
        query = """
            BEGIN;
            ALTER TABLE gn_synthese.synthese DISABLE TRIGGER tri_meta_dates_change_synthese;
            ALTER TABLE gn_synthese.synthese DISABLE TRIGGER tri_insert_cor_area_synthese;

            INSERT INTO gn_synthese.synthese ({into_part},additional_data)
            SELECT {select_part},{subquery}
            FROM {schema_name}.{table_name}
            WHERE gn_invalid_reason IS NULL;

            ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_meta_dates_change_synthese;
            ALTER TABLE gn_synthese.synthese ENABLE TRIGGER tri_insert_cor_area_synthese;
            COMMIT;
            
            """.format(
            into_part=",".join(total_columns.keys()),
            select_part=",".join(select_part),
            schema_name=schema_name,
            table_name=table_name,
            subquery=subquery
        )

        DB.session.execute(query)

        # update last_action in synthese
        DB.session.execute(
            f"""
            -- restore trigger
            -- cor_area_synthese
            BEGIN;

            INSERT INTO gn_synthese.cor_area_synthese
            SELECT
            s.id_synthese,
            a.id_area
            FROM ref_geo.l_areas a
            JOIN gn_synthese.synthese s ON public.st_intersects(s.the_geom_local, a.geom)
            WHERE a.enable = TRUE AND (
                ST_GeometryType(s.the_geom_local) = 'ST_Point' OR NOT public.ST_TOUCHES(s.the_geom_local,a.geom)
            )
            AND s.id_source = {id_source}
            ;

            COMMIT;

            BEGIN;
            UPDATE gn_synthese.synthese SET meta_create_date = NOW() WHERE meta_create_date IS NULL;
            UPDATE gn_synthese.synthese SET meta_update_date = NOW() WHERE meta_update_date IS NULL;
            UPDATE gn_synthese.synthese
            SET last_action = 'I'
            WHERE id_source = {id_source};
            COMMIT;
        """
        )

        DB.session.flush()
    except Exception:
        DB.session.rollback()
        raise


def insert_into_t_sources(schema_name, table_name, import_id, total_columns):
    try:
        DB.session.execute(
            """
            INSERT INTO gn_synthese.t_sources(name_source,desc_source,entity_source_pk_field,url_source) VALUES
            (
                'Import(id={import_id})',
                'Imported data from import module (id={import_id})',
                '{schema_name}.{table_name}.{entity_col_name}',
                NULL
            )
            """.format(
                import_id=import_id,
                entity_col_name=total_columns.get("entity_source_pk_value", None),
                schema_name=schema_name,
                table_name=table_name,
            )
        )
        DB.session.flush()
    except Exception:
        DB.session.rollback()
        raise


def get_id_source(import_id):
    try:
        id_source = DB.session.execute(
            """
            SELECT id_source
            FROM gn_synthese.t_sources
            WHERE name_source = 'Import(id={import_id})'
            """.format(
                import_id=import_id
            )
        ).fetchone()[0]
        return id_source
    except Exception:
        raise


def check_id_source(import_id):
    try:
        is_id_source = DB.session.execute(
            """
            SELECT exists (
                SELECT 1 
                FROM gn_synthese.t_sources 
                WHERE name_source = 'Import(id={import_id})' 
                LIMIT 1);
            """.format(
                import_id=import_id
            )
        ).fetchone()[0]
        return is_id_source
    except Exception:
        raise


def get_synthese_info(selected_synthese_cols):
    formated_selected_synthese_cols = "','".join(selected_synthese_cols)
    formated_selected_synthese_cols = "{}{}{}".format(
        "('", formated_selected_synthese_cols, "')"
    )

    synthese_info = DB.session.execute(
        """
        SELECT column_name,is_nullable,column_default,data_type,character_maximum_length\
        FROM INFORMATION_SCHEMA.COLUMNS\
        WHERE table_name = 'synthese'\
        AND column_name IN {};""".format(
            formated_selected_synthese_cols
        )
    ).fetchall()

    my_dict = {
        d[0]: {
            "is_nullable": d[1],
            "column_default": d[2],
            "data_type": d[3],
            "character_max_length": d[4],
        }
        for d in synthese_info
    }

    return my_dict
