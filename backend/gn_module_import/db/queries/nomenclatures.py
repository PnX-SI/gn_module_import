from types import SimpleNamespace

from flask import current_app
from sqlalchemy.sql import text
from geonature.utils.env import DB


def exist_proof_check(
    table_name, field_proof, field_digital_proof, field_non_digital_proof
):
    """
    If exist proof = Oui (code 1), digital proof or non_digital_proof must be fill
    we check if the columns exist to build the query
    """
    #  case no proof column, return an empty list -> no error
    if field_proof is None:
        return None
    #  case exist proof = 1 and no other proof col -> raise error on all column where exist_proof = 1
    query = """
        SELECT array_agg(gn_pk) as id_rows
        FROM {table}
        WHERE gn_is_valid != 'False' AND ref_nomenclatures.get_cd_nomenclature({field_proof}::integer) = '1' 
        """.format(
        table=table_name,
        field_proof=field_proof,
    )
    #  case digital proof is None and non digital proof not exist
    if field_proof and field_digital_proof and field_non_digital_proof is None:
        query = "{query} AND {field_digital_proof} IS NULL ".format(
            query=query, field_digital_proof=field_digital_proof,
        )
    #  case non digital proof is None and digital_proof not exist
    elif field_proof and field_non_digital_proof and field_digital_proof is None:
        query = "{query} AND {field_non_digital_proof} IS NULL ".format(
            query=query, field_non_digital_proof=field_non_digital_proof,
        )
    #  case both digital and non digital exist and are None
    elif field_proof and field_digital_proof and field_non_digital_proof:
        query = "{query} AND {field_digital_proof} IS NULL AND {field_non_digital_proof} IS NULL ".format(
            query=query,
            field_non_digital_proof=field_non_digital_proof,
            field_digital_proof=field_digital_proof,
        )
    return DB.session.execute(query).first()


def dee_bluring_check(table_name, id_import, bluring_col):
    """
    If ds_public = private -> bluring must be fill
    """
    query_ds_pub = """
    SELECT ref_nomenclatures.get_cd_nomenclature(id_nomenclature_data_origin) as code
    FROM gn_meta.t_datasets
    WHERE id_dataset = (
        SELECT id_dataset FROM gn_imports.t_imports
        WHERE id_import = :id_import
        )
    """
    ds_public = DB.session.execute(query_ds_pub, {"id_import": id_import}).fetchone()
    if ds_public.code != "Pr":
        return None
    else:
        if bluring_col:
            query = """
                SELECT array_agg(gn_pk) as id_rows
                FROM {schema}.{table}
                WHERE gn_is_valid != 'False' AND {bluring_col} IS NULL
            """.format(
                schema=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
                table=table_name,
                bluring_col=bluring_col,
            )
            return DB.session.execute(query).fetchone()
        else:
            return SimpleNamespace(id_rows="All")


def ref_biblio_check(table_name, field_statut_source, field_ref_biblio):
    """
    If statut_source = 'Li' the field ref_biblio must be fill
    """
    if field_statut_source is None:
        return None
    #  case no field ref_biblo: all row with statut_source = 'Li' are in errors
    query = """
        SELECT array_agg(gn_pk) as id_rows
        FROM {table}
        WHERE gn_is_valid != 'False' AND ref_nomenclatures.get_cd_nomenclature({field_statut_source}::integer) = 'Li' 
        """.format(
        table=table_name,
        field_statut_source=field_statut_source,
    )
    #  case field_ref_biblio is present, check where the fiel is null
    if field_ref_biblio:
        query = "{query} AND {field_ref_biblio} IS NULL".format(
            query=query, field_ref_biblio=field_ref_biblio
        )
    return DB.session.execute(query).fetchone()


def info_geo_attachment_check(table_name, tr_info_geo_col, grid_col, dep_col, municipality_col):
    """
    Vérifie que si type_info_geo = 1 alors aucun rattachement n'est fourni
    """
    if not tr_info_geo_col:
        return None
    if grid_col or dep_col or municipality_col:
        query = f"""
        SELECT array_agg(gn_pk) as id_rows
        FROM {current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"]}.{table_name}
        WHERE ref_nomenclatures.get_cd_nomenclature({tr_info_geo_col}::integer) = '1' 
        """
        first_where = True
        if grid_col:
            query = f"{query} AND {grid_col} IS NOT NULL"
            first_where = False
        if dep_col:
            if first_where:
                query = f"{query} AND {dep_col} IS NOT NULL"
            else:
                query = f"{query} OR {dep_col} IS NOT NULL"
                first_where = False
        if municipality_col:
            if first_where:
                query = f"{query} AND {municipality_col} IS NOT NULL"
            else:

                query = f"{query} OR {municipality_col} IS NOT NULL"
                first_where = False
        return DB.session.execute(query).fetchone()
        
    else:
        return None

def info_geo_attachment_check_2(table_name, tr_info_geo_col, grid_col, dep_col, municipality_col):
    """
    Si une entitié de rattachement est fourni alors le type_info_geo ne doit pas être null
    """
    if not tr_info_geo_col and (grid_col or dep_col or municipality_col):
        return SimpleNamespace(id_rows="All")
    elif tr_info_geo_col:
        query = """       
        SELECT array_agg(gn_pk) as id_rows
        FROM {current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"]}.{table_name}
        WHERE {tr_info_geo_col} IS NULL
        """
        first_where = True
        if grid_col:
            query = f"{query} AND {grid_col} IS NOT NULL"
            first_where = False
        if dep_col:
            if first_where:
                query = f"{query} AND {dep_col} IS NOT NULL"
            else:
                query = f"{query} OR {dep_col} IS NOT NULL"
                first_where = False
        if municipality_col:
            if first_where:
                query = f"{query} AND {municipality_col} IS NOT NULL"
            else:

                query = f"{query} OR {municipality_col} IS NOT NULL"
                first_where = False
