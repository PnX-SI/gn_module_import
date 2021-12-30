import itertools
from types import SimpleNamespace

from flask import current_app
from sqlalchemy import exc
from sqlalchemy.sql import text
from psycopg2.errors import DuplicateColumn
from geonature.utils.env import DB

from collections import defaultdict
from ...api_error import GeonatureImportApiError

from ..models import TMappingsValues, TMappings


def check_for_injection(param):
    dieWords = ["DROP", "DELETE", "INSERT", "UPDATE", "#", "CREATE", "\\"]
    for word in dieWords:
        if word in param or word.lower() in param:
            raise GeonatureImportApiError(
                message="Tentative d'injection SQL", status_code=400
            )


def add_nomenclature_transformed_col(col_name, table_name):
    check_for_injection(col_name)
    query_add = f"""
        ALTER TABLE gn_imports.{table_name}
        ADD COLUMN {col_name} character varying(5);
    """
    try:
        DB.session.execute(query_add)
        DB.session.commit()
    except exc.ProgrammingError as e:
        # pgcode of duplicateColumnError
        if e.orig.pgcode == "42701":
            DB.session.rollback()
        else:
            raise


def get_nomenc_details(nomenclature_abb):
    try:
        query = """
            SELECT 
              label_default AS name,
              id_type AS id,
              definition_default,
              ref_nomenclatures.get_nomenclature_label(
                gn_synthese.get_default_nomenclature_value(:nomenc)
              )  AS label_default_nomenclature
            FROM ref_nomenclatures.bib_nomenclatures_types
            WHERE mnemonique = :nomenc;
        """
        return DB.session.execute(text(query), {"nomenc": nomenclature_abb}).fetchone()
    except Exception:
        raise


def get_nomenclature_values(mnemoniques_type: list):
    if len(mnemoniques_type) > 0:
        query = """
        SELECT bib.mnemonique as mnemnonique,
        array_agg(nom.id_nomenclature) as id_nomenclatures
        FROM  ref_nomenclatures.t_nomenclatures as nom
        JOIN ref_nomenclatures.bib_nomenclatures_types AS bib ON nom.id_type = bib.id_type
        WHERE bib.mnemonique IN :mnemoniques_type
        GROUP BY bib.mnemonique

        """
        return DB.session.execute(
            text(query), {"mnemoniques_type": tuple(mnemoniques_type)}
        ).fetchall()
    else:
        return []


def get_nomenc_values(nommenclature_abb):
    query = """SELECT
            nom.id_nomenclature AS nomenc_id,
            nom.label_default AS nomenc_values, 
            nom.definition_default AS nomenc_definitions
        FROM ref_nomenclatures.bib_nomenclatures_types AS bib
        JOIN ref_nomenclatures.t_nomenclatures AS nom ON nom.id_type = bib.id_type
        WHERE bib.mnemonique = :nomenc
        ORDER BY nomenc_values ASC
        """
    return DB.session.execute(text(query), {"nomenc": nommenclature_abb}).fetchall()


def get_nomenclature_label_from_id(id_nomenclature):
    query = "SELECT label_default FROM ref_nomenclatures.t_nomenclatures WHERE id_nomenclature = :id_nomenclature"
    data = DB.session.execute(
        text(query), {"id_nomenclature": id_nomenclature}
    ).fetchone()
    if data:
        return data.label_default
    return data


def get_nomenc_user_values(user_nomenc_col, schema_name, table_name):
    query = """
        SELECT DISTINCT {user_nomenc_col} as user_val
        FROM {schema_name}.{table_name};
        """.format(
        user_nomenc_col=user_nomenc_col, schema_name=schema_name, table_name=table_name,
    )
    nomenc_user_values = DB.session.execute(query).fetchall()
    return nomenc_user_values


def get_nomenc_abbs(form_data):
    nomenc_abbs = DB.session.execute(
        """
        SELECT F.name_field as synthese_name, BNT.mnemonique as nomenc_abb
        FROM gn_imports.dict_fields F
        RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
        LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique;
        """
    ).fetchall()
    nomenc_list = []
    for nomenc in nomenc_abbs:
        if nomenc.synthese_name in form_data.keys():
            nomenc_list.append(nomenc.nomenc_abb)
    return nomenc_list


def get_synthese_col(abb):
    query = """
        SELECT F.name_field as synthese_name
        FROM gn_imports.dict_fields F
        RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
        LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique
        WHERE BNT.mnemonique = :abb;
    """
    nomenc_synthese_name = DB.session.execute(text(query), {"abb": abb}).fetchone()
    return nomenc_synthese_name.synthese_name


def get_SINP_synthese_cols():
    nomencs = DB.session.execute(
        """
        SELECT F.name_field as synthese_name
        FROM gn_imports.dict_fields F
        RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
        LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique;
        """
    ).fetchall()
    synthese_name_list = [nomenc.synthese_name for nomenc in nomencs]
    return synthese_name_list


def get_SINP_synthese_cols_with_mnemonique():
    nomencs = DB.session.execute(
        """
        SELECT F.name_field as synthese_name,
        CSN.mnemonique as mnemonique_type
        FROM gn_imports.dict_fields F
        RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
        LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique;
        """
    ).fetchall()
    return [
        {
            "synthese_name": nomenc.synthese_name,
            "mnemonique_type": nomenc.mnemonique_type,
        }
        for nomenc in nomencs
    ]


def get_nomenc_abb(id_nomenclature):
    query = """
        SELECT BNT.mnemonique as abb
        FROM ref_nomenclatures.t_nomenclatures N
        LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT on BNT.id_type = N.id_type
        WHERE id_nomenclature = :id_nomenclature;
        """
    nomenc_abb = DB.session.execute(
        text(query), {"id_nomenclature": int(id_nomenclature)}
    ).fetchone()
    return nomenc_abb.abb


def set_nomenclature_id(table_name, user_col, target_col, value, id_nomenclature):
    query = """
            UPDATE {schema_name}.{table_name}
            SET {target_col} = :id_nomenclature
            WHERE {user_col} = :value
            """.format(
        schema_name=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
        table_name=table_name,
        user_col=user_col,
        target_col=target_col,
    )
    DB.session.execute(
        text(query), {"id_nomenclature": id_nomenclature, "value": value}
    )


def find_row_with_nomenclatures_error(table_name, tr_col, user_col):
    """
    Return all the rows where the nomenclatures has not be found
    Only check user col with values (null values will take default nomenclature)
    """
    query = """
    SELECT array_agg(gn_pk) as gn_pk, {user_col}
    FROM {schema_name}.{table_name}
    WHERE {tr_col} IS NULL AND {user_col} IS NOT NULL
    GROUP BY {user_col}
    """.format(
        schema_name=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
        table_name=table_name,
        tr_col=tr_col,
        user_col=user_col
    )
    return DB.session.execute(query).fetchall()


def get_nomenc_abb_from_name(synthese_name):
    query = """
        SELECT BNT.mnemonique as abb
        FROM gn_imports.dict_fields F
        RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
        LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique
        WHERE F.name_field = :synthese_name;
        """
    nomenc = DB.session.execute(
        text(query), {"synthese_name": synthese_name}
    ).fetchone()[0]
    return nomenc


def set_default_value(abb):
    default_value = DB.session.execute(
        text("SELECT gn_synthese.get_default_nomenclature_value(:abb)"), {"abb": abb},
    ).fetchone()[0]
    if default_value is None:
        default_value = "NULL"
    return str(default_value)


def set_default_nomenclature_id(table_name, nomenc_abb, tr_col, user_col = None, where_user_val_none = False):
    default_value = DB.session.execute(
        text("SELECT gn_synthese.get_default_nomenclature_value(:nomenc_abb)"),
        {"nomenc_abb": nomenc_abb},
    ).fetchone()[0]

    query = """
        UPDATE {schema_name}.{table_name}
        SET {tr_col} = :default_value
        WHERE {tr_col} IS NULL
        """.format(
        schema_name=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
        table_name=table_name,
        tr_col=tr_col,
    )
    if where_user_val_none:
        query = query + " AND {user_col} IS NULL".format(user_col=user_col)
    DB.session.execute(
        text(query), {"default_value": default_value}
    )


def get_mnemo(id_nomenc):
    try:
        try:
            query = """
                SELECT mnemonique
                FROM ref_nomenclatures.t_nomenclatures
                WHERE id_nomenclature = :id;
            """
            mnemo = DB.session.execute(text(query), {"id": int(id_nomenc)}).fetchone()[
                0
            ]
            return mnemo
        except ValueError:
            return ""
    except Exception:
        raise


def get_content_mapping(id_mapping):
    contents = (
        DB.session.query(TMappingsValues)
        .filter(TMappingsValues.id_mapping == int(id_mapping))
        .all()
    )
    mapping_contents = []
    gb_mapping_contents = []

    if len(contents) > 0:
        for content in contents:
            d = {
                "id_match_values": content.id_match_values,
                "id_mapping": content.id_mapping,
                "source_value": content.source_value,
                "id_target_value": content.id_target_value,
            }
            mapping_contents.append(d)
        for key, group in itertools.groupby(
            mapping_contents, key=lambda x: x["id_target_value"]
        ):
            gb_mapping_contents.append(list(group))
    else:
        gb_mapping_contents.append("empty")

    return gb_mapping_contents


def get_saved_content_mapping(id_mapping):
    contents = (
        DB.session.query(TMappingsValues)
        .filter(TMappingsValues.id_mapping == int(id_mapping))
        .all()
    )
    mapping_contents = []

    if len(contents) > 0:
        for content in contents:
            if content.source_value != "":
                d = {str(content.id_target_value): content.source_value}
                mapping_contents.append(d)
    selected_content = defaultdict(list)
    for content in mapping_contents:
        for key, value in content.items():
            selected_content[key].append(value)
    return selected_content


#  conditional check


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
        FROM {schema}.{table}
        WHERE gn_is_valid != 'False' AND ref_nomenclatures.get_cd_nomenclature({field_proof}::integer) = '1' 
        """.format(
        schema=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
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


def statut_source_check(statut_source_col):
    pass


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
        FROM {schema}.{table}
        WHERE gn_is_valid != 'False' AND ref_nomenclatures.get_cd_nomenclature({field_statut_source}::integer) = 'Li' 
        """.format(
        schema=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
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