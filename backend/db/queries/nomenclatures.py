import itertools
from types import SimpleNamespace

from flask import current_app
from psycopg2.extensions import AsIs, QuotedString
from sqlalchemy.sql import text

from geonature.utils.env import DB

from collections import defaultdict

from ..models import TMappingsValues, TMappings


def get_nomenc_details(nomenclature_abb):
    try:
        query = """
            SELECT 
              label_default as name,
              id_type as id
            FROM ref_nomenclatures.bib_nomenclatures_types
            WHERE mnemonique = :nomenc;
        """
        nomenc_details = DB.session.execute(
            text(query), {"nomenc": nomenclature_abb}
        ).fetchone()
        return nomenc_details
    except Exception:
        raise


def get_nomenclature_values(mnemoniques_type: list):
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


def get_nomenc_values(nommenclature_abb):
    query = """SELECT
            nom.id_nomenclature AS nomenc_id,
            nom.label_default AS nomenc_values, 
            nom.definition_default AS nomenc_definitions
        FROM ref_nomenclatures.bib_nomenclatures_types AS bib
        JOIN ref_nomenclatures.t_nomenclatures AS nom ON nom.id_type = bib.id_type
        WHERE bib.mnemonique = :nomenc
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


def set_nomenclature_id(table_name, user_col, value, id_nomenclature):
    query = """
            UPDATE {schema_name}.{table_name}
            SET {user_col} = :id_nomenclature
            WHERE {user_col} = :value
            """.format(
        schema_name=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
        table_name=table_name,
        user_col=user_col,
    )
    DB.session.execute(
        text(query), {"id_nomenclature": id_nomenclature, "value": value}
    )


def find_row_with_nomenclatures_error(table_name, nomenclature_column, ids_accepted):
    """
    Return all the rows where the nomenclatures has not be found
    """
    query = """
    SELECT array_agg(gn_pk) as gn_pk, {nomenclature_column}
    FROM {schema_name}.{table_name}
    WHERE {nomenclature_column} NOT IN :ids_accepted
    GROUP BY {nomenclature_column}
    """.format(
        schema_name=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
        table_name=table_name,
        nomenclature_column=nomenclature_column,
    )
    return DB.session.execute(query, {"ids_accepted": tuple(ids_accepted)}).fetchall()


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


def set_default_nomenclature_id(table_name, nomenc_abb, user_col, id_types):
    default_value = DB.session.execute(
        text("SELECT gn_synthese.get_default_nomenclature_value(:nomenc_abb)"),
        {"nomenc_abb": nomenc_abb},
    ).fetchone()[0]

    if default_value is None:
        default_value = "NULL"
    query = """
        UPDATE {schema_name}.{table_name}
        SET {user_col} = :default_value
        WHERE {user_col} NOT IN :id_types;
        """.format(
        schema_name=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
        table_name=table_name,
        user_col=user_col,
    )
    DB.session.execute(
        text(query), {"default_value": default_value, "id_types": tuple(id_types)}
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


####### conditional check


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
