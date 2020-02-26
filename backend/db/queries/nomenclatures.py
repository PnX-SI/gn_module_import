import itertools

from psycopg2.extensions import AsIs, QuotedString
from sqlalchemy.sql import text

from geonature.utils.env import DB

from collections import defaultdict

from ..models import (
    TMappingsValues
)


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
            text(query),
            {'nomenc': nomenclature_abb}
        ).fetchone()
        return nomenc_details
    except Exception:
        raise


def get_nomenc_values(nommenclature_abb):
    try:
        query = """SELECT
                nom.id_nomenclature AS nomenc_id,
                nom.label_default AS nomenc_values, 
                nom.definition_default AS nomenc_definitions
            FROM ref_nomenclatures.bib_nomenclatures_types AS bib
            JOIN ref_nomenclatures.t_nomenclatures AS nom ON nom.id_type = bib.id_type
            WHERE bib.mnemonique = :nomenc
            """
        return DB.session.execute(
            text(query),
            {'nomenc': nommenclature_abb}
        ).fetchall()
    except Exception:
        raise


def get_nomenc_user_values(user_nomenc_col, schema_name, table_name):
    try:
        nomenc_user_values = DB.session.execute("""
            SELECT DISTINCT {user_nomenc_col} as user_val
            FROM {schema_name}.{table_name}
            WHERE gn_is_valid = 'True';
            """.format(
            user_nomenc_col=user_nomenc_col,
            schema_name=schema_name,
            table_name=table_name)) \
            .fetchall()
        return nomenc_user_values
    except Exception:
        raise


def get_nomenc_abbs(form_data):
    try:
        nomenc_abbs = DB.session.execute("""
            SELECT F.name_field as synthese_name, BNT.mnemonique as nomenc_abb
            FROM gn_imports.dict_fields F
            RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique;
            """).fetchall()
        nomenc_list = []
        for nomenc in nomenc_abbs:
            if nomenc.synthese_name in form_data.keys():
                nomenc_list.append(nomenc.nomenc_abb)
        return nomenc_list
    except Exception:
        raise


def get_synthese_col(abb):
    try:
        query = """
            SELECT F.name_field as synthese_name
            FROM gn_imports.dict_fields F
            RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique
            WHERE BNT.mnemonique = :abb;
        """
        nomenc_synthese_name = DB.session.execute(
            text(query),
            {'abb': abb}
        ).fetchone()
        return nomenc_synthese_name.synthese_name
    except Exception:
        raise


def get_SINP_synthese_cols():
    try:
        nomencs = DB.session.execute("""
            SELECT F.name_field as synthese_name
            FROM gn_imports.dict_fields F
            RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique;
            """).fetchall()
        synthese_name_list = [nomenc.synthese_name for nomenc in nomencs]
        return synthese_name_list
    except Exception:
        raise


def get_nomenc_abb(id_nomenclature):
    try:
        query = """
            SELECT BNT.mnemonique as abb
            FROM ref_nomenclatures.t_nomenclatures N
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT on BNT.id_type = N.id_type
            WHERE id_nomenclature = :id_nomenclature;
            """
        nomenc_abb = DB.session.execute(
            text(query),
            {'id_nomenclature': int(id_nomenclature)}
        ).fetchone()
        return nomenc_abb.abb
    except Exception:
        raise


def set_nomenclature_id(schema_name, table_name, user_col, value, id_type):
    try:
        query = """
                UPDATE {schema_name}.{table_name}
                SET {user_col} = :id_type
                WHERE {user_col} = :value
                """.format(
                schema_name=schema_name,
                table_name=table_name,
                user_col=user_col,
        )
        # escape paramter with sqlalchemy text to avoid injection and other errors
        DB.session.execute(
            text(query),
            {'id_type': id_type, 'value': value}
        )
    except Exception:
        raise


def get_nomenc_abb_from_name(synthese_name):
    try:
        query = """
            SELECT BNT.mnemonique as abb
            FROM gn_imports.dict_fields F
            RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.synthese_col = F.name_field
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.mnemonique = CSN.mnemonique
            WHERE F.name_field = :synthese_name;
            """
        nomenc = DB.session.execute(
            text(query),
            {'synthese_name': synthese_name}
        ).fetchone()[0]
        return nomenc
    except Exception:
        raise


def set_default_value(abb):
    try:
        default_value = DB.session.execute(
            text('SELECT gn_synthese.get_default_nomenclature_value(:abb)'),
            {'abb': abb}
        ).fetchone()[0]
        if default_value is None:
            print('LAAAAAAAAAAAAAAA')
            default_value = 'NULL'
        return str(default_value)
    except Exception:
        raise


def set_default_nomenclature_id(schema_name, table_name, nomenc_abb, user_col, id_types):
    try:
        default_value = DB.session.execute(
            text('SELECT gn_synthese.get_default_nomenclature_value(:nomenc_abb)'),
            {'nomenc_abb': nomenc_abb}
        ).fetchone()[0]

        if default_value is None:
            print('IS NULLLLLLLLLLLLLLLLll')
            print(nomenc_abb)
            default_value = 'NULL'

        query = """
            UPDATE {schema_name}.{table_name}
            SET {user_col} = :default_value
            WHERE {user_col} NOT IN :id_types;
            """.format(
            schema_name=schema_name,
            table_name=table_name,
            user_col=user_col
        )
        DB.session.execute(
            text(query),
            {
                'default_value': default_value,
                'id_types': tuple(id_types)
            }
        )

    except Exception:
        raise


def get_mnemo(id_nomenc):
    try:
        try:
            query = """
                SELECT mnemonique
                FROM ref_nomenclatures.t_nomenclatures
                WHERE id_nomenclature = :id;
            """
            mnemo = DB.session.execute(
                text(query),
                {'id': int(id_nomenc)}
            ).fetchone()[0]
            return mnemo
        except ValueError:
            return ''
    except Exception:
        raise


def get_content_mapping(id_mapping):
    try:
        contents = DB.session \
            .query(TMappingsValues) \
            .filter(TMappingsValues.id_mapping == int(id_mapping)) \
            .all()
        mapping_contents = []
        gb_mapping_contents = []

        if len(contents) > 0:
            for content in contents:
                d = {
                    'id_match_values': content.id_match_values,
                    'id_mapping': content.id_mapping,
                    'source_value': content.source_value,
                    'id_target_value': content.id_target_value
                }
                mapping_contents.append(d)
            for key, group in itertools.groupby(mapping_contents, key=lambda x: x['id_target_value']):
                gb_mapping_contents.append(list(group))
        else:
            gb_mapping_contents.append('empty')

        return gb_mapping_contents
    except Exception:
        raise


def get_saved_content_mapping(id_mapping):
    try:
        contents = DB.session \
            .query(TMappingsValues) \
            .filter(TMappingsValues.id_mapping == int(id_mapping)) \
            .all()
        mapping_contents = []

        if len(contents) > 0:
            for content in contents:
                if content.source_value != '':
                    d = {
                        str(content.id_target_value): content.source_value
                    }
                    mapping_contents.append(d)
        selected_content = defaultdict(list)
        for content in mapping_contents:
            for key, value in content.items():
                selected_content[key].append(value)

        return selected_content
    except Exception:
        raise
