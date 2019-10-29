from psycopg2.extensions import AsIs,QuotedString
from geonature.utils.env import DB


def get_nomenc_details(nomenclature_abb):

    try:
        nomenc_details = DB.session.execute("""
            SELECT 
                label_default as name,
                id_type as id
            FROM ref_nomenclatures.bib_nomenclatures_types
            WHERE mnemonique = {nomenc};"""\
                .format(nomenc = QuotedString(nomenclature_abb)))\
                .fetchone()

        return nomenc_details

    except Exception:
        raise


def get_nomenc_values(nommenclature_abb):

    try:
        nomenc_values = DB.session.execute("""
            SELECT
                nom.id_nomenclature AS nomenc_id,
                nom.label_default AS nomenc_values, 
                nom.definition_default AS nomenc_definitions
            FROM ref_nomenclatures.bib_nomenclatures_types AS bib
            JOIN ref_nomenclatures.t_nomenclatures AS nom ON nom.id_type = bib.id_type
            WHERE bib.mnemonique = {nomenc};"""\
                .format(nomenc = QuotedString(nommenclature_abb)))\
                .fetchall()

        return nomenc_values

    except Exception:
        raise


def get_nomenc_user_values(user_nomenc_col, schema_name, table_name):

    try:
        nomenc_user_values = DB.session.execute("""
            SELECT DISTINCT {user_nomenc_col} as user_val
            FROM {schema_name}.{table_name};"""\
                .format(
                    user_nomenc_col = user_nomenc_col,
                    schema_name = schema_name,
                    table_name = table_name))\
                .fetchall()

        return nomenc_user_values

    except Exception:
        raise


def get_nomenc_abbs(form_data):

    try:
        nomenc_abbs = DB.session.execute("""
            SELECT F.name_field as synthese_name, BNT.mnemonique as nomenc_abb
            FROM gn_imports.bib_fields F
            RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.id_field = F.id_field
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.id_type = CSN.id_type;
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
        nomenc_synthese_name = DB.session.execute("""
            SELECT F.name_field as synthese_name
            FROM gn_imports.bib_fields F
            RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.id_field = F.id_field
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.id_type = CSN.id_type
            WHERE BNT.mnemonique = '{abb}';
            """.format(abb=abb)).fetchone()

        return nomenc_synthese_name.synthese_name

    except Exception:
        raise


def get_synthese_cols():
    
    try:
        nomencs = DB.session.execute("""
            SELECT F.name_field as synthese_name
            FROM gn_imports.bib_fields F
            RIGHT JOIN gn_imports.cor_synthese_nomenclature CSN ON CSN.id_field = F.id_field
            LEFT JOIN ref_nomenclatures.bib_nomenclatures_types BNT ON BNT.id_type = CSN.id_type;
            """).fetchall()

        synthese_name_list = [nomenc.synthese_name for nomenc in nomencs]

        return synthese_name_list

    except Exception:
        raise