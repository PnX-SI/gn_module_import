from itertools import groupby

from geonature.utils.env import DB

from pypnnomenclature.models import TNomenclatures, BibNomenclaturesTypes

from gn_module_import.db.queries.nomenclatures import (
    get_nomenclature_values,
    get_nomenc_values,
    find_row_with_nomenclatures_error,
    get_synthese_col,
    get_nomenc_abb,
    get_SINP_synthese_cols_with_mnemonique,
    get_SINP_synthese_cols,
    set_nomenclature_id,
    get_nomenc_abb_from_name,
    set_default_nomenclature_id,
    exist_proof_check,
    dee_bluring_check,
    ref_biblio_check,
    set_default_value,
    get_mnemo,
    get_nomenc_values,
    # add_nomenclature_transformed_col,
    info_geo_attachment_check,
    info_geo_attachment_check_2,
)
from ...db.queries.user_errors import set_user_error

from ...wrappers import checker
from ...logs import logger

import sqlalchemy as sa
from flask import current_app


class NomenclatureTransformer:
    """
    Class for checking nomenclure

    Object attributes:

    :table_name str: the name of the table where we proceed the transformations
    :nomenclature_fields list<dict>: Describe all the synthese nomenclature field and their column correspodance of the import table
    :raw_mapping_content list<dict>: The content mapping from the DB
    :formated_mapping_content list<dict>: 
        Example: {'id_nomenclature': '1', 'user_values': ['Inconnu'], 'user_col': 'ocstade'}
    :accepted_id_nomencatures dict: For each nomenclature column: give all the id_nomenclature available for this type
        exemple : {'objdenbr': '['84', '83', '82', '81']'}
    """

    def __init__(self):
        pass

    def init(self, contentmapping, selected_columns, table_name):
        """
        :params contentmapping
        :params selected_columns: colums of the field mapping corresponding of the import
        """
        self.table_name = table_name
        self.contentmapping = contentmapping
        self.nomenclature_fields = self.set_nomenclature_fields(selected_columns)
        self.formated_mapping_content = self.__formated_mapping_content(
            selected_columns
        )
        self.selected_columns = selected_columns
        self.accepted_id_nomencatures = self.__set_accepted_id_nomencatures()
        self.__create_col_transformation(self.nomenclature_fields)

    def set_nomenclature_fields(self, selected_columns) -> list:
        nomenclature_fields = []
        for row in get_SINP_synthese_cols_with_mnemonique():
            if row["synthese_name"] in selected_columns:
                nomenclature_fields.append(
                    {
                        "synthese_col": row["synthese_name"],
                        "user_col": selected_columns[row["synthese_name"]],
                        "transformed_col": f"_tr_{row['synthese_name']}_{selected_columns[row['synthese_name']]}",
                        "mnemonique_type": row["mnemonique_type"],
                    }
                )
        return nomenclature_fields



    def __create_col_transformation(self, nomenclature_fields):
        """
        Create a column for tranformed nomenclature (id_nomenclature) from code or label
        for each provided nomenclature column
        """
        for nom in nomenclature_fields:
            # add_nomenclature_transformed_col(nom["transformed_col"], self.table_name)
            DB.session.execute('ALTER TABLE {table_name} ADD COLUMN {col_name} character varying(5)'.format(table_name=self.table_name, col_name=nom['transformed_col']))

    def __formated_mapping_content(self, selected_columns):
        """
        Set nomenclatures_field and formated_mapping_content attributes
        """
        key = lambda u: u[1]
        formated_mapping_content = []
        self.mapping_val_by_mnemo = {}
        for mnemo, mappings in self.contentmapping.items():
            synthese_name = get_synthese_col(mnemo)
            for cd_nomenc, values in groupby(sorted(mappings.items(), key=key), key=key):
                values = [k for k, v in values]
                self.mapping_val_by_mnemo[mnemo] = values
                nomenclature = TNomenclatures.query.filter(
                    TNomenclatures.cd_nomenclature == cd_nomenc,
                    TNomenclatures.nomenclature_type.has(
                        BibNomenclaturesTypes.mnemonique == mnemo,
                    ),
                ).one()
                if synthese_name in selected_columns:
                    d = {
                        "id_nomenclature": nomenclature.id_nomenclature,
                        "user_values": values,
                        "user_col": selected_columns[synthese_name],
                        "transformed_col": f"_tr_{synthese_name}_{selected_columns[synthese_name]}",
                    }
                    formated_mapping_content.append(d)
        return formated_mapping_content

    def __set_accepted_id_nomencatures(self):
        """
        For each nomenclature columns in the file 
        find the id nomenclature accepted for the type of nomenclature
        re
        """
        mnemonique_type = [
            field["mnemonique_type"] for field in self.nomenclature_fields
        ]
        rows = get_nomenclature_values(mnemonique_type)

        accepted_id_nom = []
        for row in rows:
            accepted_id_nom.append(
                {
                    "mnemonique_type": row.mnemnonique,
                    "transformed_col": self.__find_transformed_col(row.mnemnonique),
                    "accepted_id_nomenclature": row.id_nomenclatures,
                }
            )
        return accepted_id_nom

    def __find_transformed_col(self, mnemonique_type):
        """get transformed_col from mnemonique_type"""
        file_col_name = None
        for col in self.nomenclature_fields:
            if col["mnemonique_type"] == mnemonique_type:
                file_col_name = col["transformed_col"]
                break
        return file_col_name

    @checker("Set nomenclature ids from content mapping form")
    def set_nomenclature_ids(self):
        for element in self.formated_mapping_content:
            for val in element["user_values"]:
                set_nomenclature_id(
                    table_name=self.table_name,
                    user_col=element["user_col"],
                    target_col=element["transformed_col"],
                    value=val,
                    id_nomenclature=str(element["id_nomenclature"]),
                )

    def find_nomenclatures_errors(self, id_import):
        """
        Detect nomenclature which not check with the given value mapping
        """
        for el in self.nomenclature_fields:
            rows_with_err = find_row_with_nomenclatures_error(
                self.table_name,
                el["transformed_col"],
                el["user_col"]
            )
            for row in rows_with_err:
                # ou remplacer par un warning quand la valeur par défaut a été utilisée

                if current_app.config["IMPORT"][
                    "FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"
                ]:
                    nomenc_values = get_nomenc_values(el["mnemonique_type"])
                    nomenc_values_ids = [
                        get_mnemo(str(val[0])) for val in nomenc_values
                    ]
                    set_user_error(
                        id_import=id_import,
                        error_code="INVALID_NOMENCLATURE_WARNING",
                        col_name=el["user_col"],
                        id_rows=row.gn_pk,
                        comment="La valeur '{}' ne correspond à aucune des valeurs de la nomenclature {} et a ete remplacée par la valeur par défaut '{}'. \n Valeurs acceptées: {}".format(
                            row[1],
                            el["mnemonique_type"],
                            get_mnemo(set_default_value(el["mnemonique_type"])),
                            self.mapping_val_by_mnemo.get(el["mnemonique_type"])
                        ),
                    )
                else:
                    set_user_error(
                        id_import=id_import,
                        error_code="INVALID_NOMENCLATURE",
                        col_name=el["user_col"],
                        id_rows=row.gn_pk,
                        comment="La valeur '{}' n'existe pas pour la nomenclature {}".format(
                            row[1], el["mnemonique_type"]
                        ),
                    )
                    query = """
                    UPDATE {table}
                    SET gn_is_valid = 'False',
                    gn_invalid_reason = 'INVALID_NOMENCLATURE'
                    WHERE gn_pk in :id_rows
                    """.format(
                        table=self.table_name,
                    )
                    formated_rows_err = []
                    for r in rows_with_err:
                        formated_rows_err = formated_rows_err + r.gn_pk
                    DB.session.execute(query, {"id_rows": tuple(formated_rows_err)})

    @checker("Set nomenclature default ids")
    def set_default_nomenclature_ids(self, where_user_val_none=False):
        for el in self.nomenclature_fields:
            set_default_nomenclature_id(
                table_name=self.table_name,
                nomenc_abb=el["mnemonique_type"],
                tr_col=el["transformed_col"],
                user_col=el["user_col"],
                where_user_val_none=where_user_val_none
            )

    def check_conditionnal_values(self, id_import):
        if current_app.config['IMPORT']['CHECK_EXIST_PROOF']:
            # Proof checker
            row_with_errors_proof = exist_proof_check(
                self.table_name,
                self.__find_transformed_col("PREUVE_EXIST"),
                self.selected_columns.get("digital_proof"),
                self.selected_columns.get("non_digital_proof"),
            )
            if row_with_errors_proof and row_with_errors_proof.id_rows:
                set_user_error(
                    id_import=id_import,
                    error_code="INVALID_EXISTING_PROOF_VALUE",
                    col_name=self.selected_columns.get("id_nomenclature_exist_proof"),
                    id_rows=row_with_errors_proof.id_rows,
                )

        if current_app.config['IMPORT']['CHECK_PRIVATE_JDD_BLURING']:
            # bluering checker
            row_with_errors_blurr = dee_bluring_check(
                self.table_name, id_import, self.__find_transformed_col("DEE_FLOU"),
            )
            if row_with_errors_blurr and row_with_errors_blurr.id_rows:
                set_user_error(
                    id_import=id_import,
                    error_code="CONDITIONAL_MANDATORY_FIELD_ERROR",
                    col_name=self.selected_columns.get("id_nomenclature_blurring", ""),
                    id_rows=row_with_errors_blurr.id_rows,
                    comment="Le champ dEEFloutage doit être remplit si le jeu de données est privé",
                )

        if current_app.config['IMPORT']['CHECK_REF_BIBLIO_LITTERATURE']:
            #  literature checker
            
            row_with_errors_bib = ref_biblio_check(
                self.table_name,
                field_statut_source=self.__find_transformed_col("STATUT_SOURCE"),
                field_ref_biblio=self.selected_columns.get("reference_biblio"),
            )
            if row_with_errors_bib and row_with_errors_bib.id_rows:
                set_user_error(
                    id_import=id_import,
                    error_code="CONDITIONAL_MANDATORY_FIELD_ERROR",
                    col_name=self.selected_columns.get("reference_biblio")
                    or self.selected_columns.get("id_nomenclature_source_status"),
                    id_rows=row_with_errors_bib.id_rows,
                    comment="Le champ reference_biblio doit être remplit si le statut source est 'Littérature'",
                )
        if current_app.config['IMPORT']['CHECK_TYPE_INFO_GEO']:

            # type info geo checker
            row_with_error_info_geo = info_geo_attachment_check(
                table_name=self.table_name,
                tr_info_geo_col=self.__find_transformed_col("TYP_INF_GEO"),
                grid_col=self.selected_columns.get("codemaille"),
                dep_col=self.selected_columns.get("codedepartement"),
                municipality_col=self.selected_columns.get("codecommune"),    
            )
            if row_with_error_info_geo and row_with_error_info_geo.id_rows:
                set_user_error(
                    id_import=id_import,
                    error_code="CONDITIONAL_INVALID_DATA",
                    col_name=self.selected_columns.get("id_nomenclature_info_geo_type"),
                    id_rows=row_with_error_info_geo.id_rows,
                    comment="Le champs TypeInfoGeo ne peut valoir géoréférencement si des rattachements sont fournis (code maille/département/commune). Ils seront calculés par la plateforme",
                )

            row_with_error_info_geo_2 = info_geo_attachment_check_2(
                table_name=self.table_name,
                tr_info_geo_col=self.__find_transformed_col("TYP_INF_GEO"),
                grid_col=self.selected_columns.get("codemaille"),
                dep_col=self.selected_columns.get("codedepartement"),
                municipality_col=self.selected_columns.get("codecommune"),    
            )
            if row_with_error_info_geo_2 and row_with_error_info_geo_2.id_rows:
                set_user_error(
                    id_import=id_import,
                    error_code="CONDITIONAL_MANDATORY_FIELD_ERROR",
                    col_name=self.selected_columns.get("id_nomenclature_info_geo_type") or 'TypeInfoGeo',
                    id_rows=row_with_error_info_geo_2.id_rows,
                    comment="Si une entité de rattachement est fournie (code maille/département/commune), alors la colonne TypeInfoGeo est obligatoire",
                )


@checker("Set nomenclature default ids")
def set_default_nomenclature_ids(table_name, selected_cols):
    selected_nomenc = {
        k: v for k, v in selected_cols.items() if k in get_SINP_synthese_cols()
    }
    for k, v in selected_nomenc.items():
        mnemonique_type = get_nomenc_abb_from_name(k)
        nomenc_values = get_nomenc_values(mnemonique_type)
        ids = [str(nomenc.nomenc_id) for nomenc in nomenc_values]
        set_default_nomenclature_id(table_name, mnemonique_type, v, ids)