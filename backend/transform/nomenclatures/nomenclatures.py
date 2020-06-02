from geonature.utils.env import DB

from ...db.queries.nomenclatures import (
    get_nomenc_details,
    get_nomenclature_values,
    get_nomenc_user_values,
    get_nomenc_values,
    find_row_with_nomenclatures_error,
    get_nomenc_abbs,
    get_synthese_col,
    get_nomenc_abb,
    get_SINP_synthese_cols_with_mnemonique,
    get_SINP_synthese_cols,
    set_nomenclature_id,
    get_nomenc_abb_from_name,
    set_default_nomenclature_id,
    get_saved_content_mapping,
    exist_proof_check,
    dee_bluring_check,
    ref_biblio_check,
    set_default_value,
    get_mnemo,
    get_nomenc_values,
)
from ...db.queries.user_errors import set_user_error
from ...db.queries.utils import execute_query

from ...utils.clean_names import clean_string
from ...wrappers import checker
from ...logs import logger

from flask import current_app


class NomenclatureTransformer:
    """
    Class for checking nomenclure

    Object attributes:

    :table_name str: the name of the table where we proced the transformations
    :nomenclature_fields list<dict>: Describe all the synthese nomenclature field and their column correspodance of the import table
    :raw_mapping_content list<dict>: The content mapping from the DB
    :formated_mapping_content list<dict>: 
        Example: {'id_nomenclature': '1', 'user_values': ['Inconnu'], 'user_col': 'ocstade'}
    :accepted_id_nomencatures dict: For each nomenclature column: give all the id_nomenclature available for this type
        exemple : {'objdenbr': '['84', '83', '82', '81']'}
    """

    def __init__(self, id_mapping, selected_columns, table_name):
        """
        :params id_mapping int: the id_mapping
        :params selected_columns: colums of the field mapping corresponding of the import
        """
        self.table_name = table_name
        self.id_mapping = id_mapping
        self.nomenclature_fields = self.__set_nomenclature_fields(selected_columns)
        self.formated_mapping_content = self.__formated_mapping_content(
            selected_columns
        )
        self.selected_columns = selected_columns
        self.accepted_id_nomencatures = self.__set_accepted_id_nomencatures()

    def __set_nomenclature_fields(self, selected_columns):
        nomenclature_fields = []
        for row in get_SINP_synthese_cols_with_mnemonique():
            if row["synthese_name"] in selected_columns:
                nomenclature_fields.append(
                    {
                        "synthese_col": row["synthese_name"],
                        "file_col": selected_columns[row["synthese_name"]],
                        "mnemonique_type": row["mnemonique_type"],
                    }
                )
        return nomenclature_fields

    def __formated_mapping_content(self, selected_columns):
        """
        Set nomenclatures_field and formated_mapping_content attributes
        """
        formated_mapping_content = []
        raw_mapping_content = get_saved_content_mapping(self.id_mapping)
        for id_nomenclature, mapped_values in raw_mapping_content.items():
            mnemonique_type = get_nomenc_abb(id_nomenclature)
            synthese_name = get_synthese_col(mnemonique_type)
            if synthese_name in selected_columns:
                d = {
                    "id_nomenclature": id_nomenclature,
                    "user_values": mapped_values,
                    "user_col": selected_columns[synthese_name],
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
                    "user_col": self.__find_file_col(row.mnemnonique),
                    "accepted_id_nomenclature": row.id_nomenclatures,
                }
            )
        return accepted_id_nom

    def __find_file_col(self, mnemonique_type):
        """get col_name from mnemonique_type"""
        file_col_name = None
        for col in self.nomenclature_fields:
            if col["mnemonique_type"] == mnemonique_type:
                file_col_name = col["file_col"]
                break
        return file_col_name

    @checker("Set nomenclature ids from content mapping form")
    def set_nomenclature_ids(self):
        try:
            for element in self.formated_mapping_content:
                for val in element["user_values"]:
                    set_nomenclature_id(
                        self.table_name,
                        element["user_col"],
                        val,
                        str(element["id_nomenclature"]),
                    )
                    # DB.session.flush()

            DB.session.commit()
        except Exception:
            DB.session.rollback()
            raise

    def find_nomenclatures_errors(self, id_import):
        """
        Detect nomenclature which not check with the given value mapping
        """
        for el in self.accepted_id_nomencatures:
            rows_with_err = find_row_with_nomenclatures_error(
                self.table_name,
                el["user_col"],
                list(map(lambda id: str(id), el["accepted_id_nomenclature"])),
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
                        step="CONTENT_MAPPING",
                        error_code="INVALID_NOMENCLATURE_WARNING",
                        col_name=el["user_col"],
                        id_rows=row.gn_pk,
                        comment="La valeur '{}' ne correspond à aucune des valeurs [{}] de la nomenclature {} et a ete remplacée par la valeur par défaut '{}'".format(
                            row[1],
                            ", ".join(nomenc_values_ids),
                            el["mnemonique_type"],
                            get_mnemo(set_default_value(el["mnemonique_type"])),
                        ),
                    )
                else:
                    set_user_error(
                        id_import=id_import,
                        step="CONTENT_MAPPING",
                        error_code="INVALID_NOMENCLATURE",
                        col_name=el["user_col"],
                        id_rows=row.gn_pk,
                        comment="La valeur '{}' n'existe pas pour la nomenclature {}".format(
                            row[1], el["mnemonique_type"]
                        ),
                    )
                query = """
                UPDATE {schema}.{table}
                SET gn_is_valid = 'False',
                gn_invalid_reason = 'INVALID_NOMENCLATURE'
                WHERE gn_pk in :id_rows
                """.format(
                    schema=current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"],
                    table=self.table_name,
                )
                formated_rows_err = []
                for r in rows_with_err:
                    formated_rows_err = formated_rows_err + r.gn_pk
                execute_query(
                    query, commit=True, params={"id_rows": tuple(formated_rows_err)}
                )

    @checker("Set nomenclature default ids")
    def set_default_nomenclature_ids(self):
        try:
            for el in self.accepted_id_nomencatures:
                set_default_nomenclature_id(
                    table_name=self.table_name,
                    nomenc_abb=el["mnemonique_type"],
                    user_col=el["user_col"],
                    id_types=list(
                        map(lambda id: str(id), el["accepted_id_nomenclature"])
                    ),
                )
            DB.session.commit()
        except Exception:
            DB.session.rollback()
            raise

    def check_conditionnal_values(self, id_import):
        # Proof checker
        row_with_errors_proof = exist_proof_check(
            self.table_name,
            self.selected_columns.get("id_nomenclature_exist_proof"),
            self.selected_columns.get("digital_proof"),
            self.selected_columns.get("non_digital_proof"),
        )
        if row_with_errors_proof and row_with_errors_proof.id_rows:
            set_user_error(
                id_import=id_import,
                step="CONTENT_MAPPING",
                error_code="INVALID_EXISTING_PROOF_VALUE",
                col_name=self.selected_columns.get("id_nomenclature_exist_proof"),
                id_rows=row_with_errors_proof.id_rows,
            )

        # bluering checker
        row_with_errors_blurr = dee_bluring_check(
            self.table_name,
            id_import,
            self.selected_columns.get("id_nomenclature_blurring"),
        )
        if row_with_errors_blurr and row_with_errors_blurr.id_rows:
            set_user_error(
                id_import=id_import,
                step="CONTENT_MAPPING",
                error_code="CONDITIONAL_MANDATORY_FIELD_ERROR",
                col_name=self.selected_columns.get("id_nomenclature_blurring"),
                id_rows=row_with_errors_blurr.id_rows,
                comment="Le champ dEEFloutage doit être remplit si le jeu de données est privé",
            )

        #  literature checker
        row_with_errors_bib = ref_biblio_check(
            self.table_name,
            field_statut_source=self.selected_columns.get(
                "id_nomenclature_source_status"
            ),
            field_ref_biblio=self.selected_columns.get("reference_biblio"),
        )
        if row_with_errors_bib and row_with_errors_bib.id_rows:
            set_user_error(
                id_import=id_import,
                step="CONTENT_MAPPING",
                error_code="CONDITIONAL_MANDATORY_FIELD_ERROR",
                col_name=self.selected_columns.get("reference_biblio")
                or self.selected_columns.get("id_nomenclature_source_status"),
                id_rows=row_with_errors_bib.id_rows,
                comment="Le champ reference_biblio doit être remplit si le statut source est 'Littérature'",
            )


def get_nomenc_info(form_data, schema_name, table_name):
    try:
        logger.info("get nomenclature info")

        # get list of user-selected synthese column names dealing with SINP nomenclatures
        selected_SINP_nomenc = get_nomenc_abbs(form_data)
        front_info = []
        for nomenc in selected_SINP_nomenc:
            # get nomenclature name and id
            nomenc_info = get_nomenc_details(nomenc)
            # get nomenclature values
            nomenc_values = get_nomenc_values(nomenc)

            val_def_list = []
            for val in nomenc_values:
                d = {
                    "id": str(val.nomenc_id),
                    "value": val.nomenc_values,
                    "definition": val.nomenc_definitions,
                    "name": clean_string(val.nomenc_values),
                }
                val_def_list.append(d)

            # get user_nomenclature column name and values:
            user_nomenc_col = get_synthese_col(nomenc)
            nomenc_user_values = get_nomenc_user_values(
                form_data[user_nomenc_col], schema_name, table_name
            )

            user_values_list = []
            for index, val in enumerate(nomenc_user_values):
                user_val_dict = {"id": index, "value": val.user_val}
                user_values_list.append(user_val_dict)

            d = {
                "nomenc_abbr": nomenc,
                "nomenc_id": nomenc_info.id,
                "nomenc_name": nomenc_info.name,
                "nomenc_synthese_name": user_nomenc_col,
                "nomenc_values_def": val_def_list,
                "user_values": {
                    "column_name": form_data[user_nomenc_col],
                    "values": user_values_list,
                },
            }

            front_info.append(d)
        return front_info

    except Exception:
        raise


@checker("Set nomenclature default ids")
def set_default_nomenclature_ids(table_name, selected_cols):
    DB.session.begin(subtransactions=True)
    try:
        selected_nomenc = {
            k: v for k, v in selected_cols.items() if k in get_SINP_synthese_cols()
        }
        for k, v in selected_nomenc.items():
            mnemonique_type = get_nomenc_abb_from_name(k)
            nomenc_values = get_nomenc_values(mnemonique_type)
            ids = [str(nomenc.nomenc_id) for nomenc in nomenc_values]
            set_default_nomenclature_id(table_name, mnemonique_type, v, ids)
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise
