from flask import current_app

from geonature.utils.env import DB

from ..db.queries.load_to_synthese import get_synthese_info
from ..db.queries.utils import is_cd_nom_required

from .check_cd_nom import check_cd_nom
from .check_dates import check_dates
from .check_missing import format_missing, check_missing
from .check_uuid import check_uuid
from .check_types import check_types
from .check_other_fields import check_entity_source, check_id_digitizer, check_url
from .check_counts import check_counts
from .check_altitudes import check_altitudes
from .check_geography import check_geography
from .check_duplicated import check_row_duplicates

from ..db.models import (
    TImports,
    TMappings,
)

from ..db.queries.user_table_queries import (
    rename_table,
    get_table_info,
    set_primary_key,
    get_table_name,
    get_table_names,
    get_row_number,
    set_imports_table_name,
    check_row_number,
    alter_column_type,
    get_n_loaded_rows,
    get_n_invalid_rows,
    get_required,
    delete_table,
)

from ..db.queries.save_mapping import (
    save_field_mapping,
    save_content_mapping,
    get_selected_columns,
)

from ..db.queries.taxonomy import get_cd_nom_list

from ..db.queries.user_errors import delete_user_errors

from ..db.queries.geometries import get_local_srid

from ..utils.clean_names import *

from ..upload.upload_errors import *


from ..transform.utils import add_code_columns, remove_temp_columns
from ..transform.set_geometry import GeometrySetter
from ..transform.set_altitudes import set_altitudes
from ..transform.nomenclatures.nomenclatures import NomenclatureTransformer

from ..logs import logger
from ..api_error import GeonatureImportApiError
from ..extract.extract import extract
from ..load.load import load
from ..load.import_class import ImportDescriptor
from ..load.utils import compute_df


def data_cleaning(
    df,
    import_id,
    selected_columns,
    missing_val,
    def_count_val,
    cd_nom_list,
    srid,
    local_srid,
    is_generate_uuid,
    schema_name,
    is_generate_altitude,
    prefix,
):
    try:

        # user_error = []
        added_cols = {}

        # set gn_is_valid and gn_invalid_reason:
        df["gn_is_valid"] = True
        df["gn_invalid_reason"] = ""

        # get synthese column info:
        synthese_info = get_synthese_info(selected_columns.keys())

        # set is_nullable for cd_nom
        is_cd_nom_req = is_cd_nom_required(schema_name)
        if is_cd_nom_req:
            is_nullable = "NO"
        else:
            is_nullable = "YES"
        synthese_info["cd_nom"]["is_nullable"] = is_nullable

        if (
            "longitude" in selected_columns.keys()
            and "latitude" in selected_columns.keys()
        ):
            synthese_info["longitude"] = {
                "is_nullable": "YES",
                "column_default": None,
                "data_type": "real",
                "character_max_length": None,
            }
            synthese_info["latitude"] = {
                "is_nullable": "YES",
                "column_default": None,
                "data_type": "real",
                "character_max_length": None,
            }

        # Check data:
        check_missing(
            df, selected_columns, synthese_info, missing_val, import_id, schema_name
        )
        check_row_duplicates(df, selected_columns, import_id, schema_name)
        check_types(
            df, selected_columns, synthese_info, missing_val, schema_name, import_id,
        )
        check_cd_nom(
            df, selected_columns, missing_val, cd_nom_list, schema_name, import_id
        )
        check_dates(df, selected_columns, synthese_info, import_id, schema_name)
        check_uuid(
            df,
            selected_columns,
            synthese_info,
            is_generate_uuid,
            import_id,
            schema_name,
        )
        check_counts(
            df,
            selected_columns,
            synthese_info,
            def_count_val,
            added_cols,
            import_id,
            schema_name,
        )
        check_entity_source(
            df, added_cols, selected_columns, synthese_info, import_id, schema_name
        )
        check_id_digitizer(df, selected_columns, synthese_info, import_id, schema_name)
        check_geography(
            df, import_id, added_cols, selected_columns, srid, local_srid, schema_name
        )
        check_altitudes(
            df,
            selected_columns,
            synthese_info,
            is_generate_altitude,
            import_id,
            schema_name,
        )
        check_url(df, selected_columns, import_id)

    except Exception:
        raise


def field_mapping_data_checking(import_id, id_mapping):
    """
    High level function to perform all field mapping check and transformation
    """
    try:
        is_running = True
        is_temp_table_name = False
        is_table_names = False

        # INITIALIZE VARIABLES

        logger.info("*** START CORRESPONDANCE MAPPING")

        errors = []

        IMPORTS_SCHEMA_NAME = current_app.config["IMPORT"]["IMPORTS_SCHEMA_NAME"]
        ARCHIVES_SCHEMA_NAME = current_app.config["IMPORT"]["ARCHIVES_SCHEMA_NAME"]
        PREFIX = current_app.config["IMPORT"]["PREFIX"]
        MISSING_VALUES = current_app.config["IMPORT"]["MISSING_VALUES"]
        DEFAULT_COUNT_VALUE = current_app.config["IMPORT"]["DEFAULT_COUNT_VALUE"]

        index_col = "".join([PREFIX, "pk"])
        table_names = get_table_names(
            ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME, int(import_id)
        )
        is_table_names = True
        temp_table_name = "_".join(["temp", table_names["imports_table_name"]])
        is_temp_table_name = True

        engine = DB.engine
        column_names = get_table_info(table_names["imports_table_name"], "column_name")
        local_srid = get_local_srid()

        # get import_obj
        import_obj = DB.session.query(TImports).get(import_id)
        if not import_obj:
            raise
        import_obj_dict = import_obj.to_dict()
        is_generate_uuid = import_obj_dict["uuid_autogenerated"]
        is_generate_alt = import_obj_dict["altitude_autogenerated"]

        is_generate_uuid = False
        is_generate_alt = False

        logger.debug("import_id = %s", import_id)
        logger.debug("DB table name = %s", table_names["imports_table_name"])

        # get synthese fields filled in the user form:
        selected_columns = get_selected_columns(id_mapping)

        importObject = ImportDescriptor(
            id_import=import_id,
            id_mapping=id_mapping,
            table_name="{}.{}".format(
                IMPORTS_SCHEMA_NAME, table_names["imports_table_name"]
            ),
            column_names=column_names,
            selected_columns=selected_columns,
            import_srid=import_obj_dict["srid"],
        )

        logger.debug(
            "selected columns in correspondance mapping = %s", selected_columns
        )
        # check if column names provided in the field form exists in the user table
        for key, value in selected_columns.items():
            if key not in ["unique_id_sinp_generate", "altitudes_generate"]:
                if value not in column_names:
                    return (
                        {
                            "message": "La colonne '{}' n'existe pas. \
                            Avez-vous sélectionné le bon mapping ?".format(
                                value
                            )
                        },
                        400,
                    )

        # check if required fields are not empty:
        missing_cols = []
        required_cols = get_required(IMPORTS_SCHEMA_NAME, "dict_fields")
        try:
            selected_columns_tab = selected_columns.keys()
            if "WKT" in selected_columns_tab:
                required_cols.remove("longitude")
                required_cols.remove("latitude")
                required_cols.remove("codecommune")
                required_cols.remove("codemaille")
                required_cols.remove("codedepartement")
            if "longitude" and "latitude" in selected_columns_tab:
                required_cols.remove("WKT")
                required_cols.remove("codecommune")
                required_cols.remove("codemaille")
                required_cols.remove("codedepartement")
            if (
                "codecommune" or "codemaille" or "codedepartement"
            ) in selected_columns_tab:
                required_cols.remove("WKT")
                required_cols.remove("longitude")
                required_cols.remove("latitude")
        except ValueError:
            logger.info("Try no remove an inexisting columns... pass")
        for col in required_cols:
            if col not in selected_columns.keys():
                missing_cols.append(col)
        if len(missing_cols) > 0:
            return (
                {
                    "message": "Champs obligatoires manquants: {}".format(
                        ",".join(missing_cols)
                    )
                },
                500,
            )

        # DELETE USER ERRORS
        delete_user_errors(import_id, "FIELD_MAPPING")
        # EXTRACT
        logger.info("* START EXTRACT FROM DB TABLE TO PYTHON")
        df = extract(
            table_names["imports_table_name"],
            IMPORTS_SCHEMA_NAME,
            column_names,
            index_col,
            import_id,
        )
        # HACK: add code_commune, code_maille, code_dep columns
        #  TODO: conserver le formulaire dans le store !
        add_code_columns(selected_columns, df)

        logger.info("* END EXTRACT FROM DB TABLE TO PYTHON")

        # get cd_nom list
        cd_nom_list = get_cd_nom_list()

        # TRANSFORM (data checking and cleaning)
        # start process (transform and load)
        for i in range(df.npartitions):
            logger.info("* START DATA CLEANING partition %s", i)
            partition = df.get_partition(i)
            partition_df = compute_df(partition)
            data_cleaning(
                partition_df,
                import_id,
                selected_columns,
                MISSING_VALUES,
                DEFAULT_COUNT_VALUE,
                cd_nom_list,
                import_obj_dict["srid"],
                local_srid,
                is_generate_uuid,
                IMPORTS_SCHEMA_NAME,
                is_generate_alt,
                PREFIX,
            )

            temp_cols = [
                "valid_wkt",
                "one_comm_code",
                "one_maille_code",
                "one_dep_code",
                "line_with_code",
                "no_geom",
                "is_multiple_type_code",
                "line_with_one_code",
                "no_duplicate",
                "duplicate",
                "interval",
                "temp",
                "temp2",
                "check_dates",
            ]
            partition_df = remove_temp_columns(temp_cols, partition_df)

            logger.info("* END DATA CLEANING partition %s", i)

            # LOAD (from Dask dataframe to postgresql table, with d6tstack pd_to_psql function)
            logger.info("* START LOAD PYTHON DATAFRAME TO DB TABLE partition %s", i)
            load(partition_df, i, IMPORTS_SCHEMA_NAME, temp_table_name, engine)
            logger.info("* END LOAD PYTHON DATAFRAME TO DB TABLE partition %s", i)

        # delete original table
        delete_table(table_names["imports_full_table_name"])

        # rename temp table with original table name
        rename_table(
            IMPORTS_SCHEMA_NAME, temp_table_name, table_names["imports_table_name"]
        )

        # set primary key
        set_primary_key(
            IMPORTS_SCHEMA_NAME, table_names["imports_table_name"], index_col
        )

        # alter primary key type into integer
        alter_column_type(
            IMPORTS_SCHEMA_NAME, table_names["imports_table_name"], index_col, "integer"
        )
        # # # calculate geometries and altitudes
        geometry_setter = GeometrySetter(
            importObject,
            local_srid=local_srid,
            code_commune_col=selected_columns.get("codecommune", "codecommune"),
            code_maille_col=selected_columns.get("codemaille", "codemaille"),
            code_dep_col=selected_columns.get("codedepartement", "codedepartement"),
        )
        geometry_setter.set_geometry()

        # set_altitudes(df, selected_columns, import_id, IMPORTS_SCHEMA_NAME,
        #               table_names['imports_full_table_name'], table_names['imports_table_name'],
        #               index_col, is_generate_alt, added_cols['the_geom_local'], added_cols)

        DB.session.commit()
        DB.session.close()

        # check if df is fully loaded in postgresql table :
        is_nrows_ok = check_row_number(
            import_id, table_names["imports_full_table_name"]
        )
        if not is_nrows_ok:
            logger.error("missing rows because of loading server error")
            raise GeonatureImportApiError(
                message='INTERNAL SERVER ERROR ("postMapping() error"): \
                            lignes manquantes dans {} - refaire le matching'.format(
                    table_names["imports_full_table_name"]
                ),
                details="",
            )

        # calculate number of invalid lines
        n_invalid_rows = get_n_invalid_rows(table_names["imports_full_table_name"])

        # check total number of lines
        n_table_rows = get_row_number(table_names["imports_full_table_name"])

        logger.info("*** END CORRESPONDANCE MAPPING")

        DB.session.query(TImports).filter(TImports.id_import == int(import_id)).update(
            {TImports.id_field_mapping: int(id_mapping)}
        )

        # ############### IF field mapping skipped

        # if not current_app.config["IMPORT"]["ALLOW_VALUE_MAPPING"]:
        #     logger.info("update t_imports from step 2 to step 4")
        #     ### CONTENT MAPPING ###
        #     # get content mapping data
        #     id_mapping_value = current_app.config["IMPORT"]["DEFAULT_VALUE_MAPPING_ID"]
        #     #  check if the default mapping exist
        #     value_mapping = TMappings.query.get(id_mapping_value)
        #     if not value_mapping:
        #         return (
        #             {
        #                 "message": "Content Mapping: le mapping n'existe pas - contacter l'administrateur"
        #             },
        #             400,
        #         )

        #     # build nomenclature_transformer service
        #     nomenclature_transformer = NomenclatureTransformer(
        #         id_mapping_value, selected_columns, table_names["imports_table_name"]
        #     )
        #     # with the mapping given, find all the corresponding nomenclatures
        #     nomenclature_transformer.set_nomenclature_ids()
        #     results = nomenclature_transformer.check_conditionnal_values()
        #     logger.info("Find nomenclature with errors :")
        #     nomenclature_transformer.find_nomenclatures_errors(import_id)

        #     if current_app.config["IMPORT"][
        #         "FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"
        #     ]:
        #         nomenclature_transformer.set_default_nomenclature_ids()

        #     # update t_import
        #     DB.session.query(TImports).filter(TImports.id_import == import_id).update(
        #         {TImports.id_content_mapping: id_mapping_value, TImports.step: 4}
        #     )

        DB.session.commit()
        DB.session.close()

        is_running = False

        return (
            {
                "n_table_rows": n_table_rows,
                "import_id": import_id,
                "id_mapping": id_mapping,
                "table_name": table_names["imports_table_name"],
                "is_running": is_running,
            },
        )

    except Exception as e:
        logger.error("*** ERROR IN CORRESPONDANCE MAPPING")
        logger.exception(e)
        DB.session.rollback()

        if is_temp_table_name:
            DB.session.execute(
                """
                DROP TABLE IF EXISTS {}.{};
                """.format(
                    IMPORTS_SCHEMA_NAME, temp_table_name
                )
            )
            DB.session.commit()
            DB.session.close()

        if is_table_names:
            n_loaded_rows = get_n_loaded_rows(table_names["imports_full_table_name"])

        if is_table_names:
            if n_loaded_rows == 0:
                logger.error(
                    "Table %s vide à cause d'une erreur de copie, refaire l'upload et le mapping",
                    table_names["imports_full_table_name"],
                )
                raise GeonatureImportApiError(
                    message="INTERNAL SERVER ERROR :: Erreur pendant le mapping de correspondance :: \
                        Table {} vide à cause d'une erreur de copie, refaire l'upload et le mapping, \
                        ou contactez l'administrateur du site".format(
                        table_names["imports_full_table_name"]
                    ),
                    details="",
                )

        raise GeonatureImportApiError(
            message="INTERNAL SERVER ERROR : Erreur pendant le mapping de correspondance - contacter l'administrateur",
            details=str(e),
        )


def content_mapping_data_checking(import_id, id_mapping):
    """
    High level function to perform all content mapping check and transformation
    """
    try:
        # delete existing errors
        delete_user_errors(import_id, "CONTENT_MAPPING")

        logger.info(
            "Content mapping : transforming user values to id_types in the user table"
        )

        # SAVE MAPPING

        # Nomenclature checking
        logger.info("Check and remplace nomenclature")
        import_object = TImports.query.get(import_id)
        if not import_object:
            raise
        selected_columns = get_selected_columns(import_object.id_field_mapping)
        table_name = set_imports_table_name(get_table_name(import_id))
        # build nomenclature_transformer service
        nomenclature_transformer = NomenclatureTransformer(
            id_mapping, selected_columns, table_name
        )

        # with the mapping given, find all the corresponding nomenclatures
        nomenclature_transformer.set_nomenclature_ids()

        logger.info("Find nomenclature with errors :")
        nomenclature_transformer.find_nomenclatures_errors(import_id)

        nomenclature_transformer.check_conditionnal_values(import_id)

        if current_app.config["IMPORT"]["FILL_MISSING_NOMENCLATURE_WITH_DEFAULT_VALUE"]:
            nomenclature_transformer.set_default_nomenclature_ids()

        # UPDATE TIMPORTS

        logger.info("update t_imports from step 3 to step 4")

        DB.session.query(TImports).filter(TImports.id_import == import_id).update(
            {TImports.id_content_mapping: id_mapping, TImports.step: 4}
        )

        DB.session.commit()

        logger.info("-> t_imports updated from step 3 to step 4")

        return "content_mapping done", 200

    except Exception as e:
        DB.session.rollback()
        DB.session.close()
        logger.error("*** SERVER ERROR DURING CONTENT MAPPING (user values to id_types")
        logger.exception(e)
        raise GeonatureImportApiError(
            message="INTERNAL SERVER ERROR during content mapping (user values to id_types",
            details=str(e),
        )
