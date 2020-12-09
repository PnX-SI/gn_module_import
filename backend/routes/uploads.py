import os
import datetime

from flask import request
from werkzeug.utils import secure_filename

from utils_flask_sqla.response import json_resp
from geonature.utils.env import DB
from geonature.core.gn_permissions import decorators as permissions

from ..db.models import (
    TImports,
    CorRoleImport,
    CorImportArchives,
    generate_user_table_class,
)

from ..db.queries.user_table_queries import (
    get_table_names,
    load_csv_to_db,
    delete_tables,
)

from ..db.queries.metadata import delete_import_CorImportArchives


from ..utils.clean_names import *
from ..utils.utils import get_pk_name

from ..upload.upload_process import upload
from ..upload.upload_errors import *
from ..upload.geojson_to_csv import parse_geojson

from ..file_checks.check_user_file import check_user_file_good_table
from ..logs import logger
from ..api_error import GeonatureImportApiError
from ..wrappers import checker
from ..blueprint import blueprint

from ..db.queries.user_errors import set_user_error


@blueprint.route("/uploads", methods=["POST"])
@permissions.check_cruved_scope("C", True, module_code="MY_IMPORT")
@json_resp
@checker("Total time to post user file and fill metadata")
def post_user_file(info_role):
    """
        - load user file in "upload" directory
        - check user file data with goodtables library (column names, row duplicates, ...)
        - copy user file raw data in 2 geonature db tables (in archives and imports schemas)
        - fill t_imports, cor_role_import, cor_archive_import
        - return id_import and column names list
        - delete user file in upload directory
    """

    try:
        is_running = True
        logger.info("*** START UPLOAD USER FILE")

        # INITIALIZE VARIABLES

        is_file_saved = False
        is_id_import = False
        errors = []

        MAX_FILE_SIZE = blueprint.config["MAX_FILE_SIZE"]
        ALLOWED_EXTENSIONS = blueprint.config["ALLOWED_EXTENSIONS"]
        DIRECTORY_NAME = blueprint.config["UPLOAD_DIRECTORY"]
        MODULE_URL = blueprint.config["MODULE_URL"]
        PREFIX = blueprint.config["PREFIX"]
        ARCHIVES_SCHEMA_NAME = blueprint.config["ARCHIVES_SCHEMA_NAME"]
        IMPORTS_SCHEMA_NAME = blueprint.config["IMPORTS_SCHEMA_NAME"]
        N_MAX_ROWS_CHECK = 100000000

        # get form data
        metadata = request.form.to_dict()

        if metadata["isFileChanged"] == "true":

            # SAVES USER FILE IN UPLOAD DIRECTORY

            logger.info("* START SAVE USER FILE IN UPLOAD DIRECTORY")

            # CREATES CURRENT IMPORT IN TIMPORTS (SET STEP TO 1 AND DATE/TIME TO CURRENT DATE/TIME)
            try:
                file_name = request.files["File"].filename
                file_name = secure_filename(file_name)
                temp = file_name.split(".")
                extension = temp[len(temp) - 1]
            except Exception:
                file_name = "Filename error"
                extension = "Extension error"
            # start t_imports filling and fill cor_role_import
            if metadata["importId"] == "undefined":
                # if first file uploaded by the user in the current import process :
                init_date = datetime.datetime.now()
                insert_t_imports = TImports(
                    date_create_import=init_date,
                    date_update_import=init_date,
                    full_file_name=file_name,
                    step=1,
                    id_dataset=metadata["datasetId"],
                    encoding=metadata["encodage"],
                    format_source_file=extension,
                    srid=metadata["srid"],
                )
                DB.session.add(insert_t_imports)
                DB.session.flush()
                id_import = insert_t_imports.id_import
                logger.debug("id_import = %s", id_import)
                logger.debug("id_role = %s", info_role.id_role)
                # fill cor_role_import
                insert_cor_role_import = CorRoleImport(
                    id_role=info_role.id_role, id_import=id_import
                )
                DB.session.add(insert_cor_role_import)
                DB.session.flush()
            else:
                # if other file(s) already uploaded by the user in the current import process :
                id_import = int(metadata["importId"])
                DB.session.query(TImports).filter(
                    TImports.id_import == id_import
                ).update(
                    {
                        TImports.date_update_import: datetime.datetime.now(),
                        TImports.full_file_name: file_name,
                        TImports.encoding: metadata["encodage"],
                        TImports.format_source_file: extension,
                        TImports.srid: metadata["srid"],
                    }
                )

            if isinstance(id_import, int):
                is_id_import = True

            # CHECKS USER FILE

            uploaded_file = upload(
                id_import,
                request,
                MAX_FILE_SIZE,
                ALLOWED_EXTENSIONS,
                DIRECTORY_NAME,
                MODULE_URL,
            )

            # checks if error in user file or user http request:
            if "error" in uploaded_file:
                return {"id_import": id_import, "errors": uploaded_file["error"]}, 400

            full_path = uploaded_file["full_path"]
            is_file_saved = uploaded_file["is_uploaded"]
            logger.info("* END SAVE USER FILE IN UPLOAD DIRECTORY")

            # GEOJSON

            if uploaded_file["extension"] == ".geojson":
                output = ".".join([full_path, "csv"])
                geometry_col_name = "".join([PREFIX, "geometry"])
                try:
                    parse_geojson(full_path, output, geometry_col_name)
                except Exception:
                    return (
                        {"message": "Erreur durant la conversion du geojson en csv"},
                        500,
                    )
                os.remove(full_path)
                full_path = output

            logger.info("* START CHECK USER FILE VALIDITY")
            report = check_user_file_good_table(
                id_import, full_path, metadata["encodage"], N_MAX_ROWS_CHECK
            )

            # reports user file errors:
            if len(report["errors"]) > 0:
                logger.error(report["errors"])
                return {"id_import": id_import, "errors": report["errors"]}, 400

            logger.info("* END CHECK USER FILE VALIDITY")

            # set step to 1 in t_imports table
            DB.session.query(TImports).filter(TImports.id_import == id_import).update(
                {TImports.step: 1}
            )

            # GET/SET FILE,SCHEMA,TABLE,COLUMN NAMES

            # file name treatment
            file_name_cleaner = clean_file_name(
                uploaded_file["file_name"], uploaded_file["extension"], id_import
            )
            if len(file_name_cleaner["errors"]) > 0:
                # if user file name is not valid, an error is returned
                set_user_error(
                    id_import=id_import, step="UPLOAD", error_code="FILE_NAME_ERROR"
                )
                return (
                    {"id_import": id_import, "errors": file_name_cleaner["errors"]},
                    400,
                )

            table_names = get_table_names(
                ARCHIVES_SCHEMA_NAME,
                IMPORTS_SCHEMA_NAME,
                file_name_cleaner["clean_name"],
            )
            logger.debug(
                "full DB user table name = %s", table_names["imports_full_table_name"]
            )

            # pk name to include (because copy_from method requires a primary_key)
            pk_name = get_pk_name(PREFIX)

            # clean column names
            columns = [clean_string(x) for x in report["column_names"]]

            """
            CREATES TABLES CONTAINING RAW USER DATA IN GEONATURE DB
            """

            # if existing, delete all tables already uploaded which have the same id_import suffix
            delete_tables(id_import, ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME)
            DB.session.commit()

            # create user table classes (1 for gn_import_archives schema, 1 for gn_imports schema) \
            # corresponding to the user file structure (csv)
            user_class_gn_import_archives = generate_user_table_class(
                ARCHIVES_SCHEMA_NAME,
                table_names["archives_table_name"],
                pk_name,
                columns,
                id_import,
                schema_type="archives",
            )
            user_class_gn_imports = generate_user_table_class(
                IMPORTS_SCHEMA_NAME,
                table_names["imports_table_name"],
                pk_name,
                columns,
                id_import,
                schema_type="gn_imports",
            )

            # create 2 empty tables in geonature db (in gn_import_archives and gn_imports schemas)
            engine = DB.engine
            user_class_gn_import_archives.__table__.create(bind=engine, checkfirst=True)
            user_class_gn_imports.__table__.create(bind=engine, checkfirst=True)

            # fill the user table (copy_from function is equivalent to COPY function of postgresql)
            conn = engine.raw_connection()
            cur = conn.cursor()

            logger.info("* START COPY FROM CSV TO ARCHIVES SCHEMA")
            load_csv_to_db(
                full_path,
                cur,
                table_names["archives_full_table_name"],
                uploaded_file["separator"],
                columns,
            )
            logger.info("* END COPY FROM CSV TO ARCHIVES SCHEMA")

            logger.info("* START COPY FROM CSV TO IMPORTS SCHEMA")
            load_csv_to_db(
                full_path,
                cur,
                table_names["imports_full_table_name"],
                uploaded_file["separator"],
                columns,
            )
            logger.info("* END COPY FROM CSV TO IMPORTS SCHEMA")

            conn.commit()
            conn.close()
            cur.close()

            """
            FILLS CorImportArchives AND UPDATES TImports
            """

            delete_import_CorImportArchives(id_import)
            cor_import_archives = CorImportArchives(
                id_import=id_import, table_archive=table_names["archives_table_name"]
            )
            DB.session.add(cor_import_archives)

            SEPARATOR_MAPPING = {";": "colon", "\t": "tab", ",": "comma", " ": "space"}

            separator = SEPARATOR_MAPPING.get(uploaded_file["separator"], "Unknown")

            # update gn_import.t_imports with cleaned file name and step = 2
            DB.session.query(TImports).filter(TImports.id_import == id_import).update(
                {
                    TImports.import_table: table_names["archives_table_name"],
                    TImports.step: 2,
                    TImports.format_source_file: uploaded_file["extension"],
                    TImports.separator: separator,
                    TImports.source_count: report["row_count"] - 1,
                }
            )

            DB.session.commit()

            logger.info("*** END UPLOAD USER FILE")

            is_running = False

            return (
                {
                    "importId": id_import,
                    "columns": columns,
                    "fileName": metadata["fileName"],
                    "is_running": is_running,
                },
                200,
            )
        # file not change
        else:
            DB.session.query(TImports).filter(TImports.id_import == metadata.get('importId')).update(
                {
                    TImports.step: 2,
                    TImports.srid: metadata['srid'],
                    TImports.encoding: metadata['encodage']
                }
            )

            DB.session.commit()

        return (
            {
                "importId": metadata["importId"],
                "srid": metadata["srid"],
                "fileName": metadata["fileName"],
                "is_running": is_running,
            },
            200,
        )

    except Exception as e:
        logger.error("*** ERROR WHEN POSTING FILE")
        logger.exception(e)
        DB.session.rollback()
        if is_id_import:
            delete_tables(id_import, ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME)
        DB.session.commit()
        raise GeonatureImportApiError(message=str(e), details=str(e))
    finally:
        if is_file_saved:
            os.remove(full_path)
        DB.metadata.clear()
        DB.session.close()
