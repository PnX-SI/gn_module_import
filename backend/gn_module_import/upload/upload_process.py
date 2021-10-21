import csv
import os
import pathlib
from werkzeug.utils import secure_filename

from geonature.utils.env import ROOT_DIR

from ..logs import logger
from ..wrappers import checker
from ..api_error import GeonatureImportApiError
from ..db.queries.user_errors import set_user_error


@checker("Upload : file saved in directory")
def upload(
    id_import, request, size_max, allowed_extensions, directory_name, module_url
):
    """ check and upload user data:
        - check if user request is not empty
        - check if user file name is lower than 100 characters
        - check user file extension
        - check if user file size is allowed
        - if no error, uploads user file in the upload directory

    Args:
        - id_import: id of the created import (in order to set errors)
        - request (werkzeug.local.LocalProxy): http request posted by the user
        - size_max (int): max size allowed for the user file
        - allowed_extension (list(str)): list of file extensions allowed
        - directory_name (str): name of the upload directory
        - module_url (str)

    Returns:
        - (dict) : !!!!!
    """

    try:

        if "File" not in request.files:
            logger.error("Saving user file : No File in request files")
            set_user_error(
                id_import=id_import, step="UPLOAD", error_code="NO_FILE_SENDED"
            )
            return {"error": "NO_FILE_SENDED"}

        file = request.files["File"]

        if file.filename == "":
            logger.error("Saving user file : No File in request files")
            set_user_error(
                id_import=id_import, step="UPLOAD", error_code="NO_FILE_SENDED"
            )
            return {"error": "NO_FILE_SENDED"}

        # get file path
        upload_directory_path = directory_name
        module_directory_path = os.path.join(
            str(ROOT_DIR), "external_modules{}".format(module_url)
        )
        uploads_directory = os.path.join(module_directory_path, upload_directory_path)

        filename = secure_filename(file.filename)

        if len(filename) > 100:
            logger.error("Saving user file : file name too long")
            set_user_error(
                id_import=id_import, step="UPLOAD", error_code="FILE_NAME_TOO_LONG"
            )
            return {"error": "FILE_NAME_TOO_LONG"}

        full_path = os.path.join(uploads_directory, filename)

        # check user file extension (changer)
        extension = pathlib.Path(full_path).suffix.lower()
        if extension not in allowed_extensions:
            logger.error("Saving user file : extension not allowed")
            set_user_error(
                id_import=id_import, step="UPLOAD", error_code="FILE_EXTENSION_ERROR",
            )
            return {"error": "FILE_EXTENSION_ERROR"}

        # check file size
        file.seek(0, 2)
        size = file.tell() / (1024 * 1024)
        logger.info("File size = %s Mo", size)
        file.seek(0)
        if size > size_max:
            logger.error("Saving user file : user file size > max size allowed")
            set_user_error(
                id_import=id_import, step="UPLOAD", error_code="FILE_OVERSIZE",
            )
            return {"error": "FILE_OVERSIZE"}

        # save user file in upload directory
        file.save(full_path)

        if not os.path.isfile(full_path):
            logger.error("Saving user file : invalid path")
            set_user_error(
                id_import=id_import,
                step="UPLOAD",
                error_code="ERROR_WHILE_LOADING_FILE",
            )
            return {"error": "ERROR_WHILE_LOADING_FILE"}

        # find separator
        try:
            line_one = open(full_path, "r").readline()
            sniffer = csv.Sniffer()
            separator = sniffer.sniff(line_one).delimiter
        except Exception:
            logger.error("No separator found for this file")
            separator = ";"

        logger.debug("original file name = %s", filename)

        if extension == ".geojson":
            separator = ","

        return {
            "file_name": filename,
            "full_path": full_path,
            "separator": separator,
            "is_uploaded": True,
            "extension": extension,
        }
    except Exception:
        raise
