from goodtables import validate
from .goodtables_errors import *

from ..logs import logger
from ..wrappers import checker
from ..db.queries.user_errors import get_error_from_code, set_user_error


ERROR_MAPPING = {
    "encoding-error": "ENCODING_ERROR",
    "blank-header": "HEADER_COLUMN_EMPTY",
    "format-error": "FILE_FORMAT_ERROR",
    "duplicate-header": "HEADER_SAME_COLUMN_NAME",
    "blank-row": "EMPTY_ROW",
    "duplicate-row": "DUPLICATE_ROWS",
    "extra-value": "ROW_HAVE_TOO_MUCH_COLUMN",
    "missing-value": "ROW_HAVE_LESS_COLUMN",
}


@checker("User file validity checked")
def check_user_file_good_table(
    id_import, full_path, given_encoding, row_limit=100000000
):
    try:
        errors = []
        report = validate(full_path, skip_checks=["duplicate-row"], row_limit=row_limit)
        detected_encoding = report["tables"][0]["encoding"]
        if given_encoding.lower() != detected_encoding:
            set_user_error(
                id_import=id_import, step="UPLOAD", error_code="ENCODING_ERROR",
            )
            errors.append({"error": "ENCODING_ERROR"})
            return {"errors": errors}
        if report["valid"] is False:
            for error in report["tables"][0]["errors"]:
                # other goodtable errors :
                set_user_error(
                    id_import=id_import,
                    step="UPLOAD",
                    error_code=ERROR_MAPPING.get(error["code"], "UNKNOWN_ERROR"),
                    comment="Erreur d'origine :" + error["message"],
                )
                errors.append(error)

        # if no rows :
        if report["tables"][0]["row-count"] == 0:
            gn_error = get_error_from_code("EMPTY_FILE")
            set_user_error(
                id_import=id_import, step="UPLOAD", id_error=gn_error.id_error,
            )
            errors.append("no data")

        # get column names:
        column_names = report["tables"][0]["headers"]
        # get file format:
        file_format = report["tables"][0]["format"]
        # get row number:
        row_count = report["tables"][0]["row-count"]

        logger.debug("column_names = %s", column_names)
        logger.debug("row_count = %s", row_count)

        return {
            "column_names": column_names,
            "file_format": file_format,
            "row_count": row_count,
            "errors": errors,
        }

    except Exception:
        raise
