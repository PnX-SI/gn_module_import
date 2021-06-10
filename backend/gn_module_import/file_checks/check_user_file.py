from goodtables import validate
from .goodtables_errors import *

from ..logs import logger
from ..wrappers import checker
from ..db.queries.user_errors import set_user_error
from ..db.models import ImportUserErrorType, ImportUserError


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


def check_user_file_good_table(
    id_import, data, encoding, row_limit=100000000
):
    errors = []
    report = validate(data, skip_checks=["duplicate-row"], format='csv', encoding=encoding, row_limit=row_limit)
    if report["valid"] is False:
        for error in report["tables"][0]["errors"]:
            # other goodtable errors :
            set_user_error(
                id_import=id_import,
                step="UPLOAD",
                error_code=ERROR_MAPPING.get(error["code"], "UNKNOWN_ERROR"),
                comment="Erreur d'origine :" + error["message"],
                commit=False,
            )
            errors.append(error)
        print(errors)

    # if no rows :
    if report["tables"][0]["row-count"] == 0:
        gn_error = ImportUserErrorType.query.filter_by(name="EMPTY_FILE").one()
        set_user_error(
            id_import=id_import, step="UPLOAD", id_error=gn_error.pk,
            commit=False,
        )
        errors.append("no data")

    # get column names:
    column_names = report["tables"][0]["headers"]
    # get row number:
    row_count = report["tables"][0]["row-count"]

    return {
        "column_names": column_names,
        "row_count": row_count,
        "errors": errors,
    }
