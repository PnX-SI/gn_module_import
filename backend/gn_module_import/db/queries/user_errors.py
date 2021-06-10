from flask import current_app
from sqlalchemy import text

from geonature.utils.env import DB as db
from gn_module_import.db.models import ImportUserErrorType, ImportUserError


def set_user_error(
    id_import,
    step,
    id_error=None,
    error_code=None,
    col_name="",
    id_rows=[],
    comment=None,
    commit=True,
):
    """
    Add a entry in t_user_error_list

    :params id_import int: id of the import
    :params step str: step of the error <'UPLOAD', 'FIELD_MAPPING', 'CONTENT_MAPPING'>
    :params error_code str: the code of the error (t_user_errors)
    :params col_name str: column(s) concerned by the error
    :params n_errors int[]: id of the rows concerned by the errors
    :params comment str: additional comment about the error
    """
    if id_error:
        error_type = ImportUserErrorType.query.get(id_error)
    else:
        error_type = ImportUserErrorType.query.filter_by(name=error_code).one()
    if type(id_rows) is list:
        ordered_id_rows = sorted(list(map(lambda x: x + 1, id_rows)))
    else:
        ordered_id_rows = ['ALL']
    error = ImportUserError(id_import=id_import, type=error_type, column=col_name,
                            rows=ordered_id_rows, step=step, comment=comment)
    db.session.add(error)
    if commit:
        db.session.commit()
