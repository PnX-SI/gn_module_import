from sqlalchemy import text

from geonature.utils.env import DB
from ..models import VUserImportsErrors


def get_error_from_code(error_code):
    query = """
    SELECT * FROM gn_imports.t_user_errors
    WHERE name = :error_code
    """
    result = DB.session.execute(text(query), {"error_code": error_code}).fetchone()
    if result is None:
        raise "No error found for error_code {}".format(error_code)
    return result


def set_user_error(id_import, id_error, col_name, id_rows=[]):
    """
    Add a entry in t_user_error_list

    :params id_import int: id of the import
    :params error_code str: the code of the error (t_user_errors)
    :params col_name str: column(s) concerned by the error
    :params n_errors int[]: id of the rows concerned by the errors
    """
    query = """
        INSERT INTO gn_imports.t_user_error_list(id_import, id_error, column_error, id_rows)
        VALUES (
            :id_import, 
            :id_error, 
            :col_name, 
            :id_rows
        );
        """
    try:
        DB.session.execute(
            text(query),
            {
                "id_import": id_import,
                "id_error": id_error,
                "col_name": col_name,
                "id_rows": id_rows,
            },
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_error_message(schema_name, id_import, error_code, col_name):
    try:
        error = DB.session.execute(
            """
                SELECT *
                FROM {schema_name}.t_user_error_list UEL
                LEFT JOIN {schema_name}.t_user_errors UE ON UE.id_error=UEL.id_error
                WHERE UEL.id_import = {id_import}
                AND UEL.id_error = {id_error}
                AND UEL.column_error = '{col_name}';
            """.format(
                schema_name=schema_name,
                id_import=id_import,
                id_error=id_error,
                col_name=col_name,
            )
        ).fetchone()
        return "{name} : {col_name} column *** ".format(
            name=error.name, col_name=error.column_error
        )
    except Exception:
        raise


def delete_user_errors(schema_name, id_import):
    try:
        DB.session.execute(
            """
                DELETE FROM {schema_name}.t_user_error_list
                WHERE id_import = {id_import};
            """.format(
                schema_name=schema_name, id_import=id_import
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_user_error_list(id_import):
    data = (
        DB.session.query(VUserImportsErrors)
        .filter(VUserImportsErrors.id_import == id_import)
        .order_by(VUserImportsErrors.error_type)
        .all()
    )
    return [d.as_dict() for d in data]

    # try:
    #     user_errors = DB.session.execute(
    #         """
    #             SELECT *
    #             FROM {schema_name}.t_user_error_list UEL
    #             LEFT JOIN {schema_name}.t_user_errors UE ON UE.id_error = UEL.id_error
    #             WHERE id_import = {id_import}
    #             ORDER BY UE.error_type
    #             ;
    #         """.format(
    #             id_import=id_import, schema_name=schema_name
    #         )
    #     ).fetchall()
    #     if user_errors:
    #         user_error_list = [
    #             {
    #                 "type": error.error_type,
    #                 "description": error.description,
    #                 "column": error.column_error,
    #                 "id_error_lines": str(error.id_rows).strip("[]"),
    #             }
    #             for error in user_errors
    #         ]
    #     else:
    #         user_error_list = [
    #             {"id": "", "type": "", "description": "", "column": "", "n_errors": ""}
    #         ]
    #     return user_error_list
    # except Exception:
    #     raise
