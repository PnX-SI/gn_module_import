from geonature.utils.env import DB
import pdb


def set_user_error(id_import, id_error, col_name, n_errors):
    try:
        DB.session.execute(
            """
                INSERT INTO gn_imports.user_error_list(id_import, id_error, column_error, count_error)
                VALUES ({id_import}, {id_error}, '{col_name}', {n_errors});
            """.format(
                id_import=id_import,
                id_error=id_error,
                col_name=col_name,
                n_errors=n_errors
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_error_message(schema_name, id_import, id_error, col_name):
    try:
        error = DB.session.execute(
            """
                SELECT *
                FROM {schema_name}.user_error_list UEL
                LEFT JOIN {schema_name}.user_errors UE ON UE.id_error=UEL.id_error
                WHERE UEL.id_import = {id_import}
                AND UEL.id_error = {id_error}
                AND UEL.column_error = '{col_name}';
            """.format(
                schema_name=schema_name,
                id_import=id_import,
                id_error=id_error,
                col_name=col_name
            )
        ).fetchone()
        message = "{name} : {col_name} column *** ".format(
            name=error.name,
            col_name=error.column_error
        )
        return message
    except Exception:
        raise


def delete_user_errors(schema_name, id_import):
    try:
        DB.session.execute(
            """
                DELETE FROM {schema_name}.user_error_list
                WHERE id_import = {id_import};
            """.format(
                schema_name=schema_name,
                id_import=id_import
            )
        )
        DB.session.commit()
    except Exception:
        raise

