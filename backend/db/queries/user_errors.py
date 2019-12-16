from geonature.utils.env import DB


def set_user_error(id_import, id_error, col_name, n_errors):
    try:
        DB.session.execute(
            """
                INSERT INTO gn_imports.t_user_error_list(id_import, id_error, column_error, count_error)
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
                FROM {schema_name}.t_user_error_list UEL
                LEFT JOIN {schema_name}.t_user_errors UE ON UE.id_error=UEL.id_error
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
                DELETE FROM {schema_name}.t_user_error_list
                WHERE id_import = {id_import};
            """.format(
                schema_name=schema_name,
                id_import=id_import
            )
        )
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise


def get_user_error_list(schema_name, id_import):
    try:
        user_errors = DB.session.execute(
            """
                SELECT *
                FROM {schema_name}.t_user_error_list UEL
                LEFT JOIN {schema_name}.t_user_errors UE ON UE.id_error = UEL.id_error
                WHERE id_import = {id_import};
            """.format(
                id_import=id_import,
                schema_name=schema_name
            )
        ).fetchall()
        if user_errors:
            user_error_list = [
                {
                'id': error.id_error,
                'type': error.error_type,
                'description': error.description,
                'column': error.column_error,
                'n_errors': error.count_error
                } for error in user_errors]
        else:
            user_error_list = [
                {
                'id': '',
                'type': '',
                'description': '',
                'column': '',
                'n_errors': ''
                }]
        return user_error_list
    except Exception:
        raise
