from goodtables import validate
from .goodtables_errors import*

from ..logs import logger
from ..wrappers import checker


@checker('User file validity checked')
def check_user_file(full_path, row_limit=100000000):

    try:

        errors = []

        report = validate(full_path, skip_checks=['duplicate-row'], row_limit=row_limit)

        if report['valid'] is False:

            for error in report['tables'][0]['errors']:
                if 'No such file or directory' in error['message']:
                    # avoid printing original goodtable message error containing the user full_path (security purpose)
                    user_error = set_error(error['code'], 'No such file or directory', '')
                    errors.append(user_error)
                else:
                    # other goodtable errors :
                    user_error = set_error(error['code'], '', error['message'])
                    errors.append(user_error)

        # if no rows :
        if report['tables'][0]['row-count'] == 0:
            errors.append(no_data)

        # get column names:
        column_names = report['tables'][0]['headers']
        # get file format:
        file_format = report['tables'][0]['format']
        # get row number:
        row_count = report['tables'][0]['row-count']

        logger.debug('column_names = %s', column_names)
        logger.debug('row_count = %s', row_count)

        return {
            'column_names': column_names,
            'file_format': file_format,
            'row_count': row_count,
            'errors': errors
        }

    except Exception:
        raise
