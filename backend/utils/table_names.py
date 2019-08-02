def get_full_table_name(schema_name, table_name):
    """ Get full table name (schema_name.table_name)

    Args:
        - schema_name (str)
        - table_name (str)
    Returns:
        - full name (str)
    """
    return '.'.join([schema_name, table_name])


def set_imports_table_name(table_name):
    """ Set imports table name (prefixed with 'i_')

    Args:
        - table_name (str)
    Returns:
        - table name with 'i_' prefix (str)
    """
    return ''.join(['i_', table_name])