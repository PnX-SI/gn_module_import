import os


def create_col_name(df, col_dict, key, value, import_id):
    try:
        if value not in df.columns:
            col_dict[key] = value
        else:
            colname = '_'.join(['gn', import_id, value])
            while colname in df.columns:
                colname = '_'.join(['gn',colname])
            col_dict[key] = colname
    except Exception:
        raise


def get_upload_dir_path(module_url, directory_name):
    try:
        module_directory_path = os.path.join(os.path.dirname(os.getcwd()), 'external_modules{}'.format(module_url))
        return os.path.join(module_directory_path, directory_name)
    except Exception:
        raise


def get_pk_name(prefix):
    return "".join([prefix, 'pk'])
