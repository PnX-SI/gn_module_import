
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