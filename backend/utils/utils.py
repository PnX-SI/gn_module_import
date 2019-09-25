
def create_col_name(col_dict, key, value, import_id):
    if value not in col_dict.values():
        col_dict[key] = value
    else:
        colname = '_'.join(['gn', import_id, value])
        while colname in col_dict.values():
            colname = '_'.join(['gn',colname])
        col_dict[key] = colname