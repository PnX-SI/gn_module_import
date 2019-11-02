from ..db.queries.data_preview import (
    get_valid_user_data, 
    get_synthese_fields,
    get_id_module,
    get_id_dataset
)


def get_preview(schema_name, table_name, total_columns):

    try:

        # get valid data in user table
        preview = get_valid_user_data(schema_name, table_name, 100)
        
        # get synthese fields
        synthese_fields = get_synthese_fields()

        # fill synthese template
        valid_data_list = []
        for row in preview:
            synthese_dict = get_synthese_dict(synthese_fields)
            for key,value in synthese_dict.items():
                if value['key'] in total_columns.keys():
                    if value['key'] == 'id_module'\
                            or value['key'] == 'id_dataset'\
                                or value['key'] == 'id_source':
                        synthese_dict[key]['value'] = total_columns[value['key']]
                    else:
                        synthese_dict[key]['value'] = row[total_columns[value['key']]]
            valid_data_list.append(synthese_dict)

        return valid_data_list

    except Exception:
        raise


def get_synthese_dict(synthese_fields):
    try:
        synthese_dict = {}
        for field in synthese_fields:
            synthese_dict[field.ordinal_position] = {
                'key' : field.column_name,
                'value': None
            }
        synthese_dict.pop(1)
        return synthese_dict
    except Exception:
        raise


def set_total_columns(selected_cols, added_cols, import_id):
    try:

        total_columns = {**selected_cols, **added_cols}

        # remove from dict :
        if 'longitude' in total_columns.keys():
            del total_columns['longitude']
        if 'latitude' in total_columns.keys():
            del total_columns['latitude']
        if 'id_mapping' in total_columns.keys():
            del total_columns['id_mapping']
        if 'unique_id_sinp_generate' in total_columns.keys():
            del total_columns['unique_id_sinp_generate']
        if 'altitudes_generate' in total_columns.keys():
            del total_columns['altitudes_generate']

        # add fixed synthese fields :
        total_columns['id_module'] = get_id_module()
        total_columns['id_dataset'] = get_id_dataset(import_id)       

        return total_columns
    except Exception:
        raise