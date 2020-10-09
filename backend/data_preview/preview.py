from ..db.queries.data_preview import (
    get_valid_user_data,
    get_synthese_fields,
    get_id_module,
    get_id_dataset,
)

from ..db.queries.nomenclatures import (
    get_SINP_synthese_cols,
    get_mnemo,
    set_default_value,
    get_nomenc_abb_from_name,
    get_nomenc_values,
    get_nomenclature_label_from_id,
)
from ..transform.nomenclatures.nomenclatures import NomenclatureTransformer


from sqlalchemy import inspect




def get_preview(schema_name, table_name, total_columns, selected_content, selected_cols):
    nomenclature_fields = NomenclatureTransformer().set_nomenclature_fields(selected_cols)
    data_preview = get_valid_user_data(schema_name, table_name, 100)
    valid_data_list = []
    # build a dict from rowProxy
    for row in data_preview:
        row_dict = {}
        key_to_remove = []
        for key, value in row.items():
            nomenclature_col_dict = find_nomenclature_col(key, nomenclature_fields)
            # build a key with source nomenclenture -> target nomenclature with decoded value
            if nomenclature_col_dict:
                user_file_col = nomenclature_col_dict["user_col"]
                new_dict_key = "{source}->{target}".format(
                    source=user_file_col,
                    target=nomenclature_col_dict["synthese_col"]
                )
                row_dict[new_dict_key] = get_nomenclature_label_from_id(value)
                key_to_remove.append(nomenclature_col_dict["user_col"])
            else:
                row_dict[key] = value
        # remove untransformed nomenclatures for preview
        for key in key_to_remove:
            try:
                row_dict.pop(key)
            except KeyError:
                pass
        
        valid_data_list.append(row_dict)

    return valid_data_list

def find_nomenclature_col(col_name: str, nomenclature_field: list)-> dict:
    nomenclature_col_dict = None
    for el in nomenclature_field:
        if el['transformed_col'] == col_name:
            nomenclature_col_dict = el 
            break 
    return nomenclature_col_dict

def get_synthese_dict(synthese_fields):
    try:
        synthese_dict = {}
        for field in synthese_fields:
            if field.column_name.startswith("id_nomenclature"):
                synthese_dict[field.ordinal_position] = {
                    "key": f"_transformed_{field.column_name}",
                    "value": None,
                }
            else:
                synthese_dict[field.ordinal_position] = {
                    "key": field.column_name,
                    "value": None,
                }
        synthese_dict.pop(1)
        return synthese_dict
    except Exception:
        raise


def set_total_columns(selected_cols, added_cols, import_id, module_name):
    """
    remove non synthese fields from dict 
    and set fixed synthese fields 
    """
    total_columns = {
        **selected_cols,
        **added_cols,
    }

    # remove non synthese fields from dict :
    sf = get_synthese_fields()
    sf_names = [f.column_name for f in sf]
    for field in list(total_columns):
        if field not in sf_names:
            del total_columns[field]

    # add fixed synthese fields :
    total_columns["id_module"] = get_id_module(module_name)
    total_columns["id_dataset"] = get_id_dataset(import_id)

    return total_columns


def get_nomenc_name(synthese_col_name, user_value, selected_content):
    try:
        nomenc_abb = get_nomenc_abb_from_name(synthese_col_name)
        nomenc_values = get_nomenc_values(nomenc_abb)
        nomenc_values_ids = [str(val[0]) for val in nomenc_values]

        for k, v in selected_content.items():
            for val in v:
                if val == user_value:
                    if k in nomenc_values_ids:
                        return get_mnemo(k)
        # return default value if not provided:
        return get_mnemo(set_default_value(get_nomenc_abb_from_name(synthese_col_name)))
    except Exception:
        raise
