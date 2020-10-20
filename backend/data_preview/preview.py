from geonature.utils.env import DB

from ..db.queries.data_preview import (
    get_valid_user_data,
    get_synthese_fields,
    get_id_module,
    get_id_dataset,
)

from ..db.models import TImports
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


def get_preview(
    import_id,
    module_code,
    schema_name,
    table_name,
    total_columns,
    selected_content,
    selected_cols,
):
    nomenclature_fields = NomenclatureTransformer().set_nomenclature_fields(
        selected_cols
    )
    for field in nomenclature_fields:
        total_columns[field["synthese_col"]] = field["transformed_col"]
    data_preview = get_valid_user_data(schema_name, table_name, total_columns, 100)
    valid_data_list = []

    #  for source columns which are mapped to the same synthese target col
    # build a dict like {'source_col': [target_col_1, target_col_2 ...]}
    modified_dict = {}
    for target, source in total_columns.items():
        if source in modified_dict:
            modified_dict[source].append(target)
        else:
            modified_dict[source] = [target]

    # calculate fixed cols
    id_module = get_id_module(module_code)
    id_dataset = get_id_dataset(import_id)
    #  build a dict from rowProxy
    for row in data_preview:
        row_dict = {}
        key_to_remove = []
        # add fixed synthese fields
        row_dict["id_dataset"] = id_dataset
        row_dict["id_module"] = id_module

        for key, value in row.items():
            #  check if source field is twice or more
            nomenclature_col_dict = find_nomenclature_col(key, nomenclature_fields)
            #  build a key with source nomenclenture -> target nomenclature with decoded value
            if nomenclature_col_dict:
                user_file_col = nomenclature_col_dict["user_col"]
                new_dict_key = "{source}->{target}".format(
                    source=user_file_col, target=nomenclature_col_dict["synthese_col"]
                )
                row_dict[new_dict_key] = get_nomenclature_label_from_id(value)
                key_to_remove.append(nomenclature_col_dict["user_col"])
                # find target columns in the modified dict create bellow
            syn_targets = modified_dict[key]
            for syn_target in syn_targets:
                row_dict[syn_target] = value

        #  remove untransformed nomenclatures for preview
        for key in key_to_remove:
            try:
                row_dict.pop(key)
            except KeyError:
                pass

        valid_data_list.append(row_dict)

    return valid_data_list


def find_nomenclature_col(col_name: str, nomenclature_field: list) -> dict:
    nomenclature_col_dict = None
    for el in nomenclature_field:
        if el["transformed_col"] == col_name:
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
    import_obj = DB.session.query(TImports).get(import_id)

    total_columns = {
        **selected_cols,
        **added_cols,
    }

    if import_obj.uuid_autogenerated is True:
        total_columns["unique_id_sinp"] = selected_cols.get(
            "unique_id_sinp", "gn_unique_id_sinp"
        )

    if import_obj.altitude_autogenerated is True:
        total_columns["altitude_min"] = selected_cols.get(
            "altitude_min", "gn_altitude_min"
        )
        total_columns["altitude_max"] = selected_cols.get(
            "altitude_max", "gn_altitude_max"
        )

    # remove non synthese fields from dict :
    sf = get_synthese_fields()
    sf_names = [f.column_name for f in sf]
    final_total_col = {}

    for source, target in total_columns.items():
        if source in sf_names or source.startswith("gn"):
            final_total_col[source] = target

    return final_total_col


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
