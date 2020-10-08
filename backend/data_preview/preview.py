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

from sqlalchemy import inspect


def get_preview(schema_name, table_name, total_columns, selected_content):
    total_colums_with_transformed = {}
    for key, value in total_columns.items():
        total_colums_with_transformed[key] = value
        if key.startswith("id_nomenclature"):
            total_colums_with_transformed[
                f"_transformed_{key}"
            ] = f"_transformed_{value}"
    # get valid data in user table
    preview = get_valid_user_data(schema_name, table_name, 100)
    # get synthese fields
    synthese_fields = get_synthese_fields()

    # fill synthese template
    valid_data_list = []
    nomenclature_synthese_cols = get_SINP_synthese_cols()
    #  set _transformed_ as a prefix to get the rigth transformed col
    nomenclature_synthese_cols = list(
        map(lambda n: f"_transformed_{n}", nomenclature_synthese_cols)
    )
    for row in preview:
        synthese_dict = get_synthese_dict(synthese_fields)
        for key, value in synthese_dict.items():
            if value["key"] in total_colums_with_transformed.keys():
                if (
                    value["key"] == "id_module"
                    or value["key"] == "id_dataset"
                    or value["key"] == "id_source"
                ):
                    synthese_dict[key]["value"] = total_colums_with_transformed[
                        value["key"]
                    ]
                else:
                    # if this is a nomenclature column : replace user voc by nomenclature voc
                    if value["key"] in nomenclature_synthese_cols and value[
                        "key"
                    ].startswith("_transformed"):
                        synthese_dict[key]["value"] = get_nomenclature_label_from_id(
                            id_nomenclature=row[
                                total_colums_with_transformed[value["key"]]
                            ]
                        )
                    else:
                        synthese_dict[key]["value"] = row[
                            total_colums_with_transformed[value["key"]]
                        ]
            else:
                # if it is a nomenclature column and it is not provided by user : set default value
                if value["key"] in get_SINP_synthese_cols():
                    synthese_dict[key]["value"] = get_mnemo(
                        set_default_value(get_nomenc_abb_from_name(value["key"]))
                    )

                if value["key"] == "last_action":
                    synthese_dict[key]["value"] = "I"
        if synthese_dict[4]["key"] == "id_source":
            del synthese_dict[4]
        valid_data_list.append(synthese_dict)

    return valid_data_list

    # except Exception:
    #     raise


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
