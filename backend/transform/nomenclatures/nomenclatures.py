from geonature.utils.env import DB

from ...db.queries.nomenclatures import (
    get_nomenc_details,
    get_nomenc_values,
    get_nomenc_user_values,
    get_nomenc_abbs,
    get_synthese_col,
    get_nomenc_abb,
    get_SINP_synthese_cols,
    set_nomenclature_id,
    get_nomenc_abb_from_name,
    set_default_nomenclature_id
)

from ...utils.clean_names import clean_string
from ...wrappers import checker
from ...logs import logger


def get_nomenc_info(form_data, schema_name, table_name):
    try:

        logger.info('get nomenclature info')

        # get list of user-selected synthese column names dealing with SINP nomenclatures
        selected_SINP_nomenc = get_nomenc_abbs(form_data)

        front_info = []

        for nomenc in selected_SINP_nomenc:

            # get nomenclature name and id
            nomenc_info = get_nomenc_details(nomenc)

            # get nomenclature values
            nomenc_values = get_nomenc_values(nomenc)

            val_def_list = []
            for val in nomenc_values:
                d = {
                    'id': str(val.nomenc_id),
                    'value': val.nomenc_values,
                    'definition': val.nomenc_definitions,
                    'name': clean_string(val.nomenc_values)
                }
                val_def_list.append(d)

            # get user_nomenclature column name and values:
            user_nomenc_col = get_synthese_col(nomenc)
            nomenc_user_values = get_nomenc_user_values(form_data[user_nomenc_col], schema_name, table_name)

            user_values_list = []
            for index, val in enumerate(nomenc_user_values):
                user_val_dict = {
                    'id': index,
                    'value': val.user_val
                }
                user_values_list.append(user_val_dict)

            d = {
                'nomenc_abbr': nomenc,
                'nomenc_id': nomenc_info.id,
                'nomenc_name': nomenc_info.name,
                'nomenc_synthese_name': user_nomenc_col,
                'nomenc_values_def': val_def_list,
                'user_values': {
                    'column_name': form_data[user_nomenc_col],
                    'values': user_values_list
                }
            }

            front_info.append(d)

        return front_info

    except Exception:
        raise


@checker('Set nomenclature ids from content mapping form')
def set_nomenclature_ids(IMPORTS_SCHEMA_NAME, table_name, selected_content, selected_cols):
    DB.session.begin(subtransactions=True)
    try:
        content_list = []
        for key, value in selected_content.items():
            abb = get_nomenc_abb(key)
            synthese_name = get_synthese_col(abb)
            d = {
                'id_type': key,
                'user_values': value,
                'user_col': selected_cols[synthese_name]
            }
            content_list.append(d)

        for element in content_list:
            for val in element['user_values']:
                set_nomenclature_id(IMPORTS_SCHEMA_NAME, table_name, element['user_col'], val, str(element['id_type']))
                DB.session.flush()

        DB.session.commit()

    except Exception:
        DB.session.rollback()
        raise


@checker('Set nomenclature default ids')
def set_default_nomenclature_ids(schema_name, table_name, selected_cols):
    DB.session.begin(subtransactions=True)
    try:
        selected_nomenc = {k: v for k, v in selected_cols.items() if k in get_SINP_synthese_cols()}
        for k, v in selected_nomenc.items():
            abb = get_nomenc_abb_from_name(k)
            nomenc_values = get_nomenc_values(abb)
            ids = [str(nomenc.nomenc_id) for nomenc in nomenc_values]
            set_default_nomenclature_id(schema_name, table_name, abb, v, ids)
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise
