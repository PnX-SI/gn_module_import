from psycopg2.extensions import AsIs,QuotedString
from geonature.utils.env import DB

from ...logs import logger

from ...db.queries.nomenclatures import get_nomenc_details, get_nomenc_values, get_nomenc_user_values

import pdb

def get_nomenc_info(form_data, SINP_COLS, schema_name):

    try:

        logger.info('get nomenclature info')

        # get list of synthese column names dealing with SINP nomenclatures
        selected_SINP_nomenc = [nomenclature['nomenclature_abb'] for nomenclature in SINP_COLS\
                                if nomenclature['synthese_col'] in form_data.keys()]

        front_info = []

        for nomenc in selected_SINP_nomenc:

            # get nomenclature name and id
            nomenc_info = get_nomenc_details(nomenc)

            # get nomenclature values
            nomenc_values = get_nomenc_values(nomenc)

            val_def_list = []
            for val in nomenc_values:
                d = {
                    'value' : val.nomenc_values,
                    'definition' : val.nomenc_definitions
                }
                val_def_list.append(d)

            # get user_nomenclature column name and values
            for col in SINP_COLS:
                if col['nomenclature_abb'] == nomenc:
                    user_nomenc_col = col['synthese_col']

            nomenc_user_values = get_nomenc_user_values(form_data[user_nomenc_col], schema_name, form_data['table_name'])
            user_values_list = [val.user_val for val in nomenc_user_values] 

            d = {
                    'nomenc_abbr' : nomenc,
                    'nomenc_id' : nomenc_info.id,
                    'nomenc_name' : nomenc_info.name,
                    'nomenc_values_def' : val_def_list,
                    'user_values' : {
                        'column_name' : form_data[user_nomenc_col],
                        'values' : user_values_list
                    }
                }

            front_info.append(d)

        return front_info

    except Exception:
        raise