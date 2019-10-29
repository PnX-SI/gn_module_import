import datetime

from ..db.query import (
    get_synthese_info
)

from .check_cd_nom import check_cd_nom
from .check_dates import check_dates
from .check_missing import format_missing, check_missing
from .check_uuid import check_uuid
from .check_types import check_types
from .check_other_fields import check_entity_source
from .check_counts import check_counts
from .check_altitudes import check_altitudes
from .check_geography import check_geography
from ..logs import logger

import pdb



def data_cleaning(df, import_id, selected_columns, dc_user_errors, missing_val, def_count_val, cd_nom_list, srid, local_srid, is_generate_uuid):

    try:

        user_error = []
        added_cols = {}

        # set gn_is_valid and gn_invalid_reason:
        df['gn_is_valid'] = True # à mettre dans la bdd à true avant
        df['gn_invalid_reason'] = ''

        # get synthese column info:
        selected_synthese_cols = [*list(selected_columns.keys())]
        synthese_info = get_synthese_info(selected_synthese_cols)
        synthese_info['cd_nom']['is_nullable'] = 'NO' # mettre en conf
        if 'longitude' in selected_synthese_cols and 'latitude' in selected_synthese_cols:
            synthese_info['longitude'] = {'is_nullable': 'NO', 'column_default': None, 'data_type': 'real', 'character_max_length': None}
            synthese_info['latitude'] = {'is_nullable': 'NO', 'column_default': None, 'data_type': 'real', 'character_max_length': None}


        # Check data:
        check_missing(df, selected_columns, dc_user_errors, synthese_info, missing_val)
        check_types(df, added_cols, selected_columns, dc_user_errors, synthese_info, missing_val)
        check_cd_nom(df, selected_columns, dc_user_errors, missing_val, cd_nom_list)
        check_dates(df, added_cols, selected_columns, dc_user_errors, synthese_info)
        check_uuid(df, added_cols, selected_columns, dc_user_errors, synthese_info, is_generate_uuid)
        check_counts(df, selected_columns, dc_user_errors, synthese_info, def_count_val)
        check_entity_source(df, added_cols, selected_columns, dc_user_errors, synthese_info)
        check_geography(df, import_id, added_cols, selected_columns, dc_user_errors, srid, local_srid)
        check_altitudes(df, selected_columns, dc_user_errors, synthese_info, calcul=False)

        return {
            'user_errors' : user_error,
            'added_cols' : added_cols
        }

    except Exception:
        raise
