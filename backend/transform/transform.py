import datetime

from ..db.query import (
    get_synthese_info
)

from .check_cd_nom import check_cd_nom
from .check_dates import check_dates
from .check_missing import format_missing, check_missing
from .check_id_sinp import check_uuid
from .check_types import check_types
from .check_other_fields import check_entity_source
from .check_counts import check_counts
from .check_altitudes import check_altitudes
from ..logs import logger

import pdb


def data_cleaning(df, selected_columns, missing_val, def_count_val, cd_nom_list):

    try:

        user_error = []
        added_cols = {}

        # set gn_is_valid and gn_invalid_reason:
        df['gn_is_valid'] = True # à mettre dans la bdd à true avant
        df['gn_invalid_reason'] = ''

        # get synthese column info:
        selected_synthese_cols = [*list(selected_columns.keys())]
        synthese_info = get_synthese_info(selected_synthese_cols)
        synthese_info['cd_nom']['is_nullable'] = 'NO' # mettre en conf?

        
        # Check data:
        error_missing = check_missing(df, selected_columns, synthese_info, missing_val)
        error_types = check_types(df, added_cols, selected_columns, synthese_info, missing_val)
        error_cd_nom = check_cd_nom(df, selected_columns, missing_val, cd_nom_list)
        error_dates = check_dates(df, added_cols, selected_columns, synthese_info)
        error_uuid = check_uuid(df, added_cols, selected_columns, synthese_info)
        error_check_counts = check_counts(df, selected_columns, synthese_info, def_count_val)
        error_altitudes = check_altitudes(df, selected_columns, synthese_info, calcul=False)
        check_entity_source(df, added_cols, selected_columns, synthese_info)

        # User error to front interface:
        if error_missing != '':
            for error in error_missing:
                user_error.append(error)
        if error_types != '':
            for error in error_types:
                user_error.append(error)
        if error_cd_nom != '':
            for error in error_cd_nom:
                user_error.append(error)
        if error_dates != '':
            for error in error_dates:
                user_error.append(error)
        if error_uuid != '':
            for error in error_uuid:
                user_error.append(error)
        if error_check_counts != '':
            for error in error_check_counts:
                user_error.append(error)
        

        #ajouter altitudes
        
        return {
            'user_errors' : user_error,
            'added_cols' : added_cols
        }

    except Exception:
        raise
