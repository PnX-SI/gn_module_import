from ..db.queries.load_to_synthese import get_synthese_info
from ..db.queries.utils import is_cd_nom_required

from .check_cd_nom import check_cd_nom
from .check_dates import check_dates
from .check_missing import format_missing, check_missing
from .check_uuid import check_uuid
from .check_types import check_types
from .check_other_fields import check_entity_source, check_id_digitizer
from .check_counts import check_counts
from .check_altitudes import check_altitudes
from .check_geography import check_geography
from .check_duplicated import check_row_duplicates


def data_cleaning(df, import_id, selected_columns, missing_val, def_count_val, cd_nom_list, srid,
                  local_srid, is_generate_uuid, schema_name, is_generate_altitude, prefix):
    try:

        #user_error = []
        added_cols = {}

        # set gn_is_valid and gn_invalid_reason:
        df['gn_is_valid'] = True
        df['gn_invalid_reason'] = ''

        # get synthese column info:
        synthese_info = get_synthese_info(selected_columns.keys())

        # set is_nullable for cd_nom
        is_cd_nom_req = is_cd_nom_required(schema_name)
        if is_cd_nom_req:
            is_nullable = 'NO'
        else:
            is_nullable = 'YES'
        synthese_info['cd_nom']['is_nullable'] = is_nullable

        if 'longitude' in selected_columns.keys() and 'latitude' in selected_columns.keys():
            synthese_info['longitude'] = {'is_nullable': 'NO', 'column_default': None, 'data_type': 'real', 'character_max_length': None}
            synthese_info['latitude'] = {'is_nullable': 'NO', 'column_default': None, 'data_type': 'real', 'character_max_length': None}

        # Check data:
        check_missing(df, selected_columns, synthese_info, missing_val, import_id, schema_name)
        check_row_duplicates(df, selected_columns, import_id, schema_name)
        check_types(df, added_cols, selected_columns, synthese_info, missing_val, schema_name, import_id)
        check_cd_nom(df, selected_columns, missing_val, cd_nom_list, schema_name, import_id)
        check_dates(df, added_cols, selected_columns, synthese_info, import_id, schema_name)
        check_uuid(df, added_cols, selected_columns, synthese_info, is_generate_uuid, import_id, schema_name)
        check_counts(df, selected_columns, synthese_info, def_count_val, added_cols, import_id, schema_name)
        check_entity_source(df, added_cols, selected_columns, synthese_info, import_id, schema_name)
        check_id_digitizer(df, selected_columns, synthese_info, import_id, schema_name)
        check_geography(df, import_id, added_cols, selected_columns, srid, local_srid, schema_name)
        check_altitudes(df, selected_columns, synthese_info, is_generate_altitude, import_id, schema_name)

        return {
            #'user_errors': user_error,
            'added_cols': added_cols
        }

    except Exception:
        raise
