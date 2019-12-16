import pandas as pd

from ..db.queries.user_errors import set_user_error
from .utils import fill_col, fill_map, set_is_valid, set_invalid_reason
from ..wrappers import checker
from ..logs import logger


def check_count_min_max(min_val, max_val):
    try:
        if int(min_val) > int(max_val):
            return False
        else:
            return True
    except Exception:
        return True


def check_missing_count_max(min_val, max_val):
    try:
        if pd.isnull(max_val):
            if int(min_val) > 0:
                return min_val
        else:
            return max_val
    except Exception:
        return max_val


@checker('Data cleaning : counts checked')
def check_counts(df, selected_columns, synthese_info, def_count_val, added_cols, import_id, schema_name):
    """
    - every time :
        -> count_min = def_count_val if NA
        -> if count_min < 0 or count_max < 0 (previously check during integer type checks)
    - if only count_min column is provided : count_max column = count_min column
    - if only count_max column is provided : count_min column = default count_min value (defined in parameters)
    - if both count_min and max columns are provided :
        -> if count_max = NA : count_max = count_min if count_min > 0
    - if count_min not provided, or both count_min and max provided :
        -> checks if count_min <= count_max
    """

    try:

        logger.info('CHECKING COUNTS: ')

        # define combination of counts provided:
        if 'count_min' in list(selected_columns.keys()) \
                and 'count_max' not in list(selected_columns.keys()):
            status = 'only count_min'
            logger.info('- only count_min (= %s user column) provided', selected_columns['count_min'])

        elif 'count_min' not in list(selected_columns.keys()) \
                and 'count_max' in list(selected_columns.keys()):
            status = 'only count_max'
            logger.info('- only count_max (= %s user column) provided', selected_columns['count_max'])

        elif 'count_min' in list(selected_columns.keys()) \
                and 'count_max' in list(selected_columns.keys()):
            status = 'min and max'
            logger.info('- count_min (= %s user column) and count_max (= %s user column) provided',
                        selected_columns['count_min'], selected_columns['count_max'])

        else:
            status = 'no count'
            logger.info('- no count column provided')

        if status != 'no count':

            # set missing in count_min
            if 'count_min' in list(selected_columns.keys()):
                if df[selected_columns['count_min']].isnull().any():
                    logger.info('- set count_min default value (=%s) for missing rows in count_min column',
                                def_count_val)
                    df[selected_columns['count_min']] = df[selected_columns['count_min']] \
                        .replace(pd.np.nan, str(def_count_val))

            # if only count_max is indicated, then set count_min to default count_min values
            if status == 'only count_max':
                logger.info('- count_min not provided: creating count_min column filled with count_min default values')
                df['count_min'] = str(def_count_val)
                added_cols['count_min'] = 'count_min'
                synthese_info.update({'count_min': synthese_info['count_max']})  # utile?

            # if only count_min is indicated, then set count_max equal to count_min
            if status == 'only count_min':
                logger.info('- count_max not provided: setting count_max equal to count_min')
                added_cols['count_max'] = selected_columns['count_min']
                df['count_max'] = df[selected_columns['count_min']]  # utile?
                synthese_info.update({'count_max': synthese_info['count_min']})  # utile?

            if status == 'only count_max' or status == 'min and max':

                if status == 'only count_max':
                    count_min_col = added_cols['count_min']
                else:
                    count_min_col = selected_columns['count_min']

                # Checking and filling missing count_max values
                if df[selected_columns['count_max']].isnull().any():
                    logger.info('- setting count_max equal to count_min for count_max missing values')
                    df['temp'] = ''
                    df['temp'] = df \
                        .apply(
                            lambda x: x[count_min_col] \
                                if pd.isnull(x[selected_columns['count_max']]) \
                                    else x[selected_columns['count_max']], axis=1)
                    df[selected_columns['count_max']] = df['temp']

                # check if count_max >= count_min
                logger.info('- checking if count_max >= count_min:')

                df['temp'] = ''
                df['temp'] = pd.to_numeric(df[selected_columns['count_max']], errors='coerce') - pd.to_numeric(
                    df[count_min_col], errors='coerce') < 0
                df['temp'] = -df['temp'] \
                    .map(fill_map) \
                    .astype('bool')

                set_is_valid(df, 'temp')
                n_count_min_sup = df['temp'].astype(str).str.contains('False').sum()

                logger.info('%s count_max < count_min errors detected', n_count_min_sup)

                if n_count_min_sup > 0:
                    set_user_error(import_id, 8, count_min_col, n_count_min_sup)
                    set_invalid_reason(df, schema_name, 'temp', import_id, 8, count_min_col)

    except Exception:
        raise
