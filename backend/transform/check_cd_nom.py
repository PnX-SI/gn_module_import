import dask
import numpy as np

from geonature.utils.env import DB

from ..db.queries.user_errors import set_user_error, set_invalid_reason
from .utils import fill_col, fill_map, set_is_valid
from ..wrappers import checker
from ..logs import logger


@checker('Data cleaning : cd_nom checked')
def check_cd_nom(df, selected_columns, dc_user_errors, missing_values, cd_nom_list):

    try:

        logger.info('CHECKING CD_NOM VALIDITY in %s column', selected_columns['cd_nom'])

        # return False if invalid cd_nom, else (including missing values) return True
        df['temp'] = ''
        df['temp'] = df['temp']\
            .where(
                cond=df[selected_columns['cd_nom']].isin(cd_nom_list), 
                other=False)\
            .where(
                cond=df[selected_columns['cd_nom']].notnull(), 
                other='')\
            .map(fill_map)\
            .astype('bool')

        # set gn_is_valid and invalid_reason
        set_is_valid(df, 'temp')
        set_invalid_reason(df, 'temp', 'invalid cd_nom value in {} column', selected_columns['cd_nom'])
        n_cd_nom_error = df['temp'].astype(str).str.contains('False').sum()

        logger.info('%s invalid cd_nom detected in %s column', n_cd_nom_error, selected_columns['cd_nom'])

        # set front interface error
        if n_cd_nom_error > 0:
            set_user_error(dc_user_errors, 9, selected_columns['cd_nom'], n_cd_nom_error)

    except Exception:
        raise
