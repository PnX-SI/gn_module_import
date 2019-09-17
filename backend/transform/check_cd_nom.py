import dask
import numpy as np

from geonature.utils.env import DB

from ..db.query import get_synthese_info
from .utils import fill_col, fill_map, set_is_valid, set_invalid_reason, set_user_error
from ..wrappers import checker
from ..logs import logger

import pdb


@checker('Data cleaning : cd_nom checked')
def check_cd_nom(df, selected_columns, missing_values, df_type):

    try:

        # note : améliorer performances du comptage d'erreurs
        logger.info('checking cd_nom validity for %s column', selected_columns['cd_nom'])

        user_error = []

        # get cd_nom list
        cd_nom_taxref = DB.session.execute(\
            "SELECT cd_nom \
             FROM taxonomie.taxref")
        cd_nom_list = [str(row.cd_nom) for row in cd_nom_taxref]

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

        if df_type == 'dask':
                    n_cd_nom_error = n_cd_nom_error.compute()

        if n_cd_nom_error > 0:
            user_error.append(
                set_user_error(
                    'invalid cd_nom',
                    selected_columns['cd_nom'],
                    n_cd_nom_error
                )
            )

        if len(user_error) == 0:
            user_error = ''

        return user_error

    except Exception:
        raise