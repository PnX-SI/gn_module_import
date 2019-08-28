import dask
import numpy as np

from geonature.utils.env import DB

from ..db.query import get_synthese_info
from .transform import fill_col

import pdb


def cleaning_cd_nom(df, selected_columns, missing_values):

    # note : améliorer performances du comptage d'erreurs
    
    # get cd_nom list
    cd_nom_taxref = DB.session.execute("SELECT cd_nom FROM taxonomie.taxref")
    cd_nom_list = [row.cd_nom for row in cd_nom_taxref]
    cd_nom_list = [str(i) for i in cd_nom_list]

    # return False if invalid cd_nom, else (including missing values) return True
    df['temp'] = ''
    df['temp'] = df['temp']\
        .where(cond=df[selected_columns['cd_nom']].isin(cd_nom_list), other=False)\
        .where(cond=df[selected_columns['cd_nom']].notnull(), other='')

    # set gn_is_valid and invalid_reason
    df['gn_is_valid'] = df['gn_is_valid'].where(cond=df['temp'].apply(lambda x: fill_col(x)), other=False)
    df['gn_invalid_reason'] = df['gn_invalid_reason'].where(
        cond=df['temp'].apply(lambda x: fill_col(x)), 
        other=df['gn_invalid_reason'] + 'invalid cd_nom value in {} column -- '.format(selected_columns['cd_nom']))

    n_cd_nom_error = df['temp'].astype(str).str.contains('False').sum().compute()

    if n_cd_nom_error > 0:
        user_error = {
            'code': 'cd_nom error',
            'message': 'Des cd_nom sont invalides',
            'message_data': 'nombre de lignes avec erreurs : {}'.format(n_cd_nom_error)
        }
    else:
        user_error = ''

    return user_error