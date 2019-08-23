from geonature.utils.env import DB
from ..db.query import get_synthese_info
import pdb
import dask


def cleaning_cd_nom(df, selected_columns):

    # améliorer performances du comptage d'erreurs
    
    # get cd_nom list
    cd_nom_taxref = DB.session.execute("SELECT cd_nom FROM taxonomie.taxref")
    cd_nom_list = [row.cd_nom for row in cd_nom_taxref]
    cd_nom_list = [str(i) for i in cd_nom_list]

    # if invalid cd_nom (including missing value), return False else return True
    df['gn_is_valid'] = df[selected_columns['cd_nom']].isin(cd_nom_list)

    # fill invalid reason
    df['gn_invalid_reason'] = df['gn_invalid_reason'].where(cond=df[selected_columns['cd_nom']].isin(cd_nom_list), other='invalid cd_nom; ')

    n_cd_nom_error = df['gn_is_valid'].astype(str).str.contains('False').sum()

    if n_cd_nom_error > 0:
        user_error = {
            'code': 'cd_nom error',
            'message': 'cd_nom invalides',
            'message_data': 'nombre de lignes avec erreurs : {}'.format(n_cd_nom_error)
        }
    else:
        user_error = ''

    return user_error