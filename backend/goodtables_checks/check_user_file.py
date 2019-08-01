from goodtables import validate

from .goodtables_errors import*

import pdb

"""
Vérifications :
- doublon de nom colonne
- aucun nom de colonne
- un nom de colonne manquant
- fichier vide (ni nom de colonne, ni ligne)
- pas de données (noms de colonne mais pas de ligne contenant les données)
- doublon ligne
- extra-value : une ligne a une valeur en trop
- less-value : une ligne a moins de colonnes que de noms de colonnes

Notes encodages :
- encodage : utf8 et 16 : pas d'erreur
- encodage : Europe occidentale ISO-8859-15/EURO (=latin-9) et ISO-8859-1 (=latin-1) : erreur ('source-error')
- Donc il faut convertir en utf-8 avant de passer dans goodtables
"""

def check_user_file(full_path, row_limit=100000000):

    errors = []

    report = validate(full_path, row_limit=row_limit)

    if report['valid'] is False:

        for error in report['tables'][0]['errors']:
            if 'No such file or directory' in error['message']:
                # avoid printing original goodtable message error containing the user full_path (security purpose)
                user_error = set_error(error['code'], 'No such file or directory', '')
                errors.append(user_error)
            else:
                #print(error['message'])
                # other goodtable errors :
                user_error = set_error(error['code'], '', error['message'])
                errors.append(user_error)

    # if no rows :
    if report['tables'][0]['row-count'] == 0:
        errors.append(no_data)

    # get column names:
    column_names = report['tables'][0]['headers']
    # get file format:
    file_format = report['tables'][0]['format']
    # get row number:
    row_count = report['tables'][0]['row-count']

    return {
        'column_names': column_names,
        'file_format': file_format,
        'row_count': row_count,
        'errors': errors
    }
