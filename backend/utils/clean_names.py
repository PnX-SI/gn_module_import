import unicodedata
import re

from ..goodtables_checks.goodtables_errors import digit_name


def clean_string(my_string):
    """ Clean strings (file or column names) from user data:
            - white spaces: removed if located on the extremities of the string, otherwise replaced by '_'
            - remove special characters (not '_')
            - decode in utf-8
            - remove uppercases

    Args:
        my_string (str): string to clean

    Returns:
        my_string (str): cleaned string

    """
    my_string = my_string.strip().replace(' ', '_')
    my_string = re.sub('[^A-Za-z0-9_]+', '', my_string)
    my_string = unicodedata.normalize('NFKD', my_string).encode('ASCII', 'ignore').decode('utf-8')
    my_string = my_string.lower()
    return my_string


def clean_file_name(my_string, extension, id_import):
    """ Clean user file name:
            - remove extension
            - remove first characters if digit (return an error if the full name is made of digits)
            - clean name with clean_string function
            - add id_import suffix

    Args:
        - my_string (str): file name to clean
        - extension (str): file name extension
        - id_import (int): import id to add as suffix

    Returns:
        - dicts {
            'clean_name' (str): cleaned file name
            'errors': if error(s), returns error(s) defined in user_file_errors.py
    """
    errors = []
    my_string = my_string.replace(extension, '')
    while my_string[0].isdigit():
        if len(my_string) == 1:
            errors.append(digit_name)
            return {
               'errors': errors
            }
        my_string = re.sub(r'^(\D*)\d', r'\1', my_string)
    my_string = clean_string(my_string)
    my_string = "_".join([my_string, str(id_import)])
    return {
        'clean_name': my_string,
        'errors': errors
    }
