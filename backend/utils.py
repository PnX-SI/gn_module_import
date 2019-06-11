import unicodedata
import re
from .user_file_errors import digit_name
import pdb

def clean_string(my_string):
    # white spaces : removed if located on the extremities of the string, otherwise replaced by '_'
    my_string = my_string.strip().replace(' ', '_')
    # remove special characters (not '_')
    my_string = re.sub('[^A-Za-z0-9_]+', '', my_string)
    # decode in utf-8
    my_string = unicodedata.normalize('NFKD', my_string).encode('ASCII', 'ignore').decode('utf-8')
    # remove uppercases
    my_string = my_string.lower()
    return my_string


def clean_file_name(my_string, extension, id_import):
    errors = []
    # remove extension
    my_string = my_string.replace(extension, '')
    # remove first characters if digit
    while my_string[0].isdigit():
        if len(my_string) == 1:
            errors.append(digit_name)
            return {
                'errors':errors
                }
        my_string = re.sub(r'^(\D*)\d', r'\1', my_string)
    # clean name
    my_string = clean_string(my_string)
    # add id_import suffix
    my_string = "_".join([my_string, str(id_import)])
    return {
        'clean_name':my_string,
        'errors':errors
    }


def get_full_table_name(schema_name, table_name):
    return '.'.join([schema_name, table_name])


def set_imports_table_name(table_name):
    return ''.join(['i_', table_name])
