import unicodedata
import re


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
    # remove extension
    my_string = my_string.replace(extension, '')
    # clean name
    my_string = clean_string(my_string)
    # add id_import suffix
    my_string = "_".join([my_string, str(id_import)])
    return my_string