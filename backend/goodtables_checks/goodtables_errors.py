def set_error(code, message, message_data):
    """ set a user data error as a dict:

    Args: 
        code (str): short message error
        message (str): error
        message_data (str) : error localisation in user data
    Returns:
        - (dict): {
            - 'code' (str)
            - 'message' (str)
            - 'message_data' (str)
        }

    """
    return {
        'code': code,
        'message': message,
        'message_data': message_data
    }


no_data = \
    {
        'code': 'empty file',
        'message': 'no data',
        'message_data': ''
    }

digit_name = \
    {
        'code': 'digit_name',
        'message': 'Nom de fichier non valide car seulement composé de chiffres',
        'message_data': ''
    }
