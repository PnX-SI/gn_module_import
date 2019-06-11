def set_error(code ,message, message_data):
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
    'code' : 'digit_name',
    'message' : 'Nom de fichier non valide car seulement composé de chiffres',
    'message_data' : ''
}