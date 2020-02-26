import os
import pathlib
from werkzeug.utils import secure_filename

from ..logs import logger
from ..wrappers import checker


@checker('Upload : file saved in directory')
def upload(request, size_max, allowed_extensions, directory_name, module_url):
    """ check and upload user data:
        - check if user request is not empty
        - check if user file name is lower than 100 characters
        - check user file extension
        - check if user file size is allowed
        - if no error, uploads user file in the upload directory

    Args:
        - request (werkzeug.local.LocalProxy): http request posted by the user
        - size_max (int): max size allowed for the user file
        - allowed_extension (list(str)): list of file extensions allowed
        - directory_name (str): name of the upload directory
        - module_url (str)

    Returns:
        - (dict) : !!!!!
    """

    try:

        if request.method == 'POST':

            if 'File' not in request.files:
                logger.error('Saving user file : No File in request files')
                return {
                    'error': 'no_file'
                }

            file = request.files['File']

            if file.filename == '':
                logger.error('Saving user file : No File in request files')
                return {
                    'error': 'empty'
                }

            # get file path
            upload_directory_path = directory_name
            module_directory_path = os.path.join(os.path.dirname(os.getcwd()), 'external_modules{}'.format(module_url))
            uploads_directory = os.path.join(module_directory_path, upload_directory_path)

            filename = secure_filename(file.filename)

            if len(filename) > 100:
                logger.error('Saving user file : file name too long')
                return {
                    'error': 'long_name'
                }

            full_path = os.path.join(uploads_directory, filename)

            # check user file extension (changer)
            extension = pathlib.Path(full_path).suffix.lower()
            if extension not in allowed_extensions:
                logger.error('Saving user file : extension not allowed')
                return {
                    'error': 'bad_extension'
                }

            # check file size
            file.seek(0, 2)
            size = file.tell() / (1024 * 1024)
            logger.info('File size = %s Mo', size)
            file.seek(0)
            if size > size_max:
                logger.error('Saving user file : user file size > max size allowed')
                return {
                    'error': 'max_size'
                }

            # save user file in upload directory
            file.save(full_path)

            if not os.path.isfile(full_path):
                logger.error('Saving user file : invalid path')
                return {
                    'error': 'unknown',
                    'is_uploaded': False
                }

            logger.debug('original file name = %s', filename)

            return {
                'file_name': filename,
                'full_path': full_path,
                'extension': extension,
                'error': '',
                'is_uploaded': True
            }
    except Exception:
        raise
