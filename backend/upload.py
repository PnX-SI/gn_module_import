import os
import pathlib
from werkzeug.utils import secure_filename

def upload(request, size_max, allowed_extensions, directory_name, module_url):

    if request.method == 'POST':
    
        if 'File' not in request.files:
            return {
                'error':'no_file'
            }
        
        file = request.files['File']

        if file.filename == '':
            return {
                'error':'empty'
            }


        # get file path
        upload_directory_path = directory_name
        module_directory_path = os.path.join(os.path.dirname(os.getcwd()), 'external_modules/{}'.format(module_url))
        uploads_directory = os.path.join(module_directory_path, upload_directory_path)

        filename = secure_filename(file.filename)

        if len(filename) > 100:
            return {
                'error':'long_name'
            }

        full_path = os.path.join(uploads_directory, filename)

        # check user file extension (changer)
        extension = pathlib.Path(full_path).suffix.lower()
        if extension not in allowed_extensions:
            return {
                'error':'bad_extension'
            }

        # check file size
        file.seek(0, 2)
        size = file.tell() / (1024 * 1024)
        file.seek(0)
        if size > size_max:
            return {
                'error':'max_size'
            }

        # save user file in upload directory
        file.save(full_path)

        if not os.path.isfile(full_path):
            return {
                'error':'unknown',
                'is_uploaded':False
            }

        return {
            'file_name':filename,
            'full_path':full_path,
            'extension':extension,
            'error':'',
            'is_uploaded':True
            }
