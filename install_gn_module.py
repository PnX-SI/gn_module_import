import os
import subprocess
import psycopg2
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).absolute().parent



def gnmodule_install_app(gn_db, gn_app):
    '''
        Fonction principale permettant de réaliser les opérations d'installation du module :
            - Base de données
            - Installer librairie Python goodtables
    '''
    
    with gn_app.app_context():
        try:
            subprocess.call(['./install_db.sh'], cwd=str(ROOT_DIR))
        except Exception as e:
            print(e)

        try:
            here = Path(__file__).parent
            requirements_path = here / 'backend' / 'requirements.txt'
            assert requirements_path.is_file()
            subprocess.call([sys.executable, '-m', 'pip', 'install', '-r', '{}'.format(requirements_path)],cwd=str(ROOT_DIR))
        except Exception as e:
            print(e)