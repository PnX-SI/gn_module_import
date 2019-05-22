import os
import subprocess
import psycopg2
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).absolute().parent



def gnmodule_install_app(gn_db, gn_app):
    '''
        Fonction principale permettant de réaliser les opérations d'installation du module :
            - Base de données
            - Module (pour le moment rien)
    '''
    with gn_app.app_context() :
	# install python package 'goodtables'
	subprocess.call([sys.executable, '-m', 'pip', 'install', '{0}=={1}'.format('goodtables', '2.1.4')], shell=True)

def execute_script(file_name):
    """
        Execute a script to set or delete sample data before test
    """
    conn = psycopg2.connect(config['SQLALCHEMY_DATABASE_URI'])
    cur = conn.cursor()
    sql_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name)
    cur.execute(open(sql_file, 'r').read())
    conn.commit()
    cur.close()
    conn.close()
