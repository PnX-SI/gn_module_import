import subprocess
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).absolute().parent


def gnmodule_install_app(gn_db, gn_app):
    '''
        Fonction principale permettant de réaliser les opérations d'installation du module :
            - Base de données
            - Installer librairies Python
    '''

    with gn_app.app_context():
        # install db schema and tables
        gn_db.session.execute(
            open(str(ROOT_DIR / "data/gn_imports.sql"), "r").read()
        )
        gn_db.session.execute(
            open(str(ROOT_DIR / "data/gn_import_archives.sql"), "r").read()
        )
        gn_db.session.commit()
