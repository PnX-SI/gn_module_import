from geonature.utils.env import DB


def get_cd_nom_list():
    cd_nom_taxref = DB.session.execute(
        """
        SELECT cd_nom
        FROM taxonomie.taxref
        """)
    cd_nom_list = [str(row.cd_nom) for row in cd_nom_taxref]
    DB.session.close()
    return cd_nom_list
