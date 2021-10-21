from geonature.utils.env import DB


def get_cd_hab_list():
    cd_hab_habref = DB.session.execute(
        """
        SELECT cd_hab
        FROM ref_habitats.habref
        """)
    cd_hab_list = [str(row.cd_hab) for row in cd_hab_habref]
    DB.session.close()
    return cd_hab_list
