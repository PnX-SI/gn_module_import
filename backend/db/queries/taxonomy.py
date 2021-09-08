from geonature.utils.env import DB

from ...utils.utils import get_config


ID_LIST_TAXA_RESTRICTION = 'ID_LIST_TAXA_RESTRICTION'


def get_cd_nom_list():

    taxa_list_id = get_config().get(ID_LIST_TAXA_RESTRICTION, -1)

    if taxa_list_id == -1:
        # No list provided, all taxa in taxref are taken
        query = """
                SELECT cd_nom
                FROM taxonomie.taxref
                """
    else:
        # List provided, select the cd_noms in the list
        query = """
                SELECT cd_nom
                FROM taxonomie.bib_listes bl
                RIGHT OUTER JOIN taxonomie.cor_nom_liste cnl on bl.id_liste=cnl.id_liste 
                RIGHT OUTER JOIN taxonomie.bib_noms tb on cnl.id_nom =tb.id_nom 
                WHERE bl.id_liste = {id_liste};
                """.format(id_liste=taxa_list_id)
    
    cd_nom_taxref = DB.session.execute(query)
    cd_nom_list = [str(row.cd_nom) for row in cd_nom_taxref]
    DB.session.close()
    return cd_nom_list
