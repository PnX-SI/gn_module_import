"""
Vérificateur de reférentiel. Utilisé pour vérifier la confirmité avec Habref et Taxref
"""


from geonature.utils.env import DB

from .utils import fill_col, fill_map, set_is_valid, set_error_and_invalid_reason
from ..wrappers import checker
from ..logs import logger


@checker("Data cleaning : referentiel (habref/taxref) checked")
def check_referential(
    df, selected_columns, ref_list, import_id, ref_col
):
    """
    Véfifie que les valeurs de la colonne (ref_col) sont dans la liste de valeurs fournies (ref_list)
    Utilisé pour vérifier que les cd_nom et cd_hab sont dans les référentiels Taxref et Habred

    Parameters
    ----------
    ref_list: list<str>
        Liste des valeurs auxquelles on compare la colonne(cd_nom, cd_hab)
    
    ref_col: str
        Nom de la colonne a vérifié (cd_nom, cd_hab)


    Returns
    -------
    Void
    """

    try:

        logger.info(f"CHECKING CD_NOM/CD_HAB VALIDITY in {selected_columns[ref_col]} column")

        # return False if invalid cd_nom/cdhab, else return True (including missing values)
        df["temp"] = True
        df["temp"] = (
            df["temp"]
            .where(cond=df[selected_columns[ref_col]].isin(ref_list), other=False)
            .where(cond=df[selected_columns[ref_col]].notnull(), other=True)
        )

        # set gn_is_valid and invalid_reason
        set_is_valid(df, "temp")
        # get invalid id rows
        id_rows_invalid = df.index[df["temp"] == False].to_list()

        logger.info(
            "%s invalid cd_nom detected in %s column",
            len(id_rows_invalid),
            selected_columns[ref_col],
        )
        error_code = "CD_NOM_NOT_FOUND" if ref_col == "cd_nom" else "CD_HAB_NOT_FOUND"
        # set front interface error
        if len(id_rows_invalid) > 0:
            set_error_and_invalid_reason(
                df=df,
                id_import=import_id,
                error_code=error_code,
                col_name_error=selected_columns[ref_col],
                df_col_name_valid="temp",
                id_rows_error=id_rows_invalid,
            )
    except Exception:
        raise
