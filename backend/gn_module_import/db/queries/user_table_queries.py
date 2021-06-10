import json

from flask import current_app
from sqlalchemy.sql import text

from psycopg2.extensions import AsIs, QuotedString

from geonature.utils.env import DB
from geonature.utils.env import DB as db

from ..models import TImports
from ...logs import logger
from ...wrappers import checker


def get_n_taxa(schema_name, table_name, cd_nom_col):
    try:
        n_taxa = DB.session.execute(
            """
            SELECT COUNT(DISTINCT TAXREF.cd_ref)
            FROM {schema_name}.{table_name} SOURCE
            JOIN taxonomie.taxref TAXREF ON TAXREF.cd_nom = SOURCE.{cd_nom_col}::integer
            WHERE SOURCE.gn_is_valid = 'True';
            """.format(
                schema_name=schema_name, table_name=table_name, cd_nom_col=cd_nom_col
            )
        ).fetchone()[0]
        return n_taxa
    except Exception:
        raise


def get_date_ext(schema_name, table_name, date_min_col, date_max_col):
    try:
        dates = DB.session.execute(
            """
            SELECT min({date_min_col}), max({date_max_col})
            FROM {schema_name}.{table_name}
            WHERE gn_is_valid = 'True';
            """.format(
                schema_name=schema_name,
                table_name=table_name,
                date_min_col=date_min_col,
                date_max_col=date_max_col,
            )
        ).fetchall()[0]

        return {"date_min": dates[0], "date_max": dates[1]}
    except Exception:
        raise
