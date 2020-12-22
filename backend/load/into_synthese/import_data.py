from flask import current_app
from geonature.utils.env import DB

from ...db.queries.load_to_synthese import (
    get_data_type,
    insert_into_synthese,
    get_id_source,
)
from ...db.queries.data_preview import get_id_module
from ...db.models import TImports


def load_data_to_synthese(schema_name, table_name, total_columns, import_obj):
    try:
        total_columns["id_source"] = get_id_source(import_obj.id_import)
        total_columns["id_dataset"] = import_obj.id_dataset
        total_columns["id_module"] = get_id_module("IMPORT")
        # add key type info to value ('value::type')
        select_part = []
        for key, value in total_columns.items():
            if key.startswith("id_nomenclature"):
                value = f"_tr_{key}_{value}"
            if key == "the_geom_4326":
                key_type = "geometry(Geometry,4326)"
            elif key == "the_geom_point":
                key_type = "geometry(Point,4326)"
            elif key == "the_geom_local":
                key_type = f"geometry(Geometry,{current_app.config['LOCAL_SRID']})"
            elif key == "gn_unique_id_sinp":
                key_type = "uuid"
            elif key in ("gn_altitude_min", "gn_altitude_max"):
                key_type = "integer"
            else:
                key_type = get_data_type(key)
            select_part.append("::".join([str(value), key_type]))

        # insert into synthese
        insert_into_synthese(
            schema_name, table_name, select_part, total_columns, import_obj
        )
    except Exception:
        DB.session.rollback()
        raise
