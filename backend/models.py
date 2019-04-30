from geonature.utils.env import DB
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func
from geonature.utils.utilssqlalchemy import (
    serializable, geoserializable
)
from geoalchemy2 import Geometry
import datetime
from geonature.core.gn_meta.models import TDatasets


@serializable
class TImports(DB.Model):
    __tablename__ = 't_imports'
    __table_args__ = {'schema': 'gn_imports', "extend_existing":True}

    id_import = DB.Column(DB.Integer, primary_key=True,autoincrement=True)
    format_source_file = DB.Column(DB.Unicode,nullable=True)
    srid = DB.Column(DB.Integer,nullable=True)
    import_table = DB.Column(DB.Unicode,nullable=True)
    id_dataset = DB.Column(DB.Integer,nullable=True)
    id_mapping = DB.Column(DB.Integer,nullable=True)
    date_create_import = DB.Column(DB.DateTime,nullable=True)
    date_update_import = DB.Column(DB.DateTime,nullable=True)
    date_end_import = DB.Column(DB.DateTime,nullable=True)
    source_count = DB.Column(DB.Integer,nullable=True)
    import_count = DB.Column(DB.Integer,nullable=True)
    taxa_count = DB.Column(DB.Integer,nullable=True)
    date_min_data = DB.Column(DB.DateTime,nullable=True)
    date_max_data = DB.Column(DB.DateTime,nullable=True)


@serializable
class CorRoleImport(DB.Model):
    __tablename__ = 'cor_role_import'
    __table_args__ = {'schema': 'gn_imports', "extend_existing":True}

    id_role = DB.Column(DB.Integer, primary_key=True)
    id_import = DB.Column(DB.Integer, primary_key=True)
