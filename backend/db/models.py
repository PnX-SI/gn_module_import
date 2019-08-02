from geonature.utils.env import DB
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, PrimaryKeyConstraint
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
    step = DB.Column(DB.Integer,nullable=True)


@serializable
class CorRoleImport(DB.Model):
    __tablename__ = 'cor_role_import'
    __table_args__ = {'schema': 'gn_imports', "extend_existing":True}

    id_role = DB.Column(DB.Integer, primary_key=True)
    id_import = DB.Column(DB.Integer, primary_key=True)


# ne pas mettre gn_import_archives en dur
@serializable
class CorImportArchives(DB.Model):
    __tablename__ = 'cor_import_archives'
    __table_args__ = {'schema': 'gn_import_archives', "extend_existing":True}

    id_import = DB.Column(DB.Integer, primary_key=True)
    table_archive = DB.Column(DB.Integer, primary_key=True)


def generate_user_table_class(schema_name,table_name,pk_name,user_columns,id,schema_type):
    """
        Generate dynamically the user file class used to copy user data (csv) into db tables
        parameters :
        - schema_name, table_name, pk_name = string
        - user_columns : list of strings (strings = csv column names)
        - id : integer id_import
        - schema_type : = 'archives' or 't_imports' (because the table containing user data in t_imports schema has additionnal fields)
    """
    
    # create dict in order to create dynamically the user file class

    if schema_type not in ['archives','gn_imports']:
        # penser à gérer retour d'erreur en front
        return 'Wrong schema type',400

    user_table = {
        '__tablename__': table_name,
        '__table_args__' : (PrimaryKeyConstraint(pk_name),{'schema': schema_name, "extend_existing":False})
    }
    
    if schema_type == 'gn_imports':
        user_table.update({'gn_is_valid' : DB.Column(DB.Boolean,nullable=True)})
        user_table.update({'gn_invalid_reason' : DB.Column(DB.Text,nullable=True)})

    user_table.update({pk_name : DB.Column(DB.Integer,autoincrement=True)})
    for column in user_columns:
        user_table.update({column:DB.Column(DB.Text,nullable=True)})
    
    # creation of the user file class :
    if schema_type == 'archives':
        UserTableClass = type('UserArchivesTableClass{}'.format(id), (DB.Model,), user_table)
    else:
        UserTableClass = type('UserTimportsTableClass{}'.format(id), (DB.Model,), user_table)

    return UserTableClass