from datetime import datetime
from collections.abc import Mapping

import chardet
from flask import current_app, g
import sqlalchemy as sa
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, PrimaryKeyConstraint
from sqlalchemy.orm import relationship, deferred
from sqlalchemy.types import ARRAY
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import and_, or_, all_, any_, true, false
from werkzeug.exceptions import Forbidden
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.ext.mutable import MutableDict
from flask_sqlalchemy import BaseQuery

from utils_flask_sqla.serializers import serializable

from geonature.utils.env import DB
from geonature.utils.env import DB as db
from geonature.core.gn_meta.models import TDatasets
from geonature.core.gn_synthese.models import TSources
from geonature.core.gn_permissions.tools import get_scopes_by_action

from pypnnomenclature.models import TNomenclatures, BibNomenclaturesTypes
from pypnusershub.db.models import User



IMPORT_SCHEMA = 'gn_imports'
ARCHIVE_SCHEMA = 'gn_import_archives'


"""
Erreurs
=======

ImportErrorType = un type d’erreur, avec sa description
ImportErrorType.category = la catégorie auquelle est rattaché ce type d’erreur
  ex: le type d’erreur « date invalide » est rattaché à la catégorie « erreur de format »
  note: c’est un champs texte libre, il n’y a pas de modèle ImportErrorCategory
ImportError = occurance d’un genre d’erreur, associé à une ou plusieurs ligne d’un import précis
"""

@serializable
class ImportUserErrorType(db.Model):
    __tablename__ = "t_user_errors"
    __table_args__ = {"schema": IMPORT_SCHEMA,}

    pk = db.Column('id_error', db.Integer, primary_key=True)
    category = db.Column('error_type', db.Unicode, nullable=False)
    name = db.Column(db.Unicode, nullable=False, unique=True)
    description = db.Column(db.Unicode)
    level = db.Column('error_level', db.Unicode)

    def __str__(self):
        return f'<ImportErrorType {self.name}>'

    
@serializable
class ImportUserError(db.Model):
    __tablename__ = "t_user_error_list"
    __table_args__ = {"schema": IMPORT_SCHEMA,}

    pk = db.Column('id_user_error', db.Integer, primary_key=True)
    id_import = db.Column(db.Integer, db.ForeignKey(f"{IMPORT_SCHEMA}.t_imports.id_import"))
    imprt = DB.relationship("TImports", back_populates='errors')
    id_type = db.Column("id_error", db.Integer, db.ForeignKey(ImportUserErrorType.pk))
    type = db.relationship("ImportUserErrorType")
    column = db.Column('column_error', db.Unicode)
    rows = db.Column('id_rows', db.ARRAY(db.Integer))
    comment = db.Column(db.UnicodeText)

    def __str__(self):
        return f'<ImportError import={self.id_import},type={self.type.name},rows={self.rows}>'


class InstancePermissionMixin:
    def get_instance_permissions(self, scopes, user=None):
        if user is None:
            user = g.current_user
        if isinstance(scopes, Mapping):
            return { key: self.has_instance_permission(scope, user=user)
                     for key, scope in scopes.items() }
        else:
            return [ self.has_instance_permission(scope, user=user)
                     for scope in scopes ]

    def check_instance_permission(self, scope, user=None):
        if not self.has_instance_permission(scope, user=user):
            raise Forbidden()


cor_role_import = db.Table('cor_role_import',
    db.Column('id_role', db.Integer, db.ForeignKey(User.id_role), primary_key=True),
    db.Column('id_import', db.Integer, db.ForeignKey(f'{IMPORT_SCHEMA}.t_imports.id_import'), primary_key=True),
    schema=IMPORT_SCHEMA,
)


class ImportQuery(BaseQuery):
    def filter_by_scope(self, scope, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:
            return self.filter(sa.false())
        elif scope in (1, 2):
            filters = [ TImports.authors.any(id_role=user.id_role) ]
            if scope == 2 and user.id_organisme is not None:
                filters.append(TImports.authors.any(id_organisme=user.id_organisme))
            return self.filter(or_(*filters)).distinct()
        elif scope == 3:
            return self
        else:
            raise Exception(f"Unexpected scope {scope}")


@serializable(fields=['authors.nom_complet'], exclude=['source_file'])
class TImports(InstancePermissionMixin, DB.Model):
    __tablename__ = "t_imports"
    __table_args__ = {"schema": IMPORT_SCHEMA} #, "extend_existing": True}
    query_class = ImportQuery

    # https://docs.python.org/3/library/codecs.html
    # https://chardet.readthedocs.io/en/latest/supported-encodings.html
    # TODO: move in configuration file
    AVAILABLE_ENCODINGS = {
        'utf-8',
        'iso-8859-1',
        'iso-8859-15',
    }
    AVAILABLE_FORMATS = ['csv', 'geojson']

    id_import = DB.Column(DB.Integer, primary_key=True, autoincrement=True)
    format_source_file = DB.Column(DB.Unicode, nullable=True)
    srid = DB.Column(DB.Integer, nullable=True)
    separator = DB.Column(DB.Unicode, nullable=True)
    encoding = DB.Column(DB.Unicode, nullable=True)
    detected_encoding = DB.Column(DB.Unicode, nullable=True)
    import_table = DB.Column(DB.Unicode, nullable=True)
    full_file_name = DB.Column(DB.Unicode, nullable=True)
    id_dataset = DB.Column(DB.Integer, ForeignKey("gn_meta.t_datasets.id_dataset"), nullable=True)
    id_field_mapping = DB.Column(DB.Integer, ForeignKey('gn_imports.t_mappings.id_mapping'), nullable=True)
    field_mapping = DB.relationship('TMappings', foreign_keys='TImports.id_field_mapping')
    id_content_mapping = DB.Column(DB.Integer, ForeignKey('gn_imports.t_mappings.id_mapping'), nullable=True)
    content_mapping = DB.relationship('TMappings', foreign_keys='TImports.id_content_mapping')
    date_create_import = DB.Column(DB.DateTime, default=datetime.now)
    date_update_import = DB.Column(DB.DateTime, default=datetime.now, onupdate=datetime.now)
    date_end_import = DB.Column(DB.DateTime, nullable=True)
    source_count = DB.Column(DB.Integer, nullable=True)
    import_count = DB.Column(DB.Integer, nullable=True)
    taxa_count = DB.Column(DB.Integer, nullable=True)
    date_min_data = DB.Column(DB.DateTime, nullable=True)
    date_max_data = DB.Column(DB.DateTime, nullable=True)
    uuid_autogenerated = DB.Column(DB.Boolean)
    altitude_autogenerated = DB.Column(DB.Boolean)
    authors = DB.relationship(
        User,
        lazy="joined",
        secondary=cor_role_import,
    )
    is_finished = DB.Column(DB.Boolean, nullable=False, default=False)
    processing = DB.Column(DB.Boolean, nullable=False, default=False)
    in_error = DB.Column(DB.Boolean)
    errors = DB.relationship("ImportUserError", back_populates='imprt',
                             order_by='ImportUserError.id_type',
                             cascade="all, delete-orphan")  # TODO order by type.category
    dataset = DB.relationship("TDatasets", lazy="joined")
    source_file = deferred(db.Column(DB.LargeBinary))
    # the columns field is a mapping with cleaned columns names (also db fields)
    # as keys and original columns name (in uploaded csv) as values
    columns = DB.Column(MutableDict.as_mutable(HSTORE))

    @hybrid_property
    def cruved(self):
        if not hasattr(g, 'scopes_by_action'):
            g.scopes_by_action = get_scopes_by_action(module_code="IMPORT", object_code="IMPORT")
        return { action: self.has_instance_permission(scope)
                 for action, scope in g.scopes_by_action.items() }

    def has_instance_permission(self, scope, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:  # pragma: no cover (should not happen as already checked by the decorator)
            return False
        elif scope == 1:  # self
            return user.id_role in [ author.id_role for author in self.authors ]
        elif scope == 2:  # organism
            return user.id_role in [ author.id_role for author in self.authors ] or \
                (user.id_organisme is not None and user.id_organisme in [ author.id_organisme for author in self.authors ])
        elif scope == 3:  # all
            return True

    def as_dict(self, import_as_dict):
        if import_as_dict["date_end_import"] is None:
            import_as_dict["date_end_import"] = "En cours"
        import_as_dict["authors_name"] = "; ".join([ author.nom_complet for author in self.authors])
        if self.detected_encoding:
            import_as_dict['available_encodings'] = sorted(TImports.AVAILABLE_ENCODINGS | {self.detected_encoding,})
        else:
            import_as_dict['available_encodings'] = sorted(TImports.AVAILABLE_ENCODINGS)
        import_as_dict["available_formats"] = TImports.AVAILABLE_FORMATS
        if self.full_file_name and '.' in self.full_file_name:
            extension = self.full_file_name.rsplit('.', 1)[-1]
            if extension in TImports.AVAILABLE_FORMATS:
                import_as_dict["detected_format"] = extension
        return import_as_dict


cor_role_mapping = db.Table('cor_role_mapping',
    db.Column('id_role', db.Integer, db.ForeignKey(User.id_role), primary_key=True),
    db.Column('id_mapping', db.Integer, db.ForeignKey(f'{IMPORT_SCHEMA}.t_mappings.id_mapping'), primary_key=True),
    schema=IMPORT_SCHEMA,
)


class MappingQuery(BaseQuery):
    def filter_by_scope(self, scope, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:
            return self.filter(sa.false())
        elif scope in (1, 2):
            filters = [
                TMappings.is_public == True,
                TMappings.owners.any(id_role=user.id_role),
            ]
            if scope == 2 and user.id_organisme is not None:
                filters.append(TMappings.owners.any(id_organisme=user.id_organisme))
            return self.filter(or_(*filters)).distinct()
        elif scope == 3:
            return self
        else:
            raise Exception(f"Unexpected scope {scope}")


@serializable
class TMappings(InstancePermissionMixin, DB.Model):
    __tablename__ = "t_mappings"
    __table_args__ = {"schema": IMPORT_SCHEMA}
    query_class = MappingQuery

    id_mapping = DB.Column(DB.Integer, primary_key=True, autoincrement=True)
    mapping_label = DB.Column(DB.Unicode, nullable=False)
    mapping_type = DB.Column(DB.Unicode, nullable=False)
    active = DB.Column(DB.Boolean, nullable=False, default=True)
    is_public = DB.Column(DB.Boolean, nullable=False, default=False)

    owners = DB.relationship(
        User,
        lazy="joined",
        secondary=cor_role_mapping,
    )
    
    def has_instance_permission(self, scope: int, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:  # pragma: no cover (should not happen as already checked by the decorator)
            return False
        elif self.is_public:
            return True
        else:
            if scope == 1:  # self
                return user.id_role in [ owner.id_role for owner in self.owners ]
            elif scope == 2:  # organism
                return user.id_role in [ owner.id_role for owner in self.owners ] or \
                    (user.id_organisme is not None and user.id_organisme in [ owner.id_organisme for owner in self.owners ])
            elif scope == 3:  # all
                return True  # no check to perform

    def as_dict_with_cruved(self):
        data = self.as_dict()
        scopes = get_scopes_by_action(module_code="IMPORT", object_code="MAPPING")
        data['cruved'] = self.get_instance_permissions(scopes)
        return data


@serializable
class BibThemes(DB.Model):
    __tablename__ = "dict_themes"
    __table_args__ = {"schema": IMPORT_SCHEMA}#, "extend_existing": True}

    id_theme = DB.Column(DB.Integer, primary_key=True)
    name_theme = DB.Column(DB.Unicode, nullable=False)
    fr_label_theme = DB.Column(DB.Unicode, nullable=False)
    eng_label_theme = DB.Column(DB.Unicode, nullable=True)
    desc_theme = DB.Column(DB.Unicode, nullable=True)
    order_theme = DB.Column(DB.Integer, nullable=False)


@serializable
class BibFields(DB.Model):
    __tablename__ = "dict_fields"
    __table_args__ = {"schema": IMPORT_SCHEMA}#, "extend_existing": True}

    id_field = DB.Column(DB.Integer, primary_key=True)
    name_field = DB.Column(DB.Unicode, nullable=False, unique=True)
    fr_label = DB.Column(DB.Unicode, nullable=False)
    eng_label = DB.Column(DB.Unicode, nullable=True)
    desc_field = DB.Column(DB.Unicode, nullable=True)
    type_field = DB.Column(DB.Unicode, nullable=True)
    synthese_field = DB.Column(DB.Boolean, nullable=False)
    mandatory = DB.Column(DB.Boolean, nullable=False)
    autogenerated = DB.Column(DB.Boolean, nullable=False)
    nomenclature = DB.Column(DB.Boolean, nullable=False) # ???
    mnemonique = db.Column(db.Unicode, db.ForeignKey(BibNomenclaturesTypes.mnemonique))
    nomenclature_type = relationship("BibNomenclaturesTypes")
    id_theme = DB.Column(DB.Integer, db.ForeignKey(BibThemes.id_theme), nullable=False)
    theme = relationship("BibThemes")
    order_field = DB.Column(DB.Integer, nullable=False)
    display = DB.Column(DB.Boolean, nullable=False)
    comment = DB.Column(DB.Unicode)


@serializable
class TMappingsFields(DB.Model):
    __tablename__ = "t_mappings_fields"
    __table_args__ = {"schema": IMPORT_SCHEMA}

    id_match_fields = DB.Column(DB.Integer, primary_key=True, autoincrement=True)  # TODO rename this to pk or id…
    id_mapping = DB.Column(DB.Integer, db.ForeignKey(TMappings.id_mapping), nullable=False)
    mapping = relationship("TMappings", backref="fields")
    source_field = DB.Column(DB.Unicode, nullable=False)
    target_field = DB.Column(DB.Unicode, db.ForeignKey(BibFields.name_field), nullable=False)
    target = relationship("BibFields")


@serializable
class TMappingsValues(DB.Model):
    __tablename__ = "t_mappings_values"
    __table_args__ = {"schema": IMPORT_SCHEMA}

    id_match_values = DB.Column(DB.Integer, primary_key=True, autoincrement=True)  # TODO rename this to pk or id…
    id_mapping = DB.Column(DB.Integer, db.ForeignKey(TMappings.id_mapping))
    mapping = relationship("TMappings", backref="values")
    target_field_name = DB.Column('target_field', DB.VARCHAR(length=50), db.ForeignKey(BibFields.name_field))
    target_field = relationship("BibFields")
    source_value = DB.Column(DB.Unicode, nullable=False)
    id_target_value = DB.Column(DB.Integer, db.ForeignKey(TNomenclatures.id_nomenclature), nullable=False)
    target_value = relationship("TNomenclatures")
