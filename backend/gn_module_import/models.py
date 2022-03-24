from datetime import datetime
from collections.abc import Mapping

from flask import g
import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, deferred, joinedload
from sqlalchemy.dialects.postgresql import HSTORE, JSON
from sqlalchemy.ext.mutable import MutableDict
from flask_sqlalchemy import BaseQuery
from jsonschema.exceptions import ValidationError as JSONValidationError
from jsonschema import validate as validate_json

from utils_flask_sqla.serializers import serializable

from geonature.utils.env import db
from geonature.core.gn_permissions.tools import get_scopes_by_action

from pypnnomenclature.models import BibNomenclaturesTypes
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
    __table_args__ = {"schema": IMPORT_SCHEMA}

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
    __table_args__ = {"schema": IMPORT_SCHEMA}

    pk = db.Column('id_user_error', db.Integer, primary_key=True)
    id_import = db.Column(db.Integer, db.ForeignKey(f"{IMPORT_SCHEMA}.t_imports.id_import"))
    imprt = db.relationship("TImports", back_populates='errors')
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
            return {key: self.has_instance_permission(scope, user=user)
                    for key, scope in scopes.items()}
        else:
            return [self.has_instance_permission(scope, user=user)
                    for scope in scopes]


cor_role_import = db.Table(
    'cor_role_import',
    db.Column('id_role', db.Integer,
              db.ForeignKey(User.id_role), primary_key=True),
    db.Column('id_import', db.Integer,
              db.ForeignKey(f'{IMPORT_SCHEMA}.t_imports.id_import'), primary_key=True),
    schema=IMPORT_SCHEMA,
)


class ImportQuery(BaseQuery):
    def filter_by_scope(self, scope, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:
            return self.filter(sa.false())
        elif scope in (1, 2):
            filters = [User.id_role == user.id_role]
            if scope == 2 and user.id_organisme is not None:
                filters += [User.id_organisme == user.id_organisme]
            return self.filter(TImports.authors.any(sa.or_(*filters)))
        elif scope == 3:
            return self
        else:
            raise Exception(f"Unexpected scope {scope}")


@serializable(fields=['authors.nom_complet'], exclude=['source_file'])
class TImports(InstancePermissionMixin, db.Model):
    __tablename__ = "t_imports"
    __table_args__ = {"schema": IMPORT_SCHEMA}
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

    id_import = db.Column(db.Integer, primary_key=True, autoincrement=True)
    format_source_file = db.Column(db.Unicode, nullable=True)
    srid = db.Column(db.Integer, nullable=True)
    separator = db.Column(db.Unicode, nullable=True)
    encoding = db.Column(db.Unicode, nullable=True)
    detected_encoding = db.Column(db.Unicode, nullable=True)
    import_table = db.Column(db.Unicode, nullable=True)
    full_file_name = db.Column(db.Unicode, nullable=True)
    id_dataset = db.Column(db.Integer, ForeignKey("gn_meta.t_datasets.id_dataset"), nullable=True)
    date_create_import = db.Column(db.DateTime, default=datetime.now)
    date_update_import = db.Column(db.DateTime, default=datetime.now, onupdate=datetime.now)
    date_end_import = db.Column(db.DateTime, nullable=True)
    source_count = db.Column(db.Integer, nullable=True)
    import_count = db.Column(db.Integer, nullable=True)
    taxa_count = db.Column(db.Integer, nullable=True)
    date_min_data = db.Column(db.DateTime, nullable=True)
    date_max_data = db.Column(db.DateTime, nullable=True)
    uuid_autogenerated = db.Column(db.Boolean)
    altitude_autogenerated = db.Column(db.Boolean)
    authors = db.relationship(
        User,
        lazy="joined",
        secondary=cor_role_import,
    )
    is_finished = db.Column(db.Boolean, nullable=False, default=False)
    processing = db.Column(db.Boolean, nullable=False, default=False)
    in_error = db.Column(db.Boolean)
    errors = db.relationship("ImportUserError", back_populates='imprt',
                             order_by='ImportUserError.id_type',  # TODO order by type.category
                             cascade="all, delete-orphan")
    dataset = db.relationship("TDatasets", lazy="joined")
    source_file = deferred(db.Column(db.LargeBinary))
    # the columns field is a mapping with cleaned columns names (also db fields)
    # as keys and original columns name (in uploaded csv) as values
    columns = db.Column(MutableDict.as_mutable(HSTORE))
    # keys are target names, values are source names
    fieldmapping = db.Column(MutableDict.as_mutable(HSTORE))
    contentmapping = db.Column(MutableDict.as_mutable(JSON))

    @property
    def cruved(self):
        scopes_by_action = get_scopes_by_action(module_code="IMPORT", object_code="IMPORT")
        return {action: self.has_instance_permission(scope)
                for action, scope in scopes_by_action.items()}

    def has_instance_permission(self, scope, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:  # pragma: no cover (should not happen as already checked by the decorator)
            return False
        elif scope == 1:  # self
            return user.id_role in [author.id_role for author in self.authors]
        elif scope == 2:  # organism
            return (
                user.id_role in [author.id_role for author in self.authors]
                or
                (
                    user.id_organisme is not None
                    and user.id_organisme in [author.id_organisme for author in self.authors]
                )
            )
        elif scope == 3:  # all
            return True

    def as_dict(self, import_as_dict):
        if import_as_dict["date_end_import"] is None:
            import_as_dict["date_end_import"] = "En cours"
        import_as_dict["authors_name"] = "; ".join([author.nom_complet for author in self.authors])
        if self.detected_encoding:
            import_as_dict['available_encodings'] = sorted(
                TImports.AVAILABLE_ENCODINGS | {self.detected_encoding, }
            )
        else:
            import_as_dict['available_encodings'] = sorted(TImports.AVAILABLE_ENCODINGS)
        import_as_dict["available_formats"] = TImports.AVAILABLE_FORMATS
        if self.full_file_name and '.' in self.full_file_name:
            extension = self.full_file_name.rsplit('.', 1)[-1]
            if extension in TImports.AVAILABLE_FORMATS:
                import_as_dict["detected_format"] = extension
        return import_as_dict


@serializable
class BibThemes(db.Model):
    __tablename__ = "dict_themes"
    __table_args__ = {"schema": IMPORT_SCHEMA}

    id_theme = db.Column(db.Integer, primary_key=True)
    name_theme = db.Column(db.Unicode, nullable=False)
    fr_label_theme = db.Column(db.Unicode, nullable=False)
    eng_label_theme = db.Column(db.Unicode, nullable=True)
    desc_theme = db.Column(db.Unicode, nullable=True)
    order_theme = db.Column(db.Integer, nullable=False)


@serializable
class BibFields(db.Model):
    __tablename__ = "dict_fields"
    __table_args__ = {"schema": IMPORT_SCHEMA}

    id_field = db.Column(db.Integer, primary_key=True)
    name_field = db.Column(db.Unicode, nullable=False, unique=True)
    fr_label = db.Column(db.Unicode, nullable=False)
    eng_label = db.Column(db.Unicode, nullable=True)
    desc_field = db.Column(db.Unicode, nullable=True)
    type_field = db.Column(db.Unicode, nullable=True)
    synthese_field = db.Column(db.Boolean, nullable=False)
    mandatory = db.Column(db.Boolean, nullable=False)
    autogenerated = db.Column(db.Boolean, nullable=False)
    nomenclature = db.Column(db.Boolean, nullable=False)  # ??? → équivalent à mnemonique is null
    mnemonique = db.Column(db.Unicode, db.ForeignKey(BibNomenclaturesTypes.mnemonique))
    nomenclature_type = relationship("BibNomenclaturesTypes")
    id_theme = db.Column(db.Integer, db.ForeignKey(BibThemes.id_theme), nullable=False)
    theme = relationship(BibThemes)
    order_field = db.Column(db.Integer, nullable=False)
    display = db.Column(db.Boolean, nullable=False)
    comment = db.Column(db.Unicode)

    def __str__(self):
        return self.fr_label


class MappingQuery(BaseQuery):
    def filter_by_scope(self, scope, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:
            return self.filter(sa.false())
        elif scope in (1, 2):
            filters = [
                Mapping.public == True,
                Mapping.owners.any(id_role=user.id_role),
            ]
            if scope == 2 and user.id_organisme is not None:
                filters.append(Mapping.owners.any(id_organisme=user.id_organisme))
            return self.filter(sa.or_(*filters)).distinct()
        elif scope == 3:
            return self
        else:
            raise Exception(f"Unexpected scope {scope}")


cor_role_mapping = db.Table(
    "cor_role_mapping",
    db.Column("id_role", db.Integer,
              db.ForeignKey(User.id_role), primary_key=True),
    db.Column("id_mapping", db.Integer,
              db.ForeignKey("gn_imports.t_mappings.id"), primary_key=True),
    schema="gn_imports",
)


class MappingTemplate(db.Model):
    __tablename__ = "t_mappings"
    __table_args__ = {"schema": "gn_imports"}

    query_class = MappingQuery

    id = db.Column(db.Integer, primary_key=True)
    label = db.Column(db.Unicode(255), nullable=False)
    type = db.Column(db.Unicode(10), nullable=False)
    active = db.Column(db.Boolean, nullable=False, default=True, server_default="true")
    public = db.Column(db.Boolean, nullable=False, default=False, server_default="false")

    @property
    def cruved(self):
        scopes_by_action = get_scopes_by_action(module_code="IMPORT", object_code="MAPPING")
        return {action: self.has_instance_permission(scope)
                for action, scope in scopes_by_action.items()}

    __mapper_args__ = {
        'polymorphic_on': type,
    }

    owners = relationship(
        User,
        lazy="joined",
        secondary=cor_role_mapping,
    )

    def has_instance_permission(self, scope: int, user=None):
        if user is None:
            user = g.current_user
        if scope == 0:
            return False
        elif scope in (1, 2):
            return (
                user in self.owners
                or (
                    scope == 2
                    and
                    user.id_organisme is not None
                    and
                    user.id_organisme in [owner.id_organisme for owner in self.owners]
                )
            )
        elif scope == 3:
            return True


@serializable
class FieldMapping(MappingTemplate):
    __tablename__ = "t_fieldmappings"
    __table_args__ = {"schema": "gn_imports"}

    id = db.Column(db.Integer, ForeignKey(MappingTemplate.id), primary_key=True)
    values = db.Column(MutableDict.as_mutable(HSTORE))

    __mapper_args__ = {
        "polymorphic_identity": "FIELD",
    }

    @staticmethod
    def validate_values(values):
        fields = BibFields.query.with_entities(BibFields.name_field)
        schema = {
            'type': 'object',
            'properties': {
                field.name_field: {'type': 'string'}
                for field in fields
            },
            'additionalProperties': False,
        }
        try:
            validate_json(values, schema)
        except JSONValidationError as e:
            raise ValueError(e.message)


@serializable
class ContentMapping(MappingTemplate):
    __tablename__ = "t_contentmappings"
    __table_args__ = {"schema": "gn_imports"}

    id = db.Column(db.Integer, ForeignKey(MappingTemplate.id), primary_key=True)
    values = db.Column(MutableDict.as_mutable(JSON))

    __mapper_args__ = {
        "polymorphic_identity": "CONTENT",
    }

    @staticmethod
    def validate_values(values):
        nomenclature_fields = (
            BibFields.query
            .filter(BibFields.nomenclature_type != None)
            .options(
                joinedload(BibFields.nomenclature_type).joinedload(
                    BibNomenclaturesTypes.nomenclatures
                ),
            )
            .all()
        )
        properties = {}
        for nomenclature_field in nomenclature_fields:
            cd_nomenclatures = [
                nomenclature.cd_nomenclature
                for nomenclature in nomenclature_field.nomenclature_type.nomenclatures
            ]
            properties[nomenclature_field.mnemonique] = {
                'type': 'object',
                'patternProperties': {
                    '^.*$': {
                        'type': 'string',
                        'enum': cd_nomenclatures,
                    },
                },
            }
        schema = {
            'type': 'object',
            'properties': properties,
            'additionalProperties': False,
        }
        try:
            validate_json(values, schema)
        except JSONValidationError as e:
            raise ValueError(e.message)
