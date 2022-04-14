from datetime import datetime
from collections.abc import Mapping

from flask import g
import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, deferred, joinedload
from sqlalchemy.types import ARRAY
from sqlalchemy.dialects.postgresql import HSTORE, JSON, UUID, JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import column_property
from sqlalchemy.sql.expression import exists
from flask_sqlalchemy import BaseQuery
from jsonschema.exceptions import ValidationError as JSONValidationError
from jsonschema import validate as validate_json
from geoalchemy2 import Geometry

from utils_flask_sqla.serializers import serializable

from geonature.utils.env import db
from geonature.core.gn_permissions.tools import get_scopes_by_action

from pypnnomenclature.models import BibNomenclaturesTypes, TNomenclatures
from pypnusershub.db.models import User
from apptax.taxonomie.models import Taxref
from pypn_habref_api.models import Habref


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
    __tablename__ = "dict_errors"
    __table_args__ = {"schema": "gn_imports"}

    pk = db.Column("id_error", db.Integer, primary_key=True)
    category = db.Column("error_type", db.Unicode, nullable=False)
    name = db.Column(db.Unicode, nullable=False, unique=True)
    description = db.Column(db.Unicode)
    level = db.Column("error_level", db.Unicode)

    def __str__(self):
        return f"<ImportErrorType {self.name}>"


@serializable
class ImportUserError(db.Model):
    __tablename__ = "t_user_errors"
    __table_args__ = {"schema": "gn_imports"}

    pk = db.Column("id_user_error", db.Integer, primary_key=True)
    id_import = db.Column(
        db.Integer,
        db.ForeignKey(
            "gn_imports.t_imports.id_import", onupdate="CASCADE", ondelete="CASCADE"
        ),
    )
    imprt = db.relationship("TImports", back_populates="errors")
    id_type = db.Column(
        "id_error",
        db.Integer,
        db.ForeignKey(ImportUserErrorType.pk, onupdate="CASCADE", ondelete="CASCADE"),
    )
    type = db.relationship("ImportUserErrorType")
    column = db.Column("column_error", db.Unicode)
    rows = db.Column("id_rows", db.ARRAY(db.Integer))
    comment = db.Column(db.UnicodeText)

    def __str__(self):
        return f"<ImportError import={self.id_import},type={self.type.name},rows={self.rows}>"


class InstancePermissionMixin:
    def get_instance_permissions(self, scopes, user=None):
        if user is None:
            user = g.current_user
        if isinstance(scopes, Mapping):
            return {
                key: self.has_instance_permission(scope, user=user)
                for key, scope in scopes.items()
            }
        else:
            return [self.has_instance_permission(scope, user=user) for scope in scopes]


cor_role_import = db.Table(
    "cor_role_import",
    db.Column("id_role", db.Integer, db.ForeignKey(User.id_role), primary_key=True),
    db.Column(
        "id_import",
        db.Integer,
        db.ForeignKey("gn_imports.t_imports.id_import"),
        primary_key=True,
    ),
    schema="gn_imports",
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


@serializable(fields=["authors.nom_complet"])
class TImports(InstancePermissionMixin, db.Model):
    __tablename__ = "t_imports"
    __table_args__ = {"schema": "gn_imports"}
    query_class = ImportQuery

    # https://docs.python.org/3/library/codecs.html
    # https://chardet.readthedocs.io/en/latest/supported-encodings.html
    # TODO: move in configuration file
    AVAILABLE_ENCODINGS = {
        "utf-8",
        "iso-8859-1",
        "iso-8859-15",
    }
    AVAILABLE_FORMATS = ["csv", "geojson"]
    AVAILABLE_SEPARATORS = [",", ";"]

    id_import = db.Column(db.Integer, primary_key=True, autoincrement=True)
    format_source_file = db.Column(db.Unicode, nullable=True)
    srid = db.Column(db.Integer, nullable=True)
    separator = db.Column(db.Unicode, nullable=True)
    detected_separator = db.Column(db.Unicode, nullable=True)
    encoding = db.Column(db.Unicode, nullable=True)
    detected_encoding = db.Column(db.Unicode, nullable=True)
    # import_table = db.Column(db.Unicode, nullable=True)
    full_file_name = db.Column(db.Unicode, nullable=True)
    id_dataset = db.Column(
        db.Integer, ForeignKey("gn_meta.t_datasets.id_dataset"), nullable=True
    )
    date_create_import = db.Column(db.DateTime, default=datetime.now)
    date_update_import = db.Column(
        db.DateTime, default=datetime.now, onupdate=datetime.now
    )
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
    dataset = db.relationship("TDatasets", lazy="joined")
    source_file = deferred(db.Column(db.LargeBinary))
    columns = db.Column(ARRAY(db.Unicode))
    # keys are target names, values are source names
    fieldmapping = db.Column(MutableDict.as_mutable(JSON))
    contentmapping = db.Column(MutableDict.as_mutable(JSON))

    errors = db.relationship(
        "ImportUserError",
        back_populates="imprt",
        order_by="ImportUserError.id_type",  # TODO order by type.category
        cascade="all, delete-orphan",
    )
    synthese_data = db.relationship(
        "ImportSyntheseData",
        back_populates="imprt",
        order_by="ImportSyntheseData.line_no",
        cascade="all, delete-orphan",
    )

    @property
    def cruved(self):
        scopes_by_action = get_scopes_by_action(
            module_code="IMPORT", object_code="IMPORT"
        )
        return {
            action: self.has_instance_permission(scope)
            for action, scope in scopes_by_action.items()
        }

    errors_exists = column_property(
        exists().where(ImportUserError.id_import == id_import)
    )

    @property
    def source_name(self):
        return "Import(id={})".format(self.id_import)

    def has_instance_permission(self, scope, user=None):
        if user is None:
            user = g.current_user
        if (
            scope == 0
        ):  # pragma: no cover (should not happen as already checked by the decorator)
            return False
        elif scope == 1:  # self
            return user.id_role in [author.id_role for author in self.authors]
        elif scope == 2:  # organism
            return user.id_role in [author.id_role for author in self.authors] or (
                user.id_organisme is not None
                and user.id_organisme
                in [author.id_organisme for author in self.authors]
            )
        elif scope == 3:  # all
            return True

    def as_dict(self, import_as_dict):
        if import_as_dict["date_end_import"] is None:
            import_as_dict["date_end_import"] = "En cours"
        import_as_dict["authors_name"] = "; ".join(
            [author.nom_complet for author in self.authors]
        )
        if self.detected_encoding:
            import_as_dict["available_encodings"] = sorted(
                TImports.AVAILABLE_ENCODINGS
                | {
                    self.detected_encoding,
                }
            )
        else:
            import_as_dict["available_encodings"] = sorted(TImports.AVAILABLE_ENCODINGS)
        import_as_dict["available_formats"] = TImports.AVAILABLE_FORMATS
        import_as_dict["available_separators"] = TImports.AVAILABLE_SEPARATORS
        if self.full_file_name and "." in self.full_file_name:
            extension = self.full_file_name.rsplit(".", 1)[-1]
            if extension in TImports.AVAILABLE_FORMATS:
                import_as_dict["detected_format"] = extension
        return import_as_dict


@serializable
class ImportSyntheseData(db.Model):
    __tablename__ = "t_imports_synthese"
    __table_args__ = {"schema": "gn_imports"}

    id_import = db.Column(db.Integer, ForeignKey(TImports.id_import), primary_key=True)
    imprt = db.relationship(TImports, back_populates="synthese_data")
    line_no = db.Column(db.Integer, primary_key=True)

    valid = db.Column(db.Boolean, nullable=False, server_default=sa.false())

    """
    source fields
    Load data as unicode. They will be casted during check & transformation process.
    """
    # non-synthese fields: used to populate synthese fields
    src_WKT = db.Column(db.Unicode)
    src_codecommune = db.Column(db.Unicode)
    src_codedepartement = db.Column(db.Unicode)
    src_codemaille = db.Column(db.Unicode)
    src_hour_max = db.Column(db.Unicode)
    src_hour_min = db.Column(db.Unicode)
    src_latitude = db.Column(db.Unicode)
    src_longitude = db.Column(db.Unicode)

    # synthese fields
    src_unique_id_sinp = db.Column(db.Unicode)
    src_unique_id_sinp_grp = db.Column(db.Unicode)
    # nomenclature fields
    src_id_nomenclature_geo_object_nature = db.Column(db.Unicode)
    src_id_nomenclature_grp_typ = db.Column(db.Unicode)
    src_id_nomenclature_obs_technique = db.Column(db.Unicode)
    src_id_nomenclature_bio_status = db.Column(db.Unicode)
    src_id_nomenclature_bio_condition = db.Column(db.Unicode)
    src_id_nomenclature_naturalness = db.Column(db.Unicode)
    src_id_nomenclature_exist_proof = db.Column(db.Unicode)
    src_id_nomenclature_valid_status = db.Column(db.Unicode)
    src_id_nomenclature_exist_proof = db.Column(db.Unicode)
    src_id_nomenclature_diffusion_level = db.Column(db.Unicode)
    src_id_nomenclature_life_stage = db.Column(db.Unicode)
    src_id_nomenclature_sex = db.Column(db.Unicode)
    src_id_nomenclature_obj_count = db.Column(db.Unicode)
    src_id_nomenclature_type_count = db.Column(db.Unicode)
    src_id_nomenclature_sensitivity = db.Column(db.Unicode)
    src_id_nomenclature_observation_status = db.Column(db.Unicode)
    src_id_nomenclature_blurring = db.Column(db.Unicode)
    src_id_nomenclature_source_status = db.Column(db.Unicode)
    src_id_nomenclature_info_geo_type = db.Column(db.Unicode)
    src_id_nomenclature_behaviour = db.Column(db.Unicode)
    src_id_nomenclature_biogeo_status = db.Column(db.Unicode)
    src_id_nomenclature_determination_method = db.Column(db.Unicode)
    src_count_min = db.Column(db.Unicode)
    src_count_max = db.Column(db.Unicode)
    src_cd_nom = db.Column(db.Unicode)
    src_cd_hab = db.Column(db.Unicode)
    src_altitude_min = db.Column(db.Unicode)
    src_altitude_max = db.Column(db.Unicode)
    src_depth_min = db.Column(db.Unicode)
    src_depth_max = db.Column(db.Unicode)
    src_precision = db.Column(db.Unicode)
    src_id_area_attachment = db.Column(db.Unicode)
    src_date_min = db.Column(db.Unicode)
    src_date_max = db.Column(db.Unicode)
    src_id_digitiser = db.Column(db.Unicode)
    src_meta_validation_date = db.Column(db.Unicode)
    src_meta_create_date = db.Column(db.Unicode)
    src_meta_update_date = db.Column(db.Unicode)
    # un-mapped fields
    extra_fields = db.Column(HSTORE)

    """
    synthese fields
    """
    unique_id_sinp = db.Column(UUID(as_uuid=True))
    unique_id_sinp_grp = db.Column(UUID(as_uuid=True))
    entity_source_pk_value = db.Column(db.Unicode)
    grp_method = db.Column(db.Unicode)  # length=255
    # nomenclature fields
    id_nomenclature_geo_object_nature = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_geo_object_nature = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_geo_object_nature]
    )
    id_nomenclature_grp_typ = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_grp_typ = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_grp_typ]
    )
    id_nomenclature_obs_technique = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_obs_technique = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_obs_technique]
    )
    id_nomenclature_bio_status = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_bio_status = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_bio_status]
    )
    id_nomenclature_bio_condition = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_bio_condition = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_bio_condition]
    )
    id_nomenclature_naturalness = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_naturalness = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_naturalness]
    )
    id_nomenclature_exist_proof = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_exist_proof = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_exist_proof]
    )
    id_nomenclature_valid_status = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_valid_status = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_valid_status]
    )
    id_nomenclature_exist_proof = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_exist_proof = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_exist_proof]
    )
    id_nomenclature_diffusion_level = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_diffusion_level = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_diffusion_level]
    )
    id_nomenclature_life_stage = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_life_stage = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_life_stage]
    )
    id_nomenclature_sex = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_sex = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_sex]
    )
    id_nomenclature_obj_count = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_obj_count = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_obj_count]
    )
    id_nomenclature_type_count = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_type_count = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_type_count]
    )
    id_nomenclature_sensitivity = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_sensitivity = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_sensitivity]
    )
    id_nomenclature_observation_status = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_observation_status = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_observation_status]
    )
    id_nomenclature_blurring = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_blurring = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_blurring]
    )
    id_nomenclature_source_status = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_source_status = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_source_status]
    )
    id_nomenclature_info_geo_type = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_info_geo_type = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_info_geo_type]
    )
    id_nomenclature_behaviour = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_behaviour = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_behaviour]
    )
    id_nomenclature_biogeo_status = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_biogeo_status = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_biogeo_status]
    )
    id_nomenclature_determination_method = db.Column(
        db.Integer, ForeignKey(TNomenclatures.id_nomenclature)
    )
    nomenclature_determination_method = db.relationship(
        TNomenclatures, foreign_keys=[id_nomenclature_determination_method]
    )
    # others fields
    reference_biblio = db.Column(db.Unicode)  # length=5000
    count_min = db.Column(db.Integer)
    count_max = db.Column(db.Integer)
    cd_nom = db.Column(db.Integer, ForeignKey(Taxref.cd_nom))
    taxref = relationship(Taxref)
    cd_hab = db.Column(db.Integer, ForeignKey(Habref.cd_hab))
    habitat = relationship(Habref)
    nom_cite = db.Column(db.Unicode)  # length=1000
    meta_v_taxref = db.Column(db.Unicode)  # length=50
    digital_proof = db.Column(db.UnicodeText)
    non_digital_proof = db.Column(db.UnicodeText)
    altitude_min = db.Column(db.Integer)
    altitude_max = db.Column(db.Integer)
    depth_min = db.Column(db.Integer)
    depth_max = db.Column(db.Integer)
    place_name = db.Column(db.Unicode)  # length=500
    the_geom_4326 = db.Column(Geometry("GEOMETRY", 4326))
    the_geom_point = db.Column(Geometry("GEOMETRY", 4326))
    the_geom_local = db.Column(Geometry("GEOMETRY"))
    precision = db.Column(db.Integer)
    date_min = db.Column(db.DateTime)
    date_max = db.Column(db.DateTime)
    validator = db.Column(db.Unicode)  # length=1000
    validation_comment = db.Column(db.Unicode)
    observers = db.Column(db.Unicode)  # length=1000
    determiner = db.Column(db.Unicode)  # length=1000
    id_digitiser = db.Column(db.Integer, ForeignKey(User.id_role))
    digitiser = db.relationship(User, foreign_keys=[id_digitiser])
    comment_context = db.Column(db.UnicodeText)
    comment_description = db.Column(db.UnicodeText)
    additional_data = db.Column(JSONB)
    meta_validation_date = db.Column(db.DateTime)
    meta_create_date = db.Column(db.DateTime)
    meta_update_date = db.Column(db.DateTime)
    # missing fields:
    # sample_number_proof = db.Column(db.UnicodeText)
    # id_area_attachment = db.Column(db.Integer)
    # last_action = db.Column(db.Unicode)


@serializable
class BibThemes(db.Model):
    __tablename__ = "dict_themes"
    __table_args__ = {"schema": "gn_imports"}

    id_theme = db.Column(db.Integer, primary_key=True)
    name_theme = db.Column(db.Unicode, nullable=False)
    fr_label_theme = db.Column(db.Unicode, nullable=False)
    eng_label_theme = db.Column(db.Unicode, nullable=True)
    desc_theme = db.Column(db.Unicode, nullable=True)
    order_theme = db.Column(db.Integer, nullable=False)


@serializable
class BibFields(db.Model):
    __tablename__ = "dict_fields"
    __table_args__ = {"schema": "gn_imports"}

    id_field = db.Column(db.Integer, primary_key=True)
    name_field = db.Column(db.Unicode, nullable=False, unique=True)
    source_field = db.Column(db.Unicode, unique=True)
    synthese_field = db.Column(db.Unicode, unique=True)
    fr_label = db.Column(db.Unicode, nullable=False)
    eng_label = db.Column(db.Unicode, nullable=True)
    desc_field = db.Column(db.Unicode, nullable=True)
    type_field = db.Column(db.Unicode, nullable=True)
    synthese_field = db.Column(db.Boolean, nullable=False)
    mandatory = db.Column(db.Boolean, nullable=False)
    autogenerated = db.Column(db.Boolean, nullable=False)
    mnemonique = db.Column(db.Unicode, db.ForeignKey(BibNomenclaturesTypes.mnemonique))
    nomenclature_type = relationship("BibNomenclaturesTypes")
    id_theme = db.Column(db.Integer, db.ForeignKey(BibThemes.id_theme), nullable=False)
    theme = relationship(BibThemes)
    order_field = db.Column(db.Integer, nullable=False)
    display = db.Column(db.Boolean, nullable=False)
    comment = db.Column(db.Unicode)

    @property
    def source_column(self):
        return self.source_field if self.source_field else self.synthese_field

    @property
    def synthese_column(self):
        return self.synthese_field if self.synthese_field else self.source_field

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
    db.Column("id_role", db.Integer, db.ForeignKey(User.id_role), primary_key=True),
    db.Column(
        "id_mapping",
        db.Integer,
        db.ForeignKey("gn_imports.t_mappings.id"),
        primary_key=True,
    ),
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
    public = db.Column(
        db.Boolean, nullable=False, default=False, server_default="false"
    )

    @property
    def cruved(self):
        scopes_by_action = get_scopes_by_action(
            module_code="IMPORT", object_code="MAPPING"
        )
        return {
            action: self.has_instance_permission(scope)
            for action, scope in scopes_by_action.items()
        }

    __mapper_args__ = {
        "polymorphic_on": type,
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
            return user in self.owners or (
                scope == 2
                and user.id_organisme is not None
                and user.id_organisme in [owner.id_organisme for owner in self.owners]
            )
        elif scope == 3:
            return True


@serializable
class FieldMapping(MappingTemplate):
    __tablename__ = "t_fieldmappings"
    __table_args__ = {"schema": "gn_imports"}

    id = db.Column(db.Integer, ForeignKey(MappingTemplate.id), primary_key=True)
    values = db.Column(MutableDict.as_mutable(JSON))

    __mapper_args__ = {
        "polymorphic_identity": "FIELD",
    }

    @staticmethod
    def validate_values(values):
        fields = (
            BibFields.query
            .filter_by(display=True)
            .with_entities(
                BibFields.name_field,
                BibFields.autogenerated,
            )
        )
        schema = {
            "type": "object",
            "properties": {
                field.name_field: {
                    "type": "boolean" if field.autogenerated else "string",
                }
                for field in fields
            },
            "additionalProperties": False,
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
            BibFields.query.filter(BibFields.nomenclature_type != None)
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
                "type": "object",
                "patternProperties": {
                    "^.*$": {
                        "type": "string",
                        "enum": cd_nomenclatures,
                    },
                },
            }
        schema = {
            "type": "object",
            "properties": properties,
            "additionalProperties": False,
        }
        try:
            validate_json(values, schema)
        except JSONValidationError as e:
            raise ValueError(e.message)
