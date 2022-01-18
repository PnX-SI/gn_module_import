from itertools import groupby

from flask import Blueprint, request, jsonify, send_file, current_app, g
from werkzeug.exceptions import Forbidden, Conflict, BadRequest, NotFound
from jsonschema import validate as validate_json
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import joinedload

from jsonschema import validate as validate_json, ValidationError

from utils_flask_sqla.response import json_resp, json_resp_accept_empty_list
from geonature.utils.env import DB as db
from geonature.core.gn_permissions import decorators as permissions

from pypnnomenclature.models import TNomenclatures
from pypnusershub.db.tools import InsufficientRightsError
from pypnusershub.db.models import User
from pypnusershub.routes import check_auth


from ..db.models import (
    TImports,
    TMappings,
    TMappingsFields,
    TMappingsValues,
    BibFields,
    BibThemes,
)

from ..upload.upload_errors import *
from ..logs import logger
from ..blueprint import blueprint


@blueprint.route("/mappings/", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def add_mapping(scope):
    """
    .. :quickref: Import; Add a mapping.

    Post a new mapping (value or content)
    """
    data = request.get_json()

    name = data.get('name') or None

    # check if name already exists
    if name and db.session.query(TMappings.query.filter(TMappings.mapping_label == name).exists()).scalar():
        raise Conflict(description="Un mapping portant ce nom existe déjà")

    mapping_type = data['type'].upper()
    if mapping_type not in ['FIELD', 'CONTENT']:
        raise BadRequest(description='Invalid mapping type')

    # fill BibMapping
    mapping = TMappings(
        mapping_label=name,
        mapping_type=mapping_type,
    )
    owner = g.current_user
    mapping.owners.append(owner)

    db.session.add(mapping)
    db.session.commit()

    return jsonify(mapping.as_dict_with_cruved())


@blueprint.route("/mappings/", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def list_mappings(scope):
    """
    .. :quickref: Import; Return all active named mappings.

    Return all active named (non-temporary) mappings.

    :param type: Filter mapping of the given type.
    :type type: str
    """
    mapping_type = request.args['type']
    mappings = TMappings.query.filter_by_scope(scope) \
                    .filter(TMappings.mapping_label != None) \
                    .filter(TMappings.active == True) \
                    .filter(TMappings.mapping_type == mapping_type.upper())
    return jsonify([ mapping.as_dict_with_cruved()
                     for mapping in mappings ])


@blueprint.route("/mappings/<int:id_mapping>/", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def get_mapping(scope, id_mapping):
    """
    .. :quickref: Import; Return a mapping.

    Return a mapping. Mapping has to be active.
    """
    mapping = TMappings.query.get_or_404(id_mapping)
    mapping.check_instance_permission(scope)
    if mapping.active is False:
        raise Forbidden(description='Mapping is not active.')
    return jsonify(mapping.as_dict_with_cruved())


@blueprint.route("/mappings/<int:id_mapping>/name", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def rename_mapping(scope, id_mapping):
    """
    .. :quickref: Import; Update mapping name.

    Update mapping name.

    :params mappingName: new mapping name
    :type mappingName: str
    """
    mapping = TMappings.query.get_or_404(id_mapping)
    mapping.check_instance_permission(scope)

    data = request.get_json()

    name = data['name']

    if not name:
        raise BadRequest(description="Vous devez donner un nom au mapping.")
        
    # FIXME: reusable mapping label between field and content mapping
    if db.session.query(TMappings.query.filter(TMappings.mapping_label == name).exists()).scalar():
        raise Conflict(description="Ce nom de modèle existe déjà")

    mapping.mapping_label = name
    db.session.commit()

    return jsonify(mapping.as_dict_with_cruved())


@blueprint.route("/mappings/<int:id_mapping>/", methods=["DELETE"])
@permissions.check_cruved_scope("D", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def delete_mapping(scope, id_mapping):
    """
    .. :quickref: Import; Delete a mapping.

    Delete a mappping. Only permanent (named) mapping can be deleted.
    If a mapping is still used by a import, it is not deleted but marked as temporary (unnamed).
    Return the updated list of mapping of the same type of the deleted mapping.
    """
    mapping = TMappings.query.get_or_404(id_mapping)
    if not mapping.mapping_label:
        raise BadRequest(description="Only permanent (named) mapping can be deleted.")
    mapping.check_instance_permission(scope)
    mapping_type = mapping.mapping_type
    mapping_id = mapping.id_mapping
    if mapping_type == 'FIELD':
        import_filter = dict(id_field_mapping=mapping_id)
    else:
        import_filter = dict(id_content_mapping=mapping_id)
    if db.session.query(TImports.query.filter_by(**import_filter).exists()).scalar():
        mapping.mapping_label = None
    else:
        db.session.commit()
        db.session.delete(mapping)  # TODO: cascade child
    db.session.commit()

    # FIXME really usefull?
    mappings = TMappings.query.filter_by_scope(scope) \
                    .filter(TMappings.mapping_label != None) \
                    .filter(TMappings.active == True) \
                    .filter(TMappings.mapping_type == mapping_type)
    return jsonify([ mapping.as_dict_with_cruved()
                     for mapping in mappings ])


@blueprint.route("/mappings/<int:id_mapping>/fields", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def get_mapping_fields(scope, id_mapping):
    """
    .. :quickref: Import; Get fields of a mapping.

    Load source and target fields from an id_mapping.
    """
    mapping = TMappings.query.get_or_404(id_mapping)
    if mapping.mapping_type != 'FIELD':
        raise NotFound()
    mapping.check_instance_permission(scope)
    fields = (
        TMappingsFields.query
            .filter(TMappingsFields.source_field != '')  # these entries does not make sense
            .filter(TMappingsFields.id_mapping == id_mapping)
    )
    return jsonify([field.as_dict() for field in fields])


@blueprint.route("/mappings/<int:id_mapping>/fields", methods=["POST"])
@permissions.check_cruved_scope("U", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def update_mapping_fields(scope, id_mapping):
    """
    This view add fields mapping to a existing mapping-set.
    Existing fields mapping are kept.
    """
    mapping = TMappings.query.get_or_404(id_mapping)
    if mapping.mapping_type != 'FIELD':
        raise NotFound()
    mapping.check_instance_permission(scope)
    data = request.get_json()
    target_fields = BibFields.query.filter_by(display=True)
    str_target_fields = [ f.name_field for f in target_fields.filter_by(autogenerated=False).with_entities('name_field') ]
    bool_target_fields = [ f.name_field for f in target_fields.filter_by(autogenerated=True).with_entities('name_field') ]
    # Fabrication du schema de validation de la requête à partir des champs de la bdd
    schema = {
      'type': 'object',
      'properties': {},
      'additionalProperties': False,
    }
    for str_target_field in str_target_fields:
        schema['properties'][str_target_field] = { 'type': ['string', 'null'] }
    for bool_target_field in bool_target_fields:
        schema['properties'][bool_target_field] = { 'type': ['boolean', 'null'] }
    try:
        validate_json(data, schema)
    except ValidationError as err:
        raise BadRequest(description="Validation of the request against jsonschema did not pass.")
    for target_field, source_field in data.items():
        if not source_field:
            continue
        if target_field in str_target_fields:
            if type(source_field) != str:
                raise BadRequest("Expected string source field.")
        elif target_field in bool_target_fields:
            if type(source_field) != bool:
                raise BadRequest("Expected boolean source field.")
        else:
            raise BadRequest("Unknown target field.")
        # do a insert-or-update of the field mapping:
        # replace the source field if a (id_mapping, target_field) record already exists
        statement = pg_insert(TMappingsFields.__table__) \
            .values(id_mapping=id_mapping, target_field=target_field, source_field=source_field) \
            .on_conflict_do_update(
                constraint='un_t_mappings_fields',
                set_=dict(source_field=source_field))
        db.session.execute(statement)
    db.session.commit()
    fields = TMappingsFields.query.filter_by(id_mapping=id_mapping)
    return jsonify([field.as_dict() for field in fields])


@blueprint.route("/mappings/<int:id_mapping>/contents", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def get_mapping_contents(scope, id_mapping):
    """
    .. :quickref: Import; Get values of a mapping.

    Load source and target values from an id_mapping.
    """

    mapping = TMappings.query.get_or_404(id_mapping)
    if mapping.mapping_type != 'CONTENT':
        raise NotFound()
    mapping.check_instance_permission(scope)
    values = TMappingsValues.query.filter_by(id_mapping=id_mapping)
    return jsonify([value.as_dict() for value in values])


@blueprint.route("/mappings/<int:mapping_id>/contents", methods=["POST"])
@permissions.check_cruved_scope("U", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def update_mapping_contents(scope, mapping_id):
    """
    This view add fields mapping to a existing mapping-set.
    Existing fields mapping are kept.
    """
    mapping = TMappings.query.get_or_404(mapping_id)
    if mapping.mapping_type != 'CONTENT':
        raise NotFound()
    mapping.check_instance_permission(scope)
    data = request.get_json()
    try:
        validate_json(data, {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'target_field_name': { 'type': 'string' },
                    'source_value': { 'type': ['string', 'null'] },
                    'target_id_nomenclature': { 'type': 'integer' },
                },
                'required': [
                    'target_field_name',
                    'source_value',
                    'target_id_nomenclature',
                ],
                'additionalProperties': False,
            },
        })
    except ValidationError:
        raise BadRequest(description="Validation of the request against jsonschema did not pass.")
    for entry in data:
        target_field = BibFields.query.filter_by(name_field=entry['target_field_name']).one()
        if not target_field:
            raise BadRequest(description=f"Target field name '{entry['target_field_name']}' not found.")
        target_value = TNomenclatures.query \
                .filter_by(id_type=target_field.nomenclature_type.id_type, id_nomenclature=entry['target_id_nomenclature']).one()
        if not target_value:
            raise BadRequest(description=f"Target id nomenclature '{entry['target_id_nomenclature']}' not found.")
        # altough using null for source_value is a convenient way to refer to empty cells in csv source file,
        # we must keep source_value not null to ensure uniqueness (pg consider two null values as distinct)
        # (note: there is a solution to use null value based on a partial index, but not supported by on_conflict_do_update)
        # we use the blank string '' to designate empty cells in csv source file
        source_value = entry['source_value'] or ''
        # do a insert-or-update of the values mapping:
        statement = pg_insert(TMappingsValues.__table__) \
            .values(id_mapping=mapping.id_mapping, target_field=target_field.name_field,
                    source_value=source_value, id_target_value=target_value.id_nomenclature) \
            .on_conflict_do_update(
                constraint='un_t_mappings_values',
                set_=dict(id_target_value=target_value.id_nomenclature))
        db.session.execute(statement)
    db.session.commit()
    values = TMappingsValues.query.filter_by(id_mapping=mapping.id_mapping)
    return jsonify([value.as_dict() for value in values])



@blueprint.route("/synthesis/fields", methods=["GET"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT")
def get_synthesis_fields(scope):
    """
    .. :quickref: Import; Get synthesis fields.

    Get all synthesis fields
    Use in field mapping steps
    You can find a jsonschema of the returned data in the associated test.
    """
    fields = BibFields.query.filter_by(display=True) \
                            .options(joinedload(BibFields.theme)) \
                            .join(BibThemes) \
                            .order_by(BibThemes.order_theme, BibFields.order_field).all()
    data = []
    for id_theme, fields in groupby(fields, lambda field: field.id_theme):
        fields = list(fields)
        theme = fields[0].theme
        data.append({
            'theme': theme.as_dict(),
            'fields': [ field.as_dict() for field in fields ],
        })
    return jsonify(data)
