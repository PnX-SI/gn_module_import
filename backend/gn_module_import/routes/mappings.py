from itertools import groupby

from flask import request, jsonify, current_app, g
from werkzeug.exceptions import Forbidden, Conflict, BadRequest, NotFound
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from geonature.utils.env import db
from geonature.core.gn_permissions import decorators as permissions

from gn_module_import.models import (
    BibFields,
    BibThemes,
    MappingTemplate,
    FieldMapping,
    ContentMapping,
)

from gn_module_import.blueprint import blueprint


@blueprint.url_value_preprocessor
def check_mapping_type(endpoint, values):
    if current_app.url_map.is_endpoint_expecting(endpoint, "mappingtype"):
        if values["mappingtype"] not in ["field", "content"]:
            raise NotFound
        values["mappingtype"] = values["mappingtype"].upper()
        if current_app.url_map.is_endpoint_expecting(endpoint, "id_mapping"):
            mapping = MappingTemplate.query.get_or_404(values.pop("id_mapping"))
            if mapping.type != values["mappingtype"]:
                raise NotFound
            values["mapping"] = mapping


@blueprint.route("/<mappingtype>mappings/", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def list_mappings(mappingtype, scope):
    """
    .. :quickref: Import; Return all active named mappings.

    Return all active named (non-temporary) mappings.

    :param type: Filter mapping of the given type.
    :type type: str
    """
    mappings = (
        MappingTemplate.query.filter(MappingTemplate.type == mappingtype)
        .filter(MappingTemplate.active == True)
        .filter_by_scope(scope)
    )
    return jsonify([mapping.as_dict() for mapping in mappings])


@blueprint.route("/<mappingtype>mappings/<int:id_mapping>/", methods=["GET"])
@permissions.check_cruved_scope("R", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def get_mapping(mappingtype, mapping, scope):
    """
    .. :quickref: Import; Return a mapping.

    Return a mapping. Mapping has to be active.
    """
    if not mapping.public and not mapping.has_instance_permission(scope):
        raise Forbidden
    if mapping.active is False:
        raise Forbidden(description="Mapping is not active.")
    return jsonify(mapping.as_dict())


@blueprint.route("/<mappingtype>mappings/", methods=["POST"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def add_mapping(mappingtype, scope):
    """
    .. :quickref: Import; Add a mapping.
    """
    label = request.args.get("label")
    if not label:
        raise BadRequest("Missing label")

    # check if name already exists
    if db.session.query(
        MappingTemplate.query.filter_by(type=mappingtype, label=label).exists()
    ).scalar():
        raise Conflict(description="Un mapping de ce type portant ce nom existe déjà")

    MappingClass = FieldMapping if mappingtype == "FIELD" else ContentMapping
    try:
        MappingClass.validate_values(request.json)
    except ValueError as e:
        raise BadRequest(*e.args)

    mapping = MappingClass(
        type=mappingtype, label=label, owners=[g.current_user], values=request.json
    )
    db.session.add(mapping)
    db.session.commit()
    return jsonify(mapping.as_dict())


@blueprint.route("/<mappingtype>mappings/<int:id_mapping>/", methods=["POST"])
@permissions.check_cruved_scope("U", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def update_mapping(mappingtype, mapping, scope):
    """
    .. :quickref: Import; Update a mapping (label and/or content).
    """
    if not mapping.has_instance_permission(scope):
        raise Forbidden

    label = request.args.get("label")
    if label:
        # check if name already exists
        if db.session.query(
            MappingTemplate.query.filter_by(type=mappingtype, label=label).exists()
        ).scalar():
            raise Conflict(description="Un mapping de ce type portant ce nom existe déjà")
        mapping.label = label

    if request.is_json:
        try:
            mapping.validate_values(request.json)
        except ValueError as e:
            raise BadRequest(*e.args)
        if mappingtype == "FIELD":
            mapping.values.update(request.json)
        elif mappingtype == "CONTENT":
            for key, value in request.json.items():
                if key not in mapping.values:
                    mapping.values[key] = value
                else:
                    mapping.values[key].update(value)
            flag_modified(
                mapping, "values"
            )  # nested dict modification not detected by MutableDict

    db.session.commit()
    return jsonify(mapping.as_dict())


@blueprint.route("/<mappingtype>mappings/<int:id_mapping>/", methods=["DELETE"])
@permissions.check_cruved_scope("D", get_scope=True, module_code="IMPORT", object_code="MAPPING")
def delete_mapping(mappingtype, mapping, scope):
    """
    .. :quickref: Import; Delete a mapping.
    """
    if not mapping.has_instance_permission(scope):
        raise Forbidden
    db.session.delete(mapping)
    db.session.commit()
    return "", 204


@blueprint.route("/synthesis/fields", methods=["GET"])
@permissions.check_cruved_scope("C", get_scope=True, module_code="IMPORT")
def get_synthesis_fields(scope):
    """
    .. :quickref: Import; Get synthesis fields.

    Get all synthesis fields
    Use in field mapping steps
    You can find a jsonschema of the returned data in the associated test.
    """
    # TODO use selectinload
    fields = (
        BibFields.query.filter_by(display=True)
        .options(joinedload(BibFields.theme))
        .join(BibThemes)
        .order_by(BibThemes.order_theme, BibFields.order_field)
        .all()
    )
    data = []
    for id_theme, fields in groupby(fields, lambda field: field.id_theme):
        fields = list(fields)
        theme = fields[0].theme
        data.append(
            {
                "theme": theme.as_dict(
                    fields=[
                        "id_theme",
                        "name_theme",
                        "fr_label_theme",
                        "eng_label_theme",
                        "desc_theme",
                    ],
                ),
                "fields": [
                    field.as_dict(
                        fields=[
                            "id_field",
                            "name_field",
                            "fr_label",
                            "eng_label",
                            "desc_field",
                            "mandatory",
                            "autogenerated",
                            "comment",
                        ],
                    )
                    for field in fields
                ],
            }
        )
    return jsonify(data)
