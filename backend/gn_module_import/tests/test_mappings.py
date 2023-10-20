from copy import deepcopy

import pytest
from flask import url_for, g
from werkzeug.exceptions import Unauthorized, Forbidden, BadRequest, Conflict, NotFound
from jsonschema import validate as validate_json
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from geonature.utils.env import db
from geonature.tests.utils import set_logged_user

from pypnnomenclature.models import BibNomenclaturesTypes

from gn_module_import.models import (
    Destination,
    MappingTemplate,
    FieldMapping,
    ContentMapping,
    BibFields,
)

from .jsonschema_definitions import jsonschema_definitions


@pytest.fixture()
def mappings(synthese_destination, users):
    mappings = {}
    fieldmapping_values = {
        field.name_field: True
        if field.autogenerated
        else ([field.name_field] if field.multi else field.name_field)
        for field in (
            BibFields.query.filter_by(
                destination=synthese_destination, display=True
            ).with_entities(BibFields.name_field, BibFields.autogenerated, BibFields.multi)
        )
    }
    contentmapping_values = {
        field.nomenclature_type.mnemonique: {
            nomenclature.cd_nomenclature: nomenclature.cd_nomenclature
            for nomenclature in field.nomenclature_type.nomenclatures
        }
        for field in (
            BibFields.query.filter(
                BibFields.destination == synthese_destination, BibFields.nomenclature_type != None
            ).options(
                joinedload(BibFields.nomenclature_type).joinedload(
                    BibNomenclaturesTypes.nomenclatures
                ),
            )
        )
    }
    with db.session.begin_nested():
        mappings["content_public"] = ContentMapping(
            destination=synthese_destination,
            label="Content Mapping",
            active=True,
            public=True,
            values=contentmapping_values,
        )
        mappings["field_public"] = FieldMapping(
            destination=synthese_destination,
            label="Public Field Mapping",
            active=True,
            public=True,
            values=fieldmapping_values,
        )
        mappings["field"] = FieldMapping(
            destination=synthese_destination,
            label="Private Field Mapping",
            active=True,
            public=False,
        )
        mappings["field_public_disabled"] = FieldMapping(
            destination=synthese_destination,
            label="Disabled Public Field Mapping",
            active=False,
            public=True,
        )
        mappings["self"] = FieldMapping(
            destination=synthese_destination,
            label="Self’s Mapping",
            active=True,
            public=False,
            owners=[users["self_user"]],
        )
        mappings["stranger"] = FieldMapping(
            destination=synthese_destination,
            label="Stranger’s Mapping",
            active=True,
            public=False,
            owners=[users["stranger_user"]],
        )
        mappings["associate"] = FieldMapping(
            destination=synthese_destination,
            label="Associate’s Mapping",
            active=True,
            public=False,
            owners=[users["associate_user"]],
        )
        db.session.add_all(mappings.values())
    return mappings


@pytest.mark.usefixtures("client_class", "temporary_transaction", "default_synthese_destination")
class TestMappings:
    def test_list_mappings(self, users, mappings):
        set_logged_user(self.client, users["noright_user"])

        r = self.client.get(url_for("import.list_mappings", mappingtype="field"))
        assert r.status_code == Forbidden.code

        set_logged_user(self.client, users["admin_user"])

        r = self.client.get(url_for("import.list_mappings", mappingtype="field"))
        assert r.status_code == 200
        validate_json(
            r.get_json(),
            {
                "definitions": jsonschema_definitions,
                "type": "array",
                "items": {"$ref": "#/definitions/mapping"},
                "minItems": len(mappings),
            },
        )

    def test_get_mapping(self, users, mappings):
        def get_mapping(mapping):
            return self.client.get(
                url_for(
                    "import.get_mapping",
                    mappingtype=mapping.type.lower(),
                    id_mapping=mapping.id,
                )
            )

        assert get_mapping(mappings["field_public"]).status_code == Unauthorized.code

        set_logged_user(self.client, users["noright_user"])

        assert get_mapping(mappings["field_public"]).status_code == Forbidden.code

        set_logged_user(self.client, users["self_user"])

        r = self.client.get(
            url_for(
                "import.get_mapping",
                mappingtype="unexisting",
                id_mapping=mappings["content_public"].id,
            )
        )
        assert r.status_code == NotFound.code

        r = self.client.get(
            url_for(
                "import.get_mapping",
                mappingtype="field",
                id_mapping=mappings["content_public"].id,
            )
        )
        assert r.status_code == NotFound.code

        r = self.client.get(
            url_for(
                "import.get_mapping",
                destination="unexisting",
                mappingtype=mappings["content_public"].type.lower(),
                id_mapping=mappings["content_public"].id,
            )
        )
        assert r.status_code == NotFound.code

        unexisting_id = db.session.query(func.max(MappingTemplate.id)).scalar() + 1
        r = self.client.get(
            url_for(
                "import.get_mapping",
                mappingtype="field",
                id_mapping=unexisting_id,
            )
        )
        assert r.status_code == NotFound.code

        assert get_mapping(mappings["content_public"]).status_code == 200
        assert get_mapping(mappings["field_public"]).status_code == 200
        assert get_mapping(mappings["field"]).status_code == Forbidden.code
        assert get_mapping(mappings["field_public_disabled"]).status_code == Forbidden.code

        assert get_mapping(mappings["self"]).status_code == 200
        assert get_mapping(mappings["associate"]).status_code == Forbidden.code
        assert get_mapping(mappings["stranger"]).status_code == Forbidden.code

        set_logged_user(self.client, users["user"])

        assert get_mapping(mappings["self"]).status_code == 200
        assert get_mapping(mappings["associate"]).status_code == 200
        assert get_mapping(mappings["stranger"]).status_code == Forbidden.code

        set_logged_user(self.client, users["admin_user"])

        assert get_mapping(mappings["self"]).status_code == 200
        assert get_mapping(mappings["associate"]).status_code == 200
        assert get_mapping(mappings["stranger"]).status_code == 200

        g.destination = Destination.query.filter_by(code="synthese").one()
        fm = get_mapping(mappings["field_public"]).json
        FieldMapping.validate_values(fm["values"])
        cm = get_mapping(mappings["content_public"]).json
        ContentMapping.validate_values(cm["values"])
        delattr(g, "destination")

    # def test_mappings_permissions(self, users, mappings):
    #    set_logged_user(self.client, users['self_user'])

    #    r = self.client.get(url_for('import.list_mappings', mappingtype='field'))
    #    assert(r.status_code == 200)
    #    mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
    #    assert(mappings['content_public'].id_mapping not in mapping_ids)  # wrong mapping type
    #    assert(mappings['field_public'].id_mapping in mapping_ids)
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['field_public'].id_mapping)).status_code == 200)
    #    assert(mappings['field'].id_mapping not in mapping_ids)  # not public
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['field'].id_mapping)).status_code == Forbidden.code)
    #    assert(mappings['field_public_disabled'].id_mapping not in mapping_ids)  # not active
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['field_public_disabled'].id_mapping)).status_code == Forbidden.code)
    #    assert(mappings['self'].id_mapping in mapping_ids)  # not public but user is owner
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['self'].id_mapping)).status_code == 200)
    #    assert(mappings['stranger'].id_mapping not in mapping_ids)  # not public and owned by another user
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['stranger'].id_mapping)).status_code == Forbidden.code)
    #    assert(mappings['associate'].id_mapping not in mapping_ids)  # not public and owned by an user in the same organism whereas read scope is 1
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['associate'].id_mapping)).status_code == Forbidden.code)

    #    set_logged_user(self.client, users['user'])

    #    # refresh mapping list
    #    r = self.client.get(url_for('import.list_mappings', mappingtype='field'))
    #    mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
    #    assert(mappings['stranger'].id_mapping not in mapping_ids)  # not public and owned by another user
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['stranger'].id_mapping)).status_code == Forbidden.code)
    #    assert(mappings['associate'].id_mapping in mapping_ids)  # not public but owned by an user in the same organism whereas read scope is 2
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['associate'].id_mapping)).status_code == 200)

    #    set_logged_user(self.client, users['admin_user'])

    #    # refresh mapping list
    #    r = self.client.get(url_for('import.list_mappings', mappingtype='field'))
    #    mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
    #    assert(mappings['stranger'].id_mapping in mapping_ids)  # not public and owned by another user but we have scope = 3
    #    assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['stranger'].id_mapping)).status_code == 200)

    def test_add_field_mapping(self, users, mappings):
        fieldmapping = {
            "WKT": "geometrie",
            "nom_cite": "nomcite",
            "cd_nom": "cdnom",
            "cd_hab": "cdhab",
            "observers": "observateurs",
        }
        url = url_for("import.add_mapping", mappingtype="field")

        assert self.client.post(url, data=fieldmapping).status_code == Unauthorized.code

        set_logged_user(self.client, users["user"])

        # label is missing
        assert self.client.post(url, data=fieldmapping).status_code == BadRequest.code

        # label already exist
        url = url_for(
            "import.add_mapping",
            mappingtype="field",
            label=mappings["field"].label,
        )
        assert self.client.post(url, data=fieldmapping).status_code == Conflict.code

        # label may be reused between field and content
        url = url_for(
            "import.add_mapping",
            mappingtype="field",
            label=mappings["content_public"].label,
        )

        r = self.client.post(url, data={"unexisting": "source column"})
        assert r.status_code == BadRequest.code

        r = self.client.post(url, data=fieldmapping)
        assert r.status_code == BadRequest.code  # missing date_min

        fieldmapping.update(
            {
                "date_min": "date_debut",
            }
        )
        r = self.client.post(url, data=fieldmapping)
        assert r.status_code == 200, r.json
        assert r.json["label"] == mappings["content_public"].label
        assert r.json["type"] == "FIELD"
        assert r.json["values"] == fieldmapping
        mapping = db.session.get(MappingTemplate, r.json["id"])
        assert mapping.owners == [users["user"]]

    def test_add_content_mapping(self, users, mappings):
        url = url_for(
            "import.add_mapping",
            mappingtype="content",
            label="test content mapping",
        )
        set_logged_user(self.client, users["user"])

        contentmapping = {
            "NAT_OBJ_GEO": {
                "ne sais pas": "invalid",
            },
        }
        r = self.client.post(url, data=contentmapping)
        assert r.status_code == BadRequest.code

        contentmapping = {
            "NAT_OBJ_GEO": {
                "ne sais pas": "NSP",
                "ne sais toujours pas": "NSP",
            },
        }
        r = self.client.post(url, data=contentmapping)
        assert r.status_code == 200
        assert r.json["label"] == "test content mapping"
        assert r.json["type"] == "CONTENT"
        assert r.json["values"] == contentmapping
        mapping = db.session.get(MappingTemplate, r.json["id"])
        assert mapping.owners == [users["user"]]

    def test_update_mapping_label(self, users, mappings):
        mapping = mappings["associate"]

        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=mapping.type.lower(),
                id_mapping=mapping.id,
            )
        )
        assert r.status_code == Unauthorized.code

        set_logged_user(self.client, users["self_user"])

        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=mapping.type.lower(),
                id_mapping=mapping.id,
            )
        )
        assert r.status_code == Forbidden.code

        set_logged_user(self.client, users["user"])

        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=mapping.type.lower(),
                id_mapping=mapping.id,
                label=mappings["field"].label,
            )
        )
        assert r.status_code == Conflict.code

        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype="content",
                id_mapping=mapping.id,
                label="New mapping label",
            )
        )
        assert r.status_code == NotFound.code

        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=mapping.type.lower(),
                id_mapping=mapping.id,
                label="New mapping label",
            )
        )
        assert r.status_code == 200
        assert mappings["associate"].label == "New mapping label"

    def test_update_field_mapping_values(self, users, mappings):
        set_logged_user(self.client, users["admin_user"])

        fm = mappings["field_public"]
        fieldvalues_update = deepcopy(fm.values)
        fieldvalues_update["WKT"] = "WKT2"
        fieldvalues_should = deepcopy(fieldvalues_update)
        del fieldvalues_update["validator"]  # should not removed from mapping!
        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=fm.type.lower(),
                id_mapping=fm.id,
            ),
            data=fieldvalues_update,
        )
        assert r.status_code == 200
        assert fm.values == fieldvalues_should
        fieldvalues_update = deepcopy(fm.values)
        fieldvalues_update["unexisting"] = "unexisting"
        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=fm.type.lower(),
                id_mapping=fm.id,
            ),
            data=fieldvalues_update,
        )
        assert r.status_code == BadRequest.code
        assert fm.values == fieldvalues_should

    def test_update_content_mapping_values(self, users, mappings):
        set_logged_user(self.client, users["admin_user"])
        cm = mappings["content_public"]
        contentvalues_update = deepcopy(cm.values)
        del cm.values["METH_OBS"]  # to test adding of new key in mapping
        contentvalues_update["NAT_OBJ_GEO"]["In"] = "St"  # change existing mapping
        contentvalues_update["NAT_OBJ_GEO"]["ne sais pas"] = "NSP"  # add new mapping
        contentvalues_should = deepcopy(contentvalues_update)
        del contentvalues_update["NAT_OBJ_GEO"]["St"]  # should not be removed!
        db.session.flush()
        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=cm.type.lower(),
                id_mapping=cm.id,
            ),
            data=contentvalues_update,
        )
        assert r.status_code == 200
        assert cm.values == contentvalues_should
        contentvalues_update = deepcopy(cm.values)
        contentvalues_update["NAT_OBJ_GEO"] = "invalid"
        r = self.client.post(
            url_for(
                "import.update_mapping",
                mappingtype=cm.type.lower(),
                id_mapping=cm.id,
            ),
            data=contentvalues_update,
        )
        assert r.status_code == BadRequest.code
        assert cm.values == contentvalues_should

    def test_delete_mapping(self, users, mappings):
        mapping = mappings["associate"]
        r = self.client.delete(
            url_for(
                "import.delete_mapping",
                mappingtype=mapping.type.lower(),
                id_mapping=mapping.id,
            )
        )
        assert r.status_code == Unauthorized.code
        assert db.session.get(MappingTemplate, mapping.id) is not None

        set_logged_user(self.client, users["self_user"])
        r = self.client.delete(
            url_for(
                "import.delete_mapping",
                mappingtype=mapping.type.lower(),
                id_mapping=mapping.id,
            )
        )
        assert r.status_code == Forbidden.code
        assert db.session.get(MappingTemplate, mapping.id) is not None

        set_logged_user(self.client, users["user"])
        r = self.client.delete(
            url_for(
                "import.delete_mapping",
                mappingtype="content",
                id_mapping=mapping.id,
            )
        )
        assert r.status_code == NotFound.code
        assert db.session.get(MappingTemplate, mapping.id) is not None

        r = self.client.delete(
            url_for(
                "import.delete_mapping",
                mappingtype=mapping.type.lower(),
                id_mapping=mapping.id,
            )
        )
        assert r.status_code == 204
        assert db.session.get(MappingTemplate, mapping.id) is None
