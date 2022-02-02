from pathlib import Path

import pytest
from flask import testing, url_for
from werkzeug.datastructures import Headers
from werkzeug.exceptions import Unauthorized, Forbidden, BadRequest, Conflict
from jsonschema import validate as validate_json
from sqlalchemy import func

from geonature.utils.env import DB as db
from geonature.tests.utils import set_logged_user_cookie
from geonature.core.gn_permissions.tools import UserCruved
from geonature.core.gn_permissions.models import TActions, TFilters, CorRoleActionFilterModuleObject
from geonature.core.gn_commons.models import TModules
from geonature.core.gn_meta.models import TNomenclatures, TAcquisitionFramework, TDatasets

from pypnnomenclature.models import BibNomenclaturesTypes
from pypnusershub.db.models import User, Organisme, Application, Profils as Profil, UserApplicationRight

from gn_module_import.db.models import TImports, TMappings, TMappingsFields, TMappingsValues, \
                                       BibThemes, BibFields, ImportUserError, ImportUserErrorType
from gn_module_import.steps import Step
from gn_module_import.utils.imports import get_table_class, get_import_table_name, \
                                           get_archive_table_name

from .jsonschema_definitions import jsonschema_definitions


tests_path = Path(__file__).parent



@pytest.fixture()
def mappings(users):
    mappings = {}
    with db.session.begin_nested():
        mappings['content_public'] = TMappings(mapping_type='CONTENT', mapping_label='Content Mapping', active=True, is_public=True)
        mappings['field_public'] = TMappings(mapping_type='FIELD', mapping_label='Public Field Mapping', active=True, is_public=True)
        mappings['field'] = TMappings(mapping_type='FIELD', mapping_label='Private Field Mapping', active=True, is_public=False)
        mappings['field_public_disabled'] = TMappings(mapping_type='FIELD', mapping_label='Disabled Public Field Mapping', active=False, is_public=True)
        mappings['self'] = TMappings(mapping_type='FIELD', mapping_label='Self’s Mapping', active=True, is_public=False)
        mappings['self'].owners.append(users['self_user'])
        mappings['stranger'] = TMappings(mapping_type='FIELD', mapping_label='Stranger’s Mapping', active=True, is_public=False)
        mappings['stranger'].owners.append(users['stranger_user'])
        mappings['associate'] = TMappings(mapping_type='FIELD', mapping_label='Associate’s Mapping', active=True, is_public=False)
        mappings['associate'].owners.append(users['associate_user'])
        db.session.add_all(mappings.values())
    return mappings


@pytest.mark.usefixtures("client_class", "temporary_transaction")
class TestMappings:
    def test_mapping_list(self, users, mappings):
        set_logged_user_cookie(self.client, users['noright_user'])

        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        assert(r.status_code == Forbidden.code)

        set_logged_user_cookie(self.client, users['admin_user'])

        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        assert(r.status_code == 200)
        validate_json(r.get_json(), {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/mapping' },
            'minItems': 1,
        })

    def test_mapping_permissions(self, users, mappings):
        set_logged_user_cookie(self.client, users['self_user'])

        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        assert(r.status_code == 200)
        mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
        assert(mappings['content_public'].id_mapping not in mapping_ids)  # wrong mapping type
        assert(mappings['field_public'].id_mapping in mapping_ids)
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['field_public'].id_mapping)).status_code == 200)
        assert(mappings['field'].id_mapping not in mapping_ids)  # not public
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['field'].id_mapping)).status_code == Forbidden.code)
        assert(mappings['field_public_disabled'].id_mapping not in mapping_ids)  # not active
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['field_public_disabled'].id_mapping)).status_code == Forbidden.code)
        assert(mappings['self'].id_mapping in mapping_ids)  # not public but user is owner
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['self'].id_mapping)).status_code == 200)
        assert(mappings['stranger'].id_mapping not in mapping_ids)  # not public and owned by another user
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['stranger'].id_mapping)).status_code == Forbidden.code)
        assert(mappings['associate'].id_mapping not in mapping_ids)  # not public and owned by an user in the same organism whereas read scope is 1
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['associate'].id_mapping)).status_code == Forbidden.code)

        set_logged_user_cookie(self.client, users['user'])

        # refresh mapping list
        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
        assert(mappings['stranger'].id_mapping not in mapping_ids)  # not public and owned by another user
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['stranger'].id_mapping)).status_code == Forbidden.code)
        assert(mappings['associate'].id_mapping in mapping_ids)  # not public but owned by an user in the same organism whereas read scope is 2
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['associate'].id_mapping)).status_code == 200)

        set_logged_user_cookie(self.client, users['admin_user'])

        # refresh mapping list
        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
        assert(mappings['stranger'].id_mapping in mapping_ids)  # not public and owned by another user but we have scope = 3
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mappings['stranger'].id_mapping)).status_code == 200)


    def test_add_mapping(self, users):
        r = self.client.post(url_for('import.add_mapping'), data={'name': 'test', 'type': 'field'})
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users['user'])
        r = self.client.post(url_for('import.add_mapping'), data={'name': 'test', 'type': 'invalid'})
        assert(r.status_code == 400)
        r = self.client.post(url_for('import.add_mapping'), data={'name': 'test', 'type': 'field'})
        assert(r.status_code == 200)
        mapping1 = r.get_json()
        assert(mapping1['mapping_label'] == 'test')
        assert(mapping1['mapping_type'] == 'FIELD')
        mapping1 = TMappings.query.get(mapping1['id_mapping'])
        assert(len(mapping1.owners) == 1)
        r = self.client.post(url_for('import.add_mapping'), data={'name': '', 'type': 'field'})
        assert(r.status_code == 200)
        mapping2 = TMappings.query.get(r.get_json()['id_mapping'])
        r = self.client.post(url_for('import.add_mapping'), data={'name': '', 'type': 'field'})
        assert(r.status_code == 200)
        mapping3 = TMappings.query.get(r.get_json()['id_mapping'])
        # be sure we can create 2 unnamed mapping (a.k.a. temporary mapping)
        assert(mapping2.id_mapping != mapping3.id_mapping)


    def test_rename_mapping(self, users):
        with db.session.begin_nested():
            mapping1 = TMappings(mapping_label='mapping1', mapping_type='FIELD', active=True)
            db.session.add(mapping1)
            mapping2 = TMappings(mapping_label='mapping2', mapping_type='FIELD', active=True)
            db.session.add(mapping2)
        r = self.client.post(url_for('import.rename_mapping', id_mapping=mapping1.id_mapping),
                        data={'name': 'mapping2'})
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.post(url_for('import.rename_mapping', id_mapping=mapping1.id_mapping),
                        data={'name': 'mapping2'})
        assert(r.status_code == Conflict.code)
        mapping = TMappings.query.get(mapping1.id_mapping)
        assert(mapping.mapping_label == 'mapping1')
        r = self.client.post(url_for('import.rename_mapping', id_mapping=mapping1.id_mapping),
                        data={'name': 'mapping3'})
        assert(r.status_code == 200)
        assert(r.get_json()['mapping_label'] == 'mapping3')
        mapping = TMappings.query.get(mapping1.id_mapping)
        assert(mapping.mapping_label == 'mapping3')


    def test_delete_mapping(self, users):
        schema = {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/mapping' },
            'minItems': 1,
        }
        with db.session.begin_nested():
            mapping1 = TMappings(mapping_label=None, mapping_type='FIELD', active=True)
            db.session.add(mapping1)
            mapping2 = TMappings(mapping_label='mapping 2', mapping_type='FIELD', active=True)
            db.session.add(mapping2)
            imprt = TImports(field_mapping=mapping2)
            db.session.add(imprt)
            mapping3 = TMappings(mapping_label='mapping 3', mapping_type='FIELD', active=True)
            db.session.add(mapping3)
            mapping4 = TMappings(mapping_label='mapping 4', mapping_type='CONTENT', active=True)
            db.session.add(mapping4)
            mapping5 = TMappings(mapping_label='mapping 5', mapping_type='CONTENT', active=True)
            db.session.add(mapping5)
        r = self.client.delete(url_for('import.delete_mapping', id_mapping=mapping1.id_mapping))
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.delete(url_for('import.delete_mapping', id_mapping=mapping1.id_mapping))
        assert(r.status_code == BadRequest.code)
        r = self.client.delete(url_for('import.delete_mapping', id_mapping=mapping2.id_mapping))
        assert(r.status_code == 200)
        mappings = r.get_json()
        validate_json(mappings, schema)
        mappings_ids = { mapping['id_mapping'] for mapping in mappings }
        assert({ mapping2.id_mapping, mapping4.id_mapping, mapping5.id_mapping }.isdisjoint(mappings_ids))
        assert(TMappings.query.get(mapping2.id_mapping).mapping_label == None) # le mapping 2 n’est pas renvoyé mais existe encore (temporaire)
        assert({ mapping3.id_mapping }.issubset(mappings_ids))
        r = self.client.delete(url_for('import.delete_mapping', id_mapping=mapping4.id_mapping))
        assert(r.status_code == 200)
        mappings = r.get_json()
        validate_json(mappings, schema)
        mappings_ids = { mapping['id_mapping'] for mapping in mappings }
        assert({ mapping2.id_mapping, mapping3.id_mapping, mapping4.id_mapping }.isdisjoint(mappings_ids))
        assert({ mapping5.id_mapping }.issubset(mappings_ids))
        assert(TMappings.query.get(mapping4.id_mapping) == None)


    def test_get_mapping_fields(self, users):
        with db.session.begin_nested():
            mapping = TMappings(mapping_label="test", mapping_type="FIELD", active=True)
            db.session.add(mapping)
        r = self.client.get(url_for('import.get_mapping_fields', id_mapping=mapping.id_mapping))
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.get(url_for('import.get_mapping_fields', id_mapping=mapping.id_mapping))
        assert(r.status_code == 200)
        data = r.get_json()
        assert(data == [])
        target = BibFields.query.first()
        with db.session.begin_nested():
            db.session.add(TMappingsFields(mapping=mapping, source_field='source', target_field=target.name_field))
        r = self.client.get(url_for('import.get_mapping_fields', id_mapping=mapping.id_mapping))
        assert(r.status_code == 200)
        data = r.get_json()
        validate_json(data, {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/mappingfield' },
            'minItems': 1,
            'maxItems': 1,
        })
        assert(data[0]['id_mapping'] == mapping.id_mapping)


    def test_update_mapping_fields(self, users):
        with db.session.begin_nested():
            mapping = TMappings(mapping_label="test", mapping_type="FIELD", active=True)
            db.session.add(mapping)
            target_fields = BibFields.query.filter_by(display=True)
            target_1 = target_fields.filter_by(autogenerated=True)[0].name_field
            fieldmapping1 = TMappingsFields(mapping=mapping, source_field=True, target_field=target_1)
            db.session.add(fieldmapping1)
            target_2 = target_fields.filter_by(autogenerated=False)[0].name_field
            fieldmapping2 = TMappingsFields(mapping=mapping, source_field="source_1", target_field=target_2)
            db.session.add(fieldmapping2)
        target_3 = target_fields.filter_by(autogenerated=False)[1].name_field
        id_mapping = mapping.id_mapping  # save id mapping to re-use after session expunge
        db.session.expunge_all()  # detach previous created object from db session to avoid merge conflict (merge is used in the view)
        mappings = {
            target_2: 'source_2',
            target_3: 'source_3',
        }
        r = self.client.post(url_for('import.update_mapping_fields', id_mapping=id_mapping), data=mappings)
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.post(url_for('import.update_mapping_fields', id_mapping=id_mapping), data=mappings)
        assert(r.status_code == 200)
        fieldmappings = { (mf.target_field, mf.source_field) for mf in TMappingsFields.query.filter_by(id_mapping=id_mapping).all() }
        # we should get target 1 un-changed, target 2 updated to be mapped to source 2, and the new target 3 mapped to source 3 
        assert(fieldmappings == { (target_1, 'true'), (target_2, 'source_2'), (target_3, 'source_3') })
        data = r.get_json()
        validate_json(data, {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/mappingfield' },
            'minItems': 3,
            'maxItems': 3,
        })
        assert(data[0]['id_mapping'] == id_mapping)


    def test_get_mapping_contents(self, users):
        with db.session.begin_nested():
            mapping = TMappings(mapping_label="test", mapping_type="CONTENT", active=True)
            db.session.add(mapping)
        r = self.client.get(url_for('import.get_mapping_contents', id_mapping=mapping.id_mapping))
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.get(url_for('import.get_mapping_contents', id_mapping=mapping.id_mapping))
        assert(r.status_code == 200)
        data = r.get_json()
        assert(data == [])
        # get the first standardized field
        target_field = BibFields.query.filter(BibFields.nomenclature_type != None).first()
        # get the first nomenclature acceptable for this field
        target_value = TNomenclatures.query.filter_by(id_type=target_field.nomenclature_type.id_type).first()
        with db.session.begin_nested():
            db.session.add(TMappingsValues(mapping=mapping, target_field=target_field, source_value='source', target_value=target_value))
        r = self.client.get(url_for('import.get_mapping_contents', id_mapping=mapping.id_mapping))
        assert(r.status_code == 200)
        data = r.get_json()
        schema = {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/mappingcontent' },
            'minItems': 1,
            'maxItems': 1,
        }
        validate_json(data, schema)


    def test_update_mapping_contents(self, users):
        with db.session.begin_nested():
            mapping = TMappings(mapping_label="test", mapping_type="CONTENT", active=True)
            db.session.add(mapping)
            # Select a standardized field, with at least 4 acceptable nomenclatures (to have a sufficient amount of values for testing)
            target_field, _ = db.session.query(BibFields, func.count(TNomenclatures.id_nomenclature)) \
                .join(BibNomenclaturesTypes, BibNomenclaturesTypes.mnemonique == BibFields.mnemonique) \
                .join(TNomenclatures, TNomenclatures.id_type == BibNomenclaturesTypes.id_type) \
                .filter(BibFields.display == True, BibFields.nomenclature_type != None) \
                .group_by(BibFields.id_field) \
                .having(func.count(TNomenclatures.id_nomenclature) >= 4) \
                .first()
            nomenclatures = target_field.nomenclature_type.nomenclatures
            contentmapping1 = TMappingsValues(mapping=mapping, target_field=target_field, source_value='', target_value=nomenclatures[0])
            db.session.add(contentmapping1)
            contentmapping2 = TMappingsValues(mapping=mapping, target_field=target_field, source_value='value 1', target_value=nomenclatures[1])
            db.session.add(contentmapping2)
        r = self.client.post(url_for('import.update_mapping_contents', mapping_id=mapping.id_mapping), data=[])
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.post(url_for('import.update_mapping_contents', mapping_id=mapping.id_mapping), data=[{'wrong': 'format'}])
        assert(r.status_code == BadRequest.code)
        data = [
            {'target_field_name': target_field.name_field, 'source_value': None, 'target_id_nomenclature': nomenclatures[2].id_nomenclature},
            {'target_field_name': target_field.name_field, 'source_value': 'value 2', 'target_id_nomenclature': nomenclatures[3].id_nomenclature},
        ]
        r = self.client.post(url_for('import.update_mapping_contents', mapping_id=mapping.id_mapping), data=data)
        assert(r.status_code == 200)
        data = r.get_json()
        validate_json(data, {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/mappingcontent' },
            'minItems': 3,
            'maxItems': 3,
        })
        cm = TMappingsValues.query.filter_by(mapping=mapping, target_field=target_field, source_value='').one()
        assert(cm and cm.target_value == nomenclatures[2])  # updated
        cm = TMappingsValues.query.filter_by(mapping=mapping, target_field=target_field, source_value='value 1').one()
        assert(cm and cm.target_value == nomenclatures[1])  # not updated
        cm = TMappingsValues.query.filter_by(mapping=mapping, target_field=target_field, source_value='value 2').one()
        assert(cm and cm.target_value == nomenclatures[3])  # created
