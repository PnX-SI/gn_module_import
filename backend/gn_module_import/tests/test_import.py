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



@pytest.fixture(scope='class')
def imports(users):
    def create_import(authors=[]):
        with db.session.begin_nested():
            imprt = TImports(authors=authors)
            db.session.add(imprt)
        return imprt
    return {
        'own_import': create_import(authors=[users['user']]),
        'associate_import': create_import(authors=[users['associate_user']]),
        'stranger_import': create_import(authors=[users['stranger_user']]),
        'orphan_import': create_import(),
    }


@pytest.mark.usefixtures("client_class", "temporary_transaction")
class TestImports:
    def test_import_permissions(self):
        with db.session.begin_nested():
            organisme = Organisme(nom_organisme='test_import')
            db.session.add(organisme)
            group = User(groupe=True)
            db.session.add(group)
            user = User(groupe=False)
            db.session.add(user)
            other_user = User(groupe=False, organisme=organisme)
            db.session.add(other_user)
            user.groups.append(group)
            imprt = TImports()
            db.session.add(imprt)

        cruved = UserCruved(id_role=user.id_role, code_filter_type='SCOPE',
                            module_code='IMPORT', object_code='IMPORT')
        for action in 'CRUVED':
            info_role = cruved.get_herited_user_cruved_by_action(action)
            assert(info_role is None)

        update_action = TActions.query.filter(TActions.code_action == 'U').one()
        none_filter, self_filter, organism_filter, all_filter = [
            TFilters.query.filter(TFilters.value_filter == str(i)).one()
            for i in [ 0, 1, 2, 3 ]
        ]
        geonature_module, import_module = [
            TModules.query.filter(TModules.module_code == module_code).one()
            for module_code in [ 'GEONATURE', 'IMPORT' ]
        ]

        # Add permission for it-self
        with db.session.begin_nested():
            permission = CorRoleActionFilterModuleObject(
                    role=user, action=update_action,
                    filter=self_filter, module=geonature_module)
            db.session.add(permission)
        info_role = cruved.get_herited_user_cruved_by_action("U")[0]
        assert(info_role)
        assert(info_role.id_organisme is None)
        scope = int(info_role.value_filter)
        assert(scope == 1)
        with pytest.raises(Forbidden):
            imprt.check_instance_permission(scope, user=user)
        imprt.authors.append(user)
        imprt.check_instance_permission(scope, user=user)

        # Change permission to organism filter
        permission.filter = organism_filter
        db.session.commit()
        info_role = cruved.get_herited_user_cruved_by_action("U")[0]
        assert(info_role)
        scope = int(info_role.value_filter)
        assert(scope == 2)
        imprt.check_instance_permission(scope, user=user)  # right as we still are author
        imprt.authors.remove(user)
        with pytest.raises(Forbidden):
            imprt.check_instance_permission(scope, user=user)
        imprt.authors.append(other_user)
        db.session.commit()
        with pytest.raises(Forbidden):  # we are not in the same organism than other_user
            imprt.check_instance_permission(scope, user=user)
        organisme.members.append(user)
        db.session.commit()
        info_role = cruved.get_herited_user_cruved_by_action("U")[0]
        scope = int(info_role.value_filter)
        imprt.check_instance_permission(scope, user=user)

        permission.filter = all_filter
        imprt.authors.remove(other_user)
        db.session.commit()
        info_role = cruved.get_herited_user_cruved_by_action("U")[0]
        assert(info_role)
        scope = int(info_role.value_filter)
        assert(scope == 3)
        imprt.check_instance_permission(scope, user=user)


    def test_list_imports(self, imports, users):
        r = self.client.get(url_for('import.get_import_list'))
        assert r.status_code == Unauthorized.code
        set_logged_user_cookie(self.client, users['noright_user'])
        r = self.client.get(url_for('import.get_import_list'))
        assert r.status_code == Forbidden.code
        set_logged_user_cookie(self.client, users['user'])
        r = self.client.get(url_for('import.get_import_list'))
        assert r.status_code == 200
        json_data = r.get_json()
        validate_json(json_data, {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/import' },
        })
        imports_ids = [ imprt['id_import'] for imprt in json_data ]
        assert(set(imports_ids) == { imports[imprt].id_import for imprt in ['own_import', 'associate_import'] })

    def test_get_import(self, users):
        with db.session.begin_nested():
            imprt = TImports()
            db.session.add(imprt)
        r =  self.client.get(url_for('import.get_one_import', import_id=imprt.id_import))
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r =  self.client.get(url_for('import.get_one_import', import_id=imprt.id_import))
        assert(r.status_code == 200)
        imprt_json = r.get_json()
        assert(imprt_json['id_import'] == imprt.id_import)
    
    def test_delete_import(self, users):
        with db.session.begin_nested():
            imprt = TImports()
            db.session.add(imprt)
        import_id = imprt.id_import
        r = self.client.delete(url_for('import.delete_import', import_id=imprt.id_import))
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.delete(url_for('import.delete_import', import_id=imprt.id_import))
        assert(r.status_code == 200)
        TImports.query.get(import_id)
        # TODO: check data from synthese, and import tables are also removed (need a more avanced import)
        r = self.client.delete(url_for('import.delete_import', import_id=imprt.id_import))
        assert(r.status_code == 404)

    def test_import_columns(self, users, datasets):
        set_logged_user_cookie(self.client, users['user'])
        with open(tests_path / 'files' / 'one_line.csv', 'rb') as f:
            data = {
                'file': (f, 'one_line.csv'),
                'datasetId': datasets['own_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
        assert(r.status_code == 200)
        imprt_json = r.get_json()
        import_id = imprt_json['id_import']
        r = self.client.get(url_for('import.get_import_columns_name', import_id=import_id))
        assert(r.status_code == Conflict.code)
        data = {
            'encoding': 'utf-8',
            'format': 'csv',
            'srid': 2154,
        }
        r = self.client.post(url_for('import.decode_file', import_id=import_id), data=data)
        assert(r.status_code == 200)
        imprt = TImports.query.get(import_id)
        r = self.client.get(f'/import/imports/{import_id}/columns')
        assert(r.status_code == 200)
        data = r.get_json()
        assert('cd_nom' in data)

    def test_import_preview(self, users):
        r = self.client.get(url_for('import.preview_valid_data', import_id=0))
        assert(r.status_code == Unauthorized.code)  # FIXME
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.get(url_for('import.preview_valid_data', import_id=1))
        assert(r.status_code == 404)
        # TODO validate response

    def test_import_values(self, users, datasets):
        r = self.client.get(url_for('import.get_import_values', import_id=1))
        assert(r.status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users['user'])
        with open(tests_path / 'files' / 'few_lines.csv', 'rb') as f:
            data = {
                'file': (f, 'one_line.csv'),
                'datasetId': datasets['own_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
        assert(r.status_code == 200)
        imprt_json = r.get_json()
        import_id = imprt_json['id_import']
        r = self.client.get(f'/import/imports/{import_id}/columns')
        assert(r.status_code == Conflict.code)
        data = {
            'encoding': 'utf-8',
            'format': 'csv',
            'srid': 2154,
        }
        r = self.client.post(url_for('import.decode_file', import_id=import_id), data=data)
        assert(r.status_code == 200)
        fieldmapping = TMappings.query.filter_by(mapping_type='FIELD', mapping_label='Synthese GeoNature').one()
        r = self.client.post(url_for('import.set_import_field_mapping', import_id=import_id),
                        data={'id_field_mapping': fieldmapping.id_mapping})
        assert(r.status_code == 200)
        r = self.client.get(url_for('import.get_import_values', import_id=import_id))
        assert(r.status_code == 200)
        data = r.get_json()
        schema = {
            'definitions': jsonschema_definitions,
            'type': 'object',
            'patternProperties': {
                '^.*$': {  # keys are synthesis fields
                    'type': 'object',
                    'properties': {
                        'nomenclature_type': { '$ref': '#/definitions/nomenclature_type' },
                        'nomenclatures': {  # list of acceptable nomenclatures for this field
                            'type': 'array',
                            'items': { '$ref': '#/definitions/nomenclature' },
                            'minItems': 1,
                        },
                        'values': {  # available user values in uploaded file for this field
                            'type': 'array',
                            'items': {
                                'type': ['string', 'null',],
                            },
                        },
                    },
                    'required': [
                        'nomenclature_type',
                        'nomenclatures',
                        'values',
                    ],
                },
            },
        }
        validate_json(data, schema)

    def test_import_errors(self, users, imports):
        set_logged_user_cookie(self.client, users['user'])
        r = self.client.get(url_for('import.get_import_errors', import_id=imports['own_import'].id_import))
        assert(r.status_code == 200)
        errors = r.get_json()
        assert(len(errors) == 0)


    def test_import_invalid_file(self, users, datasets):
        set_logged_user_cookie(self.client, users['user'])

        # Upload step
        test_file_name = 'invalid_file.csv'
        test_file_line_count = 3
        with open(tests_path / 'files' / test_file_name, 'rb') as f:
            data = {
                'file': (f, test_file_name),
                'datasetId': datasets['own_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
        assert(r.status_code == 200)
        imprt_json = r.get_json()
        imprt = TImports.query.get(imprt_json['id_import'])
        assert(len(imprt.authors) == 1)
        assert(imprt_json['date_create_import'])
        assert(imprt_json['date_update_import'])
        assert(imprt_json['detected_encoding'] == 'utf-8')
        assert(imprt_json['detected_format'] == 'csv')
        assert(imprt_json['full_file_name'] == test_file_name)
        assert(imprt_json['id_dataset'] == datasets['own_dataset'].id_dataset)
        assert(imprt_json['step'] == Step.Decode)

        assert(not db.session.query(ImportUserError.query.filter_by(imprt=imprt).exists()).scalar())

        # Decode step
        data = {
            'encoding': 'utf-8',
            'format': 'csv',
            'srid': 2154,
        }
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert(r.status_code == 400)
        # print(r.get_json())
        # assert(r.status_code == 200)
        # imprt_json = r.get_json()
        # assert(imprt_json['date_update_import'])
        # assert(imprt_json['encoding'] == 'utf-8')
        # assert(imprt_json['format_source_file'] == 'csv')
        # assert(imprt_json['srid'] == 2154)
        # assert(imprt_json['source_count'] is None)
        # assert(imprt_json['step'] == Step.Decode)  # style same step
        # assert(imprt_json['import_table'] is None)

        # assert(db.session.query(ImportUserError.query.filter_by(imprt=imprt).exists()).scalar())

        # r = self.client.get(url_for('import.get_import_errors', import_id=imprt.id_import))
        # assert(r.status_code == 200)
        # errors = r.get_json()
        # assert(len(errors) == 1)

    @pytest.mark.xfail(reason="There is an issue with date_max")
    def test_import_valid_file(self, users, datasets):
        set_logged_user_cookie(self.client, users['user'])

        # Upload step
        test_file_name = 'valid_file.csv'
        with open(tests_path / 'files' / test_file_name, 'rb') as f:
            test_file_line_count = sum(1 for line in f) - 1  # remove headers
            f.seek(0)
            data = {
                'file': (f, test_file_name),
                'datasetId': datasets['own_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
        assert(r.status_code == 200)
        imprt_json = r.get_json()
        imprt = TImports.query.get(imprt_json['id_import'])
        assert(len(imprt.authors) == 1)
        assert(imprt_json['date_create_import'])
        assert(imprt_json['date_update_import'])
        assert(imprt_json['detected_encoding'] == 'utf-8')
        assert(imprt_json['detected_format'] == 'csv')
        assert(imprt_json['full_file_name'] == test_file_name)
        assert(imprt_json['id_dataset'] == datasets['own_dataset'].id_dataset)
        assert(imprt_json['step'] == Step.Decode)


        # Decode step
        data = {
            'encoding': 'utf-8',
            'format': 'csv',
            'srid': 4326,
        }
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert(r.status_code == 200)
        imprt_json = r.get_json()
        assert(imprt_json['date_update_import'])
        assert(imprt_json['encoding'] == 'utf-8')
        assert(imprt_json['format_source_file'] == 'csv')
        assert(imprt_json['srid'] == 4326)
        assert(imprt_json['source_count'] == test_file_line_count)
        assert(imprt_json['step'] == Step.FieldMapping)
        assert(imprt_json['import_table'])
        assert(imprt_json['columns'])
        assert(len(imprt_json['columns']) > 0)
        imprt = TImports.query.get(imprt.id_import)
        ImportEntry = get_table_class(get_import_table_name(imprt))
        assert(db.session.query(ImportEntry).count() == imprt_json['source_count'])
        ImportArchiveEntry = get_table_class(get_archive_table_name(imprt))
        assert(db.session.query(ImportArchiveEntry).count() == imprt_json['source_count'])

        # Field mapping step
        fieldmapping = TMappings.query.filter_by(mapping_label='Synthese GeoNature').one()
        data = {
            'id_field_mapping': fieldmapping.id_mapping,
            'step': Step.ContentMapping,
        }
        r = self.client.post(url_for('import.set_import_field_mapping', import_id=imprt.id_import), data=data)
        assert(r.status_code == 200)
        data = r.get_json()
        validate_json(data, {'definitions': jsonschema_definitions, '$ref': '#/definitions/import'})
        assert(data['id_field_mapping'] == fieldmapping.id_mapping)
        assert(data['step'] == Step.ContentMapping)

        # Content mapping step
        contentmapping = TMappings.query.filter_by(mapping_label='Nomenclatures SINP (labels)').one()
        data = {
            'id_content_mapping': contentmapping.id_mapping,
        }
        r = self.client.post(url_for('import.set_import_content_mapping', import_id=imprt.id_import), data=data)
        assert(r.status_code == 200)
        data = r.get_json()
        validate_json(data, {'definitions': jsonschema_definitions, '$ref': '#/definitions/import'})
        assert(data['id_content_mapping'] == contentmapping.id_mapping)
        assert(data['step'] == Step.ContentMapping)

        # Prepare data before import
        r = self.client.post(url_for('import.prepare_import', import_id=imprt.id_import))
        assert(r.status_code == 200)
        data = r.get_json()
        validate_json(data, {'definitions': jsonschema_definitions, '$ref': '#/definitions/import'})
        ImportEntry = get_table_class(get_import_table_name(imprt))
        assert(db.session.query(ImportEntry).count() == imprt_json['source_count'])

        # (error code, error column name, frozenset of erroneous rows)
        expected_errors = {
            ('DUPLICATE_ENTITY_SOURCE_PK', 'id_synthese', frozenset([3, 4])),
            ('COUNT_MIN_SUP_COUNT_MAX', 'nombre_min', frozenset([5])),
            ('DATE_MIN_SUP_DATE_MAX', 'date_debut', frozenset([6])),
            ('MISSING_VALUE', 'date_debut', frozenset([8,10])),
            ('MISSING_VALUE', 'date_fin', frozenset([10])),
            ('INVALID_DATE', 'date_debut', frozenset([7,8,10])),
        }
        obtained_errors = {
            (error.type.name, error.column, frozenset(error.rows)) for error in imprt.errors
        }
        assert(obtained_errors == expected_errors)

        # Get valid data (preview)
        r = self.client.get(url_for('import.preview_valid_data', import_id=imprt.id_import))
        assert(r.status_code == 200)

        # Get invalid data
        r = self.client.get(url_for('import.get_import_invalid_rows_as_csv', import_id=imprt.id_import))
        assert(r.status_code == 200)

        # Import step
        r = self.client.post(url_for('import.import_valid_data', import_id=imprt.id_import))
        assert(r.status_code == 200)
        data = r.get_json()
        validate_json(data, {'definitions': jsonschema_definitions, '$ref': '#/definitions/import'})

        # Delete step
        r = self.client.delete(url_for('import.delete_import', import_id=imprt.id_import))
        assert(r.status_code == 200)


    def test_mapping_list(self, users):
        with db.session.begin_nested():
            mapping1 = TMappings(mapping_type='CONTENT', mapping_label='Mapping 1', active=True, is_public=True)
            db.session.add(mapping1)
            mapping2 = TMappings(mapping_type='FIELD', mapping_label='Mapping 2', active=True, is_public=True)
            db.session.add(mapping2)
            mapping3 = TMappings(mapping_type='FIELD', mapping_label='Mapping 3', active=True, is_public=False)
            db.session.add(mapping3)
            mapping4 = TMappings(mapping_type='FIELD', mapping_label='Mapping 4', active=False, is_public=True)
            db.session.add(mapping4)
            mapping5 = TMappings(mapping_type='FIELD', mapping_label='Mapping 5', active=True, is_public=False)
            mapping5.owners.append(users['self_user'])
            db.session.add(mapping5)
            mapping6 = TMappings(mapping_type='FIELD', mapping_label='Mapping 6', active=True, is_public=False)
            mapping6.owners.append(users['stranger_user'])
            db.session.add(mapping6)
            mapping7 = TMappings(mapping_type='FIELD', mapping_label='Mapping 7', active=True, is_public=False)
            mapping7.owners.append(users['associate_user'])
            db.session.add(mapping7)

        set_logged_user_cookie(self.client, users['noright_user'])

        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        assert(r.status_code == Forbidden.code)

        set_logged_user_cookie(self.client, users['self_user'])

        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        assert(r.status_code == 200)
        mappings = r.get_json()
        validate_json(mappings, {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': { '$ref': '#/definitions/mapping' },
            'minItems': 1,
        })
        mapping_ids = [ mapping['id_mapping'] for mapping in mappings ]
        assert(mapping1.id_mapping not in mapping_ids)  # wrong mapping type
        assert(mapping2.id_mapping in mapping_ids)
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping2.id_mapping)).status_code == 200)
        assert(mapping3.id_mapping not in mapping_ids)  # not public
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping3.id_mapping)).status_code == Forbidden.code)
        assert(mapping4.id_mapping not in mapping_ids)  # not active
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping4.id_mapping)).status_code == Forbidden.code)
        assert(mapping5.id_mapping in mapping_ids)  # not public but user is owner
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping5.id_mapping)).status_code == 200)
        assert(mapping6.id_mapping not in mapping_ids)  # not public and owned by another user
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping6.id_mapping)).status_code == Forbidden.code)
        assert(mapping7.id_mapping not in mapping_ids)  # not public and owned by an user in the same organism whereas read scope is 1
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping7.id_mapping)).status_code == Forbidden.code)

        set_logged_user_cookie(self.client, users['user'])

        # refresh mapping list
        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
        assert(mapping6.id_mapping not in mapping_ids)  # not public and owned by another user
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping6.id_mapping)).status_code == Forbidden.code)
        assert(mapping7.id_mapping in mapping_ids)  # not public but owned by an user in the same organism whereas read scope is 2
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping7.id_mapping)).status_code == 200)

        set_logged_user_cookie(self.client, users['admin_user'])

        # refresh mapping list
        r = self.client.get(url_for('import.list_mappings'), query_string={'type': 'field'})
        mapping_ids = [ mapping['id_mapping'] for mapping in r.get_json() ]
        assert(mapping6.id_mapping in mapping_ids)  # not public and owned by another user but we have scope = 3
        assert(self.client.get(url_for('import.get_mapping', id_mapping=mapping6.id_mapping)).status_code == 200)


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


    def test_synthesis_fields(self, users):
        assert(self.client.get(url_for('import.get_synthesis_fields')).status_code == Unauthorized.code)
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.get(url_for('import.get_synthesis_fields'))
        assert(r.status_code == 200)
        data = r.get_json()
        themes_count = BibThemes.query.count()
        schema = {
            'definitions': jsonschema_definitions,
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'theme': { '$ref': '#/definitions/synthesis_theme' },
                    'fields': {
                        'type': 'array',
                        'items': { '$ref': '#/definitions/synthesis_field' },
                        'uniqueItems': True,
                        'minItems': 1,
                    }
                },
                'required': [
                    'theme',
                    'fields',
                ],
                'additionalProperties': False,
            },
            'minItems': themes_count,
            'maxItems': themes_count,
        }
        validate_json(data, schema)
