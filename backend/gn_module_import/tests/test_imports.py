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
from gn_module_import.utils.imports import get_table_class, get_import_table_name

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

@pytest.fixture()
def uploaded_import(users, datasets):
    with open(tests_path / 'files' / 'one_line.csv', 'rb') as f:
        imprt = TImports(
            authors=[users['user']],
            id_dataset=datasets['own_dataset'].id_dataset,
            source_file=f.read(),
            full_file_name='one_line.csv',
        )
    with db.session.begin_nested():
        db.session.add(imprt)
    return imprt


@pytest.fixture()
def decoded_import(client, uploaded_import):
    set_logged_user_cookie(client, uploaded_import.authors[0])
    client.post(
        url_for('import.decode_file',
        import_id=uploaded_import.id_import),
        data={
            'encoding': 'utf-8',
            'format': 'csv',
            'srid': 2154,
        },
    )
    return uploaded_import

@pytest.fixture()
def prepared_import(client, decoded_import):
    decoded_import.field_mapping = (
        TMappings.query
        .filter_by(mapping_label='Synthese GeoNature')
        .one()
    )
    decoded_import.content_mapping = (
        TMappings.query
        .filter_by(mapping_label='Nomenclatures SINP (labels)')
        .one()
    )
    client.post(url_for('import.prepare_import', import_id=decoded_import.id_import))


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

    def test_get_import(self, users, imports):
        r =  self.client.get(url_for('import.get_one_import', import_id=imports['own_import'].id_import))
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["noright_user"])
        r =  self.client.get(url_for('import.get_one_import', import_id=imports['own_import'].id_import))
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r =  self.client.get(url_for('import.get_one_import', import_id=imports['stranger_import'].id_import))
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["self_user"])
        r =  self.client.get(url_for('import.get_one_import', import_id=imports['associate_import'].id_import))
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r =  self.client.get(url_for('import.get_one_import', import_id=imports['associate_import'].id_import))
        assert r.status_code == 200
        r =  self.client.get(url_for('import.get_one_import', import_id=imports['own_import'].id_import))
        assert r.status_code == 200
        assert r.json['id_import'] == imports['own_import'].id_import
    
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

    def test_import_upload(self, users, datasets):
        with open(tests_path / 'files' / 'simple_file.csv', 'rb') as f:
            data = {
                'file': (f, 'simple_file.csv'),
                'datasetId': datasets['own_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
            assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users['noright_user'])
        with open(tests_path / 'files' / 'simple_file.csv', 'rb') as f:
            data = {
                'file': (f, 'simple_file.csv'),
                'datasetId': datasets['own_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
            assert r.status_code == Forbidden.code
            assert 'cannot "C" in IMPORT' in r.json['description']

        set_logged_user_cookie(self.client, users['user'])

        unexisting_id = db.session.query(func.max(TDatasets.id_dataset)).scalar() + 1
        with open(tests_path / 'files' / 'simple_file.csv', 'rb') as f:
            data = {
                'file': (f, 'simple_file.csv'),
                'datasetId': unexisting_id,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
            assert r.status_code == BadRequest.code
            assert r.json['description'] == f"Dataset '{unexisting_id}' does not exist."

        with open(tests_path / 'files' / 'simple_file.csv', 'rb') as f:
            data = {
                'file': (f, 'simple_file.csv'),
                'datasetId': datasets['stranger_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
            assert r.status_code == Forbidden.code
            assert 'jeu de données' in r.json['description']  # this is a DS issue

        with open(tests_path / 'files' / 'simple_file.csv', 'rb') as f:
            data = {
                'file': (f, 'simple_file.csv'),
                'datasetId': datasets['own_dataset'].id_dataset,
            }
            r = self.client.post(url_for('import.upload_file'), data=data,
                            headers=Headers({'Content-Type': 'multipart/form-data'}))
            assert r.status_code == 200

        imprt = TImports.query.get(r.json['id_import'])
        assert imprt.source_file is not None
        assert imprt.full_file_name == 'simple_file.csv'

    def test_import_reupload_after_decode(self, decoded_import):
        old_file_name = decoded_import.full_file_name
        set_logged_user_cookie(self.client, decoded_import.authors[0])
        with open(tests_path / 'files' / 'utf8_file.csv', 'rb') as f:
            data = {
                'file': (f, 'utf8_file.csv'),
                'datasetId': decoded_import.id_dataset,
            }
            r = self.client.put(
                url_for('import.upload_file', import_id=decoded_import.id_import),
                data=data,
                headers=Headers({'Content-Type': 'multipart/form-data'}),
            )
            assert r.status_code == 200
        db.session.refresh(decoded_import)
        assert decoded_import.source_file is not None
        assert decoded_import.source_count == None
        assert decoded_import.full_file_name == 'utf8_file.csv'
        assert decoded_import.columns == {}

    def test_import_decode(self, users, imports):
        imprt = imports['own_import']
        data = {
            'encoding': 'utf-16',
            'format': 'csv',
            'srid': 2154,
        }

        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users['noright_user'])
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users['user'])
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert r.status_code == BadRequest.code
        assert 'first upload' in r.json['description']

        imprt.full_file_name = 'import.csv'
        imprt.detected_encoding = 'utf-8'

        with open(tests_path / 'files' / 'utf8_file.csv', 'rb') as f:
            imprt.source_file = f.read()
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert r.status_code == BadRequest.code

        data['encoding'] = 'utf-8'

        with open(tests_path / 'files' / 'duplicate_column_names.csv', 'rb') as f:
            imprt.source_file = f.read()
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert r.status_code == BadRequest.code
        assert 'Duplicates column names' in r.json['description']

        with open(tests_path / 'files' / 'wrong_line_length.csv', 'rb') as f:
            imprt.source_file = f.read()
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert r.status_code == BadRequest.code
        assert 'Expected' in r.json['description']

        with open(tests_path / 'files' / 'utf8_file.csv', 'rb') as f:
            imprt.source_file = f.read()
        r = self.client.post(url_for('import.decode_file', import_id=imprt.id_import), data=data)
        assert r.status_code == 200

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
        assert(imprt_json['import_table'])
        assert(imprt_json['columns'])
        assert(len(imprt_json['columns']) > 0)
        imprt = TImports.query.get(imprt.id_import)
        ImportEntry = get_table_class(get_import_table_name(imprt))
        assert(db.session.query(ImportEntry).count() == imprt_json['source_count'])

        # Field mapping step
        fieldmapping = TMappings.query.filter_by(mapping_label='Synthese GeoNature').one()
        data = {
            'id_field_mapping': fieldmapping.id_mapping,
        }
        r = self.client.post(url_for('import.set_import_field_mapping', import_id=imprt.id_import), data=data)
        assert(r.status_code == 200)
        data = r.get_json()
        validate_json(data, {'definitions': jsonschema_definitions, '$ref': '#/definitions/import'})
        assert(data['id_field_mapping'] == fieldmapping.id_mapping)

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
