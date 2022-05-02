from io import StringIO
from pathlib import Path
from functools import partial
from operator import or_
from functools import reduce

import pytest
from flask import url_for
from werkzeug.datastructures import Headers
from werkzeug.exceptions import Unauthorized, Forbidden, BadRequest, Conflict
from jsonschema import validate as validate_json
from sqlalchemy import func

from geonature.utils.env import db
from geonature.tests.utils import set_logged_user_cookie, unset_logged_user_cookie
from geonature.core.gn_permissions.tools import _get_scopes_by_action
from geonature.core.gn_permissions.models import (
    TActions,
    TFilters,
    CorRoleActionFilterModuleObject,
)
from geonature.core.gn_commons.models import TModules
from geonature.core.gn_meta.models import TDatasets

from pypnusershub.db.models import User, Organisme

from gn_module_import.models import TImports, ImportSyntheseData, FieldMapping, ContentMapping, BibFields
from gn_module_import.utils import insert_import_data_in_database

from .jsonschema_definitions import jsonschema_definitions


tests_path = Path(__file__).parent


valid_file_expected_errors = {
    ("DUPLICATE_ENTITY_SOURCE_PK", "entity_source_pk_value", frozenset([3, 4])),
    ("COUNT_MIN_SUP_COUNT_MAX", "count_min", frozenset([5])),
    ("DATE_MIN_SUP_DATE_MAX", "datetime_min", frozenset([6])),
    ("MISSING_VALUE", "date_min", frozenset([8, 10])),
    ("INVALID_DATE", "datetime_min", frozenset([7])),
}


@pytest.fixture(scope="function")
def imports(users):
    def create_import(authors=[]):
        with db.session.begin_nested():
            imprt = TImports(authors=authors)
            db.session.add(imprt)
        return imprt

    return {
        "own_import": create_import(authors=[users["user"]]),
        "associate_import": create_import(authors=[users["associate_user"]]),
        "stranger_import": create_import(authors=[users["stranger_user"]]),
        "orphan_import": create_import(),
    }


@pytest.fixture()
def import_file_name():
    return "valid_file.csv"


@pytest.fixture()
def new_import(users, datasets):
    with db.session.begin_nested():
        imprt = TImports(
            authors=[users["user"]],
            id_dataset=datasets["own_dataset"].id_dataset,
        )
        db.session.add(imprt)
    return imprt


@pytest.fixture()
def uploaded_import(new_import, import_file_name):
    with db.session.begin_nested():
        with open(tests_path / "files" / import_file_name, "rb") as f:
            new_import.source_file = f.read()
            new_import.full_file_name = "valid_file.csv"
    return new_import


@pytest.fixture()
def decoded_import(client, uploaded_import):
    set_logged_user_cookie(client, uploaded_import.authors[0])
    r = client.post(
        url_for(
            "import.decode_file",
            import_id=uploaded_import.id_import,
        ),
        data={
            "encoding": "utf-8",
            "format": "csv",
            "srid": 4326,
            "separator": ";",
        },
    )
    assert r.status_code == 200
    unset_logged_user_cookie(client)
    db.session.refresh(uploaded_import)
    return uploaded_import


@pytest.fixture()
def fieldmapping(import_file_name):
    if import_file_name == "valid_file.csv":
        return FieldMapping.query.filter_by(label="Synthese GeoNature").one().values
    else:
        return {f.name_field: f.name_field for f in BibFields.query.filter_by(display=True)}


@pytest.fixture()
def field_mapped_import(client, decoded_import, fieldmapping):
    with db.session.begin_nested():
        decoded_import.fieldmapping = fieldmapping
    return decoded_import


@pytest.fixture()
def loaded_import(client, field_mapped_import):
    with db.session.begin_nested():
        field_mapped_import.source_count = insert_import_data_in_database(field_mapped_import)
    return field_mapped_import


@pytest.fixture()
def content_mapped_import(client, loaded_import):
    with db.session.begin_nested():
        loaded_import.contentmapping = (
            ContentMapping.query.filter_by(label="Nomenclatures SINP (labels)")
            .one()
            .values
        )
    return loaded_import


@pytest.fixture()
def prepared_import(client, content_mapped_import):
    set_logged_user_cookie(client, content_mapped_import.authors[0])
    r = client.post(url_for("import.prepare_import", import_id=content_mapped_import.id_import))
    assert r.status_code == 200
    unset_logged_user_cookie(client)
    db.session.refresh(content_mapped_import)
    return content_mapped_import


@pytest.fixture()
def imported_import(client, prepared_import):
    set_logged_user_cookie(client, prepared_import.authors[0])
    r = client.post(
        url_for("import.import_valid_data", import_id=prepared_import.id_import)
    )
    assert r.status_code == 200
    unset_logged_user_cookie(client)
    db.session.refresh(prepared_import)
    return prepared_import


@pytest.mark.usefixtures("client_class", "temporary_transaction")
class TestImports:
    def test_import_permissions(self):
        with db.session.begin_nested():
            organisme = Organisme(nom_organisme="test_import")
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

        get_scopes_by_action = partial(
            _get_scopes_by_action,
            module_code="IMPORT",
            object_code="IMPORT",
        )
        assert get_scopes_by_action(user.id_role) == {action: 0 for action in "CRUVED"}

        update_action = TActions.query.filter(TActions.code_action == "U").one()
        none_filter, self_filter, organism_filter, all_filter = [
            TFilters.query.filter(TFilters.value_filter == str(i)).one()
            for i in [0, 1, 2, 3]
        ]
        geonature_module, import_module = [
            TModules.query.filter(TModules.module_code == module_code).one()
            for module_code in ["GEONATURE", "IMPORT"]
        ]

        # Add permission for it-self
        with db.session.begin_nested():
            permission = CorRoleActionFilterModuleObject(
                role=user,
                action=update_action,
                filter=self_filter,
                module=geonature_module,
            )
            db.session.add(permission)
        scope = get_scopes_by_action(user.id_role)["U"]
        assert scope == 1
        assert imprt.has_instance_permission(scope, user=user) is False
        imprt.authors.append(user)
        assert imprt.has_instance_permission(scope, user=user) is True

        # Change permission to organism filter
        permission.filter = organism_filter
        db.session.commit()
        scope = get_scopes_by_action(user.id_role)["U"]
        assert scope == 2
        # right as we still are author:
        assert imprt.has_instance_permission(scope, user=user) is True
        imprt.authors.remove(user)
        assert imprt.has_instance_permission(scope, user=user) is False
        imprt.authors.append(other_user)
        db.session.commit()
        # we are not in the same organism than other_user:
        assert imprt.has_instance_permission(scope, user=user) is False
        organisme.members.append(user)
        db.session.commit()
        scope = get_scopes_by_action(user.id_role)["U"]
        assert imprt.has_instance_permission(scope, user=user) is True

        permission.filter = all_filter
        imprt.authors.remove(other_user)
        db.session.commit()
        scope = get_scopes_by_action(user.id_role)["U"]
        assert scope == 3
        assert imprt.has_instance_permission(scope, user=user) is True

    def test_list_imports(self, imports, users):
        r = self.client.get(url_for("import.get_import_list"))
        assert r.status_code == Unauthorized.code
        set_logged_user_cookie(self.client, users["noright_user"])
        r = self.client.get(url_for("import.get_import_list"))
        assert r.status_code == Forbidden.code
        set_logged_user_cookie(self.client, users["user"])
        r = self.client.get(url_for("import.get_import_list"))
        assert r.status_code == 200
        json_data = r.get_json()
        validate_json(
            json_data["imports"],
            {
                "definitions": jsonschema_definitions,
                "type": "array",
                "items": {"$ref": "#/definitions/import"},
            },
        )
        imports_ids = {imprt["id_import"] for imprt in json_data["imports"]}
        expected_imports_ids = {
            imports[imprt].id_import for imprt in ["own_import", "associate_import"]
        }
        assert imports_ids == expected_imports_ids

    def test_search_import(self, users, imports, uploaded_import):
        set_logged_user_cookie(self.client, users["user"])
        r = self.client.get(url_for("import.get_import_list")+"?search=valid_file")
        assert r.status_code == 200
        json_data = r.get_json()
        assert json_data["count"] == 1

    def test_get_import(self, users, imports):
        def get(import_name):
            return self.client.get(
                url_for(
                    "import.get_one_import", import_id=imports[import_name].id_import
                )
            )

        assert get("own_import").status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["noright_user"])
        assert get("own_import").status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        assert get("stranger_import").status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["self_user"])
        assert get("associate_import").status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        assert get("associate_import").status_code == 200
        r = get("own_import")
        assert r.status_code == 200
        assert r.json["id_import"] == imports["own_import"].id_import

    def test_delete_import(self, users, imported_import):
        imprt = imported_import
        r = self.client.delete(
            url_for("import.delete_import", import_id=imprt.id_import)
        )
        assert r.status_code == Unauthorized.code
        set_logged_user_cookie(self.client, users["admin_user"])
        r = self.client.delete(
            url_for("import.delete_import", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        # TODO: check data from synthese, and import tables are also removed
        r = self.client.delete(
            url_for("import.delete_import", import_id=imprt.id_import)
        )
        assert r.status_code == 404
        assert 0 == ImportSyntheseData.query.filter_by(imprt=imprt).count()

    def test_import_upload(self, users, datasets):
        with open(tests_path / "files" / "simple_file.csv", "rb") as f:
            data = {
                "file": (f, "simple_file.csv"),
                "datasetId": datasets["own_dataset"].id_dataset,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["noright_user"])
        with open(tests_path / "files" / "simple_file.csv", "rb") as f:
            data = {
                "file": (f, "simple_file.csv"),
                "datasetId": datasets["own_dataset"].id_dataset,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == Forbidden.code
            assert 'cannot "C" in IMPORT' in r.json["description"]

        set_logged_user_cookie(self.client, users["user"])

        unexisting_id = db.session.query(func.max(TDatasets.id_dataset)).scalar() + 1
        with open(tests_path / "files" / "simple_file.csv", "rb") as f:
            data = {
                "file": (f, "simple_file.csv"),
                "datasetId": unexisting_id,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == BadRequest.code
            assert r.json["description"] == f"Dataset '{unexisting_id}' does not exist."

        with open(tests_path / "files" / "simple_file.csv", "rb") as f:
            data = {
                "file": (f, "simple_file.csv"),
                "datasetId": datasets["stranger_dataset"].id_dataset,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == Forbidden.code
            assert "jeu de données" in r.json["description"]  # this is a DS issue

        with open(tests_path / "files" / "simple_file.csv", "rb") as f:
            data = {
                "file": (f, "simple_file.csv"),
                "datasetId": datasets["own_dataset"].id_dataset,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == 200

        imprt = TImports.query.get(r.json["id_import"])
        assert imprt.source_file is not None
        assert imprt.full_file_name == "simple_file.csv"


    def test_import_error(self, users, datasets):
        set_logged_user_cookie(self.client, users["user"])
        with open(tests_path / "files" / "empty.csv", "rb") as f:
            data = {
                "file": (f, "empty.csv"),
                "datasetId": datasets["own_dataset"].id_dataset,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == 400
            assert r.json["description"] == "Impossible to upload empty files"

        with open(tests_path / "files" / "starts_with_empty_line.csv", "rb") as f:
            data = {
                "file": (f, "starts_with_empty_line.csv"),
                "datasetId": datasets["own_dataset"].id_dataset,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == 400
            assert r.json["description"] == "File must start with columns"

    def test_import_upload_after_preparation(self, prepared_import):
        imprt = prepared_import
        # TODO: check old table does not exist
        # old_file_name = decoded_import.full_file_name
        set_logged_user_cookie(self.client, imprt.authors[0])
        with open(tests_path / "files" / "utf8_file.csv", "rb") as f:
            data = {
                "file": (f, "utf8_file.csv"),
                "datasetId": imprt.id_dataset,
            }
            r = self.client.put(
                url_for("import.upload_file", import_id=imprt.id_import),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
            assert r.status_code == 200
        db.session.refresh(imprt)
        assert imprt.source_file is not None
        assert imprt.source_count == None
        assert imprt.full_file_name == "utf8_file.csv"
        assert imprt.columns == None
        assert len(imprt.errors) == 0

    def test_import_decode(self, users, new_import):
        imprt = new_import
        data = {
            "encoding": "utf-16",
            "format": "csv",
            "srid": 2154,
            "separator": ";",
        }

        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["noright_user"])
        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == BadRequest.code
        assert "first upload" in r.json["description"]

        imprt.full_file_name = "import.csv"
        imprt.detected_encoding = "utf-8"

        with open(tests_path / "files" / "utf8_file.csv", "rb") as f:
            imprt.source_file = f.read()
        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == BadRequest.code

        data["encoding"] = "utf-8"

        with open(tests_path / "files" / "duplicate_column_names.csv", "rb") as f:
            imprt.source_file = f.read()
        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == BadRequest.code
        assert "Duplicates column names" in r.json["description"]

        #with open(tests_path / "files" / "wrong_line_length.csv", "rb") as f:
        #    imprt.source_file = f.read()
        #r = self.client.post(
        #    url_for("import.decode_file", import_id=imprt.id_import), data=data
        #)
        #assert r.status_code == BadRequest.code
        #assert "Expected" in r.json["description"]

        wrong_separator_data = data.copy()
        wrong_separator_data['separator'] = 'sep'
        r = self.client.post(
            url_for('import.decode_file', import_id=imprt.id_import), data=wrong_separator_data
        )
        assert r.status_code == BadRequest.code

        with open(tests_path / "files" / "utf8_file.csv", "rb") as f:
            imprt.source_file = f.read()
        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == 200

    def test_import_decode_after_preparation(self, users, prepared_import):
        imprt = prepared_import
        data = {
            "encoding": "utf-8",
            "format": "csv",
            "srid": 4326,
            "separator": ";",
        }
        set_logged_user_cookie(self.client, users["user"])
        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == 200
        db.session.refresh(imprt)
        assert len(imprt.errors) == 0

    def test_import_columns(self, users, decoded_import):
        imprt = decoded_import

        r = self.client.get(
            url_for("import.get_import_columns_name", import_id=imprt.id_import)
        )
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["stranger_user"])
        r = self.client.get(
            url_for("import.get_import_columns_name", import_id=imprt.id_import)
        )
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r = self.client.get(
            url_for("import.get_import_columns_name", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        assert "cd_nom" in r.json

    def test_import_loading(self, users, field_mapped_import):
        imprt = field_mapped_import

        set_logged_user_cookie(self.client, users["user"])
        r = self.client.post(
            url_for("import.load_import", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        assert r.json['source_count'] > 0
        assert ImportSyntheseData.query.filter_by(id_import=imprt.id_import).count() == r.json['source_count']

    def test_import_values(self, users, loaded_import):
        imprt = loaded_import

        r = self.client.get(
            url_for("import.get_import_values", import_id=imprt.id_import)
        )
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["stranger_user"])
        r = self.client.get(
            url_for("import.get_import_values", import_id=imprt.id_import)
        )
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r = self.client.get(
            url_for("import.get_import_values", import_id=imprt.id_import)
        )
        assert r.status_code == 200, r.data
        schema = {
            "definitions": jsonschema_definitions,
            "type": "object",
            "patternProperties": {
                "^.*$": {  # keys are synthesis fields
                    "type": "object",
                    "properties": {
                        "nomenclature_type": {
                            "$ref": "#/definitions/nomenclature_type"
                        },
                        "nomenclatures": {  # list of acceptable nomenclatures for this field
                            "type": "array",
                            "items": {"$ref": "#/definitions/nomenclature"},
                            "minItems": 1,
                        },
                        "values": {  # available user values in uploaded file for this field
                            "type": "array",
                            "items": {
                                "type": [
                                    "string",
                                    "null",
                                ],
                            },
                        },
                    },
                    "required": [
                        "nomenclature_type",
                        "nomenclatures",
                        "values",
                    ],
                },
            },
        }
        validate_json(r.json, schema)

    def test_import_preview(self, users, prepared_import):
        imprt = prepared_import

        r = self.client.get(
            url_for("import.preview_valid_data", import_id=imprt.id_import)
        )
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["stranger_user"])
        r = self.client.get(
            url_for("import.preview_valid_data", import_id=imprt.id_import)
        )
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r = self.client.get(
            url_for("import.preview_valid_data", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        n_invalid_data = len(
            {row for _, _, rows in valid_file_expected_errors for row in rows}
        )
        assert r.json["n_valid_data"] == imprt.source_count - n_invalid_data
        assert r.json["n_invalid_data"] == n_invalid_data

    def test_import_invalid_rows(self, users, prepared_import):
        imprt = prepared_import
        r = self.client.get(
            url_for("import.get_import_invalid_rows_as_csv", import_id=imprt.id_import)
        )
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["stranger_user"])
        r = self.client.get(
            url_for("import.get_import_invalid_rows_as_csv", import_id=imprt.id_import)
        )
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r = self.client.get(
            url_for("import.get_import_invalid_rows_as_csv", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        csvfile = StringIO(r.data.decode('utf-8'))
        invalid_rows = reduce(or_, [rows for _, _, rows in valid_file_expected_errors])
        assert len(csvfile.readlines()) == 1 + len(invalid_rows)  # 1 = header

    def test_import_errors(self, users, imported_import):
        imprt = imported_import

        r = self.client.get(
            url_for("import.get_import_errors", import_id=imprt.id_import)
        )
        assert r.status_code == Unauthorized.code

        set_logged_user_cookie(self.client, users["stranger_user"])
        r = self.client.get(
            url_for("import.get_import_errors", import_id=imprt.id_import)
        )
        assert r.status_code == Forbidden.code

        set_logged_user_cookie(self.client, users["user"])
        r = self.client.get(
            url_for("import.get_import_errors", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        invalid_rows = reduce(or_, [rows for _, _, rows in valid_file_expected_errors])
        # (error code, error column name, frozenset of erroneous rows)
        obtained_errors = {
            (error.type.name, error.column, frozenset(error.rows or []))
            for error in imprt.errors
        }
        assert obtained_errors == valid_file_expected_errors
        validate_json(
            r.json,
            {
                "definitions": jsonschema_definitions,
                "type": "array",
                "items": {
                    "$ref": "#/definitions/error",
                },
            },
        )
        assert len(r.json) == len(valid_file_expected_errors)

    def test_import_valid_file(self, users, datasets):
        set_logged_user_cookie(self.client, users["user"])

        # Upload step
        test_file_name = "valid_file.csv"
        with open(tests_path / "files" / test_file_name, "rb") as f:
            test_file_line_count = sum(1 for line in f) - 1  # remove headers
            f.seek(0)
            data = {
                "file": (f, test_file_name),
                "datasetId": datasets["own_dataset"].id_dataset,
            }
            r = self.client.post(
                url_for("import.upload_file"),
                data=data,
                headers=Headers({"Content-Type": "multipart/form-data"}),
            )
        assert r.status_code == 200
        imprt_json = r.get_json()
        imprt = TImports.query.get(imprt_json["id_import"])
        assert len(imprt.authors) == 1
        assert imprt_json["date_create_import"]
        assert imprt_json["date_update_import"]
        assert imprt_json["detected_encoding"] == "utf-8"
        assert imprt_json["detected_format"] == "csv"
        assert imprt_json["detected_separator"] == ";"
        assert imprt_json["full_file_name"] == test_file_name
        assert imprt_json["id_dataset"] == datasets["own_dataset"].id_dataset

        # Decode step
        data = {
            "encoding": "utf-8",
            "format": "csv",
            "srid": 4326,
            "separator": ";",
        }
        r = self.client.post(
            url_for("import.decode_file", import_id=imprt.id_import), data=data
        )
        assert r.status_code == 200
        validate_json(
            r.json,
            {"definitions": jsonschema_definitions, "$ref": "#/definitions/import"},
        )
        assert imprt.date_update_import
        assert imprt.encoding == "utf-8"
        assert imprt.format_source_file == "csv"
        assert imprt.separator == ";"
        assert imprt.srid == 4326
        assert imprt.columns
        assert len(imprt.columns) > 0
        assert ImportSyntheseData.query.filter_by(imprt=imprt).count() == 0

        # Field mapping step
        fieldmapping = FieldMapping.query.filter_by(label="Synthese GeoNature").one()
        r = self.client.post(
            url_for("import.set_import_field_mapping", import_id=imprt.id_import),
            data=fieldmapping.values,
        )
        assert r.status_code == 200
        validate_json(
            r.json,
            {"definitions": jsonschema_definitions, "$ref": "#/definitions/import"},
        )
        assert r.json["fieldmapping"] == fieldmapping.values

        # Loading step
        r = self.client.post(
            url_for("import.load_import", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        assert r.json["source_count"] == test_file_line_count
        assert ImportSyntheseData.query.filter_by(imprt=imprt).count() == test_file_line_count

        # Content mapping step
        contentmapping = ContentMapping.query.filter_by(
            label="Nomenclatures SINP (labels)"
        ).one()
        r = self.client.post(
            url_for("import.set_import_content_mapping", import_id=imprt.id_import),
            data=contentmapping.values,
        )
        assert r.status_code == 200
        data = r.get_json()
        validate_json(
            data,
            {"definitions": jsonschema_definitions, "$ref": "#/definitions/import"},
        )
        assert data["contentmapping"] == contentmapping.values

        # Prepare data before import
        r = self.client.post(
            url_for("import.prepare_import", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        validate_json(
            r.json,
            {"definitions": jsonschema_definitions, "$ref": "#/definitions/import"},
        )
        invalid_rows = reduce(or_, [rows for _, _, rows in valid_file_expected_errors])
        # (error code, error column name, frozenset of erroneous rows)
        obtained_errors = {
            (error.type.name, error.column, frozenset(error.rows or []))
            for error in imprt.errors
        }
        assert obtained_errors == valid_file_expected_errors
        assert ImportSyntheseData.query.filter_by(imprt=imprt, valid=True).count() == test_file_line_count - len(invalid_rows)

        # Get errors
        r = self.client.get(
            url_for("import.get_import_errors", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        assert len(r.json) == len(valid_file_expected_errors)

        # Get valid data (preview)
        r = self.client.get(
            url_for("import.preview_valid_data", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        assert r.json["n_valid_data"] == imprt.source_count - len(invalid_rows)
        assert r.json["n_invalid_data"] == len(invalid_rows)

        # Get invalid data
        r = self.client.get(
            url_for("import.get_import_invalid_rows_as_csv", import_id=imprt.id_import)
        )
        assert r.status_code == 200

        # Import step
        r = self.client.post(
            url_for("import.import_valid_data", import_id=imprt.id_import)
        )
        assert r.status_code == 200
        data = r.get_json()
        validate_json(
            data,
            {"definitions": jsonschema_definitions, "$ref": "#/definitions/import"},
        )
        assert 0 == ImportSyntheseData.query.filter_by(imprt=imprt).count()

        # Delete step
        r = self.client.delete(
            url_for("import.delete_import", import_id=imprt.id_import)
        )
        assert r.status_code == 200

    @pytest.mark.parametrize("import_file_name", ["geom_file.csv"])
    def test_import_geometry_file(self, prepared_import):
        obtained_errors = {
            (error.type.name, error.column, frozenset(error.rows or []))
            for error in prepared_import.errors
        }
        assert obtained_errors == {
            ("INVALID_ATTACHMENT_CODE", "codecommune", frozenset([2])),
            ("INVALID_ATTACHMENT_CODE", "codedepartement", frozenset([4])),
            ("INVALID_ATTACHMENT_CODE", "codemaille", frozenset([6])),
            ("MULTIPLE_CODE_ATTACHMENT", "Champs géométriques", frozenset([7])),
            ("MULTIPLE_ATTACHMENT_TYPE_CODE", "Champs géométriques", frozenset([10, 13])),
            ("NO-GEOM", "Champs géométriques", frozenset([14])),
        }

    @pytest.mark.parametrize("import_file_name", ["cd_file.csv"])
    def test_import_cd_file(self, prepared_import):
        obtained_errors = {
            (error.type.name, error.column, frozenset(error.rows or []))
            for error in prepared_import.errors
        }
        assert obtained_errors == {
            ("MISSING_VALUE", "cd_nom", frozenset([1, 4, 5])),
            ("CD_NOM_NOT_FOUND", "cd_nom", frozenset([2, 6, 8])),
            ("CD_HAB_NOT_FOUND", "cd_hab", frozenset([4, 6, 7])),
        }
