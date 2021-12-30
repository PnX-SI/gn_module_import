"""
Routes to manage import (import list, cancel import, import info...)
"""
from flask import request, current_app
from sqlalchemy.orm import exc as SQLAlchelyExc, joinedload, raiseload
from sqlalchemy import or_
from urllib.parse import urljoin
from functools import reduce

from pypnusershub.db.models import User
from pypnusershub.db.tools import InsufficientRightsError


from utils_flask_sqla.response import json_resp
from geonature.utils.env import DB
from geonature.core.gn_permissions import decorators as permissions
from geonature.core.gn_permissions.tools import cruved_scope_for_user_in_module
from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
)
from geonature.core.gn_meta.models import TDatasets
import geonature.utils.filemanager as fm

from ..api_error import GeonatureImportApiError

from ..db.models import (
    TImports,
    CorRoleImport,
    CorImportArchives,
    TMappings,
)
from ..db.queries.user_table_queries import (
    get_full_table_name,
    set_imports_table_name,
    get_table_info,
    get_table_names,
    get_table_name,
    get_valid_bbox,
)
from ..utils.clean_names import *
from ..utils.utils import get_pk_name
from ..upload.upload_errors import *
from ..blueprint import blueprint


@blueprint.route("", methods=["GET"])
@permissions.check_cruved_scope("R", True, module_code="IMPORT")
@json_resp
def get_import_list(info_role):
    """
        return import list
    """
    ors = []
    q = DB.session.query(TImports).order_by(TImports.id_import)
    if info_role.value_filter == "1":
        ors.append(TImports.author.any(id_role=info_role.id_role))
    if info_role.value_filter == "2":
        ors.append(TImports.author.any(id_organisme=info_role.id_organisme))
    q = q.filter(or_(*ors))
    q = q.options(
        joinedload('author'),
        joinedload('dataset'),
        joinedload('errors'),
        raiseload('*'),
    )
    results = q.all()
    nrows = None
    if not results:
        return {"empty": True}, 200
    else:
        nrows = len(results)

    user_cruved = cruved_scope_for_user_in_module(
        info_role.id_role,
        module_code="IMPORT"
    )[0]

    fields = [
        'errors.id_user_error',
        'dataset.dataset_name',
        'author',
    ]

    return {"empty": False, "history": [r.to_dict(info_role, user_cruved, fields=fields) for r in results]}, 200


@blueprint.route("/update_import/<int:id_import>", methods=["POST"])
@permissions.check_cruved_scope("C", module_code="IMPORT")
@json_resp
def update_import(id_import):
    if not id_import:
        return None
    data_post = request.get_json()
    # set id_value_mapping if value mapping step is skipped
    if current_app.config['IMPORT']['ALLOW_VALUE_MAPPING'] == False:
        data_post['id_content_mapping'] = current_app.config['IMPORT']['DEFAULT_VALUE_MAPPING_ID']
    DB.session.query(TImports.id_content_mapping).filter(
        TImports.id_import == id_import).update(data_post)
    DB.session.commit()
    return TImports.query.get(id_import).to_dict()


@blueprint.route("/<int:import_id>", methods=["GET"])
@permissions.check_cruved_scope("R", module_code="IMPORT")
@json_resp
def get_one_import(import_id):
    import_obj = TImports.query.get(import_id)
    if import_obj:
        return import_obj.to_dict(fields=['errors'])
    return None


@blueprint.route("/by_dataset/<int:id_dataset>", methods=["GET"])
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def get_imports_by_dataset(info_role, id_dataset):
    try:
        results = (
            DB.session.query(TImports)
            .filter(TImports.id_dataset == int(id_dataset))
            .all()
        )
        imports = []
        for row in results:
            d = row.as_dict()
            imports.append(d)
        return imports, 200
    except Exception as e:
        raise GeonatureImportApiError(
            message="INTERNAL SERVER ERROR - get_imports_by_dataset() error : contactez l'administrateur du site",
            details=str(e),
        )


@blueprint.route("/cancel_import/<import_id>", methods=["GET"])
@permissions.check_cruved_scope("D", True, module_code="IMPORT")
@json_resp
def cancel_import(info_role, import_id):
    if import_id == "undefined":
        return {"message": "Import annulé"}, 200
    if info_role.value_filter != "3":
        try:
            if info_role.value_filter == "1":
                actors = [
                    auth[0]
                    for auth in DB.session.query(CorRoleImport.id_role)
                    .filter(CorRoleImport.id_import == import_id)
                    .all()
                ]
                assert info_role.id_role in actors
            elif info_role.value_filter == "2":
                actors = [
                    auth[0]
                    for auth in DB.session.query(CorRoleImport.id_role)
                    .filter(CorRoleImport.id_import == import_id)
                    .all()
                ]
                organisms = [
                    org[0]
                    for org in DB.session.query(User.id_organisme)
                    .join(CorRoleImport, CorRoleImport.id_role == info_role.id_role)
                    .filter(CorRoleImport.id_import == import_id)
                    .all()
                ]
                assert (
                    info_role.id_role in actors
                    or info_role.id_organisme in organisms
                )
        except AssertionError:
            raise InsufficientRightsError(
                ('User "{}" cannot delete this current import').format(
                    info_role.id_role
                ),
                403,
            )

    # delete imported data if the import is already finished
    is_finished = (
        DB.session.query(TImports.is_finished)
        .filter(TImports.id_import == import_id)
        .one()[0]
    )
    if is_finished:
        name_source = "Import(id=" + import_id + ")"
        id_source = (
            DB.session.query(TSources.id_source)
            .filter(TSources.name_source == name_source)
            .one()[0]
        )
        DB.session.query(Synthese).filter(
            Synthese.id_source == id_source).delete()
        DB.session.query(TSources).filter(
            TSources.name_source == name_source
        ).delete()

    # get step number
    step = (
        DB.session.query(TImports.step)
        .filter(TImports.id_import == import_id)
        .one()[0]
    )

    if step > 1:

        # get data table name
        user_data_name = (
            DB.session.query(TImports.import_table)
            .filter(TImports.id_import == import_id)
            .one()[0]
        )

        # set data table names
        archives_full_name = get_full_table_name(
            blueprint.config["ARCHIVES_SCHEMA_NAME"], user_data_name
        )
        imports_table_name = set_imports_table_name(user_data_name)
        imports_full_name = get_full_table_name(
            "gn_imports", imports_table_name)

        # delete tables
        engine = DB.engine
        is_gn_imports_table_exist = engine.has_table(
            imports_table_name, schema=blueprint.config["IMPORTS_SCHEMA_NAME"]
        )
        if is_gn_imports_table_exist:
            DB.session.execute(
                """\
                DROP TABLE {}
                """.format(
                    imports_full_name
                )
            )

        DB.session.execute(
            """\
            DROP TABLE {}
            """.format(
                archives_full_name
            )
        )

    # delete metadata
    DB.session.query(TImports).filter(
        TImports.id_import == import_id).delete()
    DB.session.query(CorRoleImport).filter(
        CorRoleImport.id_import == import_id
    ).delete()
    DB.session.query(CorImportArchives).filter(
        CorImportArchives.id_import == import_id
    ).delete()

    DB.session.commit()

    return {"message": "Import supprimé"}, 200



@blueprint.route("/columns_import/<int:id_import>", methods=["GET"])
@permissions.check_cruved_scope("R", module_code="IMPORT")
@json_resp
def get_import_columns_name(id_import):
    """
    Return all the columns of the file of an import
    """
    ARCHIVES_SCHEMA_NAME = blueprint.config["ARCHIVES_SCHEMA_NAME"]
    IMPORTS_SCHEMA_NAME = blueprint.config["IMPORTS_SCHEMA_NAME"]
    table_names = get_table_names(
        ARCHIVES_SCHEMA_NAME, IMPORTS_SCHEMA_NAME, id_import)
    col_names = get_table_info(
        table_names["imports_table_name"], info="column_name")
    col_names.remove("gn_invalid_reason")
    col_names.remove(get_pk_name(blueprint.config["PREFIX"]))

    return col_names


def get_import(id_import:int):
    import_obj = TImports.query.get(id_import)
    if import_obj:
        return import_obj.to_dict()
    return None

@blueprint.route("/export_pdf/<int:id_import>", methods=["POST"])
@permissions.check_cruved_scope("R", module_code="IMPORT")
def download(id_import):
    """
    Downloads the report in pdf format
    """
    filename = "rapport.pdf"
    dataset = get_import(id_import=id_import)
    dataset['map'] = request.form.get('map')
    dataset['chart'] = request.form.get('chart')

    url_list = [current_app.config['URL_APPLICATION'],
                '#',
                current_app.config['IMPORT'].get('MODULE_URL', "").replace('/',''),
                'report',
                str(dataset.get('id_import', 0))]
    dataset['url'] = '/'.join(url_list)
    pdf_file = fm.generate_pdf("import_template_pdf.html", dataset, filename)
    pdf_file_posix = Path(pdf_file)
    return send_file(pdf_file_posix, as_attachment=True)
