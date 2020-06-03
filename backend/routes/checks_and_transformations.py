from flask import Flask, request, current_app

from utils_flask_sqla.response import json_resp
from geonature.utils.env import DB
from geonature.core.gn_permissions import decorators as permissions
from ..db.models import TImports

from ..transform.transform import (
    field_mapping_data_checking,
    content_mapping_data_checking,
)
from ..api_error import GeonatureImportApiError

from ..blueprint import blueprint


import redis
from celery import Celery
from rq import Queue, Connection, Worker
import time

broker_url = 'redis://localhost:6379/0'

r = redis.Redis(host='localhost', port='6379')
q = Queue('test',connection=r)
app = Flask(__name__)
celery = Celery(app.name, broker=broker_url)

@celery.task(bind=True)
def data_checker_task(self, import_id, id_field_mapping, id_content_mapping):
    print('OK1')
    field_mapping_data_checking(import_id, id_field_mapping)
    print('OK2')
    content_mapping_data_checking(import_id, id_content_mapping)
    return "Done"

@celery.task
def add(x, y):
    print('OK4')
    print(x+y)
    return x + y


@blueprint.route(
    "/data_checker/<import_id>/field_mapping/<int:id_field_mapping>/content_mapping/<int:id_content_mapping>",
    methods=["GET", "POST"],
)
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def data_checker(info_role, import_id, id_field_mapping, id_content_mapping):
    """
    Check and transform the data for field and content mapping
    """
    # try:
    # print(import_id)
    #field_mapping_data_checking(import_id, id_field_mapping)
    #content_mapping_data_checking(import_id, id_content_mapping)
    print('source_count :')
    print(DB.session.query(TImports.source_count).filter(TImports.id_import == import_id).one())

    if (DB.session.query(TImports.source_count).filter(TImports.id_import == import_id).one()[0] > 1000):
        DB.session.query(TImports).filter(TImports.id_import == import_id).update({'processing' : True})
        DB.session.commit()
        data_checker_task.delay(import_id, id_field_mapping, id_content_mapping)
        return "Processing"
    else:
        field_mapping_data_checking(import_id, id_field_mapping)
        content_mapping_data_checking(import_id, id_content_mapping)
        return "Done"

        
    #add.delay(4, 4)
    # except Exception as e:
    #     raise GeonatureImportApiError(
    #         message="INTERNAL SERVER ERROR : Erreur pendant le mapping de correspondance - contacter l'administrateur",
    #         details=str(e),
    #     )


# Add this decorator to our send_mail function
# @celery.task
def send_async_email(data):
    print("second thread")

    import_send_mail(
            mail_to="mail_to",
            file_name="TEST"
        )

