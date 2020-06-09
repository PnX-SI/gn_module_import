from flask import Flask, request, current_app, copy_current_request_context

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

from ..send_mail import(
    import_send_mail
)

import threading
import redis
from celery import Celery
from rq import Queue, Connection, Worker
import time

app = Flask(__name__)
celery = Celery(app.name, broker='redis://localhost:6379/0')

@celery.task
def data_checker_task(import_data):

    import_id = import_data['import_id']
    id_field_mapping =  import_data['id_field_mapping']
    id_content_mapping =  import_data['id_content_mapping']

    imp = DB.session.query(TImports).filter(TImports.id_import == import_id).first()

    field_mapping_data_checking(import_id, id_field_mapping)
    content_mapping_data_checking(import_id, id_content_mapping)
    DB.session.query(TImports).filter(TImports.id_import == import_id).update({'processing' : False})
    DB.session.commit()

    for aut in imp.author:
        import_send_mail(
            mail_to=aut.email,
            file_name=imp.full_file_name
        )
    return "Done"


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

    print('source_count :')
    print(DB.session.query(TImports.source_count).filter(TImports.id_import == import_id).one())

    if (DB.session.query(TImports.source_count).filter(TImports.id_import == import_id).one()[0] > 10):

        DB.session.query(TImports).filter(TImports.id_import == import_id).update({'processing' : True})
        DB.session.commit()

        import_data = {
            "import_id": import_id,
            "id_field_mapping": id_field_mapping,
            "id_content_mapping": id_content_mapping
        }
        with Connection(redis.Redis(host='localhost', port='6379')):
            q = Queue()
            job = q.enqueue(data_checker_task, import_data)
            print("Task ({}) added to queue at {}".format(job.id, job.enqueued_at))
            # lancer geonature launch_redis_worker avec le venv pour que les taches soit traitees

        return "Processing"
    else:
        field_mapping_data_checking(import_id, id_field_mapping)
        content_mapping_data_checking(import_id, id_content_mapping)
        return "Done"


# @celery.task
def send_async_email(data):
    print("second thread")

    import_send_mail(
            mail_to="mail_to",
            file_name="TEST"
        )


@blueprint.route("/sendemail", methods=["GET"])
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def sendMail(info_role):
    
    email_data = {
    'subject': 'Hello from Flask',
    'to': "ff",
    'body': 'test.'
    }   
    # worker = Worker([q], connection=r, name='foo')
    # worker.work()
    job = q.enqueue(send_async_email, email_data)
    printf("Task ({}) added to queue at {}".format(job.id, job.enqueued_at))

    return "DONE"
