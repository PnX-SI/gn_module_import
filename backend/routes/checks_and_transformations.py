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

    if (DB.session.query(TImports.source_count).filter(TImports.id_import == import_id).one()[0] > 10):

        DB.session.query(TImports).filter(TImports.id_import == import_id).update({'processing' : True})
        DB.session.commit()

        print('Flask(__name__)')
        print(Flask(__name__))

        #data_checker_task.delay(import_id, id_field_mapping, id_content_mapping)

        @copy_current_request_context
        def data_checker_task(import_id, id_field_mapping, id_content_mapping):
            print('OK1')
            field_mapping_data_checking(import_id, id_field_mapping)
            print('OK2')
            content_mapping_data_checking(import_id, id_content_mapping)
            time.sleep(5)
            DB.session.query(TImports).filter(TImports.id_import == import_id).update({'processing' : False})
            DB.session.commit()

            import_send_mail(
                mail_to="mail_to",
                file_name="TEST"
            )
            return "Done"

        a = threading.Thread(
            name="data_checker_task",
            target=data_checker_task,
            kwargs={
                "import_id": import_id,
                "id_field_mapping": id_field_mapping,
                "id_content_mapping": id_content_mapping
            }
        )
        a.start()

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






# Add this decorator to our send_mail function
# @celery.task
def send_async_email(data):
    print("second thread")

    import_send_mail(
            mail_to="mail_to",
            file_name="TEST"
        )



def background_task(n):

    """ Function that returns len(n) and simulates a delay """

    delay = 2

    print("Task running")
    print(f"Simulating a {delay} second delay")

    time.sleep(delay)

    # print(len(n))
    print("Task complete")

    return len(n)

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
    print(f"Task ({job.id}) added to queue at {job.enqueued_at}")

    return "DONE"
    # try:
    #     job = 
    #     # print("Principal thread")

    #     #Tester si on depasse les x lignes
    #         traitement
    #     # ELse
    #         send_async_email.apply_async(args=[email_data], countdown=20)
    #     return "DONE!"
    # except Exception as e:
    #     raise GeonatureImportApiError(
    #         message="INTERNAL SERVER ERROR - sendMail() error : contactez l'administrateur du site",
    #         details=str(e),
    #     )
