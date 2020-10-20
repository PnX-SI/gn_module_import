import re
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
from ..wrappers import checker


from ..send_mail import import_send_mail, import_send_mail_error

import threading


# import redis
# from celery import Celery
# from rq import Queue, Connection, Worker
import time

# app = Flask(__name__)
# celery = Celery(app.name, broker='redis://localhost:6379/0')

# @celery.task
# def data_checker_task(import_data):

#     import_id = import_data['import_id']
#     id_field_mapping =  import_data['id_field_mapping']
#     id_content_mapping =  import_data['id_content_mapping']

#     return concurrent_data_check(import_id, id_field_mapping, id_content_mapping)


def run_control(import_id, id_field_mapping, id_content_mapping, file_name, recipients):
    try:
        field_mapping_data_checking(import_id, id_field_mapping)
        content_mapping_data_checking(import_id, id_content_mapping)
        import_send_mail(
            id_import=import_id, mail_to=recipients, file_name=file_name, step="check"
        )
        return "Done"
    except Exception as e:
        DB.session.query(TImports).filter(TImports.id_import == import_id).update(
            {"in_error": True}
        )
        DB.session.commit()
        import_send_mail_error(mail_to=recipients, file_name=file_name, error=e)
        return "Error", 500


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
    import_obj = DB.session.query(TImports).get(import_id)
    import_as_dict = import_obj.as_dict(True)
    import_obj.id_content_mapping = int(id_content_mapping)
    DB.session.commit()

    if import_obj.source_count > current_app.config["IMPORT"]["MAX_LINE_LIMIT"]:
        recipients = []
        REGEX_EMAIL = re.compile(r"[\w\.-]+@[\w\.-]+(?:\.[\w]+)+")
        for auth in import_as_dict["author"]:
            if REGEX_EMAIL.match(auth["email"]):
                recipients.append(auth["email"])
        if len(recipients) == 0:
            raise GeonatureImportApiError(
                message="L'utilisateur ne dispose pas d'email (ou il est invalide)",
                status_code=400,
            )
        import_obj.processing = True
        DB.session.commit()
        import_data = {
            "import_id": import_id,
            "id_field_mapping": id_field_mapping,
            "id_content_mapping": id_content_mapping,
        }

        # with Connection(redis.Redis(host='localhost', port='6379')):
        # q = Queue()
        # job = q.enqueue(data_checker_task, import_data)
        # print("Task ({}) added to queue at {}".format(job.id, job.enqueued_at))
        # lancer geonature launch_redis_worker avec le venv pour que les taches soit traitees

        @copy_current_request_context
        def data_checker_task(import_id, id_field_mapping, id_content_mapping):
            return run_control(
                import_id,
                id_field_mapping,
                id_content_mapping,
                import_as_dict["full_file_name"],
                recipients,
            )

        a = threading.Thread(
            name="data_checker_task", target=data_checker_task, kwargs=import_data
        )
        a.start()

        return import_as_dict
    else:
        try:
            field_mapping_data_checking(import_id, id_field_mapping)
            content_mapping_data_checking(import_id, id_content_mapping)
        except:
            DB.session.query(TImports).filter(TImports.id_import == import_id).update(
                {"in_error": True}
            )
            DB.session.commit()
            raise
        return import_as_dict


# @celery.task
def send_async_email(data):
    print("second thread")

    import_send_mail(mail_to="mail_to", file_name="TEST")


@blueprint.route("/sendemail", methods=["GET"])
@permissions.check_cruved_scope("C", True, module_code="IMPORT")
@json_resp
def sendMail(info_role):

    email_data = {"subject": "Hello from Flask", "to": "ff", "body": "test."}
    # worker = Worker([q], connection=r, name='foo')
    # worker.work()
    job = q.enqueue(send_async_email, email_data)
    printf("Task ({}) added to queue at {}".format(job.id, job.enqueued_at))

    return "DONE"
