"""
     Fonctions permettant l'envoi d'email
"""
from flask import url_for, current_app

from geonature.utils.utilsmails import send_mail


def import_send_mail(mail_to, file_name):
    """
        Send email after export is done

        .. :quickref: Send email after import is done


        :query User role: User who runs the import
        :query {} import: Import definition
        :query str file_name: Name of imported file
    """

    msg = """
        Bonjour,
        <p>
            Votre fichier à été bien importé. :D 
        </p>

    """
    send_mail(
        recipients=["aboubaker.oudghiri@gmail.com"],
        subject="[GeoNature] Import  réalisé",
        msg_html=msg
    )


def import_send_mail_error(role, export, error):
    """
        Send email after export is failed

        .. :quickref: Send email after export is failed


        :query User role: User who runs the export
        :query {} export: Export definition
        :query str error: Detail of the exception raised

    """

    label = ""
    if export:
        label = export['label']

    msg = """
        Bonjour,
        <p>
            Votre import <i>{}</i> n'a pas fonctionné correctement.
        </p>

    """.format(label, error)
    send_mail(
        recipients=[role.email],
        subject="[GeoNature][ERREUR] Import {}".format(label),
        msg_html=msg
    )
