"""
     Fonctions permettant l'envoi d'email
"""
from flask import url_for, current_app

from geonature.utils.utilsmails import send_mail


def import_send_mail(mail_to, file_name, id_import):
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
            Les contôles du fichier {file_name} sont terminés !
        </p>
        <p>  Cliquez sur ce <a target="_blank" href="{link}"> lien </a>  
        pour finir l'import </p>
    """.format(
        file_name=file_name,
        link=current_app.config['URL_APPLICATION'] +
        "/#/import/process/step/4/id_import/"+str(id_import)
    )

    send_mail(
        recipients=[mail_to],
        subject="[GeoNature] Import réalisé",
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
