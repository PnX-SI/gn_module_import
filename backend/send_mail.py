"""
     Fonctions permettant l'envoi d'email
"""
from flask import url_for, current_app

from geonature.utils.utilsmails import send_mail


def import_send_mail(mail_to, file_name, step, id_import):
    """
        Send email after export is done

        .. :quickref: Imports

        Send email after import is done
        :param mail_to: User who runs the import
        :param step: step of the process: 'import' | 'check'
    """
    if step == "check":
        link = f"{current_app.config['URL_APPLICATION']}/#/import/process/id_import/{str(id_import)}/step/4"
        msg = f"""
        Bonjour,
        <p>
            Les vérifications sur le fichier {file_name} sont terminées.
        </p>
        <p>  Cliquez sur ce <a target="_blank" href="{link}"> lien </a>  
        pour terminer l'import dans la synthèse.</p>

        """
    else:
        msg = f"""
        Bonjour,
        <p>
            L'import du fichier {file_name} dans la synthèse est terminé.
        </p>
        """

    send_mail(
        recipients=mail_to,
        subject="[GeoNature] Import réalisé"
        if (step == "import")
        else "[GeoNature] Import - Contrôles terminés",
        msg_html=msg,
    )


def import_send_mail_error(mail_to, file_name, error):
    """
        Send email after export is failed

        .. :quickref: Send email after export is failed


        :query User role: User who runs the export
        :query {} export: Export definition
        :query str error: Detail of the exception raised

    """
    msg = f"""
        Bonjour,
        <p>
            Votre import <i>{file_name}</i> n'a pas fonctionné correctement.
        </p>
        <p> {error} </p>

    """
    send_mail(
        recipients=mail_to,
        subject="[GeoNature][ERREUR] Import {}".format(file_name),
        msg_html=msg,
    )
