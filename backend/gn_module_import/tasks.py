from datetime import datetime

from flask import current_app
from sqlalchemy import func, distinct
from sqlalchemy.dialects.postgresql import array_agg, aggregate_order_by
from celery.utils.log import get_task_logger

from geonature.core.gn_synthese.models import Synthese, TSources
from geonature.utils.env import db
from geonature.utils.celery import celery_app
from geonature.utils.sentry import start_sentry_child

from gn_module_import.models import TImports, BibFields, ImportSyntheseData
from geonature.core.gn_commons.models import TModules
from gn_module_import.checks.dataframe import run_all_checks
from gn_module_import.checks.dataframe.geography import set_the_geom_column
from gn_module_import.utils import (
    load_import_data_in_dataframe,
    update_import_data_from_dataframe,
    import_data_to_synthese,
)
from gn_module_import.checks.sql import (
    do_nomenclatures_mapping,
    check_nomenclatures,
    complete_others_geom_columns,
    set_cd_nom,
    set_cd_hab,
    set_altitudes,
    set_uuid,
    check_mandatory_fields,
    check_duplicates_source_pk,
    check_dates,
    check_altitudes,
    check_depths,
    check_digital_proof_urls,
)

from geonature.core.notifications.utils import dispatch_notifications

logger = get_task_logger(__name__)


@celery_app.task(bind=True)
def do_import_checks(self, import_id):
    logger.info(f"Starting verification of import {import_id}.")
    imprt = TImports.query.get(import_id)
    if imprt is None or imprt.task_id != self.request.id:
        logger.warning("Task cancelled, doing nothing.")
        return

    self.update_state(state="PROGRESS", meta={"progress": 0})

    selected_fields_names = [
        field_name
        for field_name, source_field in imprt.fieldmapping.items()
        if source_field in imprt.columns
    ]
    selected_fields = BibFields.query.filter(BibFields.name_field.in_(selected_fields_names)).all()

    fields = {
        field.name_field: field
        for field in selected_fields
        if (  # handled in SQL, exclude from dataframe
            field.source_field is not None
            and field.mnemonique is None
            and field.name_field not in ["cd_nom", "cd_hab"]
        )
    }

    # Checks on dataframe
    logger.info("Loading import data in dataframe…")
    with start_sentry_child(op="check.df", description="load dataframe"):
        df = load_import_data_in_dataframe(imprt, fields)
    self.update_state(state="PROGRESS", meta={"progress": 0.1})
    logger.info("Running dataframe checks…")
    with start_sentry_child(op="check.df", description="run all checks"):
        run_all_checks(imprt, fields, df)
    self.update_state(state="PROGRESS", meta={"progress": 0.2})
    logger.info("Completing geometric columns…")
    with start_sentry_child(op="check.df", description="set geom column"):
        set_the_geom_column(imprt, fields, df)
    self.update_state(state="PROGRESS", meta={"progress": 0.3})
    logger.info("Updating import data from dataframe…")
    with start_sentry_child(op="check.df", description="save dataframe"):
        update_import_data_from_dataframe(imprt, fields, df)
    self.update_state(state="PROGRESS", meta={"progress": 0.4})

    fields.update({field.name_field: field for field in selected_fields})

    # Checks in SQL
    sql_checks = [
        complete_others_geom_columns,
        do_nomenclatures_mapping,
        check_nomenclatures,
        set_cd_nom,
        set_cd_hab,
        check_duplicates_source_pk,
        set_altitudes,
        check_altitudes,
        set_uuid,
        check_dates,
        check_depths,
        check_digital_proof_urls,
        check_mandatory_fields,
    ]
    with start_sentry_child(op="check.sql", description="run all checks"):
        for i, check in enumerate(sql_checks):
            logger.info(f"Running SQL check '{check.__name__}'…")
            with start_sentry_child(op="check.sql", description=check.__name__):
                check(imprt, fields)
            progress = 0.4 + ((i + 1) / len(sql_checks)) * 0.6
            self.update_state(state="PROGRESS", meta={"progress": progress})

    imprt = TImports.query.with_for_update(of=TImports).get(import_id)
    if imprt is None or imprt.task_id != self.request.id:
        logger.warning("Task cancelled, rollback changes.")
        db.session.rollback()
    else:
        logger.info("All done, committing…")
        imprt.processed = True
        imprt.task_id = None
        imprt.erroneous_rows = (
            db.session.query(
                array_agg(
                    aggregate_order_by(ImportSyntheseData.line_no, ImportSyntheseData.line_no)
                )
            )
            .filter_by(imprt=imprt, valid=False)
            .scalar()
        )
        db.session.commit()


@celery_app.task(bind=True)
def do_import_in_synthese(self, import_id):
    logger.info(f"Starting insertion in synthese of import {import_id}.")
    imprt = TImports.query.get(import_id)
    if imprt is None or imprt.task_id != self.request.id:
        logger.warning("Task cancelled, doing nothing.")
        return
    if not imprt.source:
        entity_source_pk_field = BibFields.query.filter_by(
            name_field="entity_source_pk_value"
        ).one()
        imprt.source = TSources(
            name_source=imprt.source_name,
            desc_source=f"Imported data from import module (id={import_id})",
            entity_source_pk_field=entity_source_pk_field.synthese_field,
            module=TModules.query.filter_by(module_code="IMPORT").one(),
        )
    import_data_to_synthese(imprt)
    ImportSyntheseData.query.filter_by(imprt=imprt).delete()
    imprt = TImports.query.with_for_update(of=TImports).get(import_id)
    if imprt is None or imprt.task_id != self.request.id:
        logger.warning("Task cancelled, rollback changes.")
        db.session.rollback()
    else:
        logger.info("All done, committing…")
        imprt.date_end_import = datetime.now()
        imprt.loaded = False
        imprt.task_id = None
        imprt.import_count = (
            db.session.query(func.count(Synthese.id_synthese))
            .filter_by(source=imprt.source)
            .scalar()
        )
        imprt.taxa_count = (
            db.session.query(func.count(distinct(Synthese.cd_nom)))
            .filter_by(source=imprt.source)
            .scalar()
        )

        # Send element to notification system
        notify_import_in_synthese_done(imprt)

        db.session.commit()


# Send notification
def notify_import_in_synthese_done(imprt):
    id_authors = [author.id_role for author in imprt.authors]
    dispatch_notifications(
        code_categories=["IMPORT-DONE%"],
        id_roles=id_authors,
        title="Import dans la synthèse",
        url=(current_app.config["URL_APPLICATION"] + f"/#/import/{imprt.id_import}/report"),
        context={
            "import": imprt,
            "url_notification_rules": current_app.config["URL_APPLICATION"]
            + "/#/notification/rules",
        },
    )
