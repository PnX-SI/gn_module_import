from datetime import datetime
from math import ceil

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
    mark_all_rows_as_invalid,
    update_import_data_from_dataframe,
    import_data_to_synthese,
)
from gn_module_import.checks.sql import (
    do_nomenclatures_mapping,
    check_nomenclatures,
    complete_others_geom_columns,
    check_cd_nom,
    check_cd_hab,
    set_altitudes,
    set_uuid,
    check_duplicates_source_pk,
    check_dates,
    check_altitudes,
    check_depths,
    check_digital_proof_urls,
    check_geography_outside,
    check_is_valid_geography,
)

from geonature.core.notifications.utils import dispatch_notifications

logger = get_task_logger(__name__)


@celery_app.task(bind=True)
def do_import_checks(self, import_id):
    logger.info(f"Starting verification of import {import_id}.")
    imprt = db.session.get(TImports, import_id)
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
            field.source_field is not None and field.mnemonique is None
        )
    }

    with start_sentry_child(op="check.df", description="mark_all"):
        mark_all_rows_as_invalid(imprt)
    self.update_state(state="PROGRESS", meta={"progress": 0.1})

    batch_size = current_app.config["IMPORT"]["DATAFRAME_BATCH_SIZE"]
    batch_count = ceil(imprt.source_count / batch_size)

    def update_batch_progress(batch, step):
        start = 0.1
        end = 0.4
        step_count = 4
        progress = start + ((batch + step / step_count) / batch_count) * (end - start)
        self.update_state(state="PROGRESS", meta={"progress": progress})

    for batch in range(batch_count):
        offset = batch * batch_size
        batch_fields = fields.copy()
        # Checks on dataframe
        logger.info(f"[{batch+1}/{batch_count}] Loading import data in dataframe…")
        with start_sentry_child(op="check.df", description="load dataframe"):
            df = load_import_data_in_dataframe(imprt, batch_fields, offset, batch_size)
        update_batch_progress(batch, 1)
        logger.info(f"[{batch+1}/{batch_count}] Running dataframe checks…")
        with start_sentry_child(op="check.df", description="run all checks"):
            run_all_checks(imprt, batch_fields, df)
        update_batch_progress(batch, 2)
        logger.info(f"[{batch+1}/{batch_count}] Completing geometric columns…")
        with start_sentry_child(op="check.df", description="set geom column"):
            set_the_geom_column(imprt, batch_fields, df)
        update_batch_progress(batch, 3)
        logger.info(f"[{batch+1}/{batch_count}] Updating import data from dataframe…")
        with start_sentry_child(op="check.df", description="save dataframe"):
            update_import_data_from_dataframe(imprt, batch_fields, df)
        update_batch_progress(batch, 4)

    fields = batch_fields  # retrive fields added during dataframe checks
    fields.update({field.name_field: field for field in selected_fields})

    # Checks in SQL
    sql_checks = [
        complete_others_geom_columns,
        do_nomenclatures_mapping,
        check_nomenclatures,
        check_cd_nom,
        check_cd_hab,
        check_duplicates_source_pk,
        set_altitudes,
        check_altitudes,
        set_uuid,
        check_dates,
        check_depths,
        check_digital_proof_urls,
        check_is_valid_geography,
        check_geography_outside,
    ]
    with start_sentry_child(op="check.sql", description="run all checks"):
        for i, check in enumerate(sql_checks):
            logger.info(f"Running SQL check '{check.__name__}'…")
            with start_sentry_child(op="check.sql", description=check.__name__):
                check(imprt, fields)
            progress = 0.4 + ((i + 1) / len(sql_checks)) * 0.6
            self.update_state(state="PROGRESS", meta={"progress": progress})

    imprt = db.session.get(TImports, import_id, with_for_update={"of": TImports})
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
    imprt = db.session.get(TImports, import_id)
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
    imprt = db.session.get(TImports, import_id, with_for_update={"of": TImports})
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
