from uuid import uuid4
from itertools import chain

from sqlalchemy.orm.exc import NoResultFound
from flask import current_app

from geonature.utils.env import DB as db
from geonature.core.gn_synthese.models import Synthese

from gn_module_import.db.models import ImportUserError, ImportUserErrorType

from .missing import clean_missing_values, check_required_values
from .geography import check_geography
from .types import check_types
from .dates import concat_dates


def update_dicts(generator, **kwargs):
    for item in generator:
        item.update(**kwargs)
        yield item


def check_duplicate(df, source_field):
    duplicated = df[source_field].duplicated(keep=False)
    yield dict(column=source_field, invalid_rows=df[df[source_field].notnull() & duplicated])


def check_uuid(df, selected_columns):
    if selected_columns.get('unique_id_sinp_generate') == 'true':
        df['_generated_unique_id_sinp'] = [uuid4() for _ in range(len(df.index))]
        selected_columns['unique_id_sinp'] = '_generated_unique_id_sinp'
    elif 'unique_id_sinp' in selected_columns:
        uuid_field = selected_columns['unique_id_sinp']
        yield from update_dicts(
            check_duplicate(df, uuid_field),
            error_code='DUPLICATE_UUID')
        # FIXME: this can not scale!!!!
        existing_uuids = set(chain.from_iterable(
                            Synthese.query \
                            .filter(Synthese.unique_id_sinp.isnot(None)) \
                            .with_entities(Synthese.unique_id_sinp) \
                            .all()))
        invalid_rows = df[df[uuid_field].isin(existing_uuids)]
        yield {
            'error_code': 'EXISTING_UUID',
            'column': uuid_field,
            'invalid_rows': invalid_rows,
        }
    # TODO: vérifier les champs uuid autre que unique_id_sinp
    # TODO: vérifier qu’on a bien toujours une valeur présente (check_required_values??)


def check_entity_source(df, selected_columns):
    if 'entity_source_pk_value' in selected_columns:
        yield from update_dicts(
            check_duplicate(df, selected_columns['entity_source_pk_value']),
            error_code='DUPLICATE_ENTITY_SOURCE_PK')


def check_ordering(df, min_field, max_field):
    ordered = df[min_field] <= df[max_field]
    ordered = ordered.fillna(False)
    invalid_rows = df[~ordered & df[min_field].notna() & df[max_field].notna()]
    yield dict(invalid_rows=invalid_rows, column=min_field)


def check_altitude(df, selected_columns):
    if 'altitude_min' in selected_columns and 'altitude_max' in selected_columns:
        yield from update_dicts(
            check_ordering(df, selected_columns['altitude_min'], selected_columns['altitude_max']),
            error_code='ALTI_MIN_SUP_ALTI_MAX')


def check_depth(df, selected_columns):
    if 'depth_min' in selected_columns and 'depth_max' in selected_columns:
        yield from update_dicts(
            check_ordering(df, selected_columns['depth_min'], selected_columns['depth_max']),
            error_code='DEPTH_MIN_SUP_DEPTH_MAX')


def check_counts(df, selected_columns):
    default_count_min = current_app.config["IMPORT"]["DEFAULT_COUNT_VALUE"]
    if 'count_min' in selected_columns:
        count_min_field = selected_columns['count_min']
        df[count_min_field] = df[count_min_field].where(df[count_min_field].notna(),
                                                        other=default_count_min)
        if 'count_max' in selected_columns:
            count_max_field = selected_columns['count_max']
            yield from update_dicts(
                check_ordering(df, count_min_field, count_max_field),
                error_code='COUNT_MIN_SUP_COUNT_MAX')
            df[count_max_field] = df[count_max_field].where(df[count_max_field].notna(),
                                                            other=df[count_min_field])
        else:
            count_max_field = '_count_max'
            selected_columns['count_max'] = count_max_field
            df[count_max_field] = df[count_min_field]
    else:
        if 'count_max' in selected_columns:
            count_max_field = selected_columns['count_max']
            count_min_field = '_count_min'
            selected_columns['count_min'] = count_min_field
            df.loc[df[count_max_field].notna(), count_min_field] = default_count_min
        else:
            # no count_min & no count_max
            pass


def check_dates(df, selected_columns):
    yield from update_dicts(
        check_ordering(df, 'concatened_date_min', 'concatened_date_max'),
        column=selected_columns['orig_date_min'],
        error_code='DATE_MIN_SUP_DATE_MAX')
    


def _run_all_checks(df, imprt, selected_columns, synthese_fields):
    concat_dates(df, selected_columns)
    clean_missing_values(df, selected_columns)
    yield from check_required_values(df, imprt, selected_columns, synthese_fields)
    yield from check_types(df, imprt, selected_columns, synthese_fields)
    yield from check_dates(df, selected_columns)
    yield from check_geography(df, imprt, selected_columns)
    yield from check_uuid(df, selected_columns)
    yield from check_entity_source(df, selected_columns)
    yield from check_altitude(df, selected_columns)
    yield from check_depth(df, selected_columns)
    yield from check_counts(df, selected_columns)
    # TODO: check referential cd_nom, cd_hab


def run_all_checks(df, imprt, selected_columns, synthese_fields):
    for error in _run_all_checks(df, imprt, selected_columns, synthese_fields):
        if error['invalid_rows'].empty:
            continue
        try:
            error_type = ImportUserErrorType.query.filter_by(name=error['error_code']).one()
        except NoResultFound:
            raise Exception(f"Error code '{error_code}' not found.")
        invalid_rows = error['invalid_rows']
        df['gn_is_valid'][invalid_rows.index] = False
        df['gn_invalid_reason'][invalid_rows.index & df['gn_invalid_reason'].isnull()] = \
                f'{error_type.name}'  # FIXME comment
        ordered_invalid_rows = sorted([ row + 1 for row in invalid_rows.index.to_list()])
        error = ImportUserError(
            id_import=imprt.id_import,
            type=error_type,
            column=error['column'],
            rows=ordered_invalid_rows,
            step='',
            comment=error.get('comment'),
        )
        db.session.add(error)
