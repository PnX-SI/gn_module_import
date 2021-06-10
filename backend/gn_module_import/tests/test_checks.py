import pytest
import pandas as pd
import numpy as np
from collections import namedtuple
from datetime import datetime
from uuid import UUID
from flask import current_app

from geonature.utils.config import config
from geonature import create_app
from geonature.core.gn_synthese.models import Synthese

from gn_module_import.db.models import TImports, BibFields
from gn_module_import.checks import *


Error = namedtuple('Error', ['error_code', 'column', 'invalid_rows'], defaults=([],))


@pytest.fixture()
def imprt():
    return TImports(id_import=42, srid='2154')


def get_synthese_fields(selected_columns):
    return BibFields.query \
                    .filter_by(synthese_field=True) \
                    .filter(BibFields.name_field.in_(selected_columns)) \
                    .all()


def assert_errors(errors, expected):
    errors = frozenset([ Error(error_code=error['error_code'],
                               column=error['column'],
                               invalid_rows=frozenset(error['invalid_rows'].index.to_list()))
                         for error in errors
                         if not error['invalid_rows'].empty ])
    expected = frozenset(expected)
    assert(errors == expected)


@pytest.mark.usefixtures('app')
class TestChecks:
    def test_clean_missing_values(self, imprt):
        df = pd.DataFrame([
            [None],
            [''],
            ['nan'],
            [42],
        ], columns=['wkt'])
        clean_missing_values(df, {'WKT': 'wkt'})
        expected_df = pd.DataFrame([
            [np.nan],
            [np.nan],
            ['nan'],
            [42],
        ], columns=['wkt'])
        pd.testing.assert_frame_equal(df, expected_df)

    def test_check_required_values(self):
        selected_columns = { 'nom_cite': 'nomcite' }
        synthese_fields = get_synthese_fields(selected_columns)
        df = pd.DataFrame(
            [
                ['bombix'],
                [np.nan],
            ],
            columns=['nomcite'],
        )
        errors = check_required_values(df, imprt, selected_columns, synthese_fields)
        assert_errors(errors, expected=[
            Error(error_code='MISSING_VALUE', column='nomcite', invalid_rows=frozenset([1])),
        ])

    def test_check_geography(self, imprt):
        df = pd.DataFrame(
            [
                # [0] No geometry
                [None, None, None, None, None, None],
                # [1] No geometry
                [None, 2, None, None, None, None],
                # [2] No geometry
                [None, None, 3, None, None, None],
                # [3] WKT
                ['Point(600000 7000000)', None, None, None, None, None],
                # [4] XY
                [None, '600000', '7000000', None, None, None],
                # [5] Out-of-bounding-box
                ['Point(10 10)', None, None, None, None, None],
                # [6] Out-of-bounding-box
                [None, '10', '10', None, None, None],
                # [7] WKT and XY is an error
                ['Point(10 10)', '10', '10', None, None, None],
                # [8] Multiple code is an error
                [None, None, None, '42', '42', None],
                # [9] Multiple code is an error
                [None, None, None, '42', None, '42'],
                # [10] Multiple code is an error
                [None, None, None, None, '42', '42'],
                # [11] Multiple code is an error
                [None, None, None, '42', '42', '42'],
                # [12] Ok
                [None, None, None, '42', None, None],
                # [13] Ok
                [None, None, None, None, '42', None],
                # [14] Ok
                [None, None, None, None, None, '42'],
                # [15] Invalid WKT
                ['Point(a b)', None, None, None, None, None],
                # [16] Invalid XY
                [None, 'a', 'b', None, None, None],
                # [17] Codes are ignored if wkt
                ['Point(600000 7000000)', None, None, '42', '42', '42'],
                # [18] Codes are ignored if xy
                [None, '600000', '7000000', '42', '42', '42'],
            ],
            columns=['wkt', 'latitude', 'longitude', 'commune', 'maille', 'departement'],
        )
        mapped_target_fields = {
            'WKT': 'wkt',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'codecommune': 'commune',
            'codemaille': 'maille',
            'codedepartement': 'departement',
        }
        errors = check_geography(df, imprt, mapped_target_fields)
        assert_errors(errors, expected=[
            Error(error_code='NO-GEOM', column='Champs géométriques', invalid_rows=frozenset([0, 1, 2])),
            Error(error_code='GEOMETRY_OUT_OF_BOX', column='wkt', invalid_rows=frozenset([5])),
            Error(error_code='GEOMETRY_OUT_OF_BOX', column='latitude, longitude', invalid_rows=frozenset([6])),
            Error(error_code='MULTIPLE_ATTACHMENT_TYPE_CODE', column='Champs géométriques', invalid_rows=frozenset([7])),
            Error(error_code='MULTIPLE_CODE_ATTACHMENT', column='Champs géométriques', invalid_rows=frozenset([8,9,10, 11])),
            Error(error_code='INVALID_WKT', column='wkt', invalid_rows=frozenset([15])),
            Error(error_code='INVALID_GEOMETRIE', column='latitude, longitude', invalid_rows=frozenset([16])),
        ])

    def test_check_types(self, imprt):
        uuid = '82ff094c-c3b3-11eb-9804-bfdc95e73f38'
        df = pd.DataFrame([
                ['2020-01-01', '2020-01-02', 'taxref', 'proof', '42', uuid],  # OK
                ['2020-01-01', 'AAAAAAAAAA', 'taxref', 'proof', '42', uuid],  # KO: invalid date
                ['2020-01-01', '2020-01-02', 'taxref', 'proof', 'AA', uuid],  # KO: invalid integer
                ['2020-01-01', '2020-01-02', 'taxref', 'proof', '42', 'AA'],  # KO: invalid uuid
                ['2020-01-01', '2020-01-02', 'A' * 80, 'proof', '42', uuid],  # KO: invalid length
            ],
            columns=['concatened_date_min', 'concatened_date_max', 'taxref', 'proof',
                     'digitizer', 'sinp'],
        )
        selected_columns = {
            'date_min': 'concatened_date_min',
            'orig_date_min': 'datemin',
            'date_max': 'concatened_date_max',
            'orig_date_max': 'datemax',
            'meta_v_taxref': 'taxref',  # Unicode(length=50)
            'digital_proof': 'proof',  # UnicodeText
            'id_digitiser': 'digitizer',  # Integer
            'unique_id_sinp': 'sinp',  # UUID
        }
        synthese_fields = get_synthese_fields(selected_columns)
        errors = list(check_types(df, imprt, selected_columns, synthese_fields))
        assert_errors(errors, expected=[
            Error(error_code='INVALID_DATE', column='datemax', invalid_rows=frozenset([1])),
            Error(error_code='INVALID_INTEGER', column='digitizer', invalid_rows=frozenset([2])),
            Error(error_code='INVALID_UUID', column='sinp', invalid_rows=frozenset([3])),
            Error(error_code='INVALID_CHAR_LENGTH', column='taxref', invalid_rows=frozenset([4])),
        ])

    def test_concat_dates(self, imprt):
        df = pd.DataFrame([
                ['2020-01-01', '12:00:00', '2020-01-02', '14:00:00'],
                ['2020-01-01',         '', '2020-01-02', '14:00:00'],
                ['2020-01-01', '12:00:00',           '', '14:00:00'],
                ['2020-01-01', '12:00:00', '2020-01-01',         ''],
                ['2020-01-01', '12:00:00', '2020-01-02',         ''],
                ['2020-01-01', '12:00:00',           '',         ''],
                ['2020-01-01',         '', '2020-01-02',         ''],
                ['2020-01-01',         '',           '', '14:00:00'],
                ['2020-01-01',         '',           '',         ''],
                [          '', '12:00:00', '2020-01-02', '14:00:00'],
                [     'bogus', '12:00:00', '2020-01-02', '14:00:00'],
            ],
            columns=['datemin', 'heuremin', 'datemax', 'heuremax'],
        )
        selected_columns = {
            'date_min': 'datemin',
            'hour_min': 'heuremin',
            'date_max': 'datemax',
            'hour_max': 'heuremax',
        }
        synthese_fields = get_synthese_fields(selected_columns)
        clean_missing_values(df, selected_columns)  # replace '' with np.nan
        concat_dates(df, selected_columns)
        errors = list(check_types(df, imprt, selected_columns, synthese_fields))
        assert_errors(errors, expected=[
            Error(error_code='INVALID_DATE', column='datemin', invalid_rows=frozenset([9, 10])),
        ])
        pd.testing.assert_frame_equal(
            df.loc[:, ['concatened_date_min', 'concatened_date_max']],
            pd.DataFrame([
                [datetime(2020, 1, 1, 12), datetime(2020, 1, 2, 14)],
                [datetime(2020, 1, 1,  0), datetime(2020, 1, 2, 14)],
                [datetime(2020, 1, 1, 12), datetime(2020, 1, 1, 14)],
                [datetime(2020, 1, 1, 12), datetime(2020, 1, 1, 12)],
                [datetime(2020, 1, 1, 12), datetime(2020, 1, 2,  0)],
                [datetime(2020, 1, 1, 12), datetime(2020, 1, 1, 12)],
                [datetime(2020, 1, 1,  0), datetime(2020, 1, 2,  0)],
                [datetime(2020, 1, 1,  0), datetime(2020, 1, 1, 14)],
                [datetime(2020, 1, 1,  0), datetime(2020, 1, 1,  0)],
                [                  pd.NaT, datetime(2020, 1, 2, 14)],
                [                  pd.NaT, datetime(2020, 1, 2, 14)],
            ], columns=['concatened_date_min', 'concatened_date_max']),
        )

    def test_check_dates(self):
        df = pd.DataFrame([
                [datetime(2020, 1, 1), datetime(2020, 1, 1)],
                [datetime(2020, 1, 2), datetime(2020, 1, 1)],
            ],
            columns=['concatened_date_min', 'concatened_date_max'],
        )
        selected_columns = {
            'orig_date_min': 'datemin',
        }
        errors = list(check_dates(df, selected_columns))
        assert_errors(errors, expected=[
            Error(error_code='DATE_MIN_SUP_DATE_MAX', column='datemin', invalid_rows=frozenset([1])),
        ])

    def test_check_uuid(self, imprt):
        uuid1 = UUID('82ff094c-c3b3-11eb-9804-bfdc95e73f38')
        uuid2 = UUID('acad9eb6-c773-11eb-8b3e-f3419e53c26b')
        existing_uuid = Synthese.query.first().unique_id_sinp
        df = pd.DataFrame([
                [uuid1],
                [uuid1],
                [uuid2],
                [existing_uuid],
                [None],
                [None],
            ],
            columns=['sinp'],
        )
        selected_columns = {
            'unique_id_sinp': 'sinp',
        }
        errors = list(check_uuid(df, selected_columns))
        assert_errors(errors, expected=[
            Error(error_code='DUPLICATE_UUID', column='sinp', invalid_rows=frozenset([0, 1])),
            Error(error_code='EXISTING_UUID', column='sinp', invalid_rows=frozenset([3])),
        ])

        df = pd.DataFrame([
                [None],
                [uuid1],
            ],
            columns=['sinp'],
        )
        selected_columns = {
            'unique_id_sinp': 'sinp',
            'unique_id_sinp_generate': 'true',
        }
        list(check_uuid(df, selected_columns))
        generated_sinp_field = selected_columns['unique_id_sinp']
        pd.testing.assert_series_equal(
            df[generated_sinp_field].notnull(),
            pd.Series([True, True], name=generated_sinp_field),
        )


        df = pd.DataFrame([[], []])
        selected_columns = {
            'unique_id_sinp_generate': 'true',
        }
        list(check_uuid(df, selected_columns))
        generated_sinp_field = selected_columns['unique_id_sinp']
        pd.testing.assert_series_equal(
            df[generated_sinp_field].notnull(),
            pd.Series([True, True], name=generated_sinp_field),
        )
        pd.testing.assert_series_equal(
            df[generated_sinp_field].duplicated(keep=False),
            pd.Series([False, False], name=generated_sinp_field),
        )

    def test_check_counts(self, imprt):
        default_min_value = current_app.config["IMPORT"]["DEFAULT_COUNT_VALUE"]
        df = pd.DataFrame([
                [None, None],
                [1, None],
                [None, 2],
                [1, 2],
                [2, 1],
            ],
            columns=['min', 'max'],
        )
        errors = list(check_counts(df, {
            'count_min': 'min',
            'count_max': 'max',
        }))
        assert_errors(errors, expected=[
            Error(error_code='COUNT_MIN_SUP_COUNT_MAX', column='min', invalid_rows=frozenset([4])),
        ])
        pd.testing.assert_frame_equal(
            df,
            pd.DataFrame([
                [default_min_value, default_min_value],
                [1, 1],
                [default_min_value, 2],
                [1, 2],
                [2, 1],
            ], columns=['min', 'max'], dtype=np.float),
        )

        df = pd.DataFrame([
                [None],
                [2],
            ],
            columns=['min',],
        )
        list(check_counts(df, {
            'count_min': 'min',
        }))
        pd.testing.assert_frame_equal(
            df,
            pd.DataFrame([
                [default_min_value, default_min_value],
                [2, 2],
            ], columns=['min', '_count_max'], dtype=np.float),
        )

        df = pd.DataFrame([
                [None],
                [2],
            ],
            columns=['max'],
        )
        list(check_counts(df, {
            'count_max': 'max',
        }))
        pd.testing.assert_frame_equal(
            df.loc[:, ['_count_min', 'max']],
            pd.DataFrame([
                [None, None],
                [default_min_value, 2],
            ], columns=['_count_min', 'max']),
        )

        df = pd.DataFrame()
        list(check_counts(df, {}))
        pd.testing.assert_frame_equal(df, pd.DataFrame())
