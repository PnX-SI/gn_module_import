import pytest
import pandas as pd
import numpy as np
from collections import namedtuple, OrderedDict
from datetime import datetime
from uuid import UUID
from flask import current_app

from geonature.utils.config import config
from geonature import create_app
from geonature.core.gn_synthese.models import Synthese
from geonature.tests.fixtures import synthese_data
from shapely.geometry import Point

from gn_module_import.models import TImports, BibFields
from gn_module_import.checks.dataframe import *
from gn_module_import.checks.dataframe.geography import (
    check_wkt_inside_area_id,
    check_geometry_inside_l_areas,
)
from ref_geo.models import LAreas


Error = namedtuple("Error", ["error_code", "column", "invalid_rows"], defaults=([],))


@pytest.fixture()
def sample_area():
    return LAreas.query.filter(LAreas.area_name == "Bouches-du-Rhône").one()


@pytest.fixture()
def imprt():
    return TImports(id_import=42, srid="2154")


def get_fields(names):
    fields = OrderedDict()
    for name in names:
        fields[name] = BibFields.query.filter_by(name_field=name).one()
    return fields


def assert_errors(errors, expected):
    errors = frozenset(
        [
            Error(
                error_code=error["error_code"],
                column=error["column"],
                invalid_rows=frozenset(error["invalid_rows"].index.to_list()),
            )
            for error in errors
            if not error["invalid_rows"].empty
        ]
    )
    expected = frozenset(expected)
    assert errors == expected


@pytest.mark.usefixtures("app")
class TestChecks:
    def test_clean_missing_values(self, imprt):
        fields = get_fields(["WKT"])
        df = pd.DataFrame(
            [
                [None],
                [""],
                ["str"],
                [42],
            ],
            columns=[field.source_field for field in fields.values()],
        )
        clean_missing_values(df, fields)
        expected_df = pd.DataFrame(
            [
                [np.nan],
                [np.nan],
                ["str"],
                [42],
            ],
            columns=[field.source_field for field in fields.values()],
        )
        pd.testing.assert_frame_equal(df, expected_df)

    def test_check_required_values(self):
        fields = get_fields(["precision", "cd_nom", "nom_cite"])
        df = pd.DataFrame(
            [
                ["a", np.nan, "c"],
                [np.nan, "b", np.nan],
            ],
            columns=[field.source_column for field in fields.values()],
        )
        errors = check_required_values(df, fields)
        assert_errors(
            errors,
            expected=[
                Error(error_code="MISSING_VALUE", column="cd_nom", invalid_rows=frozenset([0])),
                Error(error_code="MISSING_VALUE", column="nom_cite", invalid_rows=frozenset([1])),
            ],
        )

    def test_check_geography(self, imprt):
        fields = get_fields(
            [
                "WKT",
                "longitude",
                "latitude",
                "codecommune",
                "codemaille",
                "codedepartement",
            ]
        )
        df = pd.DataFrame(
            [
                # [0] No geometry
                [None, None, None, None, None, None],
                # [1] No geometry
                [None, 2, None, None, None, None],
                # [2] No geometry
                [None, None, 3, None, None, None],
                # [3] WKT
                ["Point(600000 7000000)", None, None, None, None, None],
                # [4] XY
                [None, "600000", "7000000", None, None, None],
                # [5] Out-of-bounding-box
                ["Point(10 10)", None, None, None, None, None],
                # [6] Out-of-bounding-box
                [None, "10", "10", None, None, None],
                # [7] WKT and XY is an error
                ["Point(10 10)", "10", "10", None, None, None],
                # [8] Multiple code is an error
                [None, None, None, "42", "42", None],
                # [9] Multiple code is an error
                [None, None, None, "42", None, "42"],
                # [10] Multiple code is an error
                [None, None, None, None, "42", "42"],
                # [11] Multiple code is an error
                [None, None, None, "42", "42", "42"],
                # [12] Ok
                [None, None, None, "42", None, None],
                # [13] Ok
                [None, None, None, None, "42", None],
                # [14] Ok
                [None, None, None, None, None, "42"],
                # [15] Invalid WKT
                ["Point(a b)", None, None, None, None, None],
                # [16] Invalid XY
                [None, "a", "b", None, None, None],
                # [17] Codes are ignored if wkt
                ["Point(600000 7000000)", None, None, "42", "42", "42"],
                # [18] Codes are ignored if xy
                [None, "600000", "7000000", "42", "42", "42"],
            ],
            columns=[field.source_field for field in fields.values()],
        )
        errors = check_geography(
            df,
            fields,
            file_srid=imprt.srid,
        )
        assert_errors(
            errors,
            expected=[
                Error(
                    error_code="NO-GEOM",
                    column="Champs géométriques",
                    invalid_rows=frozenset([0, 1, 2]),
                ),
                Error(error_code="GEOMETRY_OUT_OF_BOX", column="WKT", invalid_rows=frozenset([5])),
                Error(
                    error_code="GEOMETRY_OUT_OF_BOX",
                    column="longitude",
                    invalid_rows=frozenset([6]),
                ),
                Error(
                    error_code="MULTIPLE_ATTACHMENT_TYPE_CODE",
                    column="Champs géométriques",
                    invalid_rows=frozenset([7]),
                ),
                Error(
                    error_code="MULTIPLE_CODE_ATTACHMENT",
                    column="Champs géométriques",
                    invalid_rows=frozenset([8, 9, 10, 11]),
                ),
                Error(error_code="INVALID_WKT", column="WKT", invalid_rows=frozenset([15])),
                Error(
                    error_code="INVALID_GEOMETRY", column="longitude", invalid_rows=frozenset([16])
                ),
            ],
        )

    def test_check_geography_outside(self, imprt, sample_area):
        fields = get_fields(
            [
                "WKT",
                "longitude",
                "latitude",
                "codecommune",
                "codemaille",
                "codedepartement",
            ]
        )
        df = pd.DataFrame(
            [
                ["Point(600000 7000000)", None, None, None, None, None],
                [None, "600000", "7000000", None, None, None],
            ],
            columns=[field.source_field for field in fields.values()],
        )

        errors = check_geography(df, fields, file_srid=imprt.srid, id_area=sample_area.id_area)

        assert_errors(
            errors,
            expected=[
                Error(error_code="GEOMETRY_OUTSIDE", column="WKT", invalid_rows=frozenset([0])),
                Error(
                    error_code="GEOMETRY_OUTSIDE", column="longitude", invalid_rows=frozenset([1])
                ),
            ],
        )

    def test_check_types(self, imprt):
        uuid = "82ff094c-c3b3-11eb-9804-bfdc95e73f38"
        fields = get_fields(
            [
                "datetime_min",
                "datetime_max",
                "meta_v_taxref",
                "digital_proof",
                "id_digitiser",
                "unique_id_sinp",
            ]
        )
        df = pd.DataFrame(
            [
                ["2020-01-01", "2020-01-02", "taxref", "proof", "42", uuid],  # OK
                ["2020-01-01", "AAAAAAAAAA", "taxref", "proof", "42", uuid],  # KO: invalid date
                ["2020-01-01", "2020-01-02", "taxref", "proof", "AA", uuid],  # KO: invalid integer
                ["2020-01-01", "2020-01-02", "taxref", "proof", "42", "AA"],  # KO: invalid uuid
                ["2020-01-01", "2020-01-02", "A" * 80, "proof", "42", uuid],  # KO: invalid length
            ],
            columns=[field.source_column for field in fields.values()],
        )
        errors = list(check_types(df, fields))
        assert_errors(
            errors,
            expected=[
                Error(
                    error_code="INVALID_DATE", column="datetime_max", invalid_rows=frozenset([1])
                ),
                Error(
                    error_code="INVALID_INTEGER",
                    column="id_digitiser",
                    invalid_rows=frozenset([2]),
                ),
                Error(
                    error_code="INVALID_UUID", column="unique_id_sinp", invalid_rows=frozenset([3])
                ),
                Error(
                    error_code="INVALID_CHAR_LENGTH",
                    column="meta_v_taxref",
                    invalid_rows=frozenset([4]),
                ),
            ],
        )

    def test_concat_dates(self, imprt):
        fields = get_fields(["date_min", "hour_min", "date_max", "hour_max"])
        df = pd.DataFrame(
            [
                ["2020-01-01", "12:00:00", "2020-01-02", "14:00:00"],
                ["2020-01-01", "", "2020-01-02", "14:00:00"],
                ["2020-01-01", "12:00:00", "", "14:00:00"],
                ["2020-01-01", "12:00:00", "2020-01-01", ""],
                ["2020-01-01", "12:00:00", "2020-01-02", ""],
                ["2020-01-01", "12:00:00", "", ""],
                ["2020-01-01", "", "2020-01-02", ""],
                ["2020-01-01", "", "", "14:00:00"],
                ["2020-01-01", "", "", ""],
                ["", "12:00:00", "2020-01-02", "14:00:00"],
                ["bogus", "12:00:00", "2020-01-02", "14:00:00"],
            ],
            columns=[field.source_field for field in fields.values()],
        )
        clean_missing_values(df, fields)  # replace '' with np.nan
        concat_dates(df, fields)
        errors = list(check_required_values(df, fields))
        assert_errors(
            errors,
            expected=[
                Error(error_code="MISSING_VALUE", column="date_min", invalid_rows=frozenset([9])),
            ],
        )
        errors = list(check_types(df, fields))
        assert_errors(
            errors,
            expected=[
                Error(
                    error_code="INVALID_DATE", column="datetime_min", invalid_rows=frozenset([10])
                ),
            ],
        )
        pd.testing.assert_frame_equal(
            df.loc[
                :, [fields["datetime_min"].synthese_field, fields["datetime_max"].synthese_field]
            ],
            pd.DataFrame(
                [
                    [datetime(2020, 1, 1, 12), datetime(2020, 1, 2, 14)],
                    [datetime(2020, 1, 1, 0), datetime(2020, 1, 2, 14)],
                    [datetime(2020, 1, 1, 12), datetime(2020, 1, 1, 14)],
                    [datetime(2020, 1, 1, 12), datetime(2020, 1, 1, 12)],
                    [datetime(2020, 1, 1, 12), datetime(2020, 1, 2, 0)],
                    [datetime(2020, 1, 1, 12), datetime(2020, 1, 1, 12)],
                    [datetime(2020, 1, 1, 0), datetime(2020, 1, 2, 0)],
                    [datetime(2020, 1, 1, 0), datetime(2020, 1, 1, 14)],
                    [datetime(2020, 1, 1, 0), datetime(2020, 1, 1, 0)],
                    [pd.NaT, datetime(2020, 1, 2, 14)],
                    [pd.NaT, datetime(2020, 1, 2, 14)],
                ],
                columns=[fields[name].synthese_field for name in ("datetime_min", "datetime_max")],
            ),
        )

    def test_dates_parsing(self, imprt):
        fields = get_fields(["date_min", "hour_min"])
        df = pd.DataFrame(
            [
                ["2020-01-05", ""],
                ["2020/01/05", ""],
                ["2020-1-05", ""],
                ["2020/01/5", ""],
                ["05-01-2020", ""],
                ["05/01/2020", ""],
                ["05-1-2020", ""],
                ["5/01/2020", ""],
                ["2020.01.05", ""],
                ["05.01.2020", ""],
                ["2020-01-05", "13"],
                ["2020-01-05", "13:12"],
                ["2020-01-05", "13:12:05"],
                ["2020-01-05", "13h"],
                ["2020-01-05", "13h12"],
                ["2020-01-05", "13h12m"],
                ["2020-01-05", "13h12m05s"],
            ],
            columns=[field.source_field for field in fields.values()],
        )
        clean_missing_values(df, fields)  # replace '' with np.nan
        concat_dates(df, fields)
        errors = list(check_types(df, fields))
        assert_errors(errors, expected=[])
        pd.testing.assert_frame_equal(
            df.loc[:, [fields["datetime_min"].synthese_field]],
            pd.DataFrame(
                [
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 0)],
                    [datetime(2020, 1, 5, 13)],
                    [datetime(2020, 1, 5, 13, 12)],
                    [datetime(2020, 1, 5, 13, 12, 5)],
                    [datetime(2020, 1, 5, 13)],
                    [datetime(2020, 1, 5, 13, 12)],
                    [datetime(2020, 1, 5, 13, 12)],
                    [datetime(2020, 1, 5, 13, 12, 5)],
                ],
                columns=[fields["datetime_min"].synthese_field],
            ),
        )

    def test_check_counts(self, imprt):
        default_value = current_app.config["IMPORT"]["DEFAULT_COUNT_VALUE"]
        fields = get_fields(["count_min", "count_max"])
        df = pd.DataFrame(
            [
                [None, None],
                [1, None],
                [None, 2],
                [1, 2],
                [2, 1],
                [20, 5],
            ],
            columns=[field.synthese_field for field in fields.values()],
        )
        errors = list(check_counts(df, fields))
        assert_errors(
            errors,
            expected=[
                Error(
                    error_code="COUNT_MIN_SUP_COUNT_MAX",
                    column="count_min",
                    invalid_rows=frozenset([4, 5]),
                ),
            ],
        )
        pd.testing.assert_frame_equal(
            df.loc[:, [field.synthese_field for field in fields.values()]],
            pd.DataFrame(
                [
                    [default_value, default_value],
                    [1, 1],
                    [default_value, 2],
                    [1, 2],
                    [2, 1],
                    [20, 5],
                ],
                columns=[field.synthese_field for field in fields.values()],
                dtype=float,
            ),
        )

        fields = get_fields(["count_min", "count_max"])
        count_min_field = fields["count_min"]
        count_max_field = fields["count_max"]
        df = pd.DataFrame(
            [
                [None],
                [2],
            ],
            columns=[count_min_field.synthese_field],
        )
        list(check_counts(df, {"count_min": count_min_field}))
        pd.testing.assert_frame_equal(
            df.loc[:, [count_min_field.synthese_field, count_max_field.synthese_field]],
            pd.DataFrame(
                [
                    [default_value, default_value],
                    [2, 2],
                ],
                columns=[
                    count_min_field.synthese_field,
                    count_max_field.synthese_field,
                ],
                dtype=float,
            ),
        )

        df = pd.DataFrame(
            [
                [None],
                [2],
            ],
            columns=[count_max_field.synthese_field],
        )
        list(check_counts(df, {"count_max": count_max_field}))
        pd.testing.assert_frame_equal(
            df.loc[:, [count_min_field.synthese_field, count_max_field.synthese_field]],
            pd.DataFrame(
                [
                    [default_value, default_value],
                    [2, 2],
                ],
                columns=[
                    count_min_field.synthese_field,
                    count_max_field.synthese_field,
                ],
                dtype=float,
            ),
        )

        df = pd.DataFrame([[], []])
        list(check_counts(df, {}))
        pd.testing.assert_frame_equal(
            df.loc[:, [count_min_field.synthese_field, count_max_field.synthese_field]],
            pd.DataFrame(
                [
                    [default_value, default_value],
                    [default_value, default_value],
                ],
                columns=[
                    count_min_field.synthese_field,
                    count_max_field.synthese_field,
                ],
            ),
        )

    def test_check_wkt_inside_area_id(self, imprt, sample_area):
        wkt = "POINT(900000 6250000)"

        check = check_wkt_inside_area_id(id_area=sample_area.id_area, wkt=wkt, wkt_srid=imprt.srid)

        assert check

    def test_check_wkt_inside_area_id_outside(self, imprt, sample_area):
        wkt = "Point(6000000 700000)"

        check = check_wkt_inside_area_id(id_area=sample_area.id_area, wkt=wkt, wkt_srid=imprt.srid)

        assert not check

    def test_check_geometry_inside_l_areas(self, imprt, sample_area):
        point = Point(900000, 6250000)

        check = check_geometry_inside_l_areas(
            id_area=sample_area.id_area, geometry=point, geom_srid=imprt.srid
        )

        assert check

    def test_check_geometry_inside_l_areas_outside(self, imprt, sample_area):
        point = Point(6000000, 700000)

        check = check_geometry_inside_l_areas(
            id_area=sample_area.id_area, geometry=point, geom_srid=imprt.srid
        )

        assert not check
