from typing import Dict
from functools import partial

import sqlalchemy as sa
from geoalchemy2.functions import ST_Transform, ST_GeomFromWKB
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, Polygon
from shapely.ops import transform
from pyproj import CRS, Transformer

from geonature.utils.env import db

from gn_module_import.models import BibFields


def get_srid_bounding_box(srid):
    """
    calculate the local bounding box and
    return a shapely polygon of this BB with local coordq
    """
    xmin, ymin, xmax, ymax = CRS.from_epsg(srid).area_of_use.bounds
    bounding_polygon_4326 = Polygon([
        (xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)
    ])
    projection = Transformer.from_crs(
        CRS(4326),
        CRS(int(srid)),
        always_xy=True
    )
    return transform(projection.transform, bounding_polygon_4326)


def wkt_to_geometry(value):
    try:
        return wkt.loads(value)
    except Exception:
        return None


def x_y_to_geometry(x, y):
    try:
        return Point(float(x), float(y))
    except Exception:
        return None


def check_bound(p, bounding_box: Polygon):
    return p.within(bounding_box)


def check_geography(
    df,
    fields: Dict[str, BibFields],
    file_srid,
):
    file_srid_bounding_box = get_srid_bounding_box(file_srid)

    errors = []

    df['_geom'] = None

    if 'WKT' in fields:
        wkt_field = fields['WKT'].source_field
        wkt_mask = df[wkt_field].notnull()
        df.loc[wkt_mask, '_geom'] = df[wkt_mask][wkt_field].apply(wkt_to_geometry)
        invalid_wkt = df[wkt_mask & df['_geom'].isnull()]
        if len(invalid_wkt):
            errors.append({
                'error_code': 'INVALID_WKT',
                'column': 'WKT',
                'invalid_rows': invalid_wkt,
            })
    else:
        wkt_mask = pd.Series(False, index=df.index)
    if 'latitude' in fields and 'longitude' in fields:
        latitude_field = fields['latitude'].source_field
        longitude_field = fields['longitude'].source_field
        # take xy when no wkt and xy are not null
        xy_mask = df[latitude_field].notnull() & df[longitude_field].notnull()
        df.loc[xy_mask, '_geom'] = df[xy_mask].apply(
            lambda row: x_y_to_geometry(row[latitude_field], row[longitude_field]), axis=1)
        invalid_xy = df[xy_mask & df['_geom'].isnull()]
        if len(invalid_xy):
            errors.append({
                'error_code': 'INVALID_GEOMETRY',
                'column': 'longitude',
                'invalid_rows': invalid_xy,
            })
    else:
        xy_mask = pd.Series(False, index=df.index)

    # Check multiple geo-referencement
    multiple_georef = df[wkt_mask & xy_mask]
    if len(multiple_georef):
        df.loc[wkt_mask & xy_mask, '_geom'] = None
        errors.append({
            'error_code': 'MULTIPLE_ATTACHMENT_TYPE_CODE',
            'column': 'Champs géométriques',
            'invalid_rows': multiple_georef,
        })

    # Check out-of-bound geo-referencement
    for mask, column in [(wkt_mask, 'WKT'), (xy_mask, 'longitude')]:
        bound = df[mask & df['_geom'].notnull()]['_geom'] \
                        .apply(partial(check_bound, bounding_box=file_srid_bounding_box))
        out_of_bound = df[mask & ~bound]
        if len(out_of_bound):
            df.loc[mask & ~bound, '_geom'] = None
            errors.append({
                'error_code': 'GEOMETRY_OUT_OF_BOX',
                'column': column,
                'invalid_rows': out_of_bound,
            })

    if 'codecommune' in fields:
        codecommune_field = fields['codecommune'].source_field
        codecommune_mask = df[codecommune_field].notnull()
    else:
        codecommune_mask = pd.Series(False, index=df.index)
    if 'codemaille' in fields:
        codemaille_field = fields['codemaille'].source_field
        codemaille_mask = df[codemaille_field].notnull()
    else:
        codemaille_mask = pd.Series(False, index=df.index)
    if 'codedepartement' in fields:
        codedepartement_field = fields['codedepartement'].source_field
        codedepartement_mask = df[codedepartement_field].notnull()
    else:
        codedepartement_mask = pd.Series(False, index=df.index)

    # Check for multiple code when no wkt or xy
    multiple_code = df[~wkt_mask & ~xy_mask
                       & ((codecommune_mask & codemaille_mask)
                          | (codecommune_mask & codedepartement_mask)
                          | (codemaille_mask & codedepartement_mask))]
    if len(multiple_code):
        errors.append({
            'error_code': 'MULTIPLE_CODE_ATTACHMENT',
            'column': 'Champs géométriques',
            'invalid_rows': multiple_code,
        })

    # Rows with no geom
    no_geom = df[~wkt_mask & ~xy_mask & ~codecommune_mask
                 & ~codemaille_mask & ~codedepartement_mask]
    if len(no_geom):
        errors.append({
            'error_code': 'NO-GEOM',
            'column': 'Champs géométriques',
            'invalid_rows': no_geom,
        })

    return errors


def set_the_geom_column(imprt, fields, df):
    file_srid = imprt.srid
    local_srid = db.session.execute(
        sa.func.Find_SRID("ref_geo", "l_areas", "geom")
    ).scalar()
    geom_col = df[df["_geom"].notna()]["_geom"]
    if file_srid == 4326:
        df["the_geom_4326"] = geom_col.apply(
            lambda geom: ST_GeomFromWKB(geom.wkb, file_srid)
        )
        fields["the_geom_4326"] = BibFields.query.filter_by(
            name_field="the_geom_4326"
        ).one()
    elif file_srid == local_srid:
        df["the_geom_local"] = geom_col.apply(
            lambda geom: ST_GeomFromWKB(geom.wkb, file_srid)
        )
        fields["the_geom_local"] = BibFields.query.filter_by(
            name_field="the_geom_local"
        ).one()
    else:
        df["the_geom_4326"] = geom_col.apply(
            lambda geom: ST_Transform(ST_GeomFromWKB(geom.wkb, file_srid), 4326)
        )
        fields["the_geom_4326"] = BibFields.query.filter_by(
            name_field="the_geom_4326"
        ).one()
