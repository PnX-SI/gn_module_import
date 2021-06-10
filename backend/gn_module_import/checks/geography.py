from functools import partial

import geopandas as gpd
import pandas as pd
from shapely import wkt, wkb
from shapely.geometry import Point, Polygon
from shapely.ops import transform
import numpy as np
from pyproj import CRS, Transformer, Proj


#from ..logs import logger
#from ..wrappers import checker


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


def wkt_to_wkb(value):
    try:
        #return wkb.dumps(wkt.loads(value)).hex()
        return wkt.loads(value)
    except Exception as e:
        return None


def x_y_to_wkb(x, y):
    try:
        #return Point(float(x), float(y)).wkb.hex()
        return Point(float(x), float(y))
    except Exception:
        return None


def check_bound(p, bounding_box: Polygon):
    return p.within(bounding_box)


def check_geography(df, imprt, mapped_target_fields):
    file_srid = imprt.srid
    file_srid_bounding_box = get_srid_bounding_box(file_srid)

    errors = []

    df['_geom'] = None

    geom_fields = []
    if 'WKT' in mapped_target_fields:
        wkt_field = mapped_target_fields['WKT']
        wkt_mask = df[wkt_field].notnull()
        df.loc[wkt_mask, '_geom'] = df[wkt_mask][wkt_field].apply(wkt_to_wkb)
        invalid_wkt = df[wkt_mask & df['_geom'].isnull()]
        if len(invalid_wkt):
            errors.append({
                'error_code': 'INVALID_WKT',
                'column': wkt_field,
                'invalid_rows': invalid_wkt,
            })
    else:
        wkt_mask = pd.Series(False, index=df.index)
    if {'latitude', 'longitude'}.issubset(mapped_target_fields.keys()):
        latitude_field = mapped_target_fields['latitude']
        longitude_field = mapped_target_fields['longitude']
        xy_fields = f'{latitude_field}, {longitude_field}'
        # take xy when no wkt and xy are not null
        xy_mask = df[latitude_field].notnull() & df[longitude_field].notnull()
        df.loc[xy_mask, '_geom'] = df[xy_mask].apply(
            lambda row: x_y_to_wkb(row[latitude_field], row[longitude_field]), axis=1)
        invalid_xy = df[xy_mask & df['_geom'].isnull()]
        if len(invalid_xy):
            errors.append({
                'error_code': 'INVALID_GEOMETRIE',
                'column': xy_fields,
                'invalid_rows': invalid_xy,
            })
    else:
        xy_mask = pd.Series(False, index=df.index)
        xy_fields = None

    # Check multiple geo-referencement
    multiple_georef= df[wkt_mask & xy_mask]
    if len(multiple_georef):
        df.loc[wkt_mask & xy_mask, '_geom'] = None
        errors.append({
            'error_code': 'MULTIPLE_ATTACHMENT_TYPE_CODE',
            'column': 'Champs géométriques',
            'invalid_rows': multiple_georef,
        })

    # Check out-of-bound geo-referencement
    for mask, column in [(wkt_mask, wkt_field), (xy_mask, xy_fields)]:
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

    codecommune_field = mapped_target_fields.get('codecommune')
    if codecommune_field:
        codecommune_mask = df[codecommune_field].notnull()
    else:
        codecommune_mask = pd.Series(False, index=df.index)
    codemaille_field = mapped_target_fields.get('codemaille')
    if codemaille_field:
        codemaille_mask = df[codemaille_field].notnull()
    else:
        codemaille_mask = pd.Series(False, index=df.index)
    codedepartement_field = mapped_target_fields.get('codedepartement')
    if codedepartement_field:
        codedepartement_mask = df[codedepartement_field].notnull()
    else:
        codedepartement_mask = pd.Series(False, index=df.index)

    # Check for multiple code when no wkt or xy
    multiple_code = df[~wkt_mask & ~xy_mask \
                        & ((codecommune_mask & codemaille_mask) \
                         | (codecommune_mask & codedepartement_mask) \
                         | (codemaille_mask & codedepartement_mask))]
    if len(multiple_code):
        errors.append({
            'error_code': 'MULTIPLE_CODE_ATTACHMENT',
            'column': 'Champs géométriques',
            'invalid_rows': multiple_code,
        })

    code_mask = ~wkt_mask & ~xy_mask & (
                         (codecommune_mask & ~codemaille_mask & ~codedepartement_mask)
                       | (~codecommune_mask & codemaille_mask & ~codedepartement_mask)
                       | (~codecommune_mask & ~codemaille_mask & codedepartement_mask))

    # Rows with no geom
    no_geom = df[~wkt_mask & ~xy_mask & ~codecommune_mask \
                 & ~codemaille_mask & ~codedepartement_mask]
    if len(no_geom):
        errors.append({
            'error_code': 'NO-GEOM',
            'column': 'Champs géométriques',
            'invalid_rows': no_geom,
        })

    return errors
