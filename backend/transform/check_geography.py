import geopandas as gpd
import pandas as pd
from shapely import wkt
import numpy as np

from ..db.queries.user_errors import set_user_error
from ..logs import logger
from .utils import fill_map, set_is_valid, set_invalid_reason
from ..wrappers import checker
from ..utils.utils import create_col_name


def set_wkt(value):
    try:
        return wkt.loads(value)
    except Exception:
        return wkt.loads('POINT(nan nan)')


def check_wkt(value, min_x, max_x, min_y, max_y):
    try:
        if value.geom_type == 'Point':
            if value.x > max_x:
                return False
            if value.x < min_x:
                return False
            if value.y > max_y:
                return False
            if value.y < min_y:
                return False
        if value.geom_type == 'Polygon':
            for x_coord in value.exterior.coords.xy[0]:
                if x_coord > max_x:
                    return False
                if x_coord < min_x:
                    return False
            for y_coord in value.exterior.coords.xy[1]:
                if y_coord > max_y:
                    return False
                if y_coord < min_y:
                    return False
        return True
    except Exception:
        return True


@checker('Data cleaning : geographic data checked')
def check_geography(df, import_id, added_cols, selected_columns, srid, local_srid, schema_name):
    try:

        logger.info('CHECKING GEOGRAPHIC DATA:')

        if srid == 4326:
            max_x = 180
            min_x = -180
            max_y = 90
            min_y = -90
        if srid == 2154:
            max_x = 1300000
            min_x = 100000
            max_y = 7200000
            min_y = 6000000

        if 'WKT' not in selected_columns.keys():

            coordinates = ['longitude', 'latitude']

            for col in coordinates:

                logger.info('- converting %s in numeric values', selected_columns[col])

                col_name = '_'.join(['temp', col])
                df[col_name] = pd.to_numeric(df[selected_columns[col]], 'coerce')

                logger.info('- checking consistency of values in %s synthese column (= %s user column):', col,
                            selected_columns[col])

                if col == 'longitude':
                    df['temp'] = df[col_name].le(min_x) | df[col_name].ge(max_x)
                if col == 'latitude':
                    df['temp'] = df[col_name].le(min_y) | df[col_name].ge(max_y)
                df['temp'] = ~df['temp']
                set_is_valid(df, 'temp')
                n_bad_coo = df['temp'].astype(str).str.contains('False').sum()

                # setting eventual inconsistent values to pd.np.nan
                df[col_name] = df[col_name].where(df['temp'], pd.np.nan)

                logger.info('%s inconsistant values detected in %s synthese column (= %s user column)', n_bad_coo, col,
                            selected_columns[col])

                if n_bad_coo > 0:
                    set_user_error(import_id, 13, selected_columns[col], n_bad_coo)
                    set_invalid_reason(df, schema_name, 'temp', import_id, 13, selected_columns[col])

            # create wkt with crs provided by user
            crs = {'init': 'epsg:{}'.format(srid)}
            user_gdf = gpd.GeoDataFrame(df, crs=crs,
                                        geometry=gpd.points_from_xy(df['temp_longitude'], df['temp_latitude']))
            df['temp'] = user_gdf['geometry'].is_valid

        else:
            # create wkt with crs provided by user
            crs = {'init': 'epsg:{}'.format(srid)}

            # load wkt
            df[selected_columns['WKT']] = df[selected_columns['WKT']].apply(lambda x: set_wkt(x))
            
            # check coordinates consistancy
            df['temp'] = df[selected_columns['WKT']].apply(lambda x: check_wkt(x, min_x, max_x, min_y, max_y))
            
            # set coordinates consistancy errors as an empty wkt in order to avoid error when converting geometries to other srid
            df[selected_columns['WKT']] = df[selected_columns['WKT']] \
                .where(
                    cond=df['temp']==True,
                    other=wkt.loads('POINT(nan nan)')
                )
            set_is_valid(df, 'temp')
            n_bad_coo = df['temp'].astype(str).str.contains('False').sum()
            
            logger.info('%s inconsistant values detected in %s column', n_bad_coo,
                            selected_columns['WKT'])

            if n_bad_coo > 0:
                set_user_error(import_id, 13, selected_columns['WKT'], n_bad_coo)
                set_invalid_reason(df, schema_name, 'temp', import_id, 13, selected_columns['WKT'])
    
            user_gdf = gpd.GeoDataFrame(df, crs=crs, geometry=df[selected_columns['WKT']])
            
            df['temp'] = user_gdf['geometry'].is_valid
            

        # set column names :
        create_col_name(df=df, col_dict=added_cols, key='the_geom_4326', import_id=import_id)
        create_col_name(df=df, col_dict=added_cols, key='the_geom_point', import_id=import_id)
        create_col_name(df=df, col_dict=added_cols, key='the_geom_local', import_id=import_id)

        # convert wkt in local and 4326 crs
        if srid == 4326 and local_srid == 4326:
            df[added_cols['the_geom_4326']] = user_gdf['geometry'].where(df['temp'], pd.np.nan)
            df[added_cols['the_geom_point']] = user_gdf['geometry'].centroid.where(df['temp'], pd.np.nan)
            df[added_cols['the_geom_local']] = user_gdf['geometry'].where(df['temp'], pd.np.nan)

        elif srid == 4326 and srid != local_srid:
            df[added_cols['the_geom_4326']] = user_gdf['geometry'].where(df['temp'], pd.np.nan)
            df[added_cols['the_geom_point']] = user_gdf['geometry'].centroid.where(df['temp'], pd.np.nan)
            local_gdf = user_gdf.to_crs({'init': 'epsg:{}'.format(local_srid)})
            df[added_cols['the_geom_local']] = local_gdf['geometry'].where(df['temp'], pd.np.nan)
            del local_gdf

        elif srid != 4326 and srid == local_srid:
            df[added_cols['the_geom_local']] = user_gdf['geometry'].where(df['temp'], pd.np.nan)
            wgs84_gdf = user_gdf.to_crs({'init': 'epsg:4326'})
            df[added_cols['the_geom_4326']] = wgs84_gdf['geometry'].where(df['temp'], pd.np.nan)
            df[added_cols['the_geom_point']] = wgs84_gdf['geometry'].centroid.where(df['temp'], pd.np.nan)
            del wgs84_gdf

        else:
            wgs84_gdf = user_gdf.to_crs({'init': 'epsg:4326'})
            df[added_cols['the_geom_4326']] = wgs84_gdf['geometry'].where(df['temp'], pd.np.nan)
            df[added_cols['the_geom_point']] = wgs84_gdf['geometry'].centroid.where(df['temp'], pd.np.nan)
            local_gdf = user_gdf.to_crs({'init': 'epsg:{}'.format(local_srid)})
            df[added_cols['the_geom_local']] = local_gdf['geometry'].where(df['temp'], pd.np.nan)
            del local_gdf
            del wgs84_gdf

        del user_gdf

    except Exception:
        raise
