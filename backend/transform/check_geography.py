import geopandas as gpd
import pandas as pd

from ..logs import logger
from .utils import fill_map, get_types, set_is_valid, set_invalid_reason, set_user_error
from ..wrappers import checker
from ..utils.utils import create_col_name

import pdb

@checker('Data cleaning : geographic data checked')
def check_geography(df, import_id, added_cols, selected_columns, dc_user_errors, srid, local_srid):
    
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

        coordinates = ['longitude', 'latitude']

        for col in coordinates:

            logger.info('- converting %s in numeric values', selected_columns[col])

            col_name = '_'.join(['temp',col])
            df[col_name] = pd.to_numeric(df[selected_columns[col]], 'coerce')
            # créer une autre colonne (qu'on efface à la fin de la boucle) et garder celle là sans toucher aux valeurs
            # car à la fin de toute façon on va utiliser seuelemnt les wkt
            
            logger.info('- checking consistency of values in %s synthese column (= %s user column):', col, selected_columns[col])
            
            if col == 'longitude':
                df['temp'] = df[col_name].le(min_x) | df[col_name].ge(max_x)
            if col == 'latitude':
                df['temp'] = df[col_name].le(min_y) | df[col_name].ge(max_y)
            df['temp'] = ~df['temp']
            set_is_valid(df, 'temp')
            set_invalid_reason(df, 'temp', 'inconsistant coordinate in {} column', selected_columns[col])
            n_bad_coo = df['temp'].astype(str).str.contains('False').sum()

            # setting eventual inconsistent values to pd.np.nan
            df[col_name] = df[col_name].where(df['temp'], pd.np.nan)

            logger.info('%s inconsistant values detected in %s synthese column (= %s user column)', n_bad_coo, col, selected_columns[col])

            if n_bad_coo > 0:
                set_user_error(dc_user_errors, 12, selected_columns[col], n_bad_coo)  


        # create wkt with crs provided by user
        crs = {'init': 'epsg:{}'.format(srid)}
        user_gdf = gpd.GeoDataFrame(df, crs=crs, geometry=gpd.points_from_xy(df['temp_longitude'], df['temp_latitude']))
        df['temp'] = user_gdf['geometry'].is_valid

        # set column names :
        create_col_name(df=df, col_dict=added_cols, key='the_geom_4326', value='the_geom_4326', import_id=import_id)
        create_col_name(df=df, col_dict=added_cols, key='the_geom_point', value='the_geom_point', import_id=import_id)
        create_col_name(df=df, col_dict=added_cols, key='the_geom_local', value='the_geom_local', import_id=import_id)

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
        pdb.set_trace()
        raise
        
