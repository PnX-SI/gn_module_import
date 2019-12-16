from ..wrappers import checker
from ..logs import logger
from ..db.queries.altitudes import (
    create_column,
    generate_altitudes
)
from ..utils.utils import create_col_name


@checker('Data cleaning : altitudes created')
def set_altitudes(df, selected_columns, import_id, schema_name,
                  full_table_name, table_name, index_col,
                  is_generate_alt, the_geom_local_col, added_cols):
    try:

        logger.info('generating altitudes :')

        if is_generate_alt:

            # altitude_min

            create_col_name(df, added_cols, 'altitude_min', import_id)
            create_column(
                full_table_name=full_table_name,
                alt_col=added_cols['altitude_min'])

            if 'altitude_min' not in selected_columns.keys():
                original_alt_col = added_cols['altitude_min']
                generate_type = 'generate_all'
            else:
                original_alt_col = selected_columns['altitude_min']
                generate_type = 'generate_missing'

            generate_altitudes(
                type_alt='min',
                schema=schema_name,
                table=table_name,
                alt_col=added_cols['altitude_min'],
                original_alt_col=original_alt_col,
                table_pk=index_col,
                geom_col=the_geom_local_col,
                generate_type=generate_type)

            # altitude_max

            create_col_name(df, added_cols, 'altitude_max', import_id)
            create_column(
                full_table_name=full_table_name,
                alt_col=added_cols['altitude_max'])

            if 'altitude_max' not in selected_columns.keys():
                original_alt_col = added_cols['altitude_max']
            else:
                original_alt_col = selected_columns['altitude_max']

            generate_altitudes(
                type_alt='max',
                schema=schema_name,
                table=table_name,
                alt_col=added_cols['altitude_max'],
                original_alt_col=original_alt_col,
                table_pk=index_col,
                geom_col=the_geom_local_col,
                generate_type=generate_type)

    except Exception:
        raise
