import pandas as pd
import numpy as np

from flask import current_app

from geonature.core.gn_synthese.models import Synthese

from gn_module_import.db.models import BibFields


def clean_missing_values(df, selected_columns):
    """
    Replace empty and blank cells by NaN
    """
    for target_field, source_field in selected_columns.items():
        df[source_field] = (
            df[source_field]
            .replace(to_replace='', value=np.nan)
            .fillna(value=np.nan)
        )


def check_required_values(df, imprt, selected_columns, synthese_fields):
    required_fields = { f for f in synthese_fields
                        if not Synthese.__table__.c[f.name_field].nullable }
    for required_field in required_fields:
        target_field = required_field.name_field
        source_field = selected_columns[target_field]
        orig_source_field = selected_columns.get(f'orig_{target_field}', source_field)
        invalid_rows = df[df[source_field].isna()]
        if len(invalid_rows):
            yield {
                'error_code': 'MISSING_VALUE',
                'column': orig_source_field,
                'invalid_rows': invalid_rows,
            }
