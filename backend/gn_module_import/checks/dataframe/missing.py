from typing import Dict

import pandas as pd
import numpy as np

from flask import current_app

from gn_module_import.models import BibFields


def clean_missing_values(df, fields: Dict[str, BibFields]):
    """
    Replace empty and blank cells by NaN
    """
    cols = [field.source_field for field in fields.values() if field.source_field]
    df.loc[:, cols] = df.loc[:, cols].replace(to_replace="", value=np.nan)
    df.loc[:, cols] = df.loc[:, cols].fillna(value=np.nan)


def check_required_values(df, fields: Dict[str, BibFields]):
    for field_name, field in fields.items():
        if not field.mandatory:
            continue
        invalid_rows = df[df[field.source_column].isna()]
        if len(invalid_rows):
            yield {
                "error_code": "MISSING_VALUE",
                "column": field_name,
                "invalid_rows": invalid_rows,
            }
