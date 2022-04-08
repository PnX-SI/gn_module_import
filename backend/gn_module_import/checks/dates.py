from typing import Dict

from gn_module_import.models import BibFields


def concat_dates(df, fields: Dict[str, BibFields]):
    assert('date_min' in fields)  # date_min is a required field

    fields.update({
        "datetime_min": BibFields.query.filter_by(name_field="datetime_min").one(),
        "datetime_max": BibFields.query.filter_by(name_field="datetime_max").one(),
    })

    datetime_min_col = fields["datetime_min"].synthese_column
    datetime_max_col = fields["datetime_max"].synthese_column

    date_min_col = fields["date_min"].source_field
    date_min = df[date_min_col]

    if 'hour_min' in fields:
        hour_min_col = fields["hour_min"].source_field
        hour_min = df[hour_min_col].where(df[hour_min_col].notna(), other='00:00:00')

    if 'hour_min' in fields:
        df[datetime_min_col] = date_min + " " + hour_min
    else:
        df[datetime_min_col] = date_min

    if 'date_max' in fields:
        date_max_col = fields['date_max'].source_field
        date_max = df[date_max_col].where(df[date_max_col].notna(), date_min)
    else:
        date_max = date_min

    if 'hour_max' in fields:
        hour_max_col = fields["hour_max"].source_field
        if 'date_max' in fields:
            # hour max is set to hour min if date max is none (because date max will be set to date min), else 00:00:00
            if 'hour_min' in fields:
                # if hour_max not set, use hour_min if same day (or date_max not set, so same day)
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(),
                    other=hour_min.where(date_min == date_max, other='00:00:00'))
            else:
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(), other='00:00:00')
        else:
            if 'hour_min' in fields:
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(), other=hour_min)
            else:
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(), other='00:00:00')

    if 'hour_max' in fields:
        df[datetime_max_col] = date_max + " " + hour_max
    elif 'hour_min' in fields:
        df[datetime_max_col] = date_max + " " + hour_min
    else:
        df[datetime_max_col] = date_max
