from gn_module_import.models import BibFields


def concat_dates(df, selected_columns, synthese_fields):
    assert('date_min' in selected_columns)  # date_min is a required field

    date_min_col = selected_columns["date_min"]
    date_min = df[date_min_col]

    if 'hour_min' in selected_columns:
        hour_min_col = selected_columns["hour_min"]
        hour_min = df[hour_min_col].where(df[hour_min_col].notna(), other='00:00:00')

    if 'hour_min' in selected_columns:
        df['concatened_date_min'] = date_min + " " + hour_min
    else:
        df['concatened_date_min'] = date_min

    if 'date_max' in selected_columns:
        date_max_col = selected_columns['date_max']
        date_max = df[date_max_col].where(df[date_max_col].notna(), df[date_min_col])
    else:
        date_max = date_min

    if 'hour_max' in selected_columns:
        hour_max_col = selected_columns["hour_max"]
        if 'date_max' in selected_columns:
            # hour max is set to hour min if date max is none (because date max will be set to date min), else 00:00:00
            if 'hour_min' in selected_columns:
                # if hour_max not set, use hour_min if same day (or date_max not set, so same day)
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(),
                    other=hour_min.where(date_min == date_max, other='00:00:00'))
            else:
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(), other='00:00:00')
        else:
            if 'hour_min' in selected_columns:
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(), other=hour_min)
            else:
                hour_max = df[hour_max_col].where(df[hour_max_col].notna(), other='00:00:00')

    if 'hour_max' in selected_columns:
        df['concatened_date_max'] = date_max + " " + hour_max
    elif 'hour_min' in selected_columns:
        df['concatened_date_max'] = date_max + " " + hour_min
    else:
        df['concatened_date_max'] = date_max

    # Replace date fields in selected columns
    # 'orig_' fields are used for error reporting
    selected_columns['orig_date_min'] = selected_columns['date_min']
    selected_columns['date_min'] = 'concatened_date_min'
    if 'date_max' in selected_columns:
        selected_columns['orig_date_max'] = selected_columns['date_max']
    else:
        # date max was not selected, but we add it to synthese fields as we have a computed value for it
        synthese_fields.append(BibFields.query.filter_by(name_field='date_max').one())
    selected_columns['date_max'] = 'concatened_date_max'
