import pdb


def entity_source(df,selected_columns,synthese_info):

    user_error = []

    charvar_fields = [field for field in synthese_info]

    """
    if 'entity_source_pk_value' not in charvar_fields:
        df['entity_source_pk_value'] = ''
        df['entity_source_pk_value'] = str(df['gn_pk']) # utile?
        # selected_columns['entity_source_pk_value'] = 'gn_pk'
    """

    user_error = ''

    return user_error

