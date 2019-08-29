import pdb
import pandas as pd

def entity_source(df, selected_columns, synthese_info):

    fields = [field for field in synthese_info]

    if 'entity_source_pk_value' not in fields:
        df['entity_source_pk_value'] = ''
        df['entity_source_pk_value'] = df['gn_pk'].astype('str') # utile?
        # selected_columns['entity_source_pk_value'] = 'gn_pk'
    #else:
        # verifier que unique values
        #df[selected_columns['entity_source_pk_value']].compute().duplicated()

