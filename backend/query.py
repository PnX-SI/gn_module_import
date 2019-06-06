from geonature.utils.env import DB

def get_synthese_info(info='all'):
    synthese_info = DB.session.execute(\
        "SELECT column_name,is_nullable,column_default,data_type,character_maximum_length\
         FROM INFORMATION_SCHEMA.COLUMNS\
         WHERE table_name = 'synthese';"\
    )

    if info == 'column_name':
        data = []
        for info in synthese_info:
            data.append(info.column_name)
        return data
    
    if info == 'all':
        return synthese_info