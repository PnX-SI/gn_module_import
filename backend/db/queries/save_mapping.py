from geonature.utils.env import DB

from ..models import TMappingsFields 

def save_field_mapping(form_data, id_mapping):

    try:

        for col in form_data:

            my_query = DB.session.query(TMappingsFields)\
                .filter(TMappingsFields.id_mapping == id_mapping)\
                .filter(TMappingsFields.target_field == col).all()

            if len(my_query) > 0:
                for q in my_query:
                    DB.session.query(TMappingsFields)\
                        .filter(TMappingsFields.id_match_fields == q.id_match_fields)\
                        .update({
                            TMappingsFields.id_mapping: int(id_mapping),
                            TMappingsFields.source_field: form_data[col],
                            TMappingsFields.target_field: col
                        })
                    DB.session.commit()
                    DB.session.close()
                    
            else:
                new_fields = TMappingsFields(
                    id_mapping = int(id_mapping),
                    source_field = form_data[col],
                    target_field = col
                )
                DB.session.add(new_fields)
                DB.session.commit()
                DB.session.close()

    except Exception:
        raise