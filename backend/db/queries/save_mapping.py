from geonature.utils.env import DB

from ..models import TMappingsFields, TMappingsValues


def save_field_mapping(form_data, id_mapping, select_type):
    try:

        for col in form_data:

            my_query = DB.session.query(TMappingsFields) \
                .filter(TMappingsFields.id_mapping == id_mapping) \
                .filter(TMappingsFields.target_field == col).all()

            if select_type == 'selected':
                if form_data[col] != '':
                    is_selected = True
                    is_added = False
                else:
                    is_selected = False
                    is_added = False
            else:
                if form_data[col] != '':
                    is_selected = False
                    is_added = True

            if len(my_query) > 0:
                for q in my_query:
                    DB.session.query(TMappingsFields) \
                        .filter(TMappingsFields.id_match_fields == q.id_match_fields) \
                        .update({
                            TMappingsFields.id_mapping: int(id_mapping),
                            TMappingsFields.source_field: form_data[col],
                            TMappingsFields.target_field: col,
                            TMappingsFields.is_selected: is_selected,
                            TMappingsFields.is_added: is_added
                        })
                    DB.session.flush()

            else:
                new_fields = TMappingsFields(
                    id_mapping=int(id_mapping),
                    source_field=form_data[col],
                    target_field=col,
                    is_selected=is_selected,
                    is_added=False
                )
                DB.session.add(new_fields)
                DB.session.flush()

        DB.session.commit()

    except Exception:
        DB.session.rollback()
        raise
    finally:
        DB.session.close()


def save_content_mapping(form_data, id_mapping):
    try:
        objs = TMappingsValues.query.filter_by(id_mapping=id_mapping).all()
        for obj in objs:
            DB.session.delete(obj)
        for id_type in form_data:
            for i in range(len(form_data[id_type])):
                create_mapping_value(int(id_mapping), form_data[id_type][i], int(id_type))
                DB.session.flush()
        DB.session.commit()
    except Exception:
        DB.session.rollback()
        raise
    finally:
        DB.session.close()


def create_mapping_value(id_mapping, source_value, id_target_value):
    try:
        new_contents = TMappingsValues(
            id_mapping=id_mapping,
            source_value=source_value,
            id_target_value=id_target_value
        )
        DB.session.add(new_contents)
    except Exception:
        raise


def get_selected_columns(id_mapping):
    try:
        queries = DB.session.query(TMappingsFields) \
            .filter(TMappingsFields.id_mapping == id_mapping) \
            .filter(TMappingsFields.is_selected).all()
        selected_columns = {}
        for query in queries:
            selected_columns[query.target_field] = query.source_field
        return selected_columns        
    except Exception:
        raise


def get_added_columns(id_mapping):
    try:
        queries = DB.session.query(TMappingsFields) \
            .filter(TMappingsFields.id_mapping == id_mapping) \
            .filter(TMappingsFields.is_added).all()
        added_columns = {}
        for query in queries:
            added_columns[query.target_field] = query.source_field
        return added_columns
    except Exception:
        raise