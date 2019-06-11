from sqlalchemy.sql.elements import quoted_name

from geonature.utils.env import DB

from geonature.core.gn_synthese.models import (
    Synthese,
    TSources,
    CorObserverSynthese
)

from geonature.core.gn_meta.models import TDatasets

from .models import (
    TImports,
    CorRoleImport,
    CorImportArchives,
    generate_user_table_class
)

from .utils import (
    get_full_table_name,
    set_imports_table_name
)


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


def delete_tables_if_existing(schema_archive, schema_gnimports, archive_table, gnimport_table):
    engine = DB.engine
    archive_schema_table_name = get_full_table_name(schema_archive,archive_table)
    gn_imports_schema_table_name = get_full_table_name(schema_gnimports,gnimport_table)
    
    is_archive_table_exist = engine.has_table(archive_table, schema=schema_archive)
    is_gn_imports_table_exist = engine.has_table(gnimport_table, schema=schema_gnimports)
    
    if is_archive_table_exist:
        DB.session.execute("DROP TABLE {}".format(quoted_name(archive_schema_table_name, False)))

    if is_gn_imports_table_exist:
        DB.session.execute("DROP TABLE {}".format(quoted_name(gn_imports_schema_table_name, False)))

def get_table_list():
    try:
        table_names = DB.session.execute(\
            "SELECT table_name \
             FROM information_schema.tables \
             WHERE table_schema='gn_import_archives'"
        ) # remplacer par la variable archives_schema_name
        table_names = [table.table_name for table in table_names]
        print(table_names)
        return table_names
    except Exception:
        raise


def test_user_dataset(id_role, current_dataset_id):

    results = DB.session.query(TDatasets)\
        .filter(TDatasets.id_dataset == Synthese.id_dataset)\
        .filter(CorObserverSynthese.id_synthese == Synthese.id_synthese)\
        .filter(CorObserverSynthese.id_role == id_role)\
        .distinct(Synthese.id_dataset)\
        .all()

    dataset_ids = []

    for r in results:
        dataset_ids.append(r.id_dataset)

    if int(current_dataset_id) not in dataset_ids:
        return False
        
    return True


def delete_import_CorImportArchives(id):
    DB.session.query(CorImportArchives)\
        .filter(CorImportArchives.id_import == id)\
        .delete()


def delete_import_CorRoleImport(id):
    DB.session.query(CorRoleImport)\
        .filter(CorRoleImport.id_import == id)\
        .delete()


def delete_import_TImports(id):
    DB.session.query(TImports)\
        .filter(TImports.id_import == id)\
        .delete()


def delete_tables(id, archives_schema, imports_schema):
    table_names_list = get_table_list()
    if len(table_names_list) > 0:
        for table_name in table_names_list:
            try:
                if int(table_name.split('_')[-1]) == id:
                    imports_table_name = set_imports_table_name(table_name)
                    DB.session.execute("DROP TABLE {}".format(get_full_table_name(archives_schema,table_name)))
                    DB.session.execute("DROP TABLE {}".format(get_full_table_name(imports_schema,imports_table_name)))
            except ValueError:
                pass