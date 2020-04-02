"""
Class to manipulate a Import Object
"""


class ImportDescriptor:
    def __init__(
        self,
        id_import,
        id_mapping,
        table_name,
        column_names,
        selected_columns,
        import_srid,
    ):
        self.id_import = id_import
        self.id_mapping = id_mapping
        self.table_name = table_name
        self.column_names = column_names
        self.selected_columns = selected_columns
        self.import_srid = import_srid
