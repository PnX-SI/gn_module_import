from ..wrappers import checker
from ..logs import logger
from ..db.queries.geometries import get_id_area_type
from ..db.queries.utils import execute_query
from ..db.queries.user_errors import set_user_error
from ..load.import_class import ImportDescriptor


class GeometrySetter:
    """
    Utility class to manage geometry transformation from the import table
    """

    def __init__(
        self,
        import_object: ImportDescriptor,
        local_srid: int,
        code_commune_col=None,
        code_maille_col=None,
        code_dep_col=None,
    ):
        self.id_import = import_object.id_import
        self.table_name = import_object.table_name
        self.import_srid = import_object.import_srid
        self.column_names = import_object.column_names
        self.local_srid = local_srid
        # self.code_commune_col = (
        #     code_commune_col if code_commune_col != "" else "codecommune"
        # )
        self.code_commune_col = code_commune_col
        self.code_maille_col = code_maille_col
        self.code_dep_col = code_dep_col
        # self.code_maille_col = (
        #     code_maille_col if code_maille_col != "" else "codemaille"
        # )
        # self.code_dep_col = code_dep_col if code_dep_col != "" else "codedepartement"

    @checker("Data cleaning : geometries created")
    def set_geometry(self):
        """
        Add 3 geometry columns and 1 column id_area_attachmet to the temp table and fill them from the "given_geom" col
        calculated in python in the geom_check step
        Also calculate the attachment geoms
        """
        try:

            logger.info(
                "creating  geometry columns and transform them (srid and points):"
            )
            self.add_geom_column()
            #  take the column 'given_geom' to fill the appropriate column
            if self.import_srid == 4326:
                self.set_given_geom("gn_the_geom_4326", 4326)
                self.transform_geom(
                    target_geom_col="gn_the_geom_local",
                    origin_srid="4326",
                    target_srid=self.local_srid,
                )
            else:
                self.set_given_geom("gn_the_geom_local", self.local_srid)
                self.transform_geom(
                    target_geom_col="gn_the_geom_4326",
                    origin_srid=self.import_srid,
                    target_srid="4326",
                )

            self.calculate_geom_point(
                source_geom_column="gn_the_geom_4326",
                target_geom_column="gn_the_geom_point",
            )
            (
                id_type_comm,
                id_type_dep,
                id_type_m1,
                id_type_m6,
                id_type_m10,
            ) = get_id_area_type()
            #  retransform the geom col in text (otherwise dask not working)
            self.set_text()
            # calculate the geom attachement for communes / maille et département
            if self.code_commune_col:
                self.calculate_geom_attachement(
                    id_area_type=id_type_comm,
                    code_col=self.code_commune_col,
                    ref_geo_area_code_col="area_code",
                )
            if self.code_dep_col:
                self.calculate_geom_attachement(
                    id_area_type=id_type_dep,
                    code_col=self.code_dep_col,
                    ref_geo_area_code_col="area_code",
                )
            if self.code_maille_col:
                self.calculate_geom_attachement(
                    id_area_type=id_type_m10,
                    code_col=self.code_maille_col,
                    ref_geo_area_code_col="area_name",
                )
            #  calcul des erreurs
            # si aucun code fournis -> on ne vérifie pas les erreurs sur les codes
            if self.code_commune_col or self.code_maille_col or self.code_dep_col:
                errors = self.set_attachment_referential_errors()
                commune_errors = {"id_rows": [], "code_error": []}
                maille_errors = {"id_rows": [], "code_error": []}
                dep_errors = {"id_rows": [], "code_error": []}
                for er in errors:
                    if er.code_com:
                        commune_errors["id_rows"].append(er.gn_pk)
                        commune_errors["code_error"].append(er.code_com)
                    elif er.code_maille:
                        maille_errors["id_rows"].append(er.gn_pk)
                        maille_errors["code_error"].append(er.code_maille)
                    elif er.code_dep:
                        dep_errors["id_rows"].append(er.gn_pk)
                        dep_errors["code_error"].append(er.code_dep)

                if len(commune_errors["id_rows"]) > 0:
                    set_user_error(
                        id_import=self.id_import,
                        step="FIELD_MAPPING",
                        error_code="INVALID_GEOM_CODE",
                        col_name=self.code_commune_col,
                        id_rows=commune_errors["id_rows"],
                        comment="Les codes communes suivant sont invalides : {}".format(
                            ", ".join(commune_errors["code_error"])
                        ),
                    )
                if len(maille_errors["id_rows"]) > 0:
                    set_user_error(
                        id_import=self.id_import,
                        step="FIELD_MAPPING",
                        error_code="INVALID_GEOM_CODE",
                        col_name=self.code_maille_col,
                        id_rows=maille_errors["id_rows"],
                        comment="Les codes mailles suivant sont invalides : {}".format(
                            ", ".join(maille_errors["code_error"])
                        ),
                    )
                if len(dep_errors["id_rows"]) > 0:
                    set_user_error(
                        id_import=self.id_import,
                        step="FIELD_MAPPING",
                        error_code="INVALID_GEOM_CODE",
                        col_name=self.code_dep_col,
                        id_rows=dep_errors["id_rows"],
                        comment="Les codes départements suivant sont invalides : {}".format(
                            ", ".join(dep_errors["code_error"])
                        ),
                    )
        except Exception:
            raise

    def add_geom_column(self):
        """
        Add geom columns to the temp table
        """
        query = """
            ALTER TABLE {schema_name}.{table_name}
            DROP COLUMN IF EXISTS gn_the_geom_4326,
            DROP COLUMN IF EXISTS gn_the_geom_local,
            DROP COLUMN IF EXISTS gn_the_geom_point,
            DROP COLUMN IF EXISTS id_area_attachment,
            ADD COLUMN id_area_attachment integer;
            SELECT public.AddGeometryColumn('{schema_name}', '{table_name}', 'gn_the_geom_4326', 4326, 'Geometry', 2 );
            SELECT public.AddGeometryColumn('{schema_name}', '{table_name}', 'gn_the_geom_local', {local_srid}, 'Geometry', 2 );
            SELECT public.AddGeometryColumn('{schema_name}', '{table_name}', 'gn_the_geom_point', 4326, 'POINT', 2 );
            """.format(
            schema_name=self.table_name.split(".")[0],
            table_name=self.table_name.split(".")[1],
            local_srid=self.local_srid,
        )
        execute_query(query)

    def set_given_geom(self, target_colmun, srid):
        """
        Take the dataframe column named 'given_geom' to set the appropriate geom column
        """
        query = """
                UPDATE {table_name} 
                SET {target_colmun} = ST_SetSRID(given_geom, {srid})
                """.format(
            table_name=self.table_name, target_colmun=target_colmun, srid=srid,
        )
        execute_query(query, commit=True)

    def transform_geom(self, target_geom_col, origin_srid, target_srid):
        """
        Make the projection translation from a source to a target column
        """

        query = """
        UPDATE {table_name} 
        SET {target_geom_col} = ST_transform(
            ST_SetSRID(given_geom, {origin_srid}), 
            {target_srid}
        )
        """.format(
            table_name=self.table_name,
            target_geom_col=target_geom_col,
            origin_srid=origin_srid,
            target_srid=target_srid,
        )
        execute_query(query, commit=True)

    def calculate_geom_point(self, source_geom_column, target_geom_column):
        query = """
            UPDATE {table_name} 
            SET {target_geom_column} = ST_centroid({source_geom_column});
            """.format(
            table_name=self.table_name,
            target_geom_column=target_geom_column,
            source_geom_column=source_geom_column,
        )
        execute_query(query, commit=True)

    def set_text(self):
        """
        Retransform the geom col in text (otherwise dask not working)
        """
        query = """
                ALTER TABLE {table_name}
                ALTER COLUMN gn_the_geom_local TYPE text,
                ALTER COLUMN gn_the_geom_4326 TYPE text,
                ALTER COLUMN gn_the_geom_point TYPE text;
            """.format(
            table_name=self.table_name
        )
        execute_query(query, commit=True)

    def calculate_geom_attachement(
        self, id_area_type, code_col, ref_geo_area_code_col,
    ):
        """
        Find id_area_attachment in ref_geo.l_areas from code given in the file
        Update only columns where gn_the_geom_4326 is NULL AND id_area_attachment is NULL
        
        :params id_area_type int: the id_area_type (ref_geo.bib_area_type) of the coresponding code (example: 25 for commune)
        :params str :code_col: column name where find the code for attachment in the inital table
        :params str ref_geo_area_code_col: column of the ref_geo.l_area table where find the coresponding code (for maille its area_name, for other: area_code)
    """
        query = """
        UPDATE {table} as i
                SET 
                id_area_attachment = sub.id_area,
                gn_the_geom_local = sub.geom,
                gn_the_geom_4326 = st_transform(sub.geom, 4326),
                gn_the_geom_point = st_centroid(sub.geom)
                FROM (
                    SELECT id_area, la.geom::text, gn_pk
                    FROM {table} 
                    JOIN ref_geo.l_areas la ON la.id_type = {id_area_type} and la.{ref_geo_area_code_col} = {code_col} 
                ) as sub
                WHERE id_area_attachment IS NULL AND gn_the_geom_local IS NULL AND sub.gn_pk = i.gn_pk;
        """.format(
            table=self.table_name,
            ref_geo_area_code_col=ref_geo_area_code_col,
            code_col=code_col,
            id_area_type=id_area_type,
        )
        execute_query(query)

    def set_attachment_referential_errors(self):
        """
        Method who update the import table to set gn_is_valid=false
        where the code attachement given is not correct
        Take only the row where its valid to not take raise twice error for the same line
        """
        query = """
        UPDATE {table} as i
        SET gn_is_valid = 'False',
        gn_invalid_reason = 'INVALID_GEOM_CODE'
        FROM (
            SELECT gn_pk
            FROM {table}
            WHERE given_geom IS NULL AND id_area_attachment IS NULL AND (
            {code_commune_col} IS NOT NULL OR
            {code_maille_col} IS NOT NULL OR
            {code_dep_col} IS NOT NULL
            )
        ) as sub 
        WHERE sub.gn_pk = i.gn_pk AND i.gn_is_valid = 'True'
        RETURNING i.gn_pk, 
        {code_commune_col} as code_com,
        {code_maille_col} as code_maille,
        {code_dep_col} as code_dep

        """.format(
            table=self.table_name,
            code_commune_col=self.code_commune_col or "codecommune",
            code_maille_col=self.code_maille_col or "coodemaille",
            code_dep_col=self.code_dep_col or "codedepartement",
        )
        return execute_query(query, commit=True).fetchall()
