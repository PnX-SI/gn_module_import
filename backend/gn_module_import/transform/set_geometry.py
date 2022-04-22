from flask import current_app
from shapely.geometry import Polygon

from geonature.utils.env import DB

from ..wrappers import checker
from ..logs import logger
from ..db.queries.geometries import get_id_area_type
from ..db.queries.user_errors import set_user_error
#from .utils import set_error_and_invalid_reason FIXME
#from gn_module_import.utils.imports import get_import_table_name


class GeometrySetter:
    """
    Utility class to manage geometry transformation from the import table
    """

    def __init__(
        self,
        import_object,
        local_srid: int,
        code_commune_col=None,
        code_maille_col=None,
        code_dep_col=None,
    ):
        self.import_object = import_object
        self.id_import = import_object.id_import
        self.table_name = get_import_table_name(import_object)
        self.import_srid = import_object.srid
        #self.column_names = import_object.column_names
        self.local_srid = local_srid
        self.code_commune_col = code_commune_col
        self.code_maille_col = code_maille_col
        self.code_dep_col = code_dep_col

    @checker("Data cleaning : geometries created")
    def set_geometry(self):
        """
        - Add 3 geometry columns and 1 column id_area_attachmet to the temp table and fill them from the "source_column" col
        calculated in python in the geom_check step
        - Check if the geom are valid (not self intersected)
        - check if geom fit with the bounding box
        - calculate the attachment geoms
        """
        self.check_geom_validity()
        if current_app.config["IMPORT"]["ENABLE_BOUNDING_BOX_CHECK"]:
            #  check bounding box
            results_out_of_box = self.check_geoms_fit_bbox().fetchall()
            if results_out_of_box:
                set_user_error(
                    id_import=self.id_import,
                    error_code="GEOMETRY_OUT_OF_BOX",
                    col_name="Colonne géométriques",
                    id_rows=list(map(lambda row: row.gn_pk, results_out_of_box)),
                )
        #  retransform the geom col in text (otherwise dask not working)
        self.set_text()
        # calculate the geom attachement for communes / maille et département
        if self.code_commune_col:
            print("CODE COMMUNE")
            self.calculate_geom_attachement(
                area_type_code="COM",
                code_col=self.code_commune_col,
                ref_geo_area_code_col="area_code",
            )
        if self.code_dep_col:
            print("CODE DEP")
            self.calculate_geom_attachement(
                area_type_code="DEP",
                code_col=self.code_dep_col,
                ref_geo_area_code_col="area_code",
            )
        if self.code_maille_col:
            print("CODE MAILLE")
            self.calculate_geom_attachement(
                area_type_code="M10",
                code_col=self.code_maille_col,
                ref_geo_area_code_col="area_name",
            )
        #  calcul des erreurs
        #  si aucun code fournis -> on ne vérifie pas les erreurs sur les codes
        if self.code_commune_col or self.code_maille_col or self.code_dep_col:
            print("CODE !!!!")
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
                    error_code="INVALID_GEOM_CODE",
                    col_name=self.code_dep_col,
                    id_rows=dep_errors["id_rows"],
                    comment="Les codes départements suivant sont invalides : {}".format(
                        ", ".join(dep_errors["code_error"])
                    ),
                )
    def calculate_geom_point(self, source_geom_column, target_geom_column):
        # FIXME: what if source col is NULL?????
        query = """
            UPDATE {table_name}
            SET {target_geom_column} = ST_centroid({source_geom_column})
            WHERE gn_is_valid = 'True'
            ;
            """.format(
            table_name=self.table_name,
            target_geom_column=target_geom_column,
            source_geom_column=source_geom_column,
        )
        DB.session.execute(query)

    @checker("Calcul des rattachements")
    def calculate_geom_attachement(
        self, area_type_code, code_col, ref_geo_area_code_col,
    ):
        """
        Find id_area_attachment in ref_geo.l_areas from code given in the file
        Update only columns where gn_the_geom_4326 is NULL AND id_area_attachment is NULL
        
        :params id_area_type int: the id_area_type (ref_geo.bib_area_type) of the coresponding code (example: 25 for commune)
        :params str :code_col: column name where find the code for attachment in the inital table
        :params str ref_geo_area_code_col: column of the ref_geo.l_area table where find the coresponding code (for maille its area_name, for other: area_code)
        """
        query = """
            WITH sub as (
                SELECT id_area, la.geom, gn_pk
                FROM {table} 
                JOIN ref_geo.l_areas la ON la.{ref_geo_area_code_col} = {code_col} 
                JOIN ref_geo.bib_areas_types b ON b.id_type = la.id_type AND type_code = '{area_type_code}'
                WHERE {code_col} IS NOT NULL
            )
                UPDATE {table} as i
                    SET id_area_attachment = sub.id_area,
                    gn_the_geom_local = sub.geom,
                    gn_the_geom_4326 = st_transform(sub.geom, 4326),
                    gn_the_geom_point = st_centroid(st_transform(sub.geom,4326))
            FROM sub WHERE id_area_attachment IS NULL AND gn_the_geom_local IS NULL AND sub.gn_pk = i.gn_pk;
        """.format(
            table=self.table_name,
            ref_geo_area_code_col=ref_geo_area_code_col,
            code_col=code_col,
            area_type_code=area_type_code,
        )
        DB.session.execute(query)

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
        return DB.session.execute(query).fetchall()

    def check_geoms_fit_bbox(self):
        xmin, ymin, xmax, ymax = current_app.config["IMPORT"]["INSTANCE_BOUNDING_BOX"]
        bounding_box_poly = Polygon(
            [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax),]
        )
        bounding_box_wkt = bounding_box_poly.wkt

        query = """
        UPDATE {table} as i
        SET gn_is_valid = 'False',
        gn_invalid_reason = 'GEOMETRY_OUT_OF_BOX'
        WHERE gn_pk IN (
        SELECT gn_pk as id_rows
        FROM {table}
        WHERE NOT gn_the_geom_4326 && st_geogfromtext('{bbox}')
        )
        RETURNING gn_pk
        """.format(
            table=self.table_name, bbox=bounding_box_wkt
        )
        return DB.session.execute(query)
