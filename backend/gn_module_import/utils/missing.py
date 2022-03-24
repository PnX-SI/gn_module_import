from sqlalchemy.orm import load_only

from geonature.utils.env import db
from gn_module_import.models import BibFields


def get_missing_fields(imprt):
    mapped_target_fields = {
        target
        for target, source in imprt.fieldmapping.items()
        if source in imprt.columns.keys()
    }
    location_fields = ['WKT', 'longitude', 'latitude',
                       'codecommune', 'codemaille', 'codedepartement']
    # location fields are check manually so we exclude them from required fields
    required_fields = (
        BibFields.query
       .filter_by(mandatory=True)
       .filter(BibFields.name_field.notin_(location_fields))
       .options(load_only('name_field'))
       .all()
    )
    missing_fields = [f.name_field for f in required_fields
                      if f.name_field not in mapped_target_fields]
    if ('WKT' not in mapped_target_fields
            and not mapped_target_fields.issuperset({'longitude', 'latitude'})
            and mapped_target_fields.isdisjoint({'codecommune', 'codemaille', 'codedepartement'})):
        missing_fields.append('position (wkt, x/y ou code)')
    return missing_fields
