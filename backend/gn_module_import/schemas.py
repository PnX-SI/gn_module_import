from geonature.utils.env import db, ma

from utils_flask_sqla.schema import SmartRelationshipsMixin

from gn_module_import.models import Destination


class DestinationSchema(SmartRelationshipsMixin, ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Destination
        include_fk = True
        load_instance = True
        sqla_session = db.session
