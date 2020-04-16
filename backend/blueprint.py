import csv
from flask import Blueprint, jsonify

from .api_error import GeonatureImportApiError

blueprint = Blueprint("import", __name__)

from .routes import (
    checks_and_transformations,
    download_to_synthese,
    errors,
    imports,
    mappings,
    preview,
    uploads,
)


@blueprint.errorhandler(GeonatureImportApiError)
def handle_geonature_import_api(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

