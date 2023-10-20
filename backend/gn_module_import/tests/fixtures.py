import pytest

from geonature.core.gn_commons.models import TModules

from gn_module_import.models import Destination


@pytest.fixture(scope="session")
def synthese_destination():
    return Destination.query.filter(
        Destination.module.has(TModules.module_code == "SYNTHESE")
    ).one()


@pytest.fixture(scope="session")
def default_synthese_destination(app, synthese_destination):
    """
    This fixture set "synthese" as default destination when not specified in call to url_for.
    """

    @app.url_defaults
    def set_synthese_destination(endpoint, values):
        if (
            app.url_map.is_endpoint_expecting(endpoint, "destination")
            and "destination" not in values
        ):
            values["destination"] = "synthese"
