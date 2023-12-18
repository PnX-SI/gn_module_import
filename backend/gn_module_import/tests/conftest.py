from geonature.tests.fixtures import *
from geonature.tests.fixtures import app, _session, users
from pypnusershub.tests.fixtures import teardown_logout_user
from .fixtures import *


pytest.register_assert_rewrite("gn_module_import.tests.utils")
