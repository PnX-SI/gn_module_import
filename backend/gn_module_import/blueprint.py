from flask import Blueprint

import gn_module_import.admin  # noqa: F401

blueprint = Blueprint("import", __name__, template_folder="templates")

from .routes import (
    imports,
    mappings,
)

from .commands import fix_mappings

blueprint.cli.add_command(fix_mappings)
