from flask import Blueprint

blueprint = Blueprint("import", __name__)

from .routes import (
    errors,
    imports,
    mappings,
    uploads,
)

from .commands import fix_mappings
blueprint.cli.add_command(fix_mappings)
