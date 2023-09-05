"""try cast function

Revision ID: 74e7c35f22c6
Revises: 8611f7aab8dc
Create Date: 2023-09-05 17:51:22.487702

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "74e7c35f22c6"
down_revision = "8611f7aab8dc"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
        CREATE FUNCTION try_cast_int (p_in text, p_default int default null)
        RETURNS INTEGER
        LANGUAGE plpgsql
        AS 
            $function$ 
                BEGIN
                    BEGIN
                        RETURN p_in::INTEGER;
                    EXCEPTION
                        WHEN OTHERS THEN
                            RETURN p_default;
                    END;
                END;
            $function$
    """
    )


def downgrade():
    pass
