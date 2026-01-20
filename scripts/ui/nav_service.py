import pandas as pd
from scripts.db_engine import get_engine
from sqlalchemy import text

def get_nav_df():
    """
    Dùng cho UI: nav table, chart
    """
    engine = get_engine()

    with engine.connect() as conn:
        df = pd.read_sql(
            """
            SELECT
                nav_date,
                nav_total,
                current_units,
                nav_per_unit
            FROM nav
            ORDER BY nav_date
            """,
            conn
        )

    return df
