import pandas as pd
import os
from sqlalchemy import text
from datetime import date
from scripts.db_engine import get_engine
from scripts.pricing_yahoo import get_close_price

# SECURITY: ALLOWED TABLES
# ======================
ALLOWED_TABLES = {
    "overall_snapshot",
    "portfolio",
    "trades",
    "nav",
    "price_history",
    "fundshare_trades",
    "investors",
    "users",
    "costs",
    "fundshare_requests",
    "trade_confirmations",
}


# ======================
# READ
# ======================
def load_table(table_name: str) -> pd.DataFrame:
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table_name}' is not allowed")

    engine = get_engine()

    # ⚠️ Postgres phân biệt hoa/thường khi có "
    query = text(f'SELECT * FROM "{table_name}"')

    return pd.read_sql(query, engine)


# ======================
# WRITE (APPEND ONLY)
# ======================
def write_table(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append"
):
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table_name}' is not allowed")

    engine = get_engine()

    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        method="multi"
    )
# ======================
# UPDATE OVERALL SNAPSHOT
def update_overall_snapshot():
    engine = get_engine()
    sql = """
        BEGIN;


    -- 1️⃣ Clear snapshot trong ngày
    DELETE FROM overall_snapshot
    ;


    -- 2️⃣ TOTAL
    INSERT INTO overall_snapshot (
        attribute,
        initial_investment,
        market_value,
        profit,
        interest,
        weight,
        snapshot_time
    )
    SELECT
        'Total',
        SUM(
            CASE
                WHEN asset_type = 'Cash' THEN net_value
                ELSE buy_price * quantity
            END
        ),
        SUM(
            CASE
                WHEN asset_type = 'Cash' THEN net_value
                ELSE market_price * quantity
            END
        ),
        SUM(
            CASE
                WHEN asset_type = 'Cash' THEN 0
                ELSE (market_price - buy_price) * quantity
            END
        ),
        (
            SUM(
                CASE
                    WHEN asset_type = 'Cash' THEN net_value
                    ELSE market_price * quantity
                END
            ) /
            NULLIF(
                SUM(
                    CASE
                        WHEN asset_type = 'Cash' THEN net_value
                        ELSE buy_price * quantity
                    END
                ), 0
            )
        ) - 1,
        1.0,
        NOW()
    FROM portfolio;


    -- 3️⃣ STOCK / BOND / FUND SHARE
    INSERT INTO overall_snapshot (
        attribute,
        initial_investment,
        market_value,
        profit,
        interest,
        weight,
        snapshot_time
    )
    SELECT
        asset_type,
        SUM(buy_price * quantity),
        SUM(market_price * quantity),
        SUM(market_price * quantity) - SUM(buy_price * quantity),
        (SUM(market_price * quantity) / NULLIF(SUM(buy_price * quantity),0)) - 1,
        SUM(market_price * quantity) /
            (
                SELECT SUM(
                    CASE
                        WHEN asset_type = 'Cash' THEN net_value
                        ELSE market_price * quantity
                    END
                )
                FROM portfolio
            ),
        NOW()
    FROM portfolio
    WHERE asset_type IN ('Stock', 'Bond', 'Fund share')
    GROUP BY asset_type;
    -- 3️⃣ CASH
    INSERT INTO overall_snapshot (
        attribute,
        initial_investment,
        market_value,
        profit,
        interest,
        weight,
        snapshot_time
    )
    SELECT
        'Cash',
        SUM(net_value),
        SUM(net_value),
        0,
        0,
        SUM(net_value) /
            NULLIF(
                (
                    SELECT SUM(
                        CASE
                            WHEN asset_type = 'Cash' THEN net_value
                            ELSE market_price * quantity
                        END
                    )
                    FROM portfolio
                ), 0
            ),
        NOW()
    FROM portfolio
    WHERE asset_type = 'Cash';


    COMMIT;
    """


    with engine.begin() as conn:
        conn.execute(text(sql))


def update_costs(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM costs"""))
        # ===== MANAGEMENT FEE (NAV latest) =====
        conn.execute(
            text("""
                INSERT INTO costs (
                    cost_date, cost_type, cost, cost_category, rate
                )
                SELECT
                    n.nav_date,
                    'management_fee',
                    n.nav_total * 0.0015 / 365,
                    'Management',
                    0.0015
                FROM nav n
                WHERE n.nav_date = (
                    SELECT MAX(nav_date) FROM nav
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM costs c
                    WHERE c.cost_date = n.nav_date
                      AND c.cost_type = 'management_fee'
                )
            """)
        )

        # ===== TRANSACTION FEE (TODAY) =====
        conn.execute(
            text("""
                INSERT INTO costs (
                    cost_date, cost_type, cost, cost_category, rate
                )
                SELECT
                    :d,
                    'transaction_fee',
                    COALESCE(SUM(ABS(cash_flow) * 0.0015), 0),
                    'Trading',
                    0.0015
                FROM fundshare_trades
                WHERE trade_date = :d
            """),
            {"d": date.today()}
        )


def insert_nav_gross(engine):
    with engine.begin() as conn:
        r = conn.execute(
            text("""
                SELECT
                    COALESCE(SUM(quantity * market_price), 0) AS nav_total
                FROM portfolio
            """)
        ).mappings().first()


        nav_total = r["nav_total"]


        conn.execute(
            text("""
                INSERT INTO nav (nav_date, nav_total)
                VALUES (:d, :t)
            """),
            {
                "d": date.today(),
                "t": nav_total
            }
        )


    return nav_total
def insert_nav_per_unit(engine):
    with engine.begin() as conn:

        # --- NAV GROSS VỪA INSERT ---
        r = conn.execute(
            text("""
                SELECT nav_date, nav_total
                FROM nav
                ORDER BY nav_date DESC
                LIMIT 1
            """)
        ).mappings().first()

        if r is None:
            raise ValueError("No NAV found")

        nav_date = r["nav_date"]
        nav_gross = r["nav_total"]

        # --- TOTAL COSTS (MANAGEMENT + TRANSACTION) ---
        c = conn.execute(
            text("""
                SELECT
                    COALESCE(SUM(cost), 0) AS total_cost
                FROM costs
                WHERE cost_date = :d
            """),
            {"d": nav_date}
        ).mappings().first()

        total_cost = c["total_cost"]

        # --- NAV NET ---
        nav_net = nav_gross - total_cost

        # --- CURRENT UNITS ---
        u = conn.execute(
            text("""
                SELECT
                    COALESCE(SUM(
                        CASE
                            WHEN side = 'Buy'  THEN quantity
                            WHEN side = 'Sell' THEN -quantity
                        END
                    ), 0) AS current_units
                FROM fundshare_trades
                WHERE trade_date <= :d
            """),
            {"d": nav_date}
        ).mappings().first()

        if u["current_units"] == 0:
            raise ValueError("Current units = 0")

        nav_per_unit = nav_net / u["current_units"]

        # --- UPDATE NAV ---
        conn.execute(
            text("""
                UPDATE nav
                SET
                    nav_total      = :nav_net,
                    current_units  = :u,
                    nav_per_unit   = :p
                WHERE nav_date = :d
            """),
            {
                "d": nav_date,
                "nav_net": nav_net,
                "u": u["current_units"],
                "p": nav_per_unit
            }
        )

    return {
        "nav_date": nav_date,
        "nav_gross": nav_gross,
        "total_cost": total_cost,
        "nav_net": nav_net,
        "current_units": u["current_units"],
        "nav_per_unit": nav_per_unit
    }

def run_nav_pipeline(engine):
    logs = []

    try:
        logs.append("Step 1: Insert NAV gross")
        insert_nav_gross(engine)

        logs.append("Step 2: Update costs")
        update_costs(engine)

        logs.append("Step 3: Calculate NAV per unit")
        result = insert_nav_per_unit(engine)

        logs.append("NAV process completed successfully")
        return logs, result, None

    except Exception as e:
        return logs, None, str(e)
