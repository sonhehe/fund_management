import pandas as pd
import os
import streamlit as st
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
    "cash",
    "cash_requests",
    "fundshare_requests",
    "trade_confirmations",
}


# ======================
TABLE_COLUMN_LABELS = {
    "overall_snapshot": {
        "attribute": "Attribute",
        "initial_investment": "Initial Investment",
        "market_value": "Market Value",
        "weight": "Current Weight",
        "weight_obj": "Target Weight",
        "profit": "Profit",
        "interest": "Return (%)",
        "snapshot_time": "Snapshot Time",
    },

    "nav": {
        "nav_date": "NAV Date",
        "nav_total": "NAV Total",
        "current_units": "Current Units",
        "nav_per_unit": "NAV / Unit",
    },

    "trades": {
        "trade_id": "Trade ID",
        "trade_date": "Trade Date",
        "cash_flow": "Cash Flow",
        "price": "Price",
        "side": "Side",
        "quantity": "Quantity",
        "ticket": "Ticker",
    },
    "costs": {
        "cost_date": "Date",
        "cost_type": "Type",
        "cost": "Cost",
        "cost_category": "Category",
        "rate": "Rate",
    },
    "portfolio": {
        "price_date": "Price Date",
        "ticker": "Ticker",
        "asset_name": "Name",
        "asset_type": "Asset Type",
        "quantity": "Quantity",
        "buy_price": "Buy Price",
        "market_price": "Market Price",
        "net_value": "Net Value",
        "interest": "Interest",
        "current_weight": "Current Weight",
        "target_weight": "Target Weight",
    },
    "users": {
        "username": "Username",
        "display_name": "Display Name",
        "email": "Email",
        "cccd_mst": "CCCD/MST",
        "dob": "DoB",
        "phone": "Phone",
        "address": "Address",
        "bank_account": "Bank Account",
        "pwd_hash": "Password Hash",
        "role": "Role",
        "fund": "Fund",
        "created_at": "Created At",
        "customer_id": "Customer ID",
    },
    "investors": {
        "customer_id": "Customer ID",
        "customer_name": "Customer Name",
        "status": "Status",
        "open_account_date": "Open Account Date",
        "indentity_number": "Identity Number",
        "dob": "DoB",
        "phone": "Phone",
        "email": "Email",
        "address": "Address",
        "capital": "Capital",
        "nos": "Nos",
        "current_cash": "Current Cash",
        "interest_rate": "Interest Rate",
        "bank_account": "Bank Account",
    },
    "fundshare_trades": {
        "trade_date": "Trade Date",
        "customer_id": "Customer ID",
        "side": "Side",
        "quantity": "Quantity",
        "price": "Price",
        "cost": "Cost",
        "capital": "Capital",
        "current_fs": "Current Fund Shares",
        "cash_flow": "Cash Flow",
    },
    "cash_requests": {
        "customer_id": "Customer ID",
        "type": "Type",
        "amount": "Amount",
        "status": "Status",
        "created_at": "Created At",
        "updated_at": "Updated At",
    }
}
def apply_column_labels(df, table_name):
    labels = TABLE_COLUMN_LABELS.get(table_name, {})
    return df.rename(columns={
        col: labels.get(col, col)
        for col in df.columns
    })

# READ
# ======================
def load_table(table_name: str) -> pd.DataFrame:
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table_name}' is not allowed")

    engine = get_engine()

    # ⚠️ Postgres phân biệt hoa/thường khi có "
    query = text(f'SELECT * FROM "{table_name}"')
    df = pd.read_sql(query, engine)

    # 🔥 Convert toàn bộ Decimal -> float
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = df[col].astype(float)
            except:
                pass

    return df


# ======================
from decimal import Decimal
def smart_dataframe(df, table_name, use_container_width=True, hide_index=True):
    df_display = apply_column_labels(df, table_name)

    # Chỉ chọn numeric
    numeric_cols = df_display.select_dtypes(include="number").columns

    # Làm tròn
    df_display[numeric_cols] = df_display[numeric_cols].round(2)

    # Format dấu phẩy nghìn
    df_styled = df_display.style.format(
        {col: "{:,.2f}" for col in numeric_cols}
    )

    st.dataframe(
        df_styled,
        use_container_width=use_container_width,
        hide_index=hide_index
    )
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
            DELETE FROM costs WHER cost_date=:d"""))
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

def run_nav_pipeline(engine):

    with engine.begin() as conn:

        today = date.today()

        # =========================
        # DELETE TODAY NAV
        # =========================

        conn.execute(text("""
            DELETE FROM nav
            WHERE nav_date = :d
        """), {"d": today})


        # =========================
        # NAV GROSS
        # =========================

        nav_gross = conn.execute(text("""
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN asset_type = 'Cash'
                        THEN net_value
                        ELSE quantity * market_price
                    END
                ),0)
            FROM portfolio
        """)).scalar()


        # =========================
        # INSERT NAV GROSS
        # =========================

        conn.execute(text("""
            INSERT INTO nav (nav_date, nav_total)
            VALUES (:d, :nav)
        """), {
            "d": today,
            "nav": nav_gross
        })


        # =========================
        # DELETE TODAY COST
        # =========================

        conn.execute(text("""
            DELETE FROM costs
            WHERE cost_date = :d
        """), {"d": today})


        # =========================
        # MANAGEMENT FEE
        # =========================

        conn.execute(text("""
            INSERT INTO costs (
                cost_date,
                cost_type,
                cost,
                cost_category,
                rate
            )
            VALUES (
                :d,
                'management_fee',
                :nav * 0.0015 / 365,
                'Management',
                0.0015
            )
        """), {
            "d": today,
            "nav": nav_gross
        })


        # =========================
        # TRANSACTION FEE
        # =========================

        conn.execute(text("""
            INSERT INTO costs (
                cost_date,
                cost_type,
                cost,
                cost_category,
                rate
            )
            SELECT
                :d,
                'transaction_fee',
                COALESCE(SUM(ABS(cash_flow) * 0.0015),0),
                'Trading',
                0.0015
            FROM fundshare_trades
            WHERE trade_date = :d
            AND status='SUCCESS'
        """), {"d": today})


        # =========================
        # TOTAL COST
        # =========================

        total_cost = conn.execute(text("""
            SELECT COALESCE(SUM(cost),0)
            FROM costs
            WHERE cost_date = :d
        """), {"d": today}).scalar()


        nav_net = nav_gross - total_cost


        # =========================
        # CURRENT UNITS
        # =========================

        units = conn.execute(text("""
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN side='BUY'
                        THEN quantity
                        WHEN side='SELL'
                        THEN -quantity
                    END
                ),0)
            FROM fundshare_trades
            WHERE trade_date <= :d
            AND status='SUCCESS'
        """), {"d": today}).scalar()


        if units <= 0:
            raise ValueError("Outstanding units <= 0")


        nav_per_unit = nav_net / units


        # =========================
        # UPDATE NAV
        # =========================

        conn.execute(text("""
            UPDATE nav
            SET
                nav_total = :nav_net,
                current_units = :units,
                nav_per_unit = :p
            WHERE nav_date = :d
        """), {
            "d": today,
            "nav_net": nav_net,
            "units": units,
            "p": nav_per_unit
        })


        return {
            "nav_gross": nav_gross,
            "total_cost": total_cost,
            "nav_net": nav_net,
            "units": units,
            "nav_per_unit": nav_per_unit
        }