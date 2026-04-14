import pandas as pd
import os
from sqlalchemy import text
from datetime import date
from scripts.db_engine import get_engine
def build_trade_record(ticker, side, quantity, price, trade_date):
    side = side.capitalize()

    if side == "Buy":
        cash_flow = -quantity * price
    elif side == "Sell":
        cash_flow = quantity * price
    else:
        raise ValueError("Side must be Buy or Sell")

    return {
        "trade_date": trade_date,
        "side": side,
        "ticker": ticker,
        "quantity": quantity,
        "price": price,
        "cash_flow": cash_flow
    }
from sqlalchemy import text


def update_portfolio(engine):

    with engine.begin() as conn:

        # 1️⃣ Insert ticker mới nếu chưa có
        conn.execute(text("""
            INSERT INTO portfolio (
                ticker,
                asset_name,
                asset_type,
                quantity,
                buy_price,
                market_price,
                net_value,
                interest,
                price_date
            )
            SELECT DISTINCT
                t.ticker,
                NULL,
                'Stock',
                0,
                0,
                0,
                0,
                0,
                CURRENT_DATE
            FROM trades t
            WHERE t.is_processed = FALSE
            ON CONFLICT (ticker) DO NOTHING;
        """))

        # 2️⃣ Update positions
        conn.execute(text("""
        WITH trade_agg AS (

            SELECT
                ticker,

                SUM(
                    CASE
                        WHEN side = 'Buy'
                        THEN quantity
                        ELSE 0
                    END
                ) AS buy_qty,

                SUM(
                    CASE
                        WHEN side = 'Sell'
                        THEN quantity
                        ELSE 0
                    END
                ) AS sell_qty,

                SUM(
                    CASE
                        WHEN side = 'Buy'
                        THEN quantity * price
                        ELSE 0
                    END
                ) AS buy_value,

                SUM(cash_flow) AS total_cash

            FROM trades
            WHERE is_processed = FALSE
            GROUP BY ticker
        ),

        updated AS (

            SELECT
                p.ticker,

                p.quantity AS old_qty,

                (p.quantity + ta.buy_qty - ta.sell_qty) AS quantity_new,

                CASE
                    WHEN ta.buy_qty > 0 THEN
                        (p.buy_price * p.quantity + ta.buy_value)
                        / NULLIF(p.quantity + ta.buy_qty, 0)
                    ELSE p.buy_price
                END AS buy_price_new,

                COALESCE(p.market_price,0) AS market_price

            FROM portfolio p
            JOIN trade_agg ta
            ON p.ticker = ta.ticker
        )

        UPDATE portfolio p
        SET
            quantity =
                CASE
                    WHEN u.quantity_new < 0
                    THEN 0
                    ELSE u.quantity_new
                END,

            buy_price =
                CASE
                    WHEN u.quantity_new = 0
                    THEN 0
                    ELSE u.buy_price_new
                END,

            net_value =
                CASE
                    WHEN u.quantity_new < 0
                    THEN 0
                    ELSE u.quantity_new * u.market_price
                END,

            interest =
                CASE
                    WHEN u.buy_price_new > 0
                    THEN (u.market_price - u.buy_price_new) / u.buy_price_new
                    ELSE 0
                END

        FROM updated u
        WHERE p.ticker = u.ticker;
        """))

        # 3️⃣ Update Cash
        conn.execute(text("""
            UPDATE portfolio
            SET net_value = net_value + sub.total_cash
            FROM (
                SELECT COALESCE(SUM(cash_flow),0) AS total_cash
                FROM trades
                WHERE is_processed = FALSE
            ) sub
            WHERE asset_type = 'Cash';
        """))

        # 4️⃣ Mark processed
        conn.execute(text("""
            UPDATE trades
            SET is_processed = TRUE
            WHERE is_processed = FALSE;
        """))
        # 5️⃣ Cleanup: xóa các stock có quantity = 0 và market_price = 0
        conn.execute(text("""
            DELETE FROM portfolio
            WHERE quantity = 0
            AND asset_type = 'Stock'
            AND market_price = 0
        """))
        # 6️⃣ Recalculate current weight
        conn.execute(text("""
            WITH total_nav AS (
                SELECT COALESCE(SUM(net_value), 0) AS total_value
                FROM portfolio
            )
            UPDATE portfolio p
            SET current_weight = CASE
                WHEN t.total_value = 0 THEN 0
                ELSE COALESCE(p.net_value, 0) / t.total_value
            END
            FROM total_nav t;
        """))
    return "Portfolio updated safely."
