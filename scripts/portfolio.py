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
def apply_trade_cash_flow(engine, cash_flow):
    """
    Apply cash flow to Cash/YTM position immediately
    """
    sql = """
    UPDATE portfolio
    SET
        net_value = net_value + :cash_flow
    WHERE asset_type = 'Cash'
       OR ticker = 'YTM';
    """

    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {"cash_flow": cash_flow}
        )
def update_portfolio(engine):
    """
    Update portfolio using ONLY trades AFTER latest portfolio price_date
    """

    sql = """
    WITH last_port_date AS (
        SELECT MAX(price_date) AS last_date
        FROM portfolio
    ),

    trade_agg AS (
        SELECT
            t.ticker,

            SUM(CASE WHEN t.side = 'Buy'  THEN t.quantity ELSE 0 END) AS buy_qty,
            SUM(CASE WHEN t.side = 'Sell' THEN t.quantity ELSE 0 END) AS sell_qty,

            SUM(CASE
                WHEN t.side = 'Buy' THEN t.quantity * t.price
                ELSE 0
            END) AS buy_value

        FROM trades t
        CROSS JOIN last_port_date d
        WHERE t.trade_date >= d.last_date
        GROUP BY t.ticker
    ),

    updated AS (
        SELECT
            p.ticker,

            -- quantity mới
            (p.quantity + ta.buy_qty - ta.sell_qty) AS quantity_new,

            -- buy_price mới (WAC)
            CASE
                WHEN ta.buy_qty > 0 THEN
                    (p.buy_price * p.quantity + ta.buy_value)
                    / (p.quantity + ta.buy_qty)
                ELSE p.buy_price
            END AS buy_price_new,

            p.market_price

        FROM portfolio p
        JOIN trade_agg ta
            ON p.ticker = ta.ticker
    )

    UPDATE portfolio p
    SET
        quantity   = u.quantity_new,
        buy_price  = u.buy_price_new,
        net_value  = u.quantity_new * u.market_price,
        interest   = CASE
                        WHEN u.buy_price_new > 0 THEN
                            (u.market_price - u.buy_price_new) / u.buy_price_new
                        ELSE 0
                     END
    FROM updated u
    WHERE p.ticker = u.ticker;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))

def insert_empty_portfolio_row(engine, ticker, price):
    sql = """
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
    VALUES (
        :ticker,
        NULL,
        'Stock',
        0,
        0,
        0,
        0,
        0,
        CURRENT_DATE
    )
    ON CONFLICT (ticker) DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "ticker": ticker,
                "market_price": price
            }
        )