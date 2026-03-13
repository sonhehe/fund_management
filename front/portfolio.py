import streamlit as st
import pandas as pd
import datetime as dt
from sqlalchemy import text

from scripts.db import load_table, smart_dataframe
from scripts.db_engine import get_engine
from scripts.pricing_yahoo import update_all_prices
from scripts.portfolio import build_trade_record, update_portfolio


def render():

    st.header("Portfolio")

    df_port = load_table("portfolio")

    smart_dataframe(
        df_port,
        "portfolio",
        use_container_width=True,
        hide_index=True
    )

    df_port["ticker"] = df_port["ticker"].str.upper()

    engine = get_engine()

    if st.button("Update Market Prices (Yahoo)"):

        with st.spinner("Fetching prices from Yahoo Finance..."):
            update_all_prices(engine)

        st.success("Updated stock prices")
        st.rerun()

    portfolio_map = (
        df_port.set_index("ticker")["quantity"].to_dict()
        if not df_port.empty else {}
    )

    with st.form("trade_form"):

        ticker = st.text_input("Ticker").upper()

        side = st.selectbox("Side", ["BUY", "SELL"])

        quantity = st.number_input("Quantity", min_value=1, step=1)

        price = st.number_input("Price", min_value=0.0, step=100.0)

        submitted = st.form_submit_button("Submit trade")

    if submitted:

        error = None

        if ticker == "":
            error = "Ticker cannot be empty"

        elif side == "SELL" and ticker not in portfolio_map:
            error = f"Cannot SELL: {ticker} not found in portfolio"

        elif side == "SELL":
            max_qty = portfolio_map.get(ticker, 0)

            if quantity > max_qty:
                error = f"Cannot SELL {quantity} units of {ticker}. Available: {max_qty}"

        if error:

            st.error(error)

        else:

            trade_time = dt.datetime.now()

            trade = build_trade_record(
                trade_date=trade_time,
                ticker=ticker,
                side=side,
                quantity=quantity,
                price=price,
            )

            with engine.begin() as conn:

                conn.execute(
                    text("""
                        INSERT INTO trades
                        (trade_date, ticker, side, quantity, price, cash_flow)
                        VALUES
                        (:trade_date, :ticker, :side, :quantity, :price, :cash_flow)
                    """),
                    trade
                )

            st.success("Trade executed successfully")

            st.rerun()

    if st.button("Update Portfolio"):

        engine = get_engine()

        update_portfolio(engine)

        st.success("Portfolio updated successfully")

        st.rerun()