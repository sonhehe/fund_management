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

    engine = get_engine()

    # ======================
    # LOAD PORTFOLIO
    # ======================

    df_port = load_table("portfolio")
    if not df_port.empty and "price_date" in df_port.columns:
        last_update = pd.to_datetime(df_port["price_date"]).max()
    st.caption(f"Last updated: {last_update:%Y-%m-%d %H:%M:%S}")
    smart_dataframe(
        df_port,
        "portfolio",
        use_container_width=True,
        hide_index=True
    )

    if not df_port.empty and "ticker" in df_port.columns:
        df_port["ticker"] = df_port["ticker"].str.upper()

    # ======================
    # UPDATE MARKET PRICES
    # ======================

    if st.button("Update Market Prices (Yahoo)"):

        with st.spinner("Fetching prices from Yahoo Finance..."):
            update_all_prices(engine)

        st.success("Updated stock prices")
        st.rerun()

    # ======================
    # BUILD PORTFOLIO MAP
    # ======================

    portfolio_map = {}

    if (
        not df_port.empty
        and "ticker" in df_port.columns
        and "quantity" in df_port.columns
    ):
        portfolio_map = df_port.set_index("ticker")["quantity"].to_dict()

    # ======================
    # TRADE FORM
    # ======================

    with st.form("trade_form"):

        ticker = st.text_input("Ticker").upper()

        side = st.selectbox("Side", ["BUY", "SELL"])

        quantity = st.number_input("Quantity", min_value=1, step=1)

        price = st.number_input("Price", min_value=0.0, step=100.0)

        submitted = st.form_submit_button("Submit trade")

    # ======================
    # TRADE LOGIC
    # ======================

    if submitted:

        error = None

        if ticker == "":
            error = "Ticker cannot be empty"

        elif side == "SELL" and ticker not in portfolio_map:
            error = f"Cannot SELL: {ticker} not found in portfolio"

        elif side == "SELL":

            with engine.connect() as conn:

                max_qty = conn.execute(
                    text("""
                        SELECT COALESCE(quantity,0)
                        FROM portfolio
                        WHERE ticker = :ticker
                    """),
                    {"ticker": ticker}
                ).scalar()
            if quantity > max_qty:
                error = (
                    f"Cannot SELL {quantity} units of {ticker}. "
                    f"Available: {max_qty}"
                )

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

            df_trade_new = pd.DataFrame([trade])

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

            st.dataframe(df_trade_new)

            st.rerun()

    # ======================
    # UPDATE PORTFOLIO
    # ======================

    if st.button("Update Portfolio"):

        update_portfolio(engine)

        st.success("Portfolio updated successfully")

        st.rerun()