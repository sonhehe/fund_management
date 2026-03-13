import streamlit as st
import pandas as pd
from sqlalchemy import text

from scripts.db import write_table
from scripts.db_engine import get_engine
from scripts.fundshare import (
    execute_fundshare_trade,
    get_latest_nav_per_unit,
    calculate_fundshare_fee
)
from scripts.information import load_investor_portfolio


def render():

    st.header("Exchange Fund Share")

    engine = get_engine()

    is_admin = st.session_state.get("is_admin", False)

    if not is_admin:

        portfolio = load_investor_portfolio(st.session_state.customer_id)

        current_cash = float(portfolio.get("current_cash", 0) or 0)

        current_units = float(portfolio.get("nos", 0) or 0)

        nav_price = float(get_latest_nav_per_unit() or 0)

        st.metric("NAV per Unit", f"{nav_price:,.2f}")

        side = st.selectbox("Side", ["Buy", "Sell"])

        if side == "Buy":

            amount = st.number_input("Investment Amount", min_value=0.0)

            fee = calculate_fundshare_fee("Buy", amount)

            units = (amount - fee) / nav_price if nav_price > 0 else 0

            st.write(f"Units: {units:,.4f}")

        else:

            quantity = st.number_input("Sell Quantity", min_value=0.0)

            fee = calculate_fundshare_fee("Sell", quantity * nav_price)

        if st.button("Submit Request"):

            df = pd.DataFrame([{
                "customer_id": st.session_state.customer_id,
                "side": side.upper(),
                "price": nav_price,
                "cost": fee,
                "status": "PENDING",
                "amount": amount if side == "Buy" else 0,
                "quantity": quantity if side == "Sell" else 0
            }])

            write_table(df, "fundshare_requests")

            st.success("Request submitted")

            st.rerun()

    else:

        st.subheader("Pending Requests")

        df_req = pd.read_sql(
            "SELECT * FROM fundshare_requests WHERE status='PENDING'",
            engine
        )

        for _, r in df_req.iterrows():

            with st.expander(f"{r['customer_id']} – {r['side']}"):

                if st.button("Approve", key=f"approve_{r['id']}"):

                    execute_fundshare_trade(
                        customer_id=r["customer_id"],
                        side=r["side"],
                        amount=r["amount"] if r["side"] == "BUY" else None,
                        quantity=r["quantity"] if r["side"] == "SELL" else None
                    )

                    with engine.begin() as conn:

                        conn.execute(text("""
                            UPDATE fundshare_requests
                            SET status='SUCCESS'
                            WHERE id=:id
                        """), {"id": r["id"]})

                    st.rerun()

                if st.button("Reject", key=f"reject_{r['id']}"):

                    with engine.begin() as conn:

                        conn.execute(text("""
                            UPDATE fundshare_requests
                            SET status='REJECTED'
                            WHERE id=:id
                        """), {"id": r["id"]})

                    st.rerun()