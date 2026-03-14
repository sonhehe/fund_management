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


# ==============================
# CACHE
# ==============================

@st.cache_data(ttl=5)
def load_nav():
    return float(get_latest_nav_per_unit() or 0)


@st.cache_data(ttl=5)
def load_portfolio(customer_id):
    return load_investor_portfolio(customer_id)


@st.cache_data(ttl=5)
def load_pending_requests():
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM fundshare_requests WHERE status='PENDING'",
        engine
    )


@st.cache_data(ttl=5)
def load_cash_requests():
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM cash_requests WHERE status='PENDING'",
        engine
    )


# ==============================
# MAIN PAGE
# ==============================

def render():

    st.header("Exchange Fund Share")

    engine = get_engine()
    
    is_admin = st.session_state.get("is_admin", False)

    if is_admin:
        render_admin(engine)
    else:
        render_investor(engine)


# ==============================
# INVESTOR VIEW
# ==============================

def render_investor(engine):

    portfolio = load_portfolio(st.session_state.customer_id)

    if portfolio is None:
        st.warning("Failed to load portfolio.")
        st.stop()

    current_cash = float(portfolio.get("current_cash", 0) or 0)
    current_units = float(portfolio.get("nos", 0) or 0)

    nav_price = load_nav()

    if nav_price <= 0:
        st.error("NAV is not available")
        st.stop()

    st.subheader("Fund Unit Transaction")

    c1, c2, c3 = st.columns(3)

    c1.metric("NAV per Unit", f"{nav_price:,.2f}")
    c2.metric("Available Balance", f"{current_cash:,.0f}")
    c3.metric("Units Held", f"{current_units:,.4f}")

    side = st.selectbox("Side", ["Buy", "Sell"])

    amount = 0.0
    quantity = 0.0
    fee = 0.0
    can_submit = False
    error = None

    # ======================
    # BUY
    # ======================

    if side == "Buy":

        amount = st.number_input(
            "Investment Amount (VND)",
            min_value=0.0,
            step=1000.0,
            format="%.0f"
        )

        fee = float(calculate_fundshare_fee("Buy", amount) or 0)

        net_value = amount - fee
        units = net_value / nav_price if nav_price > 0 else 0

        if amount <= 0:
            error = "Amount must be greater than 0"

        elif amount > current_cash:
            error = "Exceeds available balance"

        elif net_value <= 0:
            error = "Invalid fee"

        else:
            can_submit = True

        with st.container(border=True):

            st.markdown("### Transaction Preview")

            c1, c2, c3 = st.columns(3)

            c1.metric("Units", f"{units:,.0f}")
            c2.metric("Fee", f"{fee:,.0f}")
            c3.metric("Net Invested", f"{net_value:,.0f}")

    # ======================
    # SELL
    # ======================

    else:

        col1, col2 = st.columns([3,1])

        with col1:

            quantity = st.number_input(
                "Sell Quantity",
                min_value=0.0,
                max_value=current_units,
                step=0.1,
                format="%.4f"
            )

        with col2:

            if st.button("Sell All"):
                quantity = current_units

        gross_value = quantity * nav_price

        fee = float(calculate_fundshare_fee("Sell", gross_value) or 0)

        net_value = gross_value - fee

        if quantity <= 0:
            error = "Quantity must be greater than 0"

        elif quantity > current_units:
            error = "Exceeds units held"

        elif net_value <= 0:
            error = "Invalid sell amount"

        else:
            can_submit = True

        with st.container(border=True):

            st.markdown("### Transaction Preview")

            c1, c2, c3 = st.columns(3)

            c1.metric("Gross Value", f"{gross_value:,.0f}")
            c2.metric("Fee", f"{fee:,.0f}")
            c3.metric("Net Proceeds", f"{net_value:,.0f}")

    if error:
        st.error(error)

    # ======================
    # SUBMIT
    # ======================

    if st.button("Submit Request", disabled=not can_submit):

        request_data = {

            "customer_id": st.session_state.customer_id,
            "side": side.upper(),
            "price": nav_price,
            "cost": fee,
            "status": "PENDING",
            "amount": float(amount) if side == "Buy" else 0.0,
            "quantity": float(quantity) if side == "Sell" else 0.0
        }

        write_table(pd.DataFrame([request_data]), "fundshare_requests")

        st.success("Request submitted successfully.")
        st.rerun()

    # ======================
    # CASH REQUEST
    # ======================

    st.divider()
    st.subheader("Deposit / Withdrawal")

    action = st.selectbox("Select", ["Deposit", "Withdraw"])

    cash_amount = st.number_input(
        "Amount (VND)",
        min_value=0.0,
        step=1_000.0
    )

    if st.button("Submit Fund Request"):

        if cash_amount <= 0:
            st.error("Amount must be greater than 0.")
            return

        if action == "Withdraw" and cash_amount > current_cash:
            st.error("Insufficient balance.")
            return

        write_table(pd.DataFrame([{

            "customer_id": st.session_state.customer_id,
            "type": action.upper(),
            "amount": cash_amount,
            "status": "PENDING"

        }]), "cash_requests")

        st.success("Request submitted successfully")
        st.rerun()

    # ======================
    # BANK INFO
    # ======================

    with engine.connect() as conn:

        setting = conn.execute(text("""
            SELECT bank_info
            FROM fund_setting
            LIMIT 1
        """)).mappings().fetchone()

    if setting and setting["bank_info"]:

        st.divider()

        st.markdown("### Bank Transfer Information")

        st.container(border=True).markdown(
            setting["bank_info"].replace("\n", "  \n")
        )


# ==============================
# ADMIN VIEW
# ==============================

def render_admin(engine):

    st.subheader("Pending Requests")

    df_req = load_pending_requests()

    if df_req.empty:
        st.info("No pending requests")

    else:

        for _, r in df_req.iterrows():

            with st.expander(f"{r['customer_id']} – {r['side']}"):

                st.dataframe(pd.DataFrame([r]))

                c1, c2 = st.columns(2)

                if c1.button("Approve", key=f"approve_{r['id']}"):

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
                            WHERE id=:rid
                        """), {"rid": r["id"]})

                    st.success("Approved")
                    st.rerun()

                if c2.button("Reject", key=f"reject_{r['id']}"):

                    with engine.begin() as conn:

                        conn.execute(text("""
                            UPDATE fundshare_requests
                            SET status='REJECTED'
                            WHERE id=:rid
                        """), {"rid": r["id"]})

                    st.warning("Request rejected")
                    st.rerun()

    st.divider()
    st.subheader("Cash Requests")

    df_req = load_cash_requests()

    if df_req.empty:
        st.info("No pending cash requests")
        return

    for _, r in df_req.iterrows():

        with st.expander(f"{r['customer_id']} – {r['type']} – {r['amount']:,.0f}"):

            c1, c2 = st.columns(2)

            if c1.button("Approve", key=f"cash_app_{r['id']}"):

                with engine.begin() as conn:

                    if r["type"] == "DEPOSIT":

                        conn.execute(text("""
                            UPDATE investors
                            SET current_cash = current_cash + :amount
                            WHERE customer_id = :cid
                        """), {
                            "amount": r["amount"],
                            "cid": r["customer_id"]
                        })

                        conn.execute(text("""
                            UPDATE portfolio
                            SET
                                net_value = net_value + :amount,
                                market_price = market_price + :amount
                            WHERE asset_type = 'Cash'
                            OR ticker = 'YTM'
                        """), {"amount": r["amount"]})

                    else:

                        conn.execute(text("""
                            UPDATE investors
                            SET current_cash = current_cash - :amount
                            WHERE customer_id = :cid
                        """), {
                            "amount": r["amount"],
                            "cid": r["customer_id"]
                        })

                        conn.execute(text("""
                            UPDATE portfolio
                            SET
                                net_value = net_value - :amount,
                                market_price = market_price - :amount
                            WHERE asset_type = 'Cash'
                            OR ticker = 'YTM'
                        """), {"amount": r["amount"]})

                    conn.execute(text("""
                        UPDATE cash_requests
                        SET status='SUCCESS'
                        WHERE id=:id
                    """), {"id": r["id"]})

                st.success("Approved")
                st.rerun()

            if c2.button("Reject", key=f"cash_rej_{r['id']}"):

                with engine.begin() as conn:

                    conn.execute(text("""
                        UPDATE cash_requests
                        SET status='REJECTED'
                        WHERE id=:id
                    """), {"id": r["id"]})

                st.warning("Rejected")
                st.rerun()