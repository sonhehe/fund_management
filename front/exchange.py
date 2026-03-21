import datetime

import streamlit as st
import pandas as pd
from sqlalchemy import engine, text

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
    # ======================
    # PENDING REQUESTS
    # ======================

    st.divider()
    st.subheader("Pending Requests")

    with engine.connect() as conn:

        # ===== TRADE REQUESTS =====
        trade_df = pd.read_sql(text("""
            SELECT 
                trade_date,
                side,
                amount,
                quantity,
                price,
                cost,
                created_at
            FROM fundshare_requests
            WHERE customer_id = :cid
            AND status = 'PENDING'
            ORDER BY created_at DESC
        """), conn, params={"cid": st.session_state.customer_id})

        # ===== CASH REQUESTS =====
        cash_df = pd.read_sql(text("""
            SELECT 
                type,
                amount,
                created_at
            FROM cash_requests
            WHERE customer_id = :cid
            AND status = 'PENDING'
            ORDER BY created_at DESC
        """), conn, params={"cid": st.session_state.customer_id})


    
    portfolio = load_portfolio(st.session_state.customer_id)

    if portfolio is None:
        st.warning("Failed to load portfolio.")
        st.stop()

    current_cash = float(portfolio.get("current_cash", 0) or 0)
    available_cash = float(portfolio.get("available_cash", current_cash) or 0)
    current_units = float(portfolio.get("nos", 0) or 0)
    nav_price = load_nav()

    if nav_price <= 0:
        st.error("NAV is not available")
        st.stop()

    st.subheader("Fund Unit Transaction")

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("NAV per Unit", f"{nav_price:,.2f}")
    c2.metric("Available Balance", f"{available_cash:,.0f}")
    c4.metric("Units Held", f"{current_units:,.0f}")
    c3.metric("Current cash", f"{current_cash:,.0f}")

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

        # ===== VALIDATION =====
        if amount <= 0:
            error = "Amount must be greater than 0"

        elif amount > available_cash:   # 🔥 FIX QUAN TRỌNG
            error = "Exceeds available balance"

        elif fee < 0:
            error = "Invalid fee"

        elif fee >= amount:
            error = "Fee exceeds investment amount"

        elif net_value <= 0:
            error = "Invalid net investment"

        elif units <= 0:
            error = "Calculated units is invalid"

        else:
            can_submit = True
            error = None

        # ===== UI GIỮ NGUYÊN =====
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

        # ===== VALIDATION =====
        if quantity <= 0:
            error = "Quantity must be greater than 0"

        elif quantity > current_units:
            error = "Exceeds units held"

        elif nav_price <= 0:
            error = "Invalid NAV"

        elif gross_value <= 0:
            error = "Invalid gross value"

        elif fee < 0:
            error = "Invalid fee"

        elif net_value <= 0:
            error = "Invalid sell amount"

        else:
            can_submit = True
            error = None

        # ===== UI GIỮ NGUYÊN =====
        with st.container(border=True):

            st.markdown("### Transaction Preview")

            c1, c2, c3 = st.columns(3)

            c1.metric("Gross Value", f"{gross_value:,.0f}")
            c2.metric("Fee", f"{fee:,.0f}")
            c3.metric("Net Proceeds", f"{net_value:,.0f}")


    if error is not None:
        st.error(error)

    # ======================
    # SUBMIT
    # ======================

    if st.button("Submit Request", disabled=not can_submit):

        with engine.begin() as conn:

            # ===== BUY → BLOCK CASH =====
            if side == "Buy":

                result = conn.execute(text("""
                    UPDATE investors
                    SET 
                        available_cash = available_cash - :amt,
                        blocked_cash = blocked_cash + :amt
                    WHERE customer_id = :cid
                    AND available_cash >= :amt
                """), {
                    "amt": amount,
                    "cid": st.session_state.customer_id
                })

                if result.rowcount == 0:
                    st.error("Exceeds available balance")
                    return

            # ===== INSERT REQUEST =====
            conn.execute(text("""
                INSERT INTO fundshare_requests
                (customer_id, customer_name, side, price, cost, status,
                amount, quantity, blocked_amount, created_at)
                VALUES
                (:cid, :name, :side, :price, :cost, 'PENDING',
                :amt, :qty, :blk, :ts)
            """), {
                "cid": st.session_state.customer_id,
                "name": st.session_state.customer_name,
                "side": side.upper(),
                "price": nav_price,
                "cost": fee,
                "amt": float(amount) if side == "Buy" else 0.0,
                "qty": float(quantity) if side == "Sell" else 0.0,
                "blk": float(amount) if side == "Buy" else 0.0,
                "ts": datetime.datetime.utcnow()
            })

        st.success("Request submitted successfully.")
        st.rerun()
    
    st.markdown("### Trade Requests (Pending)")

    if trade_df.empty:
        st.info("No pending trade requests.")
    else:
        st.dataframe(trade_df, use_container_width=True)

    # ======================
    # CASH REQUEST
    # ======================

    st.divider()
    st.subheader("Deposit / Withdrawal")

    action = st.selectbox("Select", ["Deposit", "Withdraw"])
    cash_amount = st.number_input(
        "Amount (VND)",
        min_value=0.0,
        step=1_000.0,
        format="%.0f"
    )

    cash_error = None
    can_submit_cash = False

    # ===== VALIDATION =====

    if cash_amount <= 0:
        cash_error = "Amount must be greater than 0."
        can_submit_cash = False
    elif cash_amount < 1_000:
        cash_error = "Minimum amount is 1,000 VND."
        can_submit_cash = False
    elif action == "Withdraw" and cash_amount > available_cash:
        cash_error = "Insufficient available balance."
        can_submit_cash = False
    else:
        can_submit_cash = True
        cash_error = None


    # ===== SHOW ERROR =====
    if cash_error is not None:
        st.error(cash_error)
    # ====================== SUBMIT CASH REQUEST =====
    if st.button("Submit Fund Request", disabled=not can_submit_cash):

        with engine.begin() as conn:

            # ===== WITHDRAW → BLOCK CASH =====
            if action == "Withdraw":

                result = conn.execute(text("""
                    UPDATE investors
                    SET 
                        available_cash = available_cash - :amt,
                        blocked_cash = blocked_cash + :amt
                    WHERE customer_id = :cid
                    AND available_cash >= :amt
                """), {
                    "amt": cash_amount,
                    "cid": st.session_state.customer_id
                })

                if result.rowcount == 0:
                    st.error("Insufficient balance.")
                    return

            # ===== INSERT =====
            conn.execute(text("""
                INSERT INTO cash_requests
                (customer_id, type, amount, status, blocked_amount, created_at)
                VALUES (:cid, :type, :amt, 'PENDING', :blk, :ts)
            """), {
                "cid": st.session_state.customer_id,
                "type": action.upper(),
                "amt": cash_amount,
                "blk": cash_amount if action == "Withdraw" else 0,
                "ts": datetime.datetime.utcnow()
            })

        st.success("Request submitted successfully")
        st.rerun()

    st.markdown("### Cash Requests (Pending)")

    if cash_df.empty:
        st.info("No pending cash requests.")
    else:
        st.dataframe(cash_df, use_container_width=True)
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

                    with engine.begin() as conn:

                        if r["side"] == "BUY":

                            execute_fundshare_trade(
                                customer_id=r["customer_id"],
                                side="BUY",
                                amount=r["amount"]
                            )

                            conn.execute(text("""
                                UPDATE investors
                                SET blocked_cash = blocked_cash - :amt
                                WHERE customer_id = :cid
                            """), {
                                "amt": r["blocked_amount"],
                                "cid": r["customer_id"]
                            })

                        else:

                            execute_fundshare_trade(
                                customer_id=r["customer_id"],
                                side="SELL",
                                quantity=r["quantity"]
                            )

                        updated = conn.execute(text("""
                            UPDATE fundshare_requests
                            SET status='SUCCESS'
                            WHERE id=:rid AND status='PENDING'
                        """), {"rid": r["id"]})

                        if updated.rowcount == 0:
                            st.warning("Already processed")
                            return

                    st.success("Approved")
                    st.rerun()

                if c2.button("Reject", key=f"reject_{r['id']}"):

                    with engine.begin() as conn:

                        if r["side"] == "BUY":

                            conn.execute(text("""
                                UPDATE investors
                                SET 
                                    available_cash = available_cash + :amt,
                                    blocked_cash = blocked_cash - :amt
                                WHERE customer_id = :cid
                            """), {
                                "amt": r["blocked_amount"],
                                "cid": r["customer_id"]
                            })

                        conn.execute(text("""
                            UPDATE fundshare_requests
                            SET status='REJECTED'
                            WHERE id=:rid AND status='PENDING'
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
                            SET 
                                current_cash = current_cash + :amt,
                                available_cash = available_cash + :amt
                            WHERE customer_id = :cid
                        """), {
                            "amt": r["amount"],
                            "cid": r["customer_id"]
                        })

                    else:

                        conn.execute(text("""
                            UPDATE investors
                            SET 
                                blocked_cash = blocked_cash - :amt,
                                current_cash = current_cash - :amt
                            WHERE customer_id = :cid
                        """), {
                            "amt": r["blocked_amount"],
                            "cid": r["customer_id"]
                        })

                    conn.execute(text("""
                        UPDATE cash_requests
                        SET status='SUCCESS'
                        WHERE id=:id AND status='PENDING'
                    """), {"id": r["id"]})

                st.success("Approved")
                st.rerun()

            if c2.button("Reject", key=f"cash_rej_{r['id']}"):

                with engine.begin() as conn:

                    if r["type"] == "WITHDRAW":

                        conn.execute(text("""
                            UPDATE investors
                            SET 
                                available_cash = available_cash + :amt,
                                blocked_cash = blocked_cash - :amt
                            WHERE customer_id = :cid
                        """), {
                            "amt": r["blocked_amount"],
                            "cid": r["customer_id"]
                        })

                    conn.execute(text("""
                        UPDATE cash_requests
                        SET status='REJECTED'
                        WHERE id=:id AND status='PENDING'
                    """), {"id": r["id"]})

                st.warning("Rejected")
                st.rerun()