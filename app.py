# ======================
# CONFIG
# ======================
import hashlib
from scripts.email_utils import send_reset_email
import streamlit as st
st.set_page_config(
    page_title="Fund Management System",
    layout="wide"
)
from scripts.supabase_client import supabase_admin
import secrets
import datetime as dt
from datetime import date
import datetime as dt
import pandas as pd
from sqlalchemy import text
import plotly.express as px
import plotly.graph_objects as go
from scripts.supabase_client import supabase
from scripts.db import (
    write_table,
    load_table,
    update_overall_snapshot,
    run_nav_pipeline,
    smart_dataframe
)
from scripts.db_engine import get_engine
from scripts.fundshare import (
    execute_fundshare_trade,
    get_latest_nav_per_unit,
    calculate_fundshare_fee
)
from scripts.information import (
    load_admin_information,
    load_investor_portfolio,
    load_investor_information
)
from scripts.pricing_yahoo import update_all_prices
from scripts.ui.nav_chart import render_nav_chart
from scripts.ui.nav_service import get_nav_df
from scripts.ui.allocation_pie import render_asset_allocation
from scripts.ui.relative_performance import render_relative_performance
from scripts.portfolio import (
    build_trade_record,
    update_portfolio
)

# PASSWORD RECOVERY MODE
# ==========================
params = st.query_params
reset_token = params.get("reset_token")

if reset_token:

    engine = get_engine()

    # Hash token để so sánh
    hashed_token = hashlib.sha256(reset_token.encode()).hexdigest()

    with engine.connect() as conn:
        record = conn.execute(text("""
            SELECT email
            FROM password_resets
            WHERE token = :token
            AND used = false
            AND expires_at > now()
        """), {"token": hashed_token}).mappings().fetchone()

    if not record:
        st.error("Invalid or expired link")
        st.stop()

    st.title("🔑 Reset Password")

    new_password = st.text_input("New password", type="password")
    confirm_password = st.text_input("Confirm password", type="password")

    if st.button("Update password"):

        if new_password != confirm_password:
            st.error("Passwords do not match")
            st.stop()

        # Lấy auth_user_id trực tiếp từ DB thay vì scan toàn bộ user
        with engine.connect() as conn:
            user_record = conn.execute(text("""
                SELECT auth_user_id
                FROM users
                WHERE email = :email
            """), {"email": record["email"]}).mappings().fetchone()

        if not user_record:
            st.error("User not found")
            st.stop()
        supabase_admin.auth.admin.update_user_by_id(
            str(user_record["auth_user_id"]),
            {"password": new_password}
        )
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE password_resets
                SET used = true
                WHERE token = :token
            """), {"token": hashed_token})

        st.success("Password changed successfully")
        st.query_params.clear()
        st.rerun()

    st.stop()
# AUTHENTICATION
# ======================
# ==========================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

import requests
import os
from dotenv import load_dotenv

load_dotenv()

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
if not st.session_state.logged_in:
    st.title("Authentication")


    tab1, tab2, tab3 = st.tabs(["Login", "Register", "Forgot password"])


    # ===== LOGIN =====
    with tab1:
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Login"):

            try:
                engine = get_engine()

                # 1️⃣ Lấy email từ username
                with engine.connect() as conn:
                    user_record = conn.execute(
                        text("""
                            SELECT email, username, role, customer_id
                            FROM users
                            WHERE username = :username
                        """),
                        {"username": username}
                    ).mappings().fetchone()

                if not user_record:
                    st.error("Invalid username or password")
                    st.stop()

                email = user_record["email"]

                # 2️⃣ Login Supabase
                res = supabase.auth.sign_in_with_password({
                    "email": email,
                    "password": password
                })

                if res.user is None:
                    st.error("Sai tài khoản hoặc mật khẩu")
                    st.stop()

                # 3️⃣ Set session
                st.session_state.logged_in = True
                st.session_state.user = dict(user_record)
                st.session_state.username = user_record["username"]
                st.session_state.role = user_record["role"]
                st.session_state.customer_id = user_record["customer_id"]
                st.session_state.is_admin = (user_record["role"] == "admin")

                st.success("Login successful")
                st.rerun()

            except Exception:
                # 🔒 Không lộ lỗi thật
                st.error("Sai tài khoản hoặc mật khẩu")
                st.stop()

    # ===== REGISTER =====
    with tab2:
        with st.form("register_form"):
            username = st.text_input("Username")
            display_name = st.text_input("Display name")
            email = st.text_input("Email")
            cccd = st.text_input("CCCD / MST")

            today = date.today()
            min_dob = date(today.year - 100, 1, 1)
            max_dob = date(today.year - 18, 12, 31)  # >=18 tuổi

            dob = st.date_input(
                "Date of birth",
                min_value=min_dob,
                max_value=max_dob
            )
            phone = st.text_input("Phone")
            address = st.text_input("Address")
            bank = st.text_input("Bank account")
            role = st.selectbox("Role", ["investor", "organise"])
            password = st.text_input("Password", type="password")

            submitted = st.form_submit_button("Register")

        if submitted:

            # 1️⃣ Tạo user trong Supabase Auth
            res = supabase.auth.sign_up({
                "email": email,
                "password": password
            })

            if res.user is None:
                st.error("Registration failed")
                st.stop()

            auth_user_id = res.user.id

            try:
                engine = get_engine()

                with engine.begin() as conn:

                    prefix = "CN" if role == "investor" else "TC"

                    last_id = conn.execute(
                        text("""
                            SELECT MAX(customer_id)
                            FROM investors
                            WHERE customer_id LIKE :p
                        """),
                        {"p": f"{prefix}%"}
                    ).scalar()

                    if last_id:
                        num = int(last_id.replace(prefix, ""))
                        customer_id = f"{prefix}{num + 1:02d}"
                    else:
                        customer_id = f"{prefix}01"

                    # insert users
                    conn.execute(text("""
                        INSERT INTO users (
                            username,
                            customer_id,
                            display_name,
                            email,
                            phone,
                            address,
                            bank_account,
                            role,
                            created_at,
                            auth_user_id
                        )
                        VALUES (
                            :username,
                            :customer_id,
                            :display_name,
                            :email,
                            :phone,
                            :address,
                            :bank_account,
                            :role,
                            now(),
                            :auth_user_id
                        )
                    """), {
                        "username": username,
                        "customer_id": customer_id,
                        "display_name": display_name,
                        "email": email,
                        "phone": phone,
                        "address": address,
                        "bank_account": bank,
                        "role": role,
                        "auth_user_id": auth_user_id
                    })

                    conn.execute(text("""
                        INSERT INTO investors (
                            customer_id,
                            customer_name,
                            status,
                            open_account_date,
                            identity_number,
                            dob,
                            phone,
                            email,
                            address,
                            capital,
                            nos,
                            bank_account
                        )
                        VALUES (
                            :customer_id,
                            :customer_name,
                            'Đang đầu tư',
                            CURRENT_DATE,
                            :identity_number,
                            :dob,
                            :phone,
                            :email,
                            :address,
                            0,
                            0,
                            :bank_account
                        )
                    """), {
                        "customer_id": customer_id,
                        "customer_name": display_name,
                        "identity_number": cccd,
                        "dob": dob.isoformat(),
                        "phone": phone,
                        "email": email,
                        "address": address,
                        "bank_account": bank
                    })

                st.success("Registration successful")

            except Exception as e:
                # nếu DB fail → xóa user bên Supabase để tránh lệch dữ liệu
                supabase_admin.auth.admin.delete_user(auth_user_id)
                st.error(f"Lỗi DB: {e}")
    # ===== FORGOT PASSWORD =====
    with tab3:
        with st.form("forgot_form"):
            email = st.text_input("Email")
            submitted = st.form_submit_button("Send reset link")

        if submitted:

            engine = get_engine()

            # Check email tồn tại (tránh email enumeration)
            with engine.connect() as conn:
                exists = conn.execute(
                    text("SELECT 1 FROM users WHERE email = :email"),
                    {"email": email}
                ).fetchone()

            # Luôn trả success để không lộ thông tin
            if not exists:
                st.success("Password reset link has been sent  .")
                st.stop()

            token = secrets.token_urlsafe(32)
            hashed_token = hashlib.sha256(token.encode()).hexdigest()
            expires = dt.datetime.utcnow() + dt.timedelta(minutes=15)

            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO password_resets (email, token, expires_at, used)
                    VALUES (:email, :token, :expires, false)
                """), {
                    "email": email,
                    "token": hashed_token,
                    "expires": expires
                })

            reset_link = f"https://fundmanagement.streamlit.app?reset_token={token}"

            try:
                send_reset_email(email, reset_link)
                st.success("Password reset link has been sent.")
            except Exception as e:
                st.error("Failed to send email")
                st.write(str(e))
    st.stop()


# CUSTOM CSS
# ======================


# SIDEBAR
# ======================
st.sidebar.title("Navigation")
role = st.session_state.get("role")

PAGE_MAP = {}


  # ===== ADMIN =====
if role == "admin":
    PAGE_MAP = {
        "Overall": "Overall",
        "Portfolio": "Update_price",
        "Cash Management": "Cash",
        "Pending Requests": "Exchange_FundShare",
        "Content Management": "Content_Management",
        "Information": "Information",
    }
else:
    PAGE_MAP = {
        "Dashboard": "Overall_investor",
        "Transactions": "Exchange_FundShare",
        "Investor Overview": "Information",
    }


selected_label = st.sidebar.selectbox(
    "Go to",
    list(PAGE_MAP.keys())
)
page = PAGE_MAP[selected_label]
with st.sidebar:
    st.markdown("---")
    if st.button("Log out"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
            supabase.auth.sign_out()
        st.rerun()


# ======================
# PAGE: OVERALL
# ======================
if page == "Overall":
    df = load_table("overall_snapshot")
    df_nav = load_table("nav")
    df_costs = load_table("costs")
    df_ts= pd.to_datetime(df["snapshot_time"])
    df_nav["nav_date"] = pd.to_datetime(df_nav["nav_date"])
    df_nav = df_nav.sort_values("nav_date")
    # ---------- TABLE ----------
    st.subheader("Overall")
    smart_dataframe(
        df,
        "overall_snapshot",
        use_container_width=True,
        hide_index=True
    )
    if st.button("Update Overall Snapshot"):
            update_overall_snapshot()
            st.success("Overall snapshot updated successfully")
            st.rerun()
    st.subheader("Costs")
    smart_dataframe(
        df_costs,
        "costs",
        use_container_width=True,
        hide_index=True
    )
    st.subheader("NAV")
    smart_dataframe(
        df_nav,
        "nav",
        use_container_width=True,
        hide_index=True
    )
   
    df_nav = get_nav_df()
    if st.button("Run NAV Daily Process"):
        engine = get_engine()
        logs, result, error = run_nav_pipeline(engine)

        if error:
            st.error(error)
        else:
            st.success("NAV finalized")
            st.rerun()

    st.subheader("NAV per Unit Over Time")
    fig = render_nav_chart(df_nav)


    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False}
    )

    render_asset_allocation(df)
    st.subheader("Relative Performance vs Total (%)")
    fig_perf = render_relative_performance(df)
    st.plotly_chart(fig_perf, use_container_width=True, config={"displayModeBar": False})

# ===== BAR: RETURNS =====
if page == "Update_price":
    st.header("Portfolio")
    df_port = load_table("portfolio")
    smart_dataframe(
        df_port,
        "portfolio",
        use_container_width=True,
        hide_index=True
    )
    df_port["ticker"] = df_port["ticker"].str.upper()

# 👉 LẤY TICKER TỪ PORTFOLIO
   

    engine = get_engine()
    if st.button("Update Market Prices (Yahoo)"):
        with st.spinner("Fetching prices from Yahoo Finance..."):
           update_all_prices(engine)


        st.success(f"Updated stock prices")
        st.rerun()

    df_port["ticker"] = df_port["ticker"].str.upper()

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

    # ======================
    if submitted:
        error = None

        # 0️⃣ empty ticker
        if ticker == "":
            error = "Ticker cannot be empty"

        # 1️⃣ SELL phải tồn tại ticker
        elif side == "SELL" and ticker not in portfolio_map:
            error = f"Cannot SELL: {ticker} not found in portfolio"

        # 2️⃣ SELL không được vượt quantity
        elif side == "SELL":
            max_qty = portfolio_map.get(ticker, 0)
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
                # 1️⃣ insert trade
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

            # rerun CHỈ SAU KHI INSERT XONG
            st.rerun(after=5) 

    if st.button("Update Portfolio"):
        engine = get_engine()
        update_portfolio(engine)
        st.success("Portfolio updated successfully")
        st.rerun()

# ======================
# PAGE: CASH MANAGEMENT
# ======================
if page == "Cash":
    st.header("💹Cash")
    df_tradestore = load_table("trades")
    df_exchange = load_table("fundshare_trades")
    st.subheader("History of cash movements")
    df_cash = load_table("cash")

    if not df_cash.empty:
        df_cash["created_at"] = pd.to_datetime(df_cash["created_at"])
        df_cash = df_cash.sort_values("created_at")

        y_min = df_cash["cash_end"].min()
        y_max = df_cash["cash_end"].max()
        padding = max((y_max - y_min) * 0.2, y_max * 0.02)

        fig_cash = go.Figure()

        fig_cash.add_trace(
            go.Scatter(
                x=df_cash["created_at"],
                y=df_cash["cash_end"],
                mode="lines",
                line=dict(
                    color="#4CC9F0",
                    width=3,
                    shape="spline"
                ),
                fill="tozeroy",
                fillcolor="rgba(76,201,240,0.08)",
                hovertemplate=(
                    "<b>%{x|%d %b %Y}</b><br>"
                    "Cash: %{y:,.0f} VND"
                    "<extra></extra>"
                ),
            )
        )

        fig_cash.update_layout(
            height=420,
            margin=dict(l=20, r=20, t=20, b=20),
            plot_bgcolor="#0E1A2B",
            paper_bgcolor="#0E1A2B",
            hovermode="x unified",
            showlegend=False,
            xaxis=dict(
                showgrid=False,
                tickformat="%d/%m",
                zeroline=False,
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor="rgba(255,255,255,0.05)",
                zeroline=False,
                tickformat=",.0f",
                range=[y_min - padding, y_max + padding],
            ),
            font=dict(color="#EAEAEA", size=13),
        )

        st.subheader("Cash Balance Over Time")
        st.plotly_chart(fig_cash, use_container_width=True)

    else:
        st.info("No cash snapshot available yet.")

    df_tradestore = df_tradestore.copy()
    df_tradestore["trade_date"] = pd.to_datetime(df_tradestore["trade_date"])
    df_tradestore = df_tradestore.sort_values("trade_date", ascending=False)

    df_tradestore_display = df_tradestore[
        ["trade_id", "trade_date", "cash_flow", "ticker", "side", "quantity", "price"]
    ]

    st.subheader("Trade Store")
    smart_dataframe(
        df_tradestore_display,
        "trades",
        use_container_width=True,
        hide_index=True
    )


    # ---- FUND SHARE TRADES ----
    df_exchange = df_exchange.copy()
    df_exchange["trade_date"] = pd.to_datetime(df_exchange["trade_date"])
    df_exchange = df_exchange.sort_values("trade_date", ascending=False)

    df_exchange_display = df_exchange[
        ["trade_date", "customer_id", "cash_flow", "side"]
    ]

    st.subheader("Fund Share Trades")
    smart_dataframe(
        df_exchange_display,
        "fundshare_trades",
        use_container_width=True,
        hide_index=True
    )
        

# EXCHANGE FUND SHARE
# ======================
elif page == "Exchange_FundShare":




    st.header("Exchange Fund Share")
    engine = get_engine()
    with engine.connect() as conn:
        setting = conn.execute(text("""
            SELECT bank_info
            FROM fund_setting
            LIMIT 1
        """)).mappings().fetchone()

    is_admin = st.session_state.get("is_admin", False)






    # ======================
    # TRADING BLOCK
    # ======================
    if not is_admin:
        can_submit = False
        fee = 0.0
        amount = 0.0
        quantity = 0.0

        portfolio = load_investor_portfolio(st.session_state.customer_id)

        if portfolio is None:
            st.warning("Failed to load portfolio.")
            st.stop()

        current_cash = float(portfolio.get("current_cash", 0) or 0)
        current_units = float(portfolio.get("nos", 0) or 0)
        nav_price = float(get_latest_nav_per_unit() or 0)

        if nav_price <= 0:
            st.error("NAV is not available")
            st.stop()

        st.subheader("Fund Unit Transaction")

        col1, col2, col3 = st.columns(3)
        col1.metric("NAV per Unit", f"{nav_price:,.2f}")
        col2.metric("Available Balance", f"{current_cash:,.0f}")
        col3.metric("Units Held", f"{current_units:,.4f}")

        side = st.selectbox("Side", ["Buy", "Sell"])

        # ======================
        # INPUT
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

            error = None

            if amount <= 0:
                error = "Amount must be greater than 0"
            elif amount > current_cash:
                error = "Exceeds available balance"
            elif net_value <= 0:
                error = "Invalid fee"
            else:
                can_submit = True
                st.write(f"Number of Units: **{units:,.0f} Fund Units**")
                st.write(f"Transaction Fee: **{fee:,.0f} VND**")
                st.write(f"Net Invested Amount: **{net_value:,.0f} VND**")

        else:
            quantity = st.number_input(
                "Sell Quantity",
                min_value=0.0,
                max_value=current_units,
                step=0.1,
                format="%.4f"
            )

            gross_value = quantity * nav_price
            fee = float(calculate_fundshare_fee("Sell", gross_value) or 0)
            net_value = gross_value - fee

            error = None

            if quantity <= 0:
                error = "Quantity must be greater than 0"
            elif quantity > current_units:
                error = "Exceeds units held"
            elif net_value <= 0:
                error = "Invalid sell amount"

            # ======================
            # SUMMARY
            # ======================

            st.divider()

            colA, colB = st.columns(2)
            colA.write(f"Transaction Fee: **{fee:,.0f} VND**")
            colB.write(f"Net Proceeds: **{max(net_value,0):,.0f} VND**")

            if error:
                st.error(error)
            else:
                can_submit = True

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

            df = pd.DataFrame([request_data]).astype({
                "price": "float64",
                "cost": "float64",
                "amount": "float64",
                "quantity": "float64"
            })

            write_table(df, "fundshare_requests")

            st.success("Request submitted successfully.")
            st.rerun()
        # ======================
        # CASH REQUESTS
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
                st.stop()

            if action == "Withdraw" and cash_amount > current_cash:
                st.error("Insufficient balance.")
                st.stop()

            df = pd.DataFrame([{
                "customer_id": st.session_state.customer_id,
                "type": action.upper(),
                "amount": cash_amount,
                "status": "PENDING"
            }])

            write_table(df, "cash_requests")

            st.success("Request submitted successfully")
            st.rerun()

        # ===== Bank Transfer Info Box =====
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

    # ======================
    # ADMIN
    # ======================


    # ===== Duyệt CCQ =====
    else:
        st.subheader("Pending Requests")




        df_req = pd.read_sql(
            "SELECT * FROM fundshare_requests WHERE status='PENDING'",
            engine
        )




        if df_req.empty:
            st.info("No pending requests")
        else:
            for idx, r in df_req.iterrows():
                with st.expander(f"{r['customer_id']} – {r['side']}"):
                    st.write(r)




                    col1, col2 = st.columns(2)




                    # ===== APPROVE =====
                    with col1:
                        if st.button("Approve", key=f"approve_{r['id']}"):
                            try:
                                execute_fundshare_trade(
                                    customer_id=r["customer_id"],
                                    side=r["side"],
                                    amount=r["amount"] if r["side"] == "BUY" else None,
                                    quantity=r["quantity"] if r["side"] == "SELL" else None
                                )
                            except Exception as e:
                                st.error(f"Error while approving transaction: {e}")
                                st.stop()




                            with engine.begin() as conn:
                                conn.execute(
                                    text("""
                                        UPDATE fundshare_requests
                                        SET status='SUCCESS'
                                        WHERE id=:rid
                                    """),
                                    {"rid": r["id"]}
                                )




                            st.success("Approved")
                            st.rerun()




                    # ===== REJECT =====
                    with col2:
                        if st.button("Reject", key=f"reject_{r['id']}"):
                            with engine.begin() as conn:
                                conn.execute(
                                    text("""
                                        UPDATE fundshare_requests
                                        SET status='REJECTED'
                                        WHERE id=:rid
                                    """),
                                    {"rid": r["id"]}
                                )




                            st.warning("Request rejected")
                            st.rerun()
 # ===== Duyệt tiền =====


        st.divider()
        st.subheader("Cash Requests")


        engine = get_engine()


        df_req = pd.read_sql(
            "SELECT * FROM cash_requests WHERE status='PENDING'",
            engine
        )


        for idx, r in df_req.iterrows():
            with st.expander(f"{r['customer_id']} – {r['type']} – {r['amount']:,.0f}"):


                col1, col2 = st.columns(2)


                # ===== APPROVE =====
                with col1:
                    if st.button("Approve", key=f"cash_app_{r['id']}"):


                        if r["type"] == "DEPOSIT":
                            query = """
                            UPDATE investors
                            SET current_cash = current_cash + :amount
                            WHERE customer_id = :cid
                            """
                        else:
                            query = """
                            UPDATE investors
                            SET current_cash = current_cash - :amount
                            WHERE customer_id = :cid
                            """


                        with engine.begin() as conn:
                            conn.execute(text(query), {
                                "amount": r["amount"],
                                "cid": r["customer_id"]
                            })


                            conn.execute(text("""
                                UPDATE cash_requests
                                SET status='SUCCESS'
                                WHERE id=:id
                            """), {"id": r["id"]})


                        st.success("Approved")
                        st.rerun()


                # ===== REJECT =====
                with col2:
                    if st.button("Reject", key=f"cash_rej_{r['id']}"):
                        with engine.begin() as conn:
                            conn.execute(text("""
                                UPDATE cash_requests
                                SET status='REJECTED'
                                WHERE id=:id
                            """), {"id": r["id"]})


                        st.warning("Rejected")
                        st.rerun()


# ======================
# INFORMATION
# ======================
from scripts.information import (
    load_admin_information,
    load_investor_portfolio
)




if page == "Information":
    role = st.session_state.role




    # ======================
    # ADMIN VIEW
    # ======================
    if role == "admin":
        st.header("Fund Information")
        info = load_admin_information()
        col1, col2, col3 = st.columns(3)

        col1.metric("Cash Balance", f"{info['cash']:,.0f}")
        col2.metric("Total Fund Shares", f"{info['total_ccq']:,.2f}")
        col3.metric("Fund Return", f"{info['interest']*100:.2f}%")

        st.divider()


        st.subheader("Fund Value")
        st.dataframe(pd.DataFrame([{
            "Invested Value": info["invested_value"],
            "Market Value": info["market_value"],
            "Profit": info["market_value"] - info["invested_value"]
        }]), use_container_width=True)




        st.divider()




        st.subheader("👥 Investors List")
        smart_dataframe(info["investors"], "investors", use_container_width=True)





# INVESTOR / ORGANISE VIEW
# ======================

    else:
        st.header("👤 My Information")

    customer_id = st.session_state.customer_id
    info = load_investor_information(customer_id)

    if info is None:
        st.warning("Investor information not found.")
        st.stop()

    # ======================
    # THÔNG TIN CÁ NHÂN
    # ======================

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Account Information")
        st.write(f"**Customer ID:** {info.get('customer_id','-')}")
        st.write(f"**Full Name:** {info.get('customer_name','-')}")
        st.write(f"**Account Opening Date:** {info.get('open_account_date','-')}")
        st.write(f"**Status:** {info.get('status','-')}")

    with col2:
        st.markdown("### Contact")
        st.write(f"**Email:** {info.get('email','-')}")
        st.write(f"**Phone Number:** {info.get('phone','-')}")
        st.write(f"**Address:** {info.get('address','-')}")
        st.write(f"**Bank Account:** {info.get('bank_account','-')}")

    st.divider()

    # ======================
    # PORTFOLIO
    # ======================

    data = load_investor_portfolio(customer_id)

    if data is None:
        st.warning("No portfolio data available.")
        st.stop()

    st.header("📦 My Portfolio")
    st.caption(f"👤 {data['customer_name']}")

    # ======= HÀNG 1 =======
    col1, col2, col3 = st.columns(3)

    col1.metric("Fund Units Held", f"{data['nos']:,.2f}")
    col2.metric("Current NAV per Unit", f"{data['nav_per_unit']:,.2f}")
    col3.metric("Market Value", f"{data['market_value']:,.0f}")

    # ======= HÀNG 2 =======
    col4, col5, col6 = st.columns(3)

    col4.metric("Remaining Cost Basis", f"{data['cost_basis_remaining']:,.0f}")
    col5.metric(
        "Unrealized Profit/Loss",
        f"{data['unrealized_pnl']:,.0f}",
        delta=f"{data['roi']:,.2f}%"
    )
    col6.metric("Available Balance", f"{data['current_cash']:,.0f}")

    # ======= TỔNG TÀI SẢN =======
    st.metric(
        "Total Assets",
        f"{data['total_assets']:,.0f}"
    )

    st.divider()

    # ======================
    # LỊCH SỬ GIAO DỊCH
    # ======================

    st.subheader("Fund Unit Transaction History")

    if not data["trades"].empty:
        smart_dataframe(
            data["trades"],
            "fundshare_trades",
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No fund unit transactions yet")

    st.subheader("Deposit & Withdrawal History")

    if "cash_requests" in data and not data["cash_requests"].empty:
        smart_dataframe(
            data["cash_requests"],
            "cash_requests",
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No deposit or withdrawal transactions yet.")


# ======================
# MY PORTFOLIO
# ======================
from scripts.information import load_investor_portfolio
   
  # ===== INVESTOR / ORGANISE =====




if page == "Overall_investor":
    engine = get_engine()
    with engine.connect() as conn:
        setting = conn.execute(text("""
            SELECT intro_context
            FROM fund_setting
            LIMIT 1
        """)).mappings().fetchone()

    if setting and setting["intro_context"]:
        st.markdown("## Fund Introduction")
        st.info(setting["intro_context"])
        st.divider()

    df = load_table("overall_snapshot")
    df_port = load_table("portfolio")
    df_nav = load_table("nav")
    df_ts= pd.to_datetime(df["snapshot_time"])
    df_nav["nav_date"] = pd.to_datetime(df_nav["nav_date"])
    df_nav = df_nav.sort_values("nav_date")
    # ---------- TABLE ----------
    st.subheader("Portfolio Summary")
    smart_dataframe(
        df,
        "overall_snapshot",
        use_container_width=True,
        hide_index=True
    )
    # =========================
    # PORTFOLIO DISPLAY (VIEW ONLY)
    # =========================

    cols_to_show = [
        "ticker",
        "asset_name",
        "asset_type",
        "current_weight",
        "target_weight"
    ]

    # Lọc cột an toàn
    df_display = df_port[[c for c in cols_to_show if c in df_port.columns]].copy()

    # Nhân 100 và format %
    if "current_weight" in df_display.columns:
        df_display["current_weight"] = (
            df_display["current_weight"].astype(float) * 100
        ).map(lambda x: f"{x:.2f}%")

    if "target_weight" in df_display.columns:
        df_display["target_weight"] = (
            df_display["target_weight"].astype(float) * 100
        ).map(lambda x: f"{x:.2f}%")

    smart_dataframe(
        df_display,
        "portfolio_view",
        use_container_width=True,
        hide_index=True
    )
    st.subheader("NAV per Unit Over Time")
    fig = render_nav_chart(df_nav)




    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False}
    )


# ---------- CHARTS ----------

    render_asset_allocation(df)


    st.subheader("Relative Performance vs Total (%)")
    fig_perf = render_relative_performance(df)
    st.plotly_chart(fig_perf, use_container_width=True, config={"displayModeBar": False})

# ======================
# CONTENT MANAGEMENT
# ======================
if page == "Content_Management" and role == "admin":

    st.header("🛠 Fund Content Management")

    engine = get_engine()

    with engine.connect() as conn:
        setting = conn.execute(text("""
            SELECT intro_context, bank_info
            FROM fund_setting
            LIMIT 1
        """)).mappings().fetchone()

    intro_default = setting["intro_context"] if setting else ""
    bank_default = setting["bank_info"] if setting else ""

    intro = st.text_area("Fund Introduction", value=intro_default, height=200)
    bank = st.text_area("Bank Transfer Information", value=bank_default, height=150)

    if st.button("Save Changes"):

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE fund_setting
                SET intro_context = :intro,
                    bank_info = :bank,
                    update_at = now()
                WHERE id = 1
            """), {
                "intro": intro,
                "bank": bank
            })

        st.success("Content updated successfully")
        st.rerun()