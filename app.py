# ======================
# CONFIG
# ======================
import hashlib

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
    insert_empty_portfolio_row,
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
        st.error("Link không hợp lệ hoặc đã hết hạn")
        st.stop()

    st.title("🔑 Reset Password")

    new_password = st.text_input("New password", type="password")
    confirm_password = st.text_input("Confirm password", type="password")

    if st.button("Update password"):

        if new_password != confirm_password:
            st.error("Mật khẩu không khớp")
            st.stop()

        # Lấy auth_user_id trực tiếp từ DB thay vì scan toàn bộ user
        with engine.connect() as conn:
            user_record = conn.execute(text("""
                SELECT auth_user_id
                FROM users
                WHERE email = :email
            """), {"email": record["email"]}).mappings().fetchone()

        if not user_record:
            st.error("User không tồn tại")
            st.stop()

        supabase_admin.auth.admin.update_user_by_id(
            user_record["auth_user_id"],
            {"password": new_password}
        )

        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE password_resets
                SET used = true
                WHERE token = :token
            """), {"token": hashed_token})

        st.success("Đổi mật khẩu thành công")
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
    st.title("🔐 Authentication")


    tab1, tab2, tab3 = st.tabs(["Login", "Register", "Forgot password"])


    # ===== LOGIN =====
    with tab1:
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Login"):

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
                st.error("Sai tài khoản hoặc mật khẩu")
                st.stop()

            email = user_record["email"]

            # 2️⃣ Login bằng email với Supabase
            res = supabase.auth.sign_in_with_password({
                "email": email,
                "password": password
            })

            if res.user is None:
                st.error("Sai tài khoản hoặc mật khẩu")
                st.stop()

            # 3️⃣ Set session state
            st.session_state.logged_in = True
            st.session_state.user = dict(user_record)
            st.session_state.username = user_record["username"]
            st.session_state.role = user_record["role"]
            st.session_state.customer_id = user_record["customer_id"]
            st.session_state.is_admin = (user_record["role"] == "admin")

            st.success("Đăng nhập thành công")
            st.rerun()


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
                st.error("Đăng ký thất bại (Auth)")
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

                st.success("Đăng ký thành công")

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
                st.success("Nếu email tồn tại, chúng tôi đã gửi link reset.")
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

            response = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "from": "onboarding@resend.dev",
                    "to": email,
                    "subject": "Reset your password",
                    "html": f"""
                    <h2>Reset Password</h2>
                    <p>Click link below:</p>
                    <a href="{reset_link}">{reset_link}</a>
                    <p>This link expires in 15 minutes.</p>
                    """
                }
            )

            if response.status_code in (200, 201):
                st.success("Nếu email tồn tại, chúng tôi đã gửi link reset.")
            else:
                st.error("Gửi email thất bại")
                st.write(response.text)
    st.stop()


# CUSTOM CSS
# ======================


# SIDEBAR
# ======================
st.sidebar.title("📁 Navigation")
role = st.session_state.get("role")

PAGE_MAP = {}


  # ===== ADMIN =====
if role == "admin":
    PAGE_MAP = {
        "🏠 Overall": "Overall",
        "💹 Portfolio": "Update_price",
        "💹Cash Management": "Cash",
        "📑 Pending Requests": "Exchange_FundShare",
        "🧾 Information": "Information",
    }
else:
    PAGE_MAP = {
        "🏠 Fund Overview": "Overall_investor",
        "🔄 Buy / Sell CCQ": "Exchange_FundShare",
        "🧾 My Information": "Information",
    }


selected_label = st.sidebar.selectbox(
    "Go to",
    list(PAGE_MAP.keys())
)
page = PAGE_MAP[selected_label]
with st.sidebar:
    st.markdown("---")
    if st.button("🚪 Log out"):
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
    st.subheader("📋 Overall")
    smart_dataframe(
        df,
        "overall_snapshot",
        use_container_width=True,
        hide_index=True
    )
    if st.button("📊 Update Overall Snapshot"):
            update_overall_snapshot()
            st.success("✅ Overall snapshot updated successfully")
            st.rerun()
    st.subheader("📋 Costs")
    smart_dataframe(
        df_costs,
        "costs",
        use_container_width=True,
        hide_index=True
    )
    st.subheader("📈NAV")
    smart_dataframe(
        df_nav,
        "nav",
        use_container_width=True,
        hide_index=True
    )
   
    df_nav = get_nav_df()
    if st.button("🚀 Run NAV Daily Process"):
        engine = get_engine()
        logs, result, error = run_nav_pipeline(engine)

        if error:
            st.error(error)
        else:
            st.success("NAV finalized")
            st.rerun()

    st.subheader("📈 NAV / CCQ over time")
    fig = render_nav_chart(df_nav)


    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False}
    )

    render_asset_allocation(df)
    st.subheader("📈 Relative Performance vs Total (%)")
    fig_perf = render_relative_performance(df)
    st.plotly_chart(fig_perf, use_container_width=True)

# ===== BAR: RETURNS =====
if page == "Update_price":
    st.header("💹 Portfolio")
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
    if st.button("🔄 Update Market Prices (Yahoo)"):
        with st.spinner("Fetching prices from Yahoo Finance..."):
           update_all_prices(engine)


        st.success(f"✅ Updated stock prices")
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
            error = f"❌ Cannot SELL: {ticker} not found in portfolio"

        # 2️⃣ SELL không được vượt quantity
        elif side == "SELL":
            max_qty = portfolio_map.get(ticker, 0)
            if quantity > max_qty:
                error = (
                    f"❌ Cannot SELL {quantity} units of {ticker}. "
                    f"Available: {max_qty}"
                )

        # 3️⃣ BUY: auto-add ticker nếu chưa có
        elif side == "BUY" and ticker not in portfolio_map:
            insert_empty_portfolio_row(engine, ticker, price)
            st.info(f"➕ Added new ticker {ticker} to portfolio")

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

                # 2️⃣ apply cash flow
                conn.execute(
                    text("""
                        UPDATE portfolio
                        SET net_value = net_value + :cash_flow
                        WHERE asset_type = 'Cash'
                        OR ticker = 'YTM'
                    """),
                    {"cash_flow": trade["cash_flow"]}
                )

            st.success("✅ Trade executed successfully")
            st.dataframe(df_trade_new)

            # 🔁 rerun CHỈ SAU KHI INSERT XONG
            st.rerun()

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

        st.subheader("📈 Cash Balance Over Time")
        st.plotly_chart(fig_cash, use_container_width=True)

    else:
        st.info("No cash snapshot available yet.")

    df_tradestore = df_tradestore.copy()
    df_tradestore["trade_date"] = pd.to_datetime(df_tradestore["trade_date"])
    df_tradestore = df_tradestore.sort_values("trade_date", ascending=False)

    df_tradestore_display = df_tradestore[
        ["trade_id", "trade_date", "cash_flow"]
    ]

    st.subheader("📋 Trade Store")
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
        ["trade_date", "customer_id", "cash_flow"]
    ]

    st.subheader("📋 Fund Share Trades")
    smart_dataframe(
        df_exchange_display,
        "fundshare_trades",
        use_container_width=True,
        hide_index=True
    )
        

# EXCHANGE FUND SHARE
# ======================
elif page == "Exchange_FundShare":




    st.header("🔄 Exchange Fund Share")
    engine = get_engine()
    is_admin = st.session_state.get("is_admin", False)




    # ======================
    # INVESTOR / ORGANISE
    # ======================
    if not is_admin:
        side = st.selectbox("Side", ["Buy", "Sell"])




        nav_price = get_latest_nav_per_unit()




        if side == "Buy":
            amount = st.number_input(
                "Investment Amount (VND)",
                min_value=1_000_000.0
            )




            fee = calculate_fundshare_fee("Buy", amount)
            net_amount = amount - fee
            units = net_amount / nav_price




            st.info(f"💰 Giá CCQ: {nav_price:,.2f}")
            st.info(f"💸 Phí giao dịch: {fee:,.0f}")
            st.info(f"📥 Góp vốn thực tế: {net_amount:,.0f}")
            st.info(f"📦 CCQ nhận được: {units:,.4f}")




        else:  # SELL
            quantity = st.number_input(
                "Units to Sell",
                min_value=0.0001
            )




            gross_amount = quantity * nav_price
            fee = calculate_fundshare_fee("Sell", gross_amount)
            net_amount = gross_amount - fee




            st.info(f"💰 Giá CCQ: {nav_price:,.2f}")
            st.info(f"📤 Giá trị bán: {gross_amount:,.0f}")
            st.info(f"💸 Phí giao dịch: {fee:,.0f}")
            st.info(f"💵 Tiền nhận: {net_amount:,.0f}")




        # ✅ NÚT GỬI REQUEST CCQ
        if st.button("📨 Gửi yêu cầu"):
            df = pd.DataFrame([{
                "customer_id": st.session_state.customer_id,
                "side": side.upper(),
                "amount": amount if side == "Buy" else None,
                "quantity": quantity if side == "Sell" else None,
                "price": nav_price,
                "cost": fee,
                "status": "PENDING"
            }])




            write_table(df, "fundshare_requests")
            st.success("✅ Đã gửi yêu cầu cho Admin duyệt")
            st.rerun()


    # ✅ NÚT GỬI REQUEST TIỀN
        st.divider()
        st.subheader("💸 Nạp / Rút tiền")


        action = st.selectbox("Chọn", ["Deposit", "Withdraw"])
        amount = st.number_input("Số tiền", min_value=0.0)


        if st.button("📨 Gửi yêu cầu tiền"):
            df = pd.DataFrame([{
                "customer_id": st.session_state.customer_id,
                "type": action.upper(),
                "amount": amount,
                "status": "PENDING"
            }])


            write_table(df, "cash_requests")
            st.success("✅ Đã gửi yêu cầu")
            st.rerun()




    # ======================
    # ADMIN
    # ======================


    # ===== Duyệt CCQ =====
    else:
        st.subheader("📑 Pending Requests")




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
                        if st.button("✅ Approve", key=f"approve_{r['id']}"):
                            try:
                                execute_fundshare_trade(
                                    customer_id=r["customer_id"],
                                    side=r["side"],
                                    amount=r["amount"] if r["side"] == "BUY" else None,
                                    quantity=r["quantity"] if r["side"] == "SELL" else None
                                )
                            except Exception as e:
                                st.error(f"❌ Lỗi khi duyệt giao dịch: {e}")
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




                            st.success("✅ Approved")
                            st.rerun()




                    # ===== REJECT =====
                    with col2:
                        if st.button("❌ Reject", key=f"reject_{r['id']}"):
                            with engine.begin() as conn:
                                conn.execute(
                                    text("""
                                        UPDATE fundshare_requests
                                        SET status='REJECTED'
                                        WHERE id=:rid
                                    """),
                                    {"rid": r["id"]}
                                )




                            st.warning("❌ Request rejected")
                            st.rerun()
 # ===== Duyệt tiền =====


        st.divider()
        st.subheader("💰 Cash Requests")


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
                    if st.button("✅ Approve", key=f"cash_app_{r['id']}"):


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
                    if st.button("❌ Reject", key=f"cash_rej_{r['id']}"):
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
        st.header("🧾 Fund Information (Admin)")
        info = load_admin_information()
        col1, col2, col3 = st.columns(3)

        col1.metric("💰 Cash Balance", f"{info['cash']:,.0f}")
        col2.metric("📦 Total Fund Shares", f"{info['total_ccq']:,.2f}")
        col3.metric("📈 Fund Return", f"{info['interest']*100:.2f}%")

        st.divider()


        st.subheader("📊 Fund Value")
        st.dataframe(pd.DataFrame([{
            "Invested Value": info["invested_value"],
            "Market Value": info["market_value"],
            "Profit": info["market_value"] - info["invested_value"]
        }]), use_container_width=True)




        st.divider()




        st.subheader("👥 Investors List")
        smart_dataframe(info["investors"], "investors", use_container_width=True)




    # ======================
    # INVESTOR / ORGANISE VIEW
    # ======================
    else:
        st.header("👤 My Information")




    customer_id = st.session_state.customer_id
    info = load_investor_information(customer_id)




    if info is None:
        st.stop()


    #thong tin ca nhan
    col1, col2 = st.columns(2)
    col1.write(f"**Customer ID:** {info['customer_id']}")
    col1.write(f"**Họ tên:** {info['customer_name']}")
    col1.write(f"**Ngày mở tài khoản:** {info['open_account_date']}")
    col1.write(f"**Trạng thái:** {info['status']}")




    col2.write(f"**Email:** {info['email']}")
    col2.write(f"**SĐT:** {info['phone']}")
    col2.write(f"**Địa chỉ:** {info['address']}")
    col2.write(f"**STK ngân hàng:** {info['bank_account']}")
    st.divider()
    #lich su giao dich
    data = load_investor_portfolio(st.session_state.customer_id)
    st.header("📦 My Portfolio")
    st.write(f"👤 {data['customer_name']}")


    col1, col2 = st.columns(2)
    col1.metric("CCQ nắm giữ", f"{float(data['nos']):,.2f}")
    col2.metric("Tiền vốn", f"{data['capital']:,.0f}")
    col3, col4, col5 = st.columns(3)
    col3.metric("Giá trị thị trường", f"{float(data['market_value']):,.2f}")
    col4.metric("Lãi / Lỗ", f"{float(data['pnl']):,.2f}")
    col5.metric("Số tiền khả dụng", f"{data['current_cash']:,.0f}")


    st.metric("📈 ROI (%)", f"{float(data['roi']):,.2f}")
    st.metric("💰 Tổng tài sản", f"{data['total_assets']:,.0f}")


    st.subheader("📜 Lịch sử giao dịch CCQ")
    smart_dataframe(data["trades"], "fundshare_trades", use_container_width=True, hide_index=True)
    st.subheader("💳 Lịch sử nạp / rút tiền")
    smart_dataframe(
        data["cash_requests"],
        "cash_requests",
        use_container_width=True,
        hide_index=True
    )


# ======================
# MY PORTFOLIO
# ======================
from scripts.information import load_investor_portfolio
   
  # ===== INVESTOR / ORGANISE =====




if page == "Overall_investor":
    df = load_table("overall_snapshot")
    df_nav = load_table("nav")
    df_ts= pd.to_datetime(df["snapshot_time"])
    df_nav["nav_date"] = pd.to_datetime(df_nav["nav_date"])
    df_nav = df_nav.sort_values("nav_date")
    # ---------- TABLE ----------
    st.subheader("📋 Portfolio Summary")
    smart_dataframe(
        df,
        "overall_snapshot",
        use_container_width=True,
        hide_index=True
    )
    st.subheader("📈 NAV / CCQ over time")
    fig = render_nav_chart(df_nav)




    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": False}
    )


# ---------- CHARTS ----------

    render_asset_allocation(df)
    st.subheader("📈 Relative Performance vs Total (%)")
    fig_perf = render_relative_performance(df)
    st.plotly_chart(fig_perf, use_container_width=True, use_container_height=1500)

