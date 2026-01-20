# ======================
# CONFIG
import streamlit as st

from scripts.ui.allocation_pie import render_asset_allocation
st.set_page_config(
    page_title="Fund Management System",
    layout="wide"
)
import datetime as dt
import pandas as pd
import plotly.express as px
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from scripts.db import write_table, load_table, update_overall_snapshot, build_trade_record, run_nav_pipeline
from scripts.db_engine import get_engine
from scripts.fundshare import execute_fundshare_trade, get_latest_nav_per_unit, calculate_fundshare_fee
from scripts.information import load_admin_information, load_investor_portfolio, load_investor_information
from scripts.auth import authenticate_user, authenticate_admin, register_user, reset_password
from scripts.pricing_yahoo import update_all_prices
from scripts.update_prices import update_market_price, update_portfolio_after_trade
import streamlit as st
from scripts.ui.nav_chart import render_nav_chart
from scripts.ui.nav_service import get_nav_df
from scripts.ui.allocation_pie import render_asset_allocation
from scripts.ui.relative_performance import render_relative_performance

# ======================
# AUTHENTICATION
# ======================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False


if not st.session_state.logged_in:
    st.title("🔐 Authentication")


    tab1, tab2, tab3 = st.tabs(["Login", "Register", "Forgot password"])


    # ===== LOGIN =====
    with tab1:
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")


        if st.button("Login"):
            # thử admin trước
            user = authenticate_admin(username, password)


            # nếu không phải admin → thử investor
            if not user:
                user = authenticate_user(username, password)


            if user:
                st.session_state.logged_in = True
                st.session_state.user = user
                st.session_state.username = user["username"]
                st.session_state.role = user["role"]
                st.session_state.customer_id = user.get("customer_id")
                st.session_state.customer_name = user.get("customer_name")
                st.session_state.is_admin = (user["role"] == "admin")
                st.success("Đăng nhập thành công")
                st.rerun()
            else:
                st.error("Sai tài khoản hoặc mật khẩu")


    # ===== REGISTER =====
    with tab2:
        with st.form("register_form"):
            username = st.text_input("Username")
            display_name = st.text_input("Display name")
            email = st.text_input("Email")
            cccd = st.text_input("CCCD / MST")
            dob = st.date_input("Date of birth")
            phone = st.text_input("Phone")
            address = st.text_input("Address")
            bank = st.text_input("Bank account")
            role = st.selectbox("Role", ["investor", "organise"])
            password = st.text_input("Password", type="password")


            submitted = st.form_submit_button("Register")


        if submitted:
            result = register_user({
                "username": username,
                "display_name": display_name,
                "email": email,
                "cccd_mst": cccd,
                "dob": dob,
                "phone": phone,
                "address": address,
                "bank_account": bank,
                "pw_hash": password,
                "role": role
            })


            if "error" in result:
                st.error(result["error"])
            else:
                st.success("Đăng ký thành công. Bạn có thể đăng nhập.")


    # ===== FORGOT PASSWORD =====
    with tab3:
        u = st.text_input("Username", key="fp_username")
        new_pw = st.text_input("New password", type="password", key="fp_password")


        if st.button("Reset password"):
            if reset_password(u, new_pw):
                st.success("Đổi mật khẩu thành công")
            else:
                st.error("Không tìm thấy tài khoản")


    st.stop()


# CUSTOM CSS
# ======================


# SIDEBAR
# ======================
st.sidebar.title("📁 Navigation")
role = st.session_state.role


PAGE_MAP = {}


  # ===== ADMIN =====
if role == "admin":
    PAGE_MAP = {
        "🏠 Overall": "Overall",
        "💹 Portfolio": "Update_price",
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
    if st.button("🚪 Đăng xuất"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
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
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True
    )
    if st.button("📊 Update Overall Snapshot"):
            update_overall_snapshot()
            st.success("✅ Overall snapshot updated successfully")
            st.rerun()
    st.subheader("📋 Costs")
    st.dataframe(
        df_costs,
        use_container_width=True,
        hide_index=True
    )
    st.subheader("📈NAV")
    st.dataframe(
        df_nav,
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
    st.dataframe(
        df_port,
        use_container_width=True,
        hide_index=True
    )
    TICKERS = [
    "ACB", "BCM", "BID", "CTG", "DGC",
    "FPT", "GAS", "GVR", "HDB", "MSN",
    "VHM", "VIC", "VNM"
]


    engine = get_engine()
    if st.button("🔄 Update Market Prices (Yahoo)"):
        with st.spinner("Fetching prices from Yahoo Finance..."):
           update_all_prices(engine, TICKERS)


        st.success(f"✅ Updated {len(TICKERS)} stock prices")
        st.rerun()

    with st.form("trade_form"):
        ticker = st.text_input("Ticker")
        side = st.selectbox("Side", ["BUY", "SELL"])
        quantity = st.number_input("Quantity", min_value=1, step=1)
        price = st.number_input("Price", min_value=0.0, step=0.01)
        submitted = st.form_submit_button("Submit trade")

    if submitted:
        if ticker == "":
            st.error("Ticker cannot be empty")
        else:
            trade_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            trade = build_trade_record(
                trade_date=trade_time,
                ticker=ticker,
                side=side,
                quantity=quantity,
                price=price,
            )
            
            df_trade_new = pd.DataFrame([trade])

            # 🔒 CHỈ GHI EVENT – KHÔNG UPDATE PORTFOLIO/NAV
            write_table(df_trade_new, "trades")

            st.success("✅ Trade recorded (event logged)")
            st.dataframe(df_trade_new)

   

# ======================
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


            st.info(f"💰 NAV/CCQ: {nav_price:,.2f}")
            st.info(f"💵 Tiền nhà đầu tư chuyển: {amount:,.0f}")
            st.info(f"💸 Phí giao dịch: {fee:,.0f}")
            st.info(f"📥 Góp vốn thực tế: {net_amount:,.0f}")
            st.info(f"📦 CCQ nhận được: {units:,.4f}")


        else:  # SELL
            quantity = st.number_input(
                "Units to Sell",
                min_value=1.0
            )


            gross_amount = quantity * nav_price
            fee = calculate_fundshare_fee("Sell", gross_amount)
            net_amount = gross_amount - fee


            st.info(f"💰 NAV/CCQ: {nav_price:,.2f}")
            st.info(f"📤 Giá trị bán: {gross_amount:,.0f}")
            st.info(f"💸 Phí giao dịch: {fee:,.0f}")
            st.info(f"💵 Tiền nhận: {net_amount:,.0f}")


        # ✅ NÚT GỬI REQUEST (CHỈ INVESTOR MỚI CÓ)
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


    # ======================
    # ADMIN
    # ======================
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
        st.dataframe(info["investors"], use_container_width=True)


    # ======================
    # INVESTOR / ORGANISE VIEW
    # ======================
    else:
        st.header("👤 My Information")


    customer_id = st.session_state.customer_id
    info = load_investor_information(customer_id)


    if info is None:
        st.warning("Trống")
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
    col1, col2, col3 = st.columns(3)
    col1.metric(
    "CCQ nắm giữ",
    f"{float(data['nos']):,.2f}"
)

    col2.metric("Giá trị thị trường", f"{float(data['market_value']):,.2f}")
    col3.metric("Lãi / Lỗ", f"{float(data['pnl']):,.2f}")


    st.metric("📈 ROI (%)", f"{float(data['roi']):,.2f}")


    st.subheader("📜 Lịch sử giao dịch CCQ")
    st.dataframe(data["trades"])


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
    st.dataframe(
        df,
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