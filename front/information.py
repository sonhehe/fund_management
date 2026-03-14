import streamlit as st
import pandas as pd

from scripts.information import (
    load_admin_information,
    load_investor_information,
    load_investor_portfolio
)
from scripts.db import smart_dataframe


def render():

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
        }]), width="stretch")




        st.divider()




        st.subheader("👥 Investors List")
        smart_dataframe(info["investors"], "investors", width="stretch")





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
            width="stretch",
            hide_index=True
        )
    else:
        st.info("No fund unit transactions yet")

    st.subheader("Deposit & Withdrawal History")

    if "cash_requests" in data and not data["cash_requests"].empty:
        smart_dataframe(
            data["cash_requests"],
            "cash_requests",
            width="stretch",
            hide_index=True
        )
    else:
        st.info("No deposit or withdrawal transactions yet.")
