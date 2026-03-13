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

    if role == "admin":

        st.header("Fund Information")

        info = load_admin_information()

        col1, col2, col3 = st.columns(3)

        col1.metric("Cash Balance", f"{info['cash']:,.0f}")

        col2.metric("Total Fund Shares", f"{info['total_ccq']:,.2f}")

        col3.metric("Fund Return", f"{info['interest']*100:.2f}%")

        st.subheader("Investors")

        smart_dataframe(
            info["investors"],
            "investors",
            use_container_width=True
        )

    else:

        customer_id = st.session_state.customer_id

        info = load_investor_information(customer_id)

        st.header("My Information")

        st.write(info)