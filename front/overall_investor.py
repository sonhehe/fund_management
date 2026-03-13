import streamlit as st
import pandas as pd

from scripts.db import load_table, smart_dataframe
from scripts.ui.nav_chart import render_nav_chart
from scripts.ui.allocation_pie import render_asset_allocation
from scripts.ui.relative_performance import render_relative_performance


def render():

    df = load_table("overall_snapshot")

    df_nav = load_table("nav")

    df_nav["nav_date"] = pd.to_datetime(df_nav["nav_date"])

    df_nav = df_nav.sort_values("nav_date")

    st.subheader("Portfolio Summary")

    smart_dataframe(
        df,
        "overall_snapshot",
        use_container_width=True,
        hide_index=True
    )

    st.subheader("NAV per Unit Over Time")

    fig = render_nav_chart(df_nav)

    st.plotly_chart(fig, use_container_width=True)

    render_asset_allocation(df)

    st.subheader("Relative Performance vs Total (%)")

    fig_perf = render_relative_performance(df)

    st.plotly_chart(fig_perf, use_container_width=True)