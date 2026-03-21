import streamlit as st
import pandas as pd
from scripts.db import load_table, smart_dataframe, update_overall_snapshot, run_nav_pipeline
from scripts.ui.nav_chart import render_nav_chart
from scripts.ui.nav_service import get_nav_df
from scripts.ui.allocation_pie import render_asset_allocation
from scripts.ui.relative_performance import render_relative_performance
from scripts.db_engine import get_engine
from sqlalchemy import text
def render():
    df = load_table("overall_snapshot")
    df_nav = load_table("nav")
    df_costs = load_table("costs")
    if not df.empty and "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])
        latest_time = df["snapshot_time"].max()
        df_ove = df[df["snapshot_time"] == latest_time]
    df_nav["nav_date"] = pd.to_datetime(df_nav["nav_date"])
    df_nav = df_nav.sort_values("nav_date")
    # ---------- TABLE ----------
    st.subheader("Overall")

    smart_dataframe(
        df_ove,
        "overall_snapshot",
        width="stretch",
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
        width="stretch",
        hide_index=True
    )

    st.subheader("NAV")

    smart_dataframe(
        df_nav,
        "nav",
        width="stretch",
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
        width="stretch",
        config={"displayModeBar": False}
    )


    st.subheader("Relative Performance vs Total (%)")
    
    fig_perf = render_relative_performance(df_nav, df["ticker"].tolist())
    st.plotly_chart(fig_perf, width="stretch", config={"displayModeBar": False})