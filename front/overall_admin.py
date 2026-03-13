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