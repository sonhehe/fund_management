import streamlit as st
import pandas as pd
from scripts.db import load_table, smart_dataframe
from scripts.ui.nav_chart import render_nav_chart
from scripts.ui.relative_performance import render_performance_chart
from scripts.db_engine import get_engine
from sqlalchemy import text


def render():
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
    if not df.empty and "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"])
        latest_time = df["snapshot_time"].max()
        df_ove = df[df["snapshot_time"] == latest_time]
    df_nav = df_nav.sort_values("nav_date")
    # ---------- TABLE ----------
    st.subheader("Overall")
    smart_dataframe(
        df,
        "overall_snapshot",
        width="stretch",
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
    st.subheader("Portfolio")
    smart_dataframe(
        df_display,
        "portfolio_view",
        width="stretch",
        hide_index=True
    )
    st.subheader("NAV per Unit Over Time")
    fig = render_nav_chart(df_nav)




    st.plotly_chart(
        fig,
        width="stretch",
        config={"displayModeBar": False}
    )


# ---------- CHARTS ----------

