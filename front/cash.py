import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from scripts.db import load_table, smart_dataframe


def render():

    st.header("Cash")

    df_trades = load_table("trades")
    df_exchange = load_table("fundshare_trades")
    df_cash = load_table("cash")

    st.subheader("Cash Balance Over Time")

    if not df_cash.empty:

        df_cash["created_at"] = pd.to_datetime(df_cash["created_at"])

        df_cash = df_cash.sort_values("created_at")

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=df_cash["created_at"],
                y=df_cash["cash_end"],
                mode="lines",
                line=dict(width=3),
                fill="tozeroy",
            )
        )

        st.plotly_chart(fig, use_container_width=True)

    else:

        st.info("No cash snapshot available yet.")

    st.subheader("Trade Store")

    smart_dataframe(
        df_trades,
        "trades",
        use_container_width=True,
        hide_index=True
    )

    st.subheader("Fund Share Trades")

    smart_dataframe(
        df_exchange,
        "fundshare_trades",
        use_container_width=True,
        hide_index=True
    )