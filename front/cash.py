import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from scripts.db import load_table, smart_dataframe


def render():

    st.header("💹Cash")
    df_tradestore = load_table("trades")
    df_exchange = load_table("fundshare_trades")
    st.subheader("History of cash movements")

    df_cash = load_table("cash")

    st.subheader("Cash Balance Over Time")

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