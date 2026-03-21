import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import text


def render_performance_chart(engine, portfolio_tickers):

    # ===== CLEAN TICKER =====
    tickers = [t for t in portfolio_tickers if t != "YTM"]

    if not tickers:
        return None

    # ===== QUERY PRICE HISTORY =====
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                price_date AS date,
                ticker,
                price
            FROM price_history
            WHERE ticker = ANY(:tickers)
            ORDER BY price_date
        """), conn, params={"tickers": tickers})

    if df.empty:
        return None

    df = df.sort_values("date")

    # ===== PERFORMANCE =====
    df["perf"] = df.groupby("ticker")["price"].transform(
        lambda x: x / x.iloc[0] - 1
    )

    # ===== COLOR =====
    colors = [
        "#F5C77A",
        "#5DADE2",
        "#58D68D",
        "#EC7063",
        "#AF7AC5",
        "#F4D03F",
    ]

    fig = go.Figure()

    # ===== LINE =====
    for i, ticker in enumerate(df["ticker"].unique()):
        sub = df[df["ticker"] == ticker]

        fig.add_trace(
            go.Scatter(
                x=sub["date"],
                y=sub["perf"],
                mode="lines",
                name=ticker,
                line=dict(
                    color=colors[i % len(colors)],
                    width=2.5
                ),
                hovertemplate=(
                    "<b>%{x|%d/%m/%Y}</b><br>"
                    f"{ticker}: %{{y:.2%}}"
                    "<extra></extra>"
                ),
            )
        )

    # ===== BASELINE =====
    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="rgba(255,255,255,0.25)",
        line_width=1.5
    )

    # ===== RANGE =====
    y_min = df["perf"].min()
    y_max = df["perf"].max()
    padding = max((y_max - y_min) * 0.3, 0.02)

    # ===== LAYOUT =====
    fig.update_layout(
        height=360,
        plot_bgcolor="#0E1A2B",
        paper_bgcolor="#0E1A2B",
        hovermode="x unified",
        showlegend=True,

        xaxis=dict(
            showgrid=False,
            tickformat="%d/%m",
            showspikes=True,
            spikemode="across",
            spikesnap="cursor",
            spikecolor="rgba(245,199,122,0.9)",
            spikethickness=1.5,
        ),

        yaxis=dict(
            title="Performance (%)",
            tickformat=".1%",
            range=[y_min - padding, y_max + padding],
            showgrid=True,
            gridcolor="rgba(255,255,255,0.06)",
        ),

        font=dict(color="#EAEAEA", size=13),
        margin=dict(l=40, r=20, t=20, b=30),
    )

    return fig