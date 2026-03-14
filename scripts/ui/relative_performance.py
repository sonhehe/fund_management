import plotly.graph_objects as go
import pandas as pd


def render_relative_performance(df_snapshot):

    # ======================
    # SAFETY CHECK
    # ======================

    if df_snapshot is None or df_snapshot.empty:
        fig = go.Figure()
        fig.update_layout(
            height=400,
            template="plotly_white",
            title="No data available"
        )
        return fig

    required_cols = ["snapshot_time", "attribute", "interest"]

    for col in required_cols:
        if col not in df_snapshot.columns:
            fig = go.Figure()
            fig.update_layout(
                height=400,
                template="plotly_white",
                title=f"Missing column: {col}"
            )
            return fig

    # ======================
    # GET LATEST SNAPSHOT
    # ======================

    latest_time = df_snapshot["snapshot_time"].max()

    df_latest = df_snapshot[
        df_snapshot["snapshot_time"] == latest_time
    ].copy()

    if df_latest.empty:
        fig = go.Figure()
        fig.update_layout(
            height=400,
            template="plotly_white",
            title="No snapshot data"
        )
        return fig

    # ======================
    # GET TOTAL RETURN
    # ======================

    total_row = df_latest[df_latest["attribute"] == "Total"]

    if total_row.empty:
        total_interest = 0
    else:
        total_interest = total_row["interest"].iloc[0]

    # ======================
    # RELATIVE PERFORMANCE
    # ======================

    df_latest["relative_perf"] = (
        df_latest["interest"] - total_interest
    ) * 100

    df_plot = df_latest[
        df_latest["attribute"] != "Total"
    ].copy()

    if df_plot.empty:
        fig = go.Figure()
        fig.update_layout(
            height=400,
            template="plotly_white",
            title="No asset data"
        )
        return fig

    df_plot = df_plot.sort_values("relative_perf")

    # ======================
    # COLORS
    # ======================

    GREEN = "rgb(0,200,170)"
    RED = "rgb(255,70,90)"

    colors = [
        GREEN if x > 0 else RED
        for x in df_plot["relative_perf"]
    ]

    max_abs = max(abs(df_plot["relative_perf"]).max(), 1)

    # ======================
    # BUILD FIGURE
    # ======================

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=df_plot["attribute"],
            x=df_plot["relative_perf"],
            orientation="h",

            marker=dict(
                color=colors,
                line=dict(color="rgba(0,0,0,0.1)", width=1)
            ),

            text=[
                f"{v:+.2f}%"
                for v in df_plot["relative_perf"]
            ],

            textposition="outside",

            hovertemplate="<b>%{y}</b><br>%{x:.2f}%<extra></extra>"
        )
    )

    # ======================
    # LAYOUT
    # ======================

    fig.update_layout(

        height=420,

        template="plotly_white",

        xaxis=dict(
            range=[-max_abs * 1.35, max_abs * 1.35],
            zeroline=True,
            zerolinewidth=2,
            zerolinecolor="black",
            title="Relative Performance (%)"
        ),

        yaxis=dict(
            showgrid=False,
            title=""
        ),

        margin=dict(
            l=30,
            r=30,
            t=20,
            b=40
        )
    )

    return fig