import plotly.express as px
import numpy as np

def render_relative_performance(df_snapshot):

    latest_time = df_snapshot["snapshot_time"].max()
    df_latest = df_snapshot[df_snapshot["snapshot_time"] == latest_time].copy()

    total_interest = (
        df_latest[df_latest["attribute"] == "Total"]["interest"]
        .iloc[0]
    )

    df_latest["relative_perf"] = (
        df_latest["interest"] - total_interest
    ) * 100

    df_plot = df_latest[df_latest["attribute"] != "Total"].copy()

    # 🎨 Manual color logic (discrete legend)
    df_plot["direction"] = np.where(
        df_plot["relative_perf"] > 0,
        "Outperformance",
        "Underperformance"
    )

    color_map = {
        "Outperformance": "#2A9D8F",
        "Underperformance": "#E76F51"
    }

    fig = px.bar(
        df_plot,
        y="attribute",
        x="relative_perf",
        color="direction",
        orientation="h",
        text=df_plot["relative_perf"].map(lambda x: f"{x:+.2f}%"),
        color_discrete_map=color_map
    )

    fig.update_traces(textposition="outside")

    max_abs = abs(df_plot["relative_perf"]).max()

    fig.update_layout(
        title=None,  # ❌ xoá title nhỏ
        xaxis_title="Out / Underperformance (%)",
        yaxis_title="",
        height=500,
        xaxis=dict(
            range=[-max_abs * 1.2, max_abs * 1.2],
            zeroline=True,
            zerolinewidth=2,
            zerolinecolor="black",
            showgrid=False
        ),
        yaxis=dict(showgrid=False),
        legend=dict(
            title="",
            x=0.75,  # nằm trong chart
            y=0.95
        ),
        plot_bgcolor="white",
        paper_bgcolor="white"
    )

    return fig