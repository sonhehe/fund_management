import plotly.express as px

def render_relative_performance(df_snapshot):
    # Lấy snapshot mới nhất
    latest_time = df_snapshot["snapshot_time"].max()
    df_latest = df_snapshot[df_snapshot["snapshot_time"] == latest_time].copy()

    # Lấy interest của Total
    total_interest = (
        df_latest[df_latest["attribute"] == "Total"]["interest"]
        .iloc[0]
    )

    # Tính relative performance
    df_latest["relative_perf"] = (
        df_latest["interest"] - total_interest
    ) * 100  # %

    # Bỏ Total ra khỏi chart
    df_plot = df_latest[df_latest["attribute"] != "Total"]

    fig = px.bar(
        df_plot,
        x="attribute",
        y="relative_perf",
        text=df_plot["relative_perf"].map(lambda x: f"{x:+.2f}%"),
        color="relative_perf",
        color_continuous_scale=["#d62728", "#f7f7f7", "#2ca02c"],
    )

    fig.update_layout(
        title="Relative Performance vs Total (%)",
        yaxis_title="Out / Underperformance (%)",
        xaxis_title="Asset",
        showlegend=False,
        height=500,
    )

    fig.update_traces(
        textposition="outside"
    )

    fig.add_hline(y=0, line_width=2, line_color="black")

    return fig
