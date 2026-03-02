import plotly.graph_objects as go
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
    df_plot = df_plot.sort_values("relative_perf")

    GREEN = "rgb(0,200,170)"
    RED   = "rgb(255,70,90)"

    max_abs = max(abs(df_plot["relative_perf"]).max(), 1)

    fig = go.Figure()

    for attr, val in zip(df_plot["attribute"], df_plot["relative_perf"]):

        color = GREEN if val > 0 else RED

        # ===== Glow layers (neon effect)
        for glow_width, glow_opacity in [(0.9,0.08),(0.8,0.12),(0.7,0.18)]:
            fig.add_trace(go.Bar(
                y=[attr],
                x=[val],
                orientation="h",
                marker=dict(color=color),
                width=glow_width,
                opacity=glow_opacity,
                hoverinfo="skip",
                showlegend=False
            ))

        # ===== Main bar
        fig.add_trace(go.Bar(
            y=[attr],
            x=[val],
            orientation="h",
            marker=dict(
                color=color,
                line=dict(color="rgba(0,0,0,0.15)", width=1)
            ),
            width=0.5,
            text=[f"{attr}  ({val:+.2f}%)"],
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(
                size=16,
                color="white",
                family="Arial Black"
            ),
            hovertemplate="<b>%{y}</b><br>%{x:.2f}%<extra></extra>",
            showlegend=False
        ))

    fig.update_layout(
        height=540,
        barmode="overlay",
        xaxis=dict(
            range=[-max_abs * 1.35, max_abs * 1.35],
            zeroline=True,
            zerolinewidth=5,
            zerolinecolor="black",
            showgrid=False
        ),
        yaxis=dict(showgrid=False, showticklabels=False),
        bargap=0.5,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=30, r=30, t=20, b=40)
    )

    return fig