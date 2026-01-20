import plotly.graph_objects as go

def render_nav_chart(df_nav):
    df = df_nav.sort_values("nav_date")

    y_min = df["nav_per_unit"].min()
    y_max = df["nav_per_unit"].max()
    padding = max((y_max - y_min) * 0.6, 5)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["nav_date"],
            y=df["nav_per_unit"],
            mode="lines",
            line=dict(color="#F5C77A", width=2.5),
            hovertemplate=(
                "<b>%{x|%d/%m/%Y}</b><br>"
                "NAV/CCQ: %{y:,.2f} VND"
                "<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        height=360,
        plot_bgcolor="#0E1A2B",
        paper_bgcolor="#0E1A2B",
        hovermode="x",
        showlegend=False,
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
            title="NAV / CCQ",
            tickformat=",.0f",
            range=[y_min - padding, y_max + padding],
            showgrid=True,
            gridcolor="rgba(255,255,255,0.06)",
        ),
        font=dict(color="#EAEAEA", size=13),
        margin=dict(l=40, r=20, t=20, b=30),
    )

    return fig
