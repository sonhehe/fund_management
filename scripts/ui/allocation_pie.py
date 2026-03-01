import plotly.express as px
import streamlit as st


def render_asset_allocation(df):
    st.subheader("📈 Asset Allocation")

    df_pie = df[df["attribute"].isin(
        ["Stock", "Bond", "Fund share", "Cash"]
    )].copy()

    fig = px.pie(
        df_pie,
        values="weight",
        names="attribute",
        hole=0.5,
        color_discrete_sequence=px.colors.sequential.Blues
    )

    fig.update_traces(textinfo="percent+label")

    # 🎨 Dark background
    fig.update_layout(
        paper_bgcolor="#0E1A2B",   # toàn bộ nền
        plot_bgcolor="#0E1A2B",    # nền vùng chart
        font=dict(color="white")  # chữ trắng cho dễ nhìn
    )
    fig.update_layout(
        paper_bgcolor="#0E1A2B",
        plot_bgcolor="#0E1A2B",
        font=dict(color="white"),              # toàn bộ font trắng
        legend=dict(
            font=dict(color="white")           # legend trắng
        )
    )
    st.plotly_chart(fig, use_container_width=True)