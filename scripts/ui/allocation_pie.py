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

    st.plotly_chart(fig, use_container_width=True)
