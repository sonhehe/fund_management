from sqlalchemy import create_engine

DATABASE_URL = (
    "postgresql+psycopg2://postgres.sapafzxhinqsumriihum:nghiencuukhoahoc@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"
)

import streamlit as st


@st.cache_resource
def get_engine():
    return create_engine(
        DATABASE_URL,
        pool_size=3,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

engine = get_engine()
