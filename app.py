# ======================
# CONFIG
# ======================

import streamlit as st
import importlib

from scripts.supabase_client import supabase

st.set_page_config(
    page_title="Fund Management System",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ======================
# SESSION INIT
# ======================

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False


# ======================
# AUTHENTICATION
# ======================

if not st.session_state.logged_in:
    from front.auth import render_auth
    render_auth()
    st.stop()


# ======================
# SIDEBAR NAVIGATION
# ======================

st.sidebar.title("Navigation")

role = st.session_state.get("role", "investor")

# ---------- PAGE MAP ----------

if role == "admin":

    PAGE_MAP = {

        "📊 Overall": "overall_admin",
        "📈 Portfolio": "portfolio",
        "💰 Cash Management": "cash",
        "📝 Pending Requests": "exchange",
        "⚙️ Content Management": "content",
        "👤 Information": "information",
    }

else:

    PAGE_MAP = {

        "📊 Dashboard": "overall_investor",
        "🔄 Transactions": "exchange",
        "👤 Investor Overview": "information",
    }


selected_label = st.sidebar.selectbox(
    "Go to",
    list(PAGE_MAP.keys())
)

page = PAGE_MAP[selected_label]


# ======================
# LOGOUT
# ======================

with st.sidebar:

    st.markdown("---")

    if st.button("Log out"):

        st.session_state.clear()

        try:
            supabase.auth.sign_out()
        except Exception:
            pass

        st.rerun()


# ======================
# PAGE ROUTER
# ======================

PAGE_ROUTER = {

    "overall_admin": "front.overall_admin",
    "portfolio": "front.portfolio",
    "cash": "front.cash",
    "exchange": "front.exchange",
    "information": "front.information",
    "overall_investor": "front.overall_investor",
    "content": "front.content_management",
}

module = importlib.import_module(PAGE_ROUTER[page])

module.render()