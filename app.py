# ======================
# CONFIG
# ======================

import streamlit as st

st.set_page_config(
    page_title="Fund Management System",
    layout="wide"
)

# ======================
# SESSION
# ======================

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False


# ======================
# AUTH PAGE
# ======================

if not st.session_state.logged_in:
    from front.auth import render_auth
    render_auth()
    st.stop()


# ======================
# SIDEBAR
# ======================

st.sidebar.title("Navigation")

role = st.session_state.get("role")

if role == "admin":
    PAGE_MAP = {
        "Overall": "overall_admin",
        "Portfolio": "portfolio",
        "Cash Management": "cash",
        "Pending Requests": "exchange",
        "Content Management": "content",
        "Information": "information"
    }
else:
    PAGE_MAP = {
        "Dashboard": "overall_investor",
        "Transactions": "exchange",
        "Investor Overview": "information"
    }


selected_label = st.sidebar.selectbox(
    "Go to",
    list(PAGE_MAP.keys())
)

page = PAGE_MAP[selected_label]


# ======================
# LOGOUT
# ======================

from scripts.supabase_client import supabase

with st.sidebar:
    st.markdown("---")

    if st.button("Log out"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]

        supabase.auth.sign_out()
        st.rerun()


# ======================
# PAGE ROUTER
# ======================

if page == "overall_admin":
    from front.overall_admin import render
    render()

elif page == "portfolio":
    from front.portfolio import render
    render()

elif page == "cash":
    from front.cash import render
    render()

elif page == "exchange":
    from front.exchange import render
    render()

elif page == "information":
    from front.information import render
    render()

elif page == "overall_investor":
    from front.overall_investor import render
    render()

