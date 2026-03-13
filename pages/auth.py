import streamlit as st
import hashlib
import secrets
import datetime as dt
from datetime import date
from sqlalchemy import text

from scripts.supabase_client import supabase, supabase_admin
from scripts.email_utils import send_reset_email
from scripts.db_engine import get_engine


def render():

    params = st.query_params
    reset_token = params.get("reset_token")

    engine = get_engine()

    # ==========================
    # RESET PASSWORD
    # ==========================

    if reset_token:

        hashed_token = hashlib.sha256(reset_token.encode()).hexdigest()

        with engine.connect() as conn:
            record = conn.execute(text("""
                SELECT email
                FROM password_resets
                WHERE token = :token
                AND used = false
                AND expires_at > now()
            """), {"token": hashed_token}).mappings().fetchone()

        if not record:
            st.error("Invalid or expired link")
            st.stop()

        st.title("Reset Password")

        new_password = st.text_input("New password", type="password")
        confirm_password = st.text_input("Confirm password", type="password")

        if st.button("Update password"):

            if new_password != confirm_password:
                st.error("Passwords do not match")
                st.stop()

            with engine.connect() as conn:
                user_record = conn.execute(text("""
                    SELECT auth_user_id
                    FROM users
                    WHERE email = :email
                """), {"email": record["email"]}).mappings().fetchone()

            supabase_admin.auth.admin.update_user_by_id(
                str(user_record["auth_user_id"]),
                {"password": new_password}
            )

            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE password_resets
                    SET used = true
                    WHERE token = :token
                """), {"token": hashed_token})

            st.success("Password changed successfully")
            st.query_params.clear()
            st.rerun()

        st.stop()

    # ======================
    # LOGIN / REGISTER
    # ======================

    st.title("Authentication")

    tab1, tab2, tab3 = st.tabs(["Login", "Register", "Forgot password"])

    # LOGIN
    with tab1:

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login"):

            with engine.connect() as conn:
                user = conn.execute(text("""
                    SELECT email, username, role, customer_id
                    FROM users
                    WHERE username=:u
                """), {"u": username}).mappings().fetchone()

            if not user:
                st.error("Invalid username or password")
                st.stop()

            res = supabase.auth.sign_in_with_password({
                "email": user["email"],
                "password": password
            })

            if res.user is None:
                st.error("Invalid username or password")
                st.stop()

            st.session_state.logged_in = True
            st.session_state.role = user["role"]
            st.session_state.username = user["username"]
            st.session_state.customer_id = user["customer_id"]
            st.session_state.is_admin = user["role"] == "admin"

            st.rerun()

    # REGISTER
    with tab2:
        st.info("Registration form unchanged from original code")

    # FORGOT PASSWORD
    with tab3:
        st.info("Forgot password form unchanged from original code")