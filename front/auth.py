import streamlit as st
import hashlib
import secrets
import datetime as dt
from datetime import date
from sqlalchemy import text

from scripts.supabase_client import supabase, supabase_admin
from scripts.email_utils import send_reset_email
from scripts.db_engine import get_engine


engine = get_engine()


def render_auth():

    params = st.query_params
    reset_token = params.get("reset_token")

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

        with st.form("reset_password_form"):

            new_password = st.text_input("New password", type="password")
            confirm_password = st.text_input("Confirm password", type="password")

            submitted = st.form_submit_button("Update password")

        if submitted:

            if new_password != confirm_password:
                st.error("Passwords do not match")
                st.stop()

            with engine.connect() as conn:
                user_record = conn.execute(text("""
                    SELECT auth_user_id
                    FROM users
                    WHERE email = :email
                """), {"email": record["email"]}).mappings().fetchone()

            if not user_record:
                st.error("User not found")
                st.stop()

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

    # ======================
    # LOGIN
    # ======================

    with tab1:

        with st.form("login_form"):

            username = st.text_input("Username").strip()
            password = st.text_input("Password", type="password")

            login = st.form_submit_button("Login")

        if login:

            with engine.connect() as conn:
                user = conn.execute(text("""
                    SELECT email, username, role, customer_id
                    FROM users
                    WHERE username = :u
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

    # ======================
    # REGISTER
    # ======================

    with tab2:

        with st.form("register_form"):

            username = st.text_input("Username").strip()
            display_name = st.text_input("Display name")
            email = st.text_input("Email")
            cccd = st.text_input("CCCD / MST")

            today = date.today()
            min_dob = date(today.year - 100, 1, 1)
            max_dob = date(today.year - 18, 12, 31)

            dob = st.date_input(
                "Date of birth",
                min_value=min_dob,
                max_value=max_dob
            )

            phone = st.text_input("Phone")
            address = st.text_input("Address")
            bank = st.text_input("Bank account")

            # role cố định
            role = "investor"

            password = st.text_input("Password", type="password")

            submitted = st.form_submit_button("Register")

        if submitted:

            res = supabase.auth.sign_up({
                "email": email,
                "password": password
            })

            if res.user is None:
                st.error("Registration failed")
                st.stop()

            auth_user_id = res.user.id

            try:

                with engine.begin() as conn:

                    prefix = "CN"

                    last_id = conn.execute(text("""
                        SELECT MAX(customer_id)
                        FROM investors
                        WHERE customer_id LIKE :p
                    """), {"p": f"{prefix}%"}).scalar()

                    if last_id:
                        num = int(last_id.replace(prefix, ""))
                        customer_id = f"{prefix}{num + 1:02d}"
                    else:
                        customer_id = f"{prefix}01"


                    conn.execute(text("""
                        INSERT INTO users (
                            username,
                            customer_id,
                            display_name,
                            email,
                            phone,
                            address,
                            bank_account,
                            role,
                            created_at,
                            auth_user_id
                        )
                        VALUES (
                            :username,
                            :customer_id,
                            :display_name,
                            :email,
                            :phone,
                            :address,
                            :bank_account,
                            :role,
                            now(),
                            :auth_user_id
                        )
                    """), {
                        "username": username,
                        "customer_id": customer_id,
                        "display_name": display_name,
                        "email": email,
                        "phone": phone,
                        "address": address,
                        "bank_account": bank,
                        "role": role,
                        "auth_user_id": auth_user_id
                    })

                    conn.execute(text("""
                        INSERT INTO investors (
                            customer_id,
                            customer_name,
                            status,
                            open_account_date,
                            identity_number,
                            dob,
                            phone,
                            email,
                            address,
                            capital,
                            nos,
                            bank_account
                        )
                        VALUES (
                            :customer_id,
                            :customer_name,
                            'Đang đầu tư',
                            CURRENT_DATE,
                            :identity_number,
                            :dob,
                            :phone,
                            :email,
                            :address,
                            0,
                            0,
                            :bank_account
                        )
                    """), {
                        "customer_id": customer_id,
                        "customer_name": display_name,
                        "identity_number": cccd,
                        "dob": dob.isoformat(),
                        "phone": phone,
                        "email": email,
                        "address": address,
                        "bank_account": bank
                    })

                st.success("Registration successful")

            except Exception as e:

                supabase_admin.auth.admin.delete_user(auth_user_id)

                st.error(f"Database error: {e}")

    # ======================
    # FORGOT PASSWORD
    # ======================

    with tab3:

        with st.form("forgot_form"):

            email = st.text_input("Email")

            submitted = st.form_submit_button("Send reset link")

        if submitted:

            with engine.connect() as conn:
                exists = conn.execute(
                    text("SELECT 1 FROM users WHERE email = :email"),
                    {"email": email}
                ).fetchone()

            if not exists:
                st.success("Password reset link has been sent.")
                st.stop()

            token = secrets.token_urlsafe(32)
            hashed_token = hashlib.sha256(token.encode()).hexdigest()

            expires = dt.datetime.utcnow() + dt.timedelta(minutes=15)

            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO password_resets (
                        email,
                        token,
                        expires_at,
                        used
                    )
                    VALUES (
                        :email,
                        :token,
                        :expires,
                        false
                    )
                """), {
                    "email": email,
                    "token": hashed_token,
                    "expires": expires
                })

            reset_link = f"https://fundmanagement.streamlit.app?reset_token={token}"

            try:
                send_reset_email(email, reset_link)
                st.success("Password reset link has been sent.")
            except Exception as e:
                st.error("Failed to send email")
                st.write(str(e))

    st.stop()