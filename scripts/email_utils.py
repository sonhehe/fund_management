import smtplib
from email.mime.text import MIMEText
import streamlit as st

def send_reset_email(to_email, reset_link):
    gmail = st.secrets["GMAIL_ADDRESS"]
    app_password = st.secrets["GMAIL_APP_PASSWORD"]

    body = f"""
    Reset password của bạn:

    {reset_link}

    Link hết hạn sau 15 phút.
    """

    msg = MIMEText(body)
    msg["Subject"] = "Reset Password"
    msg["From"] = gmail
    msg["To"] = to_email

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail, app_password)
        server.send_message(msg)